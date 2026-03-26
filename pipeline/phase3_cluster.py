"""Phase 3 — Cluster: UMAP + HDBSCAN face identity clustering."""

import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase3_cluster")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_cluster() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 3 — CLUSTER: UMAP + HDBSCAN face clustering")
    log.info("=" * 60)

    import umap
    import hdbscan as hdbscan_lib

    db.mark_phase_running("cluster")

    conn = db.get_db()

    rows = conn.execute(
        "SELECT face_id, embedding, photo_id, is_ground_truth FROM faces"
    ).fetchall()

    if not rows:
        log.warning("No faces found in database. Skipping cluster phase.")
        db.mark_phase_complete("cluster")
        conn.close()
        emit_phase_postmortem(
            log,
            "cluster",
            phase_start,
            True,
            metrics={"Faces loaded": 0, "Clusters": 0, "Noise faces": 0},
        )
        return True

    face_ids = [r[0] for r in rows]
    photo_ids = [r[2] for r in rows]
    is_gt_flags = [r[3] for r in rows]
    embeddings = np.array([
        np.frombuffer(r[1], dtype=np.float32) for r in rows
    ])

    log.info("Loaded %d face embeddings (dim=%d)", len(face_ids), embeddings.shape[1])
    db.update_phase_progress("cluster", 1, 4)

    # ── UMAP ──
    log.info("Running UMAP (n_neighbors=%d, n_components=%d)...",
             config.UMAP_N_NEIGHBORS, config.UMAP_N_COMPONENTS)
    reducer = umap.UMAP(
        n_neighbors=config.UMAP_N_NEIGHBORS,
        min_dist=config.UMAP_MIN_DIST,
        n_components=min(config.UMAP_N_COMPONENTS, len(face_ids) - 1),
        metric=config.UMAP_METRIC,
        random_state=42,
        verbose=False,
    )
    reduced = reducer.fit_transform(embeddings)
    log.info("UMAP complete. Reduced shape: %s", reduced.shape)
    db.update_phase_progress("cluster", 2, 4)

    # ── HDBSCAN ──
    log.info("Running HDBSCAN (min_cluster_size=%d, min_samples=%d)...",
             config.HDBSCAN_MIN_CLUSTER_SIZE, config.HDBSCAN_MIN_SAMPLES)
    clusterer = hdbscan_lib.HDBSCAN(
        min_cluster_size=config.HDBSCAN_MIN_CLUSTER_SIZE,
        min_samples=config.HDBSCAN_MIN_SAMPLES,
        metric=config.HDBSCAN_METRIC,
    )
    labels = clusterer.fit_predict(reduced)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int(np.sum(labels == -1))
    log.info("Clustering complete: %d clusters, %d noise points", n_clusters, n_noise)
    db.update_phase_progress("cluster", 3, 4)

    # ── Snapshot old cluster assignments before overwriting ──
    old_labels = _snapshot_old_labels(conn)

    # ── Write new cluster IDs back to faces ──
    for face_id, label in zip(face_ids, labels):
        conn.execute(
            "UPDATE faces SET cluster_id=? WHERE face_id=?",
            (int(label), face_id)
        )
    conn.commit()

    # ── Upsert clusters table (preserves person_label + approved) ──
    now = _now()
    new_cluster_ids = set(int(l) for l in labels)
    for cluster_id in new_cluster_ids:
        count = int(np.sum(labels == cluster_id))
        is_noise = 1 if cluster_id == -1 else 0
        conn.execute(
            """INSERT INTO clusters (cluster_id, face_count, is_noise, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(cluster_id) DO UPDATE SET
                 face_count = excluded.face_count,
                 is_noise   = excluded.is_noise,
                 updated_at = excluded.updated_at""",
            (cluster_id, count, is_noise, now)
        )
    conn.commit()

    # ── Remove stale clusters that no longer have any faces ──
    stale = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id NOT IN ({})".format(
            ",".join("?" * len(new_cluster_ids))
        ),
        list(new_cluster_ids),
    ).fetchall()
    if stale:
        stale_ids = [r[0] for r in stale]
        conn.execute(
            "DELETE FROM clusters WHERE cluster_id IN ({})".format(
                ",".join("?" * len(stale_ids))
            ),
            stale_ids,
        )
        conn.commit()
        log.info("Removed %d stale clusters: %s", len(stale_ids), stale_ids)

    # ── Carry forward labels from old clusters to new ones ──
    _carry_forward_labels(conn, face_ids, labels, old_labels)

    # ── Auto-label clusters from ground truth anchors ──
    _auto_label_from_ground_truth(conn, face_ids, labels, photo_ids, is_gt_flags)

    db.update_phase_progress("cluster", 4, 4)

    log.info("Cluster phase complete.")
    log.info("  Total clusters: %d", n_clusters)
    log.info("  Noise faces:    %d", n_noise)
    labeled_count = _count_labeled(conn)
    log.info("  Labeled:        %d", labeled_count)

    db.mark_phase_complete("cluster")
    conn.close()
    emit_phase_postmortem(
        log,
        "cluster",
        phase_start,
        True,
        metrics={
            "Faces loaded": len(face_ids),
            "Clusters": n_clusters,
            "Noise faces": n_noise,
            "Labeled clusters": labeled_count,
        },
    )
    return True


def _snapshot_old_labels(conn) -> dict[int, tuple[str | None, int]]:
    """Return {face_id: (person_label, approved)} for all faces in labeled/approved clusters."""
    rows = conn.execute(
        """SELECT f.face_id, c.person_label, c.approved
           FROM faces f
           JOIN clusters c ON f.cluster_id = c.cluster_id
           WHERE c.person_label IS NOT NULL OR c.approved = 1"""
    ).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def _carry_forward_labels(conn, face_ids, labels, old_labels) -> None:
    """Transfer person_label + approved from old clusters to new ones by face majority vote.

    For each new cluster, look at how many of its faces came from each old
    labeled cluster.  The old label that contributed the most faces wins.
    Only applies when the new cluster has no label yet.
    """
    # Build new_cluster_id → list of (old_label, old_approved) from member faces
    from collections import Counter as _Counter

    cluster_votes: dict[int, list[tuple[str | None, int]]] = {}
    for face_id, new_label in zip(face_ids, labels):
        new_cid = int(new_label)
        if new_cid == -1:
            continue
        if face_id in old_labels:
            cluster_votes.setdefault(new_cid, []).append(old_labels[face_id])

    carried = 0
    for new_cid, votes in cluster_votes.items():
        # Only carry forward if this cluster doesn't already have a label
        row = conn.execute(
            "SELECT person_label FROM clusters WHERE cluster_id=?", (new_cid,)
        ).fetchone()
        if row and row[0]:
            continue

        # Majority vote on label (ignore None votes)
        label_votes = [v[0] for v in votes if v[0]]
        if not label_votes:
            continue
        best_label, count = _Counter(label_votes).most_common(1)[0]
        # Carry approved if majority of voters for this label were approved
        approved_count = sum(1 for v in votes if v[0] == best_label and v[1])
        carry_approved = 1 if approved_count > count // 2 else 0

        conn.execute(
            "UPDATE clusters SET person_label=?, approved=?, updated_at=? WHERE cluster_id=?",
            (best_label, carry_approved, _now(), new_cid)
        )
        carried += 1
        log.info("Carried label '%s' (approved=%d) → new cluster %d (%d/%d faces voted)",
                 best_label, carry_approved, new_cid, count, len(votes))

    conn.commit()
    if carried:
        log.info("Carried forward %d labels from previous clustering", carried)


def _auto_label_from_ground_truth(conn, face_ids, labels, photo_ids, is_gt_flags) -> None:
    """
    For each cluster, collect all ground truth faces, look up their photo's
    existing_people, then majority-vote for the person_label.
    """
    # Build map: cluster_id → list of (photo_id, is_gt)
    cluster_gt_map: dict[int, list[int]] = {}
    for face_id, label, photo_id, is_gt in zip(face_ids, labels, photo_ids, is_gt_flags):
        if is_gt and label != -1:
            cluster_gt_map.setdefault(int(label), []).append(photo_id)

    for cluster_id, gt_photo_ids in cluster_gt_map.items():
        # Get existing_people for each ground truth photo
        name_votes = []
        for photo_id in gt_photo_ids:
            row = conn.execute(
                "SELECT existing_people FROM photos WHERE photo_id=?", (photo_id,)
            ).fetchone()
            if row and row[0]:
                try:
                    people = __import__("json").loads(row[0])
                    name_votes.extend(people)
                except Exception:
                    pass

        if name_votes:
            best_name = Counter(name_votes).most_common(1)[0][0]
            conn.execute(
                "UPDATE clusters SET person_label=? WHERE cluster_id=? AND person_label IS NULL",
                (best_name, cluster_id)
            )
            log.debug("Auto-labeled cluster %d → %s (%d votes)",
                      cluster_id, best_name, len(name_votes))

    conn.commit()


def _count_labeled(conn) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM clusters WHERE person_label IS NOT NULL AND is_noise=0"
    ).fetchone()
    return row[0] if row else 0


if __name__ == "__main__":
    success = run_cluster()
    sys.exit(0 if success else 1)
