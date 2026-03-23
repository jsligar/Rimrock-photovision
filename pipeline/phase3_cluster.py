"""Phase 3 — Cluster: UMAP + HDBSCAN face identity clustering."""

import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger

log = get_logger("phase3_cluster")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_cluster() -> bool:
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

    # ── Write cluster IDs back to faces ──
    for face_id, label in zip(face_ids, labels):
        conn.execute(
            "UPDATE faces SET cluster_id=? WHERE face_id=?",
            (int(label), face_id)
        )
    conn.commit()

    # ── Upsert clusters table ──
    now = _now()
    for cluster_id in set(labels):
        count = int(np.sum(labels == cluster_id))
        is_noise = 1 if cluster_id == -1 else 0
        conn.execute(
            """INSERT OR REPLACE INTO clusters
               (cluster_id, face_count, is_noise, updated_at)
               VALUES (?, ?, ?, ?)""",
            (int(cluster_id), count, is_noise, now)
        )
    conn.commit()

    # ── Auto-label clusters from ground truth anchors ──
    _auto_label_from_ground_truth(conn, face_ids, labels, photo_ids, is_gt_flags)

    db.update_phase_progress("cluster", 4, 4)

    log.info("Cluster phase complete.")
    log.info("  Total clusters: %d", n_clusters)
    log.info("  Noise faces:    %d", n_noise)
    log.info("  Labeled:        %d", _count_labeled(conn))

    db.mark_phase_complete("cluster")
    conn.close()
    return True


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
