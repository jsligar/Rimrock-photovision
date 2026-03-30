"""Phase 3 — Cluster: UMAP + HDBSCAN face identity clustering.

Supports two modes selected automatically at runtime:

  Full mode (default on first run or when CLUSTER_INCREMENTAL_MODE=false):
    Run UMAP + HDBSCAN on all faces.  Carry forward labels from old clusters
    via majority vote.

  Incremental mode (default when approved+labeled clusters already exist):
    1. Freeze faces that already belong to approved+labeled clusters.
    2. Assign free faces to known people via cosine similarity against
       person_memory centroids (threshold = CLUSTER_INCREMENTAL_ASSIGN_THRESHOLD).
    3. Run UMAP + HDBSCAN only on unmatched free faces.
    4. Remap new cluster IDs above max_existing_id to avoid collisions.
    5. Carry forward + auto-label only the newly clustered subset.

    All approved label work is preserved across re-runs.
"""

import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
import person_memory
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase3_cluster")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Helpers shared by both modes ──────────────────────────────────────────────

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
    cluster_votes: dict[int, list[tuple[str | None, int]]] = {}
    for face_id, new_label in zip(face_ids, labels):
        new_cid = int(new_label)
        if new_cid == -1:
            continue
        if face_id in old_labels:
            cluster_votes.setdefault(new_cid, []).append(old_labels[face_id])

    carried = 0
    for new_cid, votes in cluster_votes.items():
        row = conn.execute(
            "SELECT person_label FROM clusters WHERE cluster_id=?", (new_cid,)
        ).fetchone()
        if row and row[0]:
            continue

        label_votes = [v[0] for v in votes if v[0]]
        if not label_votes:
            continue
        best_label, count = Counter(label_votes).most_common(1)[0]
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
    cluster_gt_map: dict[int, list[int]] = {}
    for face_id, label, photo_id, is_gt in zip(face_ids, labels, photo_ids, is_gt_flags):
        if is_gt and label != -1:
            cluster_gt_map.setdefault(int(label), []).append(photo_id)

    for cluster_id, gt_photo_ids in cluster_gt_map.items():
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


def _run_umap_hdbscan(embeddings: np.ndarray) -> np.ndarray:
    """Run UMAP dimensionality reduction then HDBSCAN clustering.

    Returns integer label array (same length as embeddings).
    -1 = noise.  Non-negative integers = cluster IDs (sequential from 0).
    """
    import umap
    import hdbscan as hdbscan_lib

    n = len(embeddings)
    log.info("Running UMAP (n_neighbors=%d, n_components=%d, n=%d)...",
             config.UMAP_N_NEIGHBORS, config.UMAP_N_COMPONENTS, n)
    reducer = umap.UMAP(
        n_neighbors=config.UMAP_N_NEIGHBORS,
        min_dist=config.UMAP_MIN_DIST,
        n_components=min(config.UMAP_N_COMPONENTS, n - 1),
        metric=config.UMAP_METRIC,
        random_state=42,
        verbose=False,
    )
    reduced = reducer.fit_transform(embeddings)
    log.info("UMAP complete. Reduced shape: %s", reduced.shape)

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
    log.info("HDBSCAN complete: %d clusters, %d noise", n_clusters, n_noise)
    return labels


# ── Incremental-mode helpers ──────────────────────────────────────────────────

def _split_face_pool(conn, all_face_ids: list[int]) -> tuple[frozenset[int], list[int]]:
    """Split faces into frozen (approved+labeled) and free (everything else).

    Frozen faces keep their cluster_id.  Free faces are re-evaluated.
    """
    rows = conn.execute(
        """SELECT f.face_id
           FROM faces f
           JOIN clusters c ON f.cluster_id = c.cluster_id
           WHERE c.approved = 1 AND c.is_noise = 0 AND c.person_label IS NOT NULL
             AND TRIM(c.person_label) <> ''"""
    ).fetchall()
    frozen = frozenset(int(r[0]) for r in rows)
    free = [fid for fid in all_face_ids if fid not in frozen]
    return frozen, free


def _assign_to_known_people(
    free_face_ids: list[int],
    free_embeddings: np.ndarray,
    prototypes: list[dict],
    threshold: float,
) -> tuple[dict[int, str], list[int]]:
    """Assign free faces to known people via cosine similarity against prototypes.

    Returns:
      assigned        — {face_id: person_label} for faces above threshold
      unmatched_idx   — indices into free_face_ids that did not match
    """
    if not prototypes:
        return {}, list(range(len(free_face_ids)))

    centroids = np.vstack([p["centroid"] for p in prototypes]).astype(np.float32)
    proto_labels = [p["person_label"] for p in prototypes]

    # L2-normalize free embeddings row-wise
    norms = np.linalg.norm(free_embeddings, axis=1, keepdims=True)
    norms = np.where(norms > 0, norms, 1.0)
    normed = free_embeddings / norms

    scores = normed @ centroids.T  # (N, P)

    assigned: dict[int, str] = {}
    unmatched_idx: list[int] = []

    for i, face_id in enumerate(free_face_ids):
        best_idx = int(np.argmax(scores[i]))
        best_score = float(scores[i, best_idx])
        if best_score >= threshold:
            assigned[face_id] = proto_labels[best_idx]
        else:
            unmatched_idx.append(i)

    return assigned, unmatched_idx


def _lookup_cluster_ids_for_labels(
    conn,
    assigned: dict[int, str],
) -> dict[int, int]:
    """For each face in assigned, find the dominant cluster_id for that person label.

    Returns {face_id: cluster_id}.  Faces whose label has no approved cluster
    in the DB are dropped and will fall through to the HDBSCAN pool.
    """
    # Build label → cluster_id map (pick highest face_count cluster per label)
    label_to_cluster: dict[str, int] = {}
    unique_labels = set(assigned.values())
    for label in unique_labels:
        row = conn.execute(
            """SELECT cluster_id FROM clusters
               WHERE person_label = ? AND approved = 1 AND is_noise = 0
               ORDER BY face_count DESC LIMIT 1""",
            (label,),
        ).fetchone()
        if row:
            label_to_cluster[label] = int(row[0])

    result: dict[int, int] = {}
    for face_id, label in assigned.items():
        if label in label_to_cluster:
            result[face_id] = label_to_cluster[label]
    return result


def _remap_hdbscan_labels(labels: np.ndarray, offset: int) -> np.ndarray:
    """Remap non-noise HDBSCAN labels to start at offset+1 to avoid ID collisions."""
    remapped = labels.copy()
    unique_new = sorted(set(int(l) for l in labels) - {-1})
    for i, orig_id in enumerate(unique_new):
        remapped[labels == orig_id] = offset + 1 + i
    return remapped


# ── Full-mode path (unchanged from original) ─────────────────────────────────

def _run_full_cluster(conn, face_ids, embeddings, photo_ids, is_gt_flags, phase_start) -> bool:
    log.info("Mode: FULL — clustering all %d faces", len(face_ids))

    db.update_phase_progress("cluster", 1, 4)
    labels = _run_umap_hdbscan(embeddings)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int(np.sum(labels == -1))
    db.update_phase_progress("cluster", 2, 4)

    old_labels = _snapshot_old_labels(conn)

    for face_id, label in zip(face_ids, labels):
        conn.execute("UPDATE faces SET cluster_id=? WHERE face_id=?", (int(label), face_id))
    conn.commit()

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

    db.update_phase_progress("cluster", 3, 4)
    _carry_forward_labels(conn, face_ids, labels, old_labels)
    _auto_label_from_ground_truth(conn, face_ids, labels, photo_ids, is_gt_flags)
    db.update_phase_progress("cluster", 4, 4)

    labeled_count = _count_labeled(conn)
    log.info("Full cluster complete: %d clusters, %d noise, %d labeled",
             n_clusters, n_noise, labeled_count)

    person_memory.sync_person_memory_from_db(
        conn,
        min_approved_faces=config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
        min_clean_face_score=config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
        preserve_existing_on_empty=True,
    )
    db.mark_phase_complete("cluster")
    db.reset_phase_state(["tag", "push", "verify"])
    emit_phase_postmortem(
        log, "cluster", phase_start, True,
        metrics={
            "Mode": "full",
            "Faces loaded": len(face_ids),
            "Clusters": n_clusters,
            "Noise faces": n_noise,
            "Labeled clusters": labeled_count,
        },
    )
    return True


# ── Incremental-mode path ─────────────────────────────────────────────────────

def _run_incremental_cluster(conn, face_ids, embeddings, photo_ids, is_gt_flags, phase_start) -> bool:
    total_faces = len(face_ids)
    log.info("Mode: INCREMENTAL — %d total faces", total_faces)
    db.update_phase_progress("cluster", 1, 5)

    # Step 1: split face pool
    face_id_to_idx = {fid: i for i, fid in enumerate(face_ids)}
    frozen_ids, free_face_ids = _split_face_pool(conn, face_ids)
    log.info("Incremental: %d frozen faces, %d free faces", len(frozen_ids), len(free_face_ids))

    if not free_face_ids:
        log.info("No free faces to cluster — all faces are frozen. Nothing to do.")
        db.update_phase_progress("cluster", 5, 5)
        labeled_count = _count_labeled(conn)
        person_memory.sync_person_memory_from_db(
            conn,
            min_approved_faces=config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
            min_clean_face_score=config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
            preserve_existing_on_empty=True,
        )
        db.mark_phase_complete("cluster")
        db.reset_phase_state(["tag", "push", "verify"])
        emit_phase_postmortem(
            log, "cluster", phase_start, True,
            metrics={
                "Mode": "incremental",
                "Frozen faces": len(frozen_ids),
                "Free faces": 0,
                "Assigned to known people": 0,
                "Sent to HDBSCAN": 0,
                "New clusters": 0,
                "New noise faces": 0,
                "Labeled clusters (total)": labeled_count,
            },
        )
        return True

    free_embeddings = np.array([embeddings[face_id_to_idx[fid]] for fid in free_face_ids])
    free_photo_ids = [photo_ids[face_id_to_idx[fid]] for fid in free_face_ids]
    free_is_gt = [is_gt_flags[face_id_to_idx[fid]] for fid in free_face_ids]

    db.update_phase_progress("cluster", 2, 5)

    # Step 2: assign free faces to known people via centroid similarity
    prototypes = person_memory.load_person_memory()
    log.info("Loaded %d person prototypes from memory", len(prototypes))

    assigned_to_label, unmatched_idx = _assign_to_known_people(
        free_face_ids,
        free_embeddings,
        prototypes,
        threshold=config.CLUSTER_INCREMENTAL_ASSIGN_THRESHOLD,
    )
    log.info(
        "Prototype assignment: %d matched, %d unmatched (threshold=%.2f)",
        len(assigned_to_label), len(unmatched_idx), config.CLUSTER_INCREMENTAL_ASSIGN_THRESHOLD,
    )

    # Step 3: look up cluster_id for assigned faces
    face_to_cluster: dict[int, int] = _lookup_cluster_ids_for_labels(conn, assigned_to_label)
    # Faces whose label has no cluster in DB fall back to unmatched
    no_cluster_face_ids = {fid for fid in assigned_to_label if fid not in face_to_cluster}
    if no_cluster_face_ids:
        log.warning(
            "%d assigned faces have no matching cluster in DB (label deleted?) — "
            "treating as unmatched", len(no_cluster_face_ids)
        )
        extra_unmatched = [i for i, fid in enumerate(free_face_ids) if fid in no_cluster_face_ids]
        unmatched_idx = sorted(set(unmatched_idx) | set(extra_unmatched))
        for fid in no_cluster_face_ids:
            del face_to_cluster[fid]

    db.update_phase_progress("cluster", 3, 5)

    # Step 4: HDBSCAN on unmatched free faces
    n_unmatched = len(unmatched_idx)
    min_for_hdbscan = config.HDBSCAN_MIN_CLUSTER_SIZE * 2
    new_labels = np.full(n_unmatched, -1, dtype=np.int64)
    n_new_clusters = 0
    n_new_noise = n_unmatched

    if n_unmatched == 0:
        log.info("All free faces matched known people — skipping HDBSCAN")
    elif n_unmatched < min_for_hdbscan:
        log.warning(
            "%d unmatched faces is below minimum (%d) for HDBSCAN — marking all as noise",
            n_unmatched, min_for_hdbscan,
        )
    else:
        max_existing_id = conn.execute(
            "SELECT MAX(cluster_id) FROM clusters"
        ).fetchone()[0] or 0
        max_existing_id = max(int(max_existing_id), 0)

        unmatched_embeddings = free_embeddings[unmatched_idx]
        raw_labels = _run_umap_hdbscan(unmatched_embeddings)
        new_labels = _remap_hdbscan_labels(raw_labels, max_existing_id)
        n_new_clusters = len(set(int(l) for l in new_labels if l != -1))
        n_new_noise = int(np.sum(new_labels == -1))
        log.info(
            "Incremental HDBSCAN: %d new clusters, %d noise (offset base=%d)",
            n_new_clusters, n_new_noise, max_existing_id,
        )

    db.update_phase_progress("cluster", 4, 5)

    # Step 5: write assignments to DB
    # Prototype-assigned faces
    assign_rows = [(cid, fid) for fid, cid in face_to_cluster.items()]
    if assign_rows:
        conn.executemany("UPDATE faces SET cluster_id=? WHERE face_id=?", assign_rows)

    # HDBSCAN-clustered faces
    unmatched_face_ids = [free_face_ids[i] for i in unmatched_idx]
    hdbscan_rows = [(int(new_labels[j]), fid) for j, fid in enumerate(unmatched_face_ids)]
    if hdbscan_rows:
        conn.executemany("UPDATE faces SET cluster_id=? WHERE face_id=?", hdbscan_rows)
    conn.commit()

    # Upsert new clusters from HDBSCAN
    now = _now()
    new_cluster_ids = set(int(l) for l in new_labels)
    for cluster_id in new_cluster_ids:
        count = int(np.sum(new_labels == cluster_id))
        is_noise = 1 if cluster_id == -1 else 0
        conn.execute(
            """INSERT INTO clusters (cluster_id, face_count, is_noise, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(cluster_id) DO UPDATE SET
                 face_count = face_count + excluded.face_count,
                 is_noise   = excluded.is_noise,
                 updated_at = excluded.updated_at""",
            (cluster_id, count, is_noise, now)
        )

    # Update face_count for clusters that received prototype-assigned faces
    for cluster_id in set(face_to_cluster.values()):
        count = conn.execute(
            "SELECT COUNT(*) FROM faces WHERE cluster_id=?", (cluster_id,)
        ).fetchone()[0]
        conn.execute(
            "UPDATE clusters SET face_count=?, updated_at=? WHERE cluster_id=?",
            (count, now, cluster_id)
        )

    conn.commit()

    # Prune empty unapproved clusters only (never prune approved/frozen)
    conn.execute(
        """DELETE FROM clusters
           WHERE cluster_id NOT IN (
               SELECT DISTINCT cluster_id FROM faces WHERE cluster_id IS NOT NULL
           )
           AND approved = 0"""
    )
    conn.commit()

    db.update_phase_progress("cluster", 5, 5)

    # Step 6: carry-forward + auto-label for new HDBSCAN clusters only
    old_labels_snap = _snapshot_old_labels(conn)
    if unmatched_face_ids:
        unmatched_int_labels = [int(l) for l in new_labels]
        unmatched_photo_ids = [free_photo_ids[i] for i in unmatched_idx]
        unmatched_is_gt = [free_is_gt[i] for i in unmatched_idx]
        _carry_forward_labels(conn, unmatched_face_ids, unmatched_int_labels, old_labels_snap)
        _auto_label_from_ground_truth(
            conn, unmatched_face_ids, unmatched_int_labels,
            unmatched_photo_ids, unmatched_is_gt,
        )

    labeled_count = _count_labeled(conn)
    log.info(
        "Incremental cluster complete: %d new clusters, %d new noise, %d total labeled",
        n_new_clusters, n_new_noise, labeled_count,
    )

    person_memory.sync_person_memory_from_db(
        conn,
        min_approved_faces=config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
        min_clean_face_score=config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
        preserve_existing_on_empty=True,
    )
    db.mark_phase_complete("cluster")
    db.reset_phase_state(["tag", "push", "verify"])
    emit_phase_postmortem(
        log, "cluster", phase_start, True,
        metrics={
            "Mode": "incremental",
            "Frozen faces": len(frozen_ids),
            "Free faces": len(free_face_ids),
            "Assigned to known people": len(face_to_cluster),
            "Sent to HDBSCAN": n_unmatched,
            "New clusters": n_new_clusters,
            "New noise faces": n_new_noise,
            "Labeled clusters (total)": labeled_count,
        },
    )
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def run_cluster() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 3 — CLUSTER: UMAP + HDBSCAN face clustering")
    log.info("=" * 60)

    import umap  # noqa: F401 — validate import before marking phase running
    import hdbscan as hdbscan_lib  # noqa: F401

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
            log, "cluster", phase_start, True,
            metrics={"Faces loaded": 0, "Clusters": 0, "Noise faces": 0},
        )
        return True

    face_ids = [r[0] for r in rows]
    photo_ids = [r[2] for r in rows]
    is_gt_flags = [r[3] for r in rows]
    embeddings = np.array([np.frombuffer(r[1], dtype=np.float32) for r in rows])

    log.info("Loaded %d face embeddings (dim=%d)", len(face_ids), embeddings.shape[1])

    # Decide mode
    approved_count = conn.execute(
        """SELECT COUNT(*) FROM clusters
           WHERE approved=1 AND is_noise=0
             AND person_label IS NOT NULL AND TRIM(person_label) <> ''"""
    ).fetchone()[0]

    use_incremental = config.CLUSTER_INCREMENTAL_MODE and approved_count > 0

    log.info(
        "Cluster mode: %s (incremental_mode_enabled=%s, approved_labeled_clusters=%d)",
        "INCREMENTAL" if use_incremental else "FULL",
        config.CLUSTER_INCREMENTAL_MODE,
        approved_count,
    )

    try:
        if use_incremental:
            success = _run_incremental_cluster(
                conn, face_ids, embeddings, photo_ids, is_gt_flags, phase_start
            )
        else:
            success = _run_full_cluster(
                conn, face_ids, embeddings, photo_ids, is_gt_flags, phase_start
            )
    except Exception as e:
        log.exception("Cluster phase failed: %s", e)
        db.mark_phase_error("cluster", str(e))
        emit_phase_postmortem(log, "cluster", phase_start, False, error=str(e))
        success = False
    finally:
        conn.close()

    return success


if __name__ == "__main__":
    success = run_cluster()
    sys.exit(0 if success else 1)
