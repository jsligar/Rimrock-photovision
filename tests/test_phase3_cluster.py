"""Tests for phase3 cluster pipeline behavior."""

import sys
import types

import numpy as np


def test_recluster_resets_downstream_phase_state(tmp_db):
    import db
    from pipeline.phase3_cluster import run_cluster

    conn = db.get_db()
    conn.execute(
        "INSERT INTO photos (source_path, filename) VALUES ('By-Year/2025/a.jpg', 'a.jpg')"
    )
    conn.execute(
        "INSERT INTO photos (source_path, filename) VALUES ('By-Year/2025/b.jpg', 'b.jpg')"
    )
    photo_ids = [
        row[0]
        for row in conn.execute(
            "SELECT photo_id FROM photos ORDER BY photo_id"
        ).fetchall()
    ]

    emb1 = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32).tobytes()
    emb2 = np.array([0.9, 0.1, 0.0, 0.0], dtype=np.float32).tobytes()
    conn.execute(
        """
        INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
        VALUES (?, '[]', ?, 0.99, 7, 'crops/a.jpg')
        """,
        (photo_ids[0], emb1),
    )
    conn.execute(
        """
        INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
        VALUES (?, '[]', ?, 0.98, 7, 'crops/b.jpg')
        """,
        (photo_ids[1], emb2),
    )
    conn.execute(
        """
        INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved, updated_at)
        VALUES (7, 'Levi', 2, 0, 1, '2026-03-26T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()

    for phase in ("organize", "tag", "push", "verify"):
        db.mark_phase_running(phase)
        db.update_phase_progress(phase, 5, 5)
        db.mark_phase_complete(phase)

    class DummyUMAP:
        def __init__(self, *args, **kwargs):
            pass

        def fit_transform(self, embeddings):
            return embeddings

    class DummyHDBSCAN:
        def __init__(self, *args, **kwargs):
            pass

        def fit_predict(self, reduced):
            return np.array([0, 0], dtype=int)

    sys.modules["umap"] = types.SimpleNamespace(UMAP=DummyUMAP)
    sys.modules["hdbscan"] = types.SimpleNamespace(HDBSCAN=DummyHDBSCAN)

    try:
        assert run_cluster() is True
    finally:
        sys.modules.pop("umap", None)
        sys.modules.pop("hdbscan", None)

    conn = db.get_db()
    rows = {
        row["phase"]: dict(row)
        for row in conn.execute(
            """
            SELECT phase, status, progress_current, progress_total, completed_at
            FROM pipeline_state
            WHERE phase IN ('organize', 'tag', 'push', 'verify')
            """
        ).fetchall()
    }
    cluster = conn.execute(
        "SELECT status FROM pipeline_state WHERE phase='cluster'"
    ).fetchone()
    conn.close()

    assert cluster["status"] == "complete"
    assert rows["organize"]["status"] == "complete"
    assert rows["tag"]["status"] == "pending"
    assert rows["push"]["status"] == "pending"
    assert rows["verify"]["status"] == "pending"
    assert rows["tag"]["progress_current"] == 0
    assert rows["push"]["progress_total"] == 0
    assert rows["verify"]["completed_at"] is None
