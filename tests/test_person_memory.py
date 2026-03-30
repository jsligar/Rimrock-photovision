"""Tests for persistent person memory outside the working DB."""

from pathlib import Path

import numpy as np

import config
import db
import person_memory


def test_sync_person_memory_persists_usable_prototypes(tmp_db):
    conn = db.get_db()
    conn.execute("INSERT INTO photos (source_path, filename) VALUES ('a.jpg', 'a.jpg')")
    photo_id = conn.execute("SELECT photo_id FROM photos").fetchone()[0]
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved, updated_at) VALUES (1, 'Alice', 2, 0, 1, '2026-03-26T00:00:00+00:00')"
    )
    emb = np.ones(4, dtype=np.float32).tobytes()
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.95, 1, 'crops/a1.jpg')""",
        (photo_id, emb),
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.94, 1, 'crops/a2.jpg')""",
        (photo_id, emb),
    )
    conn.commit()

    prototypes = person_memory.sync_person_memory_from_db(
        conn,
        min_approved_faces=2,
        min_clean_face_score=0.75,
        preserve_existing_on_empty=True,
    )
    conn.close()

    assert len(prototypes) == 1
    assert prototypes[0]["person_label"] == "Alice"
    assert prototypes[0]["clean_approved_faces"] == 2
    assert Path(config.PERSON_MEMORY_PATH).exists()

    loaded = person_memory.load_person_memory()
    assert len(loaded) == 1
    assert loaded[0]["person_label"] == "Alice"
    assert loaded[0]["prototype_source"] == "memory"
    assert loaded[0]["usable_label"] is True


def test_sync_person_memory_preserves_existing_file_when_db_empty(tmp_db):
    person_memory.save_person_memory([
        {
            "person_label": "Alice",
            "centroid": np.ones(4, dtype=np.float32),
            "support_faces": 5,
            "support_clusters": 2,
            "clean_approved_faces": 5,
            "usable_label": True,
            "updated_at": "2026-03-26T00:00:00+00:00",
        }
    ])

    conn = db.get_db()
    prototypes = person_memory.sync_person_memory_from_db(
        conn,
        min_approved_faces=5,
        min_clean_face_score=0.75,
        preserve_existing_on_empty=True,
    )
    conn.close()

    assert prototypes == []
    loaded = person_memory.load_person_memory()
    assert len(loaded) == 1
    assert loaded[0]["person_label"] == "Alice"


def test_sync_person_memory_preserves_labels_not_in_current_db(tmp_db):
    person_memory.save_person_memory([
        {
            "person_label": "Alice",
            "centroid": np.ones(4, dtype=np.float32),
            "support_faces": 5,
            "support_clusters": 2,
            "clean_approved_faces": 5,
            "usable_label": True,
            "updated_at": "2026-03-26T00:00:00+00:00",
        },
        {
            "person_label": "Bob",
            "centroid": np.full(4, 2.0, dtype=np.float32),
            "support_faces": 7,
            "support_clusters": 3,
            "clean_approved_faces": 7,
            "usable_label": True,
            "updated_at": "2026-03-26T00:00:00+00:00",
        },
    ])

    conn = db.get_db()
    conn.execute("INSERT INTO photos (source_path, filename) VALUES ('c.jpg', 'c.jpg')")
    photo_id = conn.execute("SELECT photo_id FROM photos").fetchone()[0]
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved, updated_at) VALUES (1, 'Alice', 2, 0, 1, '2026-03-26T01:00:00+00:00')"
    )
    emb = np.full(4, 3.0, dtype=np.float32).tobytes()
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.95, 1, 'crops/c1.jpg')""",
        (photo_id, emb),
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.94, 1, 'crops/c2.jpg')""",
        (photo_id, emb),
    )
    conn.commit()

    person_memory.sync_person_memory_from_db(
        conn,
        min_approved_faces=2,
        min_clean_face_score=0.75,
        preserve_existing_on_empty=True,
    )
    conn.close()

    loaded = person_memory.load_person_memory()
    assert [item["person_label"] for item in loaded] == ["Alice", "Bob"]
    alice = next(item for item in loaded if item["person_label"] == "Alice")
    bob = next(item for item in loaded if item["person_label"] == "Bob")
    assert alice["clean_approved_faces"] == 2
    assert bob["clean_approved_faces"] == 7
