"""Tests for cluster review API endpoints."""

import pytest
from fastapi.testclient import TestClient
import numpy as np

import person_memory
from api.routes import clusters as cluster_routes


@pytest.fixture(autouse=True)
def relax_person_prototype_thresholds(monkeypatch):
    monkeypatch.setattr("config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES", 1)
    monkeypatch.setattr("config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE", 0.0)


@pytest.fixture(autouse=True)
def clear_cluster_review_caches():
    cluster_routes._clear_review_caches_for_tests()
    yield
    cluster_routes._clear_review_caches_for_tests()


@pytest.fixture
def client_with_clusters(tmp_db):
    """Client with two clusters and associated faces seeded."""
    import db
    c = db.get_db()

    # Seed photos
    c.execute(
        "INSERT INTO photos (source_path, filename) VALUES ('img/a.jpg', 'a.jpg')"
    )
    c.execute(
        "INSERT INTO photos (source_path, filename) VALUES ('img/b.jpg', 'b.jpg')"
    )
    c.commit()
    photo1 = c.execute("SELECT photo_id FROM photos WHERE filename='a.jpg'").fetchone()[0]
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]

    # Seed clusters
    c.execute("INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (1, NULL, 2, 0, 0)")
    c.execute("INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (2, NULL, 1, 0, 0)")
    c.commit()

    # Seed faces
    import numpy as np
    dummy_emb = np.zeros(512, dtype=np.float32).tobytes()
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path) VALUES (?, '[]', ?, 0.9, 1, ?)",
        (photo1, dummy_emb, "crops/face_a_0.jpg")
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path) VALUES (?, '[]', ?, 0.8, 1, ?)",
        (photo1, dummy_emb, "/local/rimrock/photos/crops/face_a_1.jpg")
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path) VALUES (?, '[]', ?, 0.7, 2, ?)",
        (photo2, dummy_emb, "face_b_0.jpg")
    )
    c.commit()
    c.close()

    from api.main import app
    with TestClient(app) as tc:
        yield tc


def test_list_clusters(client_with_clusters):
    resp = client_with_clusters.get("/api/clusters")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    # Sorted by face_count desc
    assert data[0]["cluster_id"] == 1
    assert data[0]["face_count"] == 2


def test_list_clusters_review_sort_prioritizes_unresolved_queue(client_with_clusters):
    import db
    import numpy as np

    c = db.get_db()
    photo1 = c.execute("SELECT photo_id FROM photos WHERE filename='a.jpg'").fetchone()[0]
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]
    dummy_emb = np.zeros(512, dtype=np.float32).tobytes()

    c.execute("UPDATE clusters SET person_label='Alice', approved=0 WHERE cluster_id=2")
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Bob', 10, 0, 1)"
    )
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (4, NULL, 26, 0, 0)"
    )

    for i in range(10):
        c.execute(
            """INSERT INTO faces
               (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
               VALUES (?, '[]', ?, 0.98, 3, ?)""",
            (photo1, dummy_emb, f"face_bob_{i}.jpg"),
        )
    for i in range(26):
        c.execute(
            """INSERT INTO faces
               (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
               VALUES (?, '[]', ?, 0.93, 4, ?)""",
            (photo2, dummy_emb, f"face_pending_{i}.jpg"),
        )

    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters?sort=review")
    assert resp.status_code == 200
    data = resp.json()
    ids = [cluster["cluster_id"] for cluster in data]

    assert ids[0] == 4
    assert ids.index(1) < ids.index(3)
    assert ids.index(2) < ids.index(3)

    first = data[0]
    assert first["review_state"] == "unlabeled"
    assert first["is_mega_cluster"] is True
    assert first["review_priority_bucket"] == "high"
    assert first["review_priority_rank"] == 1
    assert first["avg_detection_score"] == pytest.approx(0.93, rel=1e-4)


def test_list_clusters_adds_within_person_best_to_worst_rank(client_with_clusters):
    import db

    c = db.get_db()
    good_vec = np.ones(512, dtype=np.float32).tobytes()
    bad_vec = np.full(512, -1.0, dtype=np.float32).tobytes()
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id IN (1, 2)")
    c.execute("UPDATE faces SET embedding=?, detection_score=0.95 WHERE cluster_id=1", (good_vec,))
    c.execute("UPDATE faces SET embedding=?, detection_score=0.70 WHERE cluster_id=2", (bad_vec,))
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters")
    assert resp.status_code == 200
    data = resp.json()
    cluster1 = next(item for item in data if item["cluster_id"] == 1)
    cluster2 = next(item for item in data if item["cluster_id"] == 2)

    assert cluster1["person_cluster_rank"] == 1
    assert cluster2["person_cluster_rank"] == 2
    assert cluster1["person_cluster_count"] == 2
    assert cluster2["person_cluster_count"] == 2
    assert cluster1["person_match_score"] > cluster2["person_match_score"]


def test_list_clusters_caches_until_mutation(client_with_clusters, monkeypatch):
    calls = {"count": 0}
    real = cluster_routes._build_cluster_list_response

    def wrapped(conn, sort):
        calls["count"] += 1
        return real(conn, sort)

    monkeypatch.setattr(cluster_routes, "_build_cluster_list_response", wrapped)

    assert client_with_clusters.get("/api/clusters").status_code == 200
    assert client_with_clusters.get("/api/clusters").status_code == 200
    assert calls["count"] == 1

    assert client_with_clusters.post("/api/clusters/1/label", json={"person_label": "Alice"}).status_code == 200
    assert client_with_clusters.get("/api/clusters").status_code == 200
    assert calls["count"] == 2


def test_get_cluster_crops(client_with_clusters):
    resp = client_with_clusters.get("/api/clusters/1/crops")
    assert resp.status_code == 200
    crops = resp.json()
    assert len(crops) == 2
    assert all(c["crop_url"] and c["crop_url"].startswith("/crops/") for c in crops)
    assert all("/crops/crops/" not in c["crop_url"] for c in crops)


def test_get_noise_cluster_crops_rank_by_best_prototype_match(client_with_clusters):
    import db

    c = db.get_db()
    photo1 = c.execute("SELECT photo_id FROM photos WHERE filename='a.jpg'").fetchone()[0]
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]

    alice_vec = np.ones(512, dtype=np.float32)
    bob_vec = np.concatenate([
        np.ones(256, dtype=np.float32),
        -np.ones(256, dtype=np.float32),
    ])
    bobish_vec = np.concatenate([
        np.ones(320, dtype=np.float32),
        -np.ones(192, dtype=np.float32),
    ]).astype(np.float32)

    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=2")
    c.execute("UPDATE faces SET embedding=?, detection_score=0.95 WHERE cluster_id=2", (alice_vec.tobytes(),))
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Bob', 1, 0, 1)"
    )
    c.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.91, 3, 'crops/bob_proto.jpg')""",
        (photo1, bob_vec.tobytes()),
    )
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (-1, NULL, 2, 1, 0)"
    )
    c.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.84, -1, 'crops/noise_alice.jpg')""",
        (photo2, alice_vec.tobytes()),
    )
    c.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.90, -1, 'crops/noise_bobish.jpg')""",
        (photo2, bobish_vec.tobytes()),
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/-1/crops")
    assert resp.status_code == 200
    crops = resp.json()
    assert len(crops) >= 2
    assert crops[0]["predicted_label"] == "Alice"
    assert crops[0]["best_match_score"] >= crops[1]["best_match_score"]
    assert crops[0]["prototype_source"] in {"current", "memory"}


def test_cluster_suggestions_cache_until_mutation(client_with_clusters, monkeypatch):
    import db

    c = db.get_db()
    proto_vec = np.ones(512, dtype=np.float32).tobytes()
    target_vec = np.full(512, 0.98, dtype=np.float32).tobytes()
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=2")
    c.execute("UPDATE faces SET embedding=?, detection_score=0.95 WHERE cluster_id=2", (proto_vec,))
    c.execute("UPDATE faces SET embedding=?, detection_score=0.91 WHERE cluster_id=1", (target_vec,))
    c.commit()
    c.close()

    calls = {"count": 0}
    real = cluster_routes._review_match_sources

    def wrapped(conn, **kwargs):
        calls["count"] += 1
        return real(conn, **kwargs)

    monkeypatch.setattr(cluster_routes, "_review_match_sources", wrapped)

    first = client_with_clusters.get("/api/clusters/1/suggestions")
    second = client_with_clusters.get("/api/clusters/1/suggestions")
    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1

    assert client_with_clusters.post("/api/clusters/1/label", json={"person_label": "Bob"}).status_code == 200
    third = client_with_clusters.get("/api/clusters/1/suggestions")
    assert third.status_code == 200
    assert calls["count"] == 2


def test_get_person_review_sorts_faces_worst_to_best(client_with_clusters):
    import db

    c = db.get_db()
    face_rows = c.execute(
        "SELECT face_id, cluster_id, detection_score FROM faces ORDER BY cluster_id, face_id"
    ).fetchall()
    bad_face_id = int(face_rows[0]["face_id"])
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id IN (1, 2)")
    bad_vec = np.full(512, -1.0, dtype=np.float32).tobytes()
    good_vec = np.ones(512, dtype=np.float32).tobytes()
    c.execute("UPDATE faces SET embedding=?, detection_score=0.92 WHERE face_id=?", (bad_vec, bad_face_id))
    c.execute(
        "UPDATE faces SET embedding=?, detection_score=0.96 WHERE face_id!=?",
        (good_vec, bad_face_id),
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/person-review")
    assert resp.status_code == 200
    data = resp.json()
    assert data["person_label"] == "Alice"
    assert data["sort_mode"] == "worst_first"
    assert data["face_count"] == 3
    assert data["cluster_count"] == 2
    assert data["faces"][0]["face_id"] == bad_face_id
    assert data["faces"][0]["review_rank"] == 1
    assert data["faces"][0]["match_score"] < data["faces"][-1]["match_score"]


def test_person_review_cache_until_mutation(client_with_clusters, monkeypatch):
    import db

    c = db.get_db()
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id IN (1, 2)")
    c.execute("UPDATE faces SET embedding=?, detection_score=0.95 WHERE cluster_id IN (1, 2)", (np.ones(512, dtype=np.float32).tobytes(),))
    c.commit()
    c.close()

    calls = {"count": 0}
    real = cluster_routes._normalized_centroid

    def wrapped(vectors):
        calls["count"] += 1
        return real(vectors)

    monkeypatch.setattr(cluster_routes, "_normalized_centroid", wrapped)

    first = client_with_clusters.get("/api/clusters/1/person-review")
    second = client_with_clusters.get("/api/clusters/1/person-review")
    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1

    face_id = first.json()["faces"][0]["face_id"]
    moved = client_with_clusters.post(
        "/api/clusters/1/person-review/remove-face",
        json={"face_ids": [face_id]},
    )
    assert moved.status_code == 200

    third = client_with_clusters.get("/api/clusters/1/person-review")
    assert third.status_code == 200
    assert calls["count"] == 2


def test_remove_face_from_person_review_moves_single_face_out(client_with_clusters):
    import db

    c = db.get_db()
    face_id = c.execute(
        "SELECT face_id FROM faces WHERE cluster_id=1 ORDER BY face_id LIMIT 1"
    ).fetchone()[0]
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id IN (1, 2)")
    c.commit()
    c.close()

    resp = client_with_clusters.post(
        "/api/clusters/1/person-review/remove-face",
        json={"face_ids": [face_id]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["moved_faces"] == 1
    assert data["person_label"] == "Alice"

    clusters = client_with_clusters.get("/api/clusters").json()
    source = next(c for c in clusters if c["cluster_id"] == 1)
    target = next(c for c in clusters if c["cluster_id"] == data["target_cluster_id"])
    assert source["face_count"] == 1
    assert target["face_count"] == 1
    assert target["person_label"] is None
    assert target["approved"] == 0

    review = client_with_clusters.get("/api/clusters/1/person-review").json()
    assert review["face_count"] == 2
    assert all(face["face_id"] != face_id for face in review["faces"])


def test_remove_face_from_person_review_rejects_multi_select(client_with_clusters):
    import db

    c = db.get_db()
    face_ids = [
        row["face_id"]
        for row in c.execute(
            "SELECT face_id FROM faces WHERE cluster_id=1 ORDER BY face_id"
        ).fetchall()
    ]
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=1")
    c.commit()
    c.close()

    resp = client_with_clusters.post(
        "/api/clusters/1/person-review/remove-face",
        json={"face_ids": face_ids},
    )
    assert resp.status_code == 400


def test_label_cluster(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/1/label",
        json={"person_label": "Alice"}
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    # Verify label was set
    clusters = client_with_clusters.get("/api/clusters").json()
    cluster1 = next(c for c in clusters if c["cluster_id"] == 1)
    assert cluster1["person_label"] == "Alice"


def test_label_cluster_not_found(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/999/label",
        json={"person_label": "Nobody"}
    )
    assert resp.status_code == 404


def test_approve_cluster(client_with_clusters):
    resp = client_with_clusters.post("/api/clusters/1/approve")
    assert resp.status_code == 200

    clusters = client_with_clusters.get("/api/clusters").json()
    cluster1 = next(c for c in clusters if c["cluster_id"] == 1)
    assert cluster1["approved"] == 1


def test_approve_cluster_not_found(client_with_clusters):
    assert client_with_clusters.post("/api/clusters/999/approve").status_code == 404


def test_mark_noise(client_with_clusters):
    resp = client_with_clusters.post("/api/clusters/2/noise")
    assert resp.status_code == 200

    clusters = client_with_clusters.get("/api/clusters").json()
    cluster2 = next(c for c in clusters if c["cluster_id"] == 2)
    assert cluster2["is_noise"] == 1
    assert cluster2["approved"] == 0


def test_mark_noise_not_found(client_with_clusters):
    assert client_with_clusters.post("/api/clusters/999/noise").status_code == 404


def test_untag_cluster(client_with_clusters):
    # Seed with a label + approval first
    client_with_clusters.post("/api/clusters/1/label", json={"person_label": "Alice"})
    client_with_clusters.post("/api/clusters/1/approve")

    resp = client_with_clusters.post("/api/clusters/1/untag")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    clusters = client_with_clusters.get("/api/clusters").json()
    cluster1 = next(c for c in clusters if c["cluster_id"] == 1)
    assert cluster1["person_label"] is None
    assert cluster1["approved"] == 0


def test_untag_cluster_not_found(client_with_clusters):
    assert client_with_clusters.post("/api/clusters/999/untag").status_code == 404


def test_untag_selected_faces_moves_only_selection(client_with_clusters):
    source_faces = client_with_clusters.get("/api/clusters/1/crops").json()
    selected_face_id = source_faces[0]["face_id"]

    client_with_clusters.post(
        "/api/clusters/1/label",
        json={"person_label": "Alice"},
    )
    client_with_clusters.post("/api/clusters/1/approve")

    resp = client_with_clusters.post(
        "/api/clusters/1/untag-faces",
        json={"face_ids": [selected_face_id]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["moved_faces"] == 1

    clusters = client_with_clusters.get("/api/clusters").json()
    source = next(c for c in clusters if c["cluster_id"] == 1)
    target = next(c for c in clusters if c["cluster_id"] == data["target_cluster_id"])
    assert source["face_count"] == 1
    assert source["person_label"] == "Alice"
    assert source["approved"] == 1
    assert target["face_count"] == 1
    assert target["person_label"] is None
    assert target["approved"] == 0
    assert target["is_noise"] == 0


def test_untag_selected_faces_rejects_invalid_selection(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/1/untag-faces",
        json={"face_ids": [999999]},
    )
    assert resp.status_code == 400


def test_reassign_faces_to_existing_cluster(client_with_clusters):
    source_faces = client_with_clusters.get("/api/clusters/1/crops").json()
    face_id = source_faces[0]["face_id"]

    resp = client_with_clusters.post(
        "/api/clusters/reassign-faces",
        json={
            "source_cluster_id": 1,
            "face_ids": [face_id],
            "target_cluster_id": 2,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["moved_faces"] == 1
    assert data["target_cluster_id"] == 2

    clusters = client_with_clusters.get("/api/clusters").json()
    c1 = next(c for c in clusters if c["cluster_id"] == 1)
    c2 = next(c for c in clusters if c["cluster_id"] == 2)
    assert c1["face_count"] == 1
    assert c2["face_count"] == 2


def test_reassign_faces_to_new_person_cluster(client_with_clusters):
    source_faces = client_with_clusters.get("/api/clusters/1/crops").json()
    face_id = source_faces[0]["face_id"]

    resp = client_with_clusters.post(
        "/api/clusters/reassign-faces",
        json={
            "source_cluster_id": 1,
            "face_ids": [face_id],
            "target_person_label": "Charlie",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["moved_faces"] == 1

    clusters = client_with_clusters.get("/api/clusters").json()
    created = next(c for c in clusters if c["cluster_id"] == data["target_cluster_id"])
    assert created["person_label"] == "Charlie"
    assert created["face_count"] == 1


def test_reassign_faces_rejects_invalid_face_selection(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/reassign-faces",
        json={
            "source_cluster_id": 1,
            "face_ids": [9999],
            "target_cluster_id": 2,
        },
    )
    assert resp.status_code == 400


def test_reassign_faces_rejects_ambiguous_target(client_with_clusters):
    source_faces = client_with_clusters.get("/api/clusters/1/crops").json()
    face_id = source_faces[0]["face_id"]

    resp = client_with_clusters.post(
        "/api/clusters/reassign-faces",
        json={
            "source_cluster_id": 1,
            "face_ids": [face_id],
            "target_cluster_id": 2,
            "target_person_label": "Alice",
        },
    )
    assert resp.status_code == 400


def test_undo_last_reassign_faces(client_with_clusters):
    source_faces = client_with_clusters.get("/api/clusters/1/crops").json()
    face_id = source_faces[0]["face_id"]

    move = client_with_clusters.post(
        "/api/clusters/reassign-faces",
        json={
            "source_cluster_id": 1,
            "face_ids": [face_id],
            "target_cluster_id": 2,
        },
    )
    assert move.status_code == 200

    undo = client_with_clusters.post("/api/clusters/reassign-faces/undo-last")
    assert undo.status_code == 200
    assert undo.json()["ok"] is True
    assert undo.json()["moved_faces"] == 1

    clusters = client_with_clusters.get("/api/clusters").json()
    c1 = next(c for c in clusters if c["cluster_id"] == 1)
    c2 = next(c for c in clusters if c["cluster_id"] == 2)
    assert c1["face_count"] == 2
    assert c2["face_count"] == 1


def test_undo_last_reassign_faces_not_found(client_with_clusters):
    resp = client_with_clusters.post("/api/clusters/reassign-faces/undo-last")
    assert resp.status_code == 404


def test_cluster_suggestions_returns_ranked_matches(client_with_clusters):
    import db
    import numpy as np

    c = db.get_db()
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]
    vec_target = np.ones(512, dtype=np.float32)
    vec_match = np.ones(512, dtype=np.float32)
    vec_other = np.full(512, -1.0, dtype=np.float32)

    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=1", (vec_target.tobytes(),))
    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=2", (vec_match.tobytes(),))
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=2")
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Bob', 1, 0, 1)"
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.7, 3)",
        (photo2, vec_other.tobytes())
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/suggestions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cluster_id"] == 1
    assert len(data["suggestions"]) >= 1
    assert data["suggestions"][0]["person_label"] == "Alice"


def test_cluster_suggestions_dedupes_same_person_labels(client_with_clusters):
    import db
    import numpy as np

    c = db.get_db()
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]
    vec_target = np.ones(512, dtype=np.float32)
    vec_alice = np.ones(512, dtype=np.float32)
    vec_bob = np.full(512, -1.0, dtype=np.float32)

    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=1", (vec_target.tobytes(),))
    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=2", (vec_alice.tobytes(),))
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=2")
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Alice', 1, 0, 1)"
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.8, 3)",
        (photo2, vec_alice.tobytes()),
    )
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (4, 'Bob', 1, 0, 1)"
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.7, 4)",
        (photo2, vec_bob.tobytes()),
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/suggestions?top_k=5")
    assert resp.status_code == 200
    suggestions = resp.json()["suggestions"]
    labels = [s["person_label"] for s in suggestions]
    assert labels.count("Alice") == 1
    assert len({l.lower() for l in labels}) == len(labels)


def test_cluster_suggestions_include_pending_labels_for_diversity(client_with_clusters):
    import db
    import numpy as np

    c = db.get_db()
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]
    vec_target = np.ones(512, dtype=np.float32)
    vec_emily = np.full(512, 0.6, dtype=np.float32)
    vec_emmitt = np.ones(512, dtype=np.float32)
    vec_levi = np.full(512, 0.2, dtype=np.float32)

    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=1", (vec_target.tobytes(),))
    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=2", (vec_emily.tobytes(),))
    c.execute("UPDATE clusters SET person_label='Emily', approved=1 WHERE cluster_id=2")
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Emmitt', 1, 0, 0)"
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.8, 3)",
        (photo2, vec_emmitt.tobytes()),
    )
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (4, 'Levi', 1, 0, 0)"
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.7, 4)",
        (photo2, vec_levi.tobytes()),
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/suggestions?top_k=3")
    assert resp.status_code == 200
    data = resp.json()
    labels = [s["person_label"] for s in data["suggestions"]]

    assert data["source_pool"] == "approved_plus_labeled"
    assert "Emily" in labels
    assert any(name in labels for name in ("Emmitt", "Levi"))


def test_cluster_suggestions_mark_person_usable_only_after_min_clean_approved_faces(
    client_with_clusters,
    monkeypatch,
):
    import db
    import numpy as np

    monkeypatch.setattr("config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES", 3)
    monkeypatch.setattr("config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE", 0.75)

    c = db.get_db()
    photo2 = c.execute("SELECT photo_id FROM photos WHERE filename='b.jpg'").fetchone()[0]
    vec_target = np.ones(512, dtype=np.float32)
    vec_match = np.ones(512, dtype=np.float32)

    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=1", (vec_target.tobytes(),))
    c.execute("UPDATE faces SET embedding=?, detection_score=0.96 WHERE cluster_id=2", (vec_match.tobytes(),))
    c.execute("UPDATE clusters SET person_label='Alice', approved=1 WHERE cluster_id=2")
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Alice', 1, 0, 1)"
    )
    c.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (4, 'Alice', 1, 0, 1)"
    )
    c.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id)
           VALUES (?, '[]', ?, 0.95, 3)""",
        (photo2, vec_match.tobytes()),
    )
    c.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id)
           VALUES (?, '[]', ?, 0.94, 4)""",
        (photo2, vec_match.tobytes()),
    )
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/suggestions")
    assert resp.status_code == 200
    top = resp.json()["suggestions"][0]
    assert top["person_label"] == "Alice"
    assert top["usable_label"] is True
    assert top["clean_approved_faces"] == 3

    c = db.get_db()
    c.execute("DELETE FROM faces WHERE cluster_id=4")
    c.execute("DELETE FROM clusters WHERE cluster_id=4")
    c.commit()
    c.close()

    resp = client_with_clusters.get("/api/clusters/1/suggestions")
    assert resp.status_code == 200
    top = resp.json()["suggestions"][0]
    assert top["person_label"] == "Alice"
    assert top["usable_label"] is False
    assert top["clean_approved_faces"] == 2
    assert top["source_approved"] is False
    assert top["recommended"] is False


def test_cluster_suggestions_can_use_persisted_person_memory(client_with_clusters):
    import db

    c = db.get_db()
    vec_target = np.ones(512, dtype=np.float32)
    c.execute("UPDATE faces SET embedding=? WHERE cluster_id=1", (vec_target.tobytes(),))
    c.commit()
    c.close()

    person_memory.save_person_memory([
        {
            "person_label": "Alice",
            "centroid": vec_target,
            "support_faces": 12,
            "support_clusters": 3,
            "clean_approved_faces": 12,
            "usable_label": True,
            "updated_at": "2026-03-26T00:00:00+00:00",
        }
    ])

    resp = client_with_clusters.get("/api/clusters/1/suggestions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source_pool"] in {"memory_only", "memory_plus_labeled"}
    assert data["suggestions"][0]["person_label"] == "Alice"
    assert data["suggestions"][0]["prototype_source"] == "memory"
    assert data["suggestions"][0]["usable_label"] is True


def test_accept_cluster_suggestion_sets_label(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/1/accept-suggestion",
        json={"person_label": "Alice"}
    )
    assert resp.status_code == 200
    clusters = client_with_clusters.get("/api/clusters").json()
    cluster1 = next(c for c in clusters if c["cluster_id"] == 1)
    assert cluster1["person_label"] == "Alice"
    assert cluster1["approved"] == 0


def test_merge_clusters(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/merge",
        json={"source_cluster_id": 2, "target_cluster_id": 1}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["merged_into"] == 1
    assert data["new_face_count"] == 3  # 2 from cluster 1 + 1 from cluster 2

    # Cluster 2 should be deleted
    clusters = client_with_clusters.get("/api/clusters").json()
    cluster_ids = [c["cluster_id"] for c in clusters]
    assert 2 not in cluster_ids
    assert 1 in cluster_ids


def test_merge_target_not_found(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/merge",
        json={"source_cluster_id": 1, "target_cluster_id": 999}
    )
    assert resp.status_code == 404


def test_merge_source_not_found(client_with_clusters):
    resp = client_with_clusters.post(
        "/api/clusters/merge",
        json={"source_cluster_id": 999, "target_cluster_id": 1}
    )
    assert resp.status_code == 404
