"""Tests for cluster review API endpoints."""

import pytest
from fastapi.testclient import TestClient


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
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.9, 1)",
        (photo1, dummy_emb)
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.8, 1)",
        (photo1, dummy_emb)
    )
    c.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (?, '[]', ?, 0.7, 2)",
        (photo2, dummy_emb)
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


def test_get_cluster_crops(client_with_clusters):
    resp = client_with_clusters.get("/api/clusters/1/crops")
    assert resp.status_code == 200
    crops = resp.json()
    assert len(crops) == 2


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
