"""Tests for GET /api/status endpoint."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_db):
    from api.main import app
    with TestClient(app) as c:
        yield c


def test_status_returns_all_phases(client):
    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "phases" in data
    assert "counts" in data
    phases = [p["phase"] for p in data["phases"]]
    assert phases == ["preflight", "pull", "process", "cluster", "organize", "tag", "push", "verify"]


def test_status_all_pending_initially(client):
    resp = client.get("/api/status")
    data = resp.json()
    for phase in data["phases"]:
        assert phase["status"] == "pending"


def test_status_counts_zero_initially(client):
    resp = client.get("/api/status")
    data = resp.json()
    counts = data["counts"]
    assert counts["total_photos"] == 0
    assert counts["total_faces"] == 0
    assert counts["total_clusters"] == 0
    assert counts["labeled_clusters"] == 0
    assert counts["approved_clusters"] == 0
    assert counts["total_detections"] == 0
    assert counts["photos_organized"] == 0
