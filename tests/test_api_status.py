"""Tests for GET /api/status endpoint."""

from datetime import datetime, timedelta, timezone

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
    assert "background_jobs" in data
    assert "counts" in data
    phases = [p["phase"] for p in data["phases"]]
    assert phases == ["preflight", "pull", "process", "cluster", "organize", "tag", "push", "verify"]
    assert data["background_jobs"] == []


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


def test_status_includes_running_background_jobs(client):
    import db

    db.mark_background_job_running("ocr_backfill", total=50, detail="Search indexing (OCR)")
    db.update_background_job_progress("ocr_backfill", 12, total=50, detail="Search indexing (OCR)")

    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()

    assert data["background_jobs"] == [
        {
            "job_name": "ocr_backfill",
            "status": "running",
            "progress_current": 12,
            "progress_total": 50,
            "started_at": data["background_jobs"][0]["started_at"],
            "updated_at": data["background_jobs"][0]["updated_at"],
            "completed_at": None,
            "error_message": None,
            "detail": "Search indexing (OCR)",
        }
    ]


def test_status_marks_stale_background_jobs_as_error(client):
    import db

    db.mark_background_job_running("ocr_backfill", total=50, detail="Search indexing (OCR)")

    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    conn = db.get_db()
    conn.execute(
        "UPDATE background_jobs SET started_at=?, updated_at=? WHERE job_name='ocr_backfill'",
        (stale_at, stale_at),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["background_jobs"][0]["job_name"] == "ocr_backfill"
    assert data["background_jobs"][0]["status"] == "error"
    assert "stale" in data["background_jobs"][0]["error_message"].lower()
