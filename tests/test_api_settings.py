"""Tests for settings routes, including destructive DB reset."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_db):
    from api.main import app
    with TestClient(app) as c:
        yield c


def test_clear_db_resets_counts_and_phase_state(client):
    import db

    conn = db.get_db()
    conn.execute(
        "INSERT INTO photos (source_path, filename, exif_date) VALUES ('a/b.jpg', 'b.jpg', '2025-01-01T00:00:00+00:00')"
    )
    conn.commit()
    photo_id = conn.execute("SELECT photo_id FROM photos WHERE source_path='a/b.jpg'").fetchone()[0]
    conn.execute(
        "INSERT INTO detections (photo_id, model, tag, confidence, created_at) VALUES (?, 'clip', 'dog', 0.5, '2026-01-01T00:00:00+00:00')",
        (photo_id,),
    )
    conn.execute("UPDATE pipeline_state SET status='complete' WHERE phase='pull'")
    conn.commit()
    conn.close()

    resp = client.post("/api/settings/clear-db")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "Person memory" in data["note"]

    status = client.get("/api/status").json()
    assert status["counts"]["total_photos"] == 0
    assert status["counts"]["total_faces"] == 0
    assert status["counts"]["total_detections"] == 0
    assert status["counts"]["total_clusters"] == 0
    assert status["counts"]["photos_organized"] == 0
    assert all(p["status"] == "pending" for p in status["phases"])


def test_clear_db_rejected_when_phase_running(client):
    import db

    db.mark_phase_running("process")

    resp = client.post("/api/settings/clear-db")
    assert resp.status_code == 409
    detail = resp.json().get("detail", "")
    assert "running" in detail.lower()


def test_clear_db_rejected_when_background_job_running(client):
    import db

    db.mark_background_job_running("ocr_backfill", total=100, detail="Search indexing (OCR)")

    resp = client.post("/api/settings/clear-db")
    assert resp.status_code == 409
    detail = resp.json().get("detail", "")
    assert "background job" in detail.lower()
    assert "ocr_backfill" in detail


def test_read_env_repairs_literal_newline_corruption(tmp_path, monkeypatch):
    from api.routes import settings as settings_module

    env_path = tmp_path / ".env"
    env_path.write_text(
        "NAS_SOURCE_DIR=/mnt/mycloud/photos\\nLOCAL_BASE=/local/rimrock/photos\\n"
        "YOLO_CONF_THRESHOLD=0.45\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(settings_module, "ENV_PATH", env_path)

    data = settings_module._read_env()
    assert data["NAS_SOURCE_DIR"] == "/mnt/mycloud/photos"
    assert data["LOCAL_BASE"] == "/local/rimrock/photos"
    assert data["YOLO_CONF_THRESHOLD"] == "0.45"


def test_update_settings_accepts_batch_manifest_path(tmp_path, monkeypatch, client):
    from api.routes import settings as settings_module

    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(settings_module, "ENV_PATH", env_path)

    resp = client.post("/api/settings", json={"batch_manifest_path": "batches/next_1500.txt"})
    assert resp.status_code == 200
    text = env_path.read_text(encoding="utf-8")
    assert "BATCH_MANIFEST_PATH=batches/next_1500.txt" in text

    settings_resp = client.get("/api/settings")
    assert settings_resp.status_code == 200
    manifest_path = settings_resp.json()["batch_manifest_path"].replace("\\", "/")
    assert manifest_path.endswith("batches/next_1500.txt")
