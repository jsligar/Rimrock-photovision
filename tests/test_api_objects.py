"""Tests for object detection moderation endpoints."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client_with_detection(tmp_db):
    import db

    conn = db.get_db()
    conn.execute(
        "INSERT INTO photos (source_path, filename) VALUES ('img/a.jpg', 'a.jpg')"
    )
    conn.commit()
    photo_id = conn.execute(
        "SELECT photo_id FROM photos WHERE filename='a.jpg'"
    ).fetchone()[0]

    conn.execute(
        """INSERT INTO detections
           (photo_id, model, tag, tag_group, confidence, approved, created_at)
           VALUES (?, 'yolo', 'dog', 'animals', 0.9, 1, '2026-01-01T00:00:00+00:00')""",
        (photo_id,),
    )
    conn.execute(
        "INSERT INTO photo_tags (photo_id, tag, source) VALUES (?, 'dog', 'yolo')",
        (photo_id,),
    )
    conn.commit()
    conn.close()

    from api.main import app

    with TestClient(app) as tc:
        yield tc


def test_reject_detection_removes_photo_tag_when_last_approved(client_with_detection):
    import db

    conn = db.get_db()
    detection_id = conn.execute(
        "SELECT detection_id FROM detections WHERE tag='dog' ORDER BY detection_id LIMIT 1"
    ).fetchone()[0]
    conn.close()

    resp = client_with_detection.post(f"/api/objects/detections/{detection_id}/reject")
    assert resp.status_code == 200

    conn = db.get_db()
    approved = conn.execute(
        "SELECT approved FROM detections WHERE detection_id=?",
        (detection_id,),
    ).fetchone()[0]
    tag_row = conn.execute(
        "SELECT 1 FROM photo_tags WHERE tag='dog' AND source='yolo'"
    ).fetchone()
    conn.close()

    assert approved == 0
    assert tag_row is None


def test_reject_detection_keeps_photo_tag_if_other_approved_exists(client_with_detection):
    import db

    conn = db.get_db()
    photo_id = conn.execute("SELECT photo_id FROM photos LIMIT 1").fetchone()[0]
    conn.execute(
        """INSERT INTO detections
           (photo_id, model, tag, tag_group, confidence, approved, created_at)
           VALUES (?, 'yolo', 'dog', 'animals', 0.8, 1, '2026-01-01T00:00:00+00:00')""",
        (photo_id,),
    )
    conn.commit()
    first_detection_id = conn.execute(
        "SELECT detection_id FROM detections ORDER BY detection_id LIMIT 1"
    ).fetchone()[0]
    conn.close()

    resp = client_with_detection.post(f"/api/objects/detections/{first_detection_id}/reject")
    assert resp.status_code == 200

    conn = db.get_db()
    tag_row = conn.execute(
        "SELECT 1 FROM photo_tags WHERE tag='dog' AND source='yolo'"
    ).fetchone()
    conn.close()
    assert tag_row is not None


def test_approve_detection_recreates_missing_photo_tag(client_with_detection):
    import db

    conn = db.get_db()
    detection_id = conn.execute(
        "SELECT detection_id FROM detections ORDER BY detection_id LIMIT 1"
    ).fetchone()[0]
    conn.execute("UPDATE detections SET approved=0 WHERE detection_id=?", (detection_id,))
    conn.execute("DELETE FROM photo_tags WHERE tag='dog' AND source='yolo'")
    conn.commit()
    conn.close()

    resp = client_with_detection.post(f"/api/objects/detections/{detection_id}/approve")
    assert resp.status_code == 200

    conn = db.get_db()
    approved = conn.execute(
        "SELECT approved FROM detections WHERE detection_id=?",
        (detection_id,),
    ).fetchone()[0]
    tag_row = conn.execute(
        "SELECT 1 FROM photo_tags WHERE tag='dog' AND source='yolo'"
    ).fetchone()
    conn.close()

    assert approved == 1
    assert tag_row is not None
