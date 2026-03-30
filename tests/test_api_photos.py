"""Tests for GET /api/photos — filtering, pagination, and bounds."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_db):
    from api.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_with_photos(tmp_db):
    """Client with a small set of photos seeded into the DB."""
    import db
    c = db.get_db()
    photos = [
        ("img/2020-01-01.jpg", "2020-01-01.jpg", "2020-01-01T12:00:00+00:00", "exif_original", "2020/2020-01/2020-01-01.jpg"),
        ("img/2021-06-15.jpg", "2021-06-15.jpg", "2021-06-15T08:00:00+00:00", "exif_original", "2021/2021-06/2021-06-15.jpg"),
        ("img/undated.jpg",    "undated.jpg",    None,                         None,            "undated/undated.jpg"),
        ("img/no_dest.jpg",    "no_dest.jpg",    None,                         None,            None),  # unprocessed, no dest
    ]
    for source_path, filename, exif_date, date_source, dest_path in photos:
        c.execute(
            """INSERT INTO photos (source_path, filename, exif_date, date_source, dest_path, copy_verified)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_path, filename, exif_date, date_source, dest_path, 1 if dest_path else 0)
        )
    c.commit()
    c.close()

    from api.main import app
    with TestClient(app) as tc:
        yield tc


def test_photos_empty_db(client):
    resp = client.get("/api/photos")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["photos"] == []


def test_photos_returns_all(client_with_photos):
    resp = client_with_photos.get("/api/photos")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 4


def test_photos_pagination_defaults(client_with_photos):
    resp = client_with_photos.get("/api/photos")
    data = resp.json()
    assert data["page"] == 1
    assert data["per_page"] == 60


def test_photos_pagination_page_clamp(client_with_photos):
    """Negative page is clamped to 1."""
    resp = client_with_photos.get("/api/photos?page=-999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["page"] == 1


def test_photos_per_page_clamp(client_with_photos):
    """per_page > 500 is clamped to 500."""
    resp = client_with_photos.get("/api/photos?per_page=9999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["per_page"] == 500


def test_photos_per_page_zero_clamp(client_with_photos):
    """per_page=0 is clamped to 1."""
    resp = client_with_photos.get("/api/photos?per_page=0")
    assert resp.status_code == 200
    data = resp.json()
    assert data["per_page"] == 1


def test_photos_undated_filter_excludes_unprocessed(client_with_photos):
    """undated=true should return photos in the undated dir, NOT unprocessed ones with no dest_path."""
    resp = client_with_photos.get("/api/photos?undated=true")
    assert resp.status_code == 200
    data = resp.json()
    filenames = [p["filename"] for p in data["photos"]]
    assert "undated.jpg" in filenames
    # "no_dest.jpg" has null exif_date AND null dest_path — should NOT appear
    assert "no_dest.jpg" not in filenames


def test_photos_year_filter(client_with_photos):
    resp = client_with_photos.get("/api/photos?year=2020")
    data = resp.json()
    assert data["total"] == 1
    assert data["photos"][0]["filename"] == "2020-01-01.jpg"


def test_photos_year_month_filter(client_with_photos):
    resp = client_with_photos.get("/api/photos?year=2021&month=06")
    data = resp.json()
    assert data["total"] == 1
    assert data["photos"][0]["filename"] == "2021-06-15.jpg"


def test_get_photo_by_id(client_with_photos):
    # Get the ID of a known photo
    resp = client_with_photos.get("/api/photos")
    photo_id = resp.json()["photos"][0]["photo_id"]
    detail = client_with_photos.get(f"/api/photos/{photo_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert "faces" in data
    assert "detections" in data
    assert "tags" in data
    assert "preview_url" in data


def test_get_photo_excludes_clip_embedding_blob(client_with_photos):
    import db
    import numpy as np

    conn = db.get_db()
    photo_id = conn.execute("SELECT photo_id FROM photos WHERE filename='2020-01-01.jpg'").fetchone()[0]
    conn.execute(
        "UPDATE photos SET clip_embedding=? WHERE photo_id=?",
        (np.arange(4, dtype=np.float32).tobytes(), photo_id),
    )
    conn.commit()
    conn.close()

    detail = client_with_photos.get(f"/api/photos/{photo_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert "clip_embedding" not in data
    assert data["photo_id"] == photo_id


def test_get_photo_normalizes_legacy_crop_urls(client_with_photos):
    import db
    import numpy as np

    conn = db.get_db()
    photo_id = conn.execute("SELECT photo_id FROM photos WHERE filename='2020-01-01.jpg'").fetchone()[0]
    emb = np.ones(4, dtype=np.float32).tobytes()
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (1, 'Alice', 1, 0, 1)"
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.9, 1, ?)""",
        (photo_id, emb, "/local/rimrock/photos/crops/face_a_0.jpg"),
    )
    conn.execute(
        """INSERT INTO detections (photo_id, model, tag, confidence, bbox_json, crop_path, approved, created_at)
           VALUES (?, 'clip', 'dog', 0.8, '[]', ?, 1, '2026-01-01T00:00:00+00:00')""",
        (photo_id, "crops/dog_0.jpg"),
    )
    conn.commit()
    conn.close()

    detail = client_with_photos.get(f"/api/photos/{photo_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert data["faces"][0]["crop_url"] == "/crops/face_a_0.jpg"
    assert data["detections"][0]["crop_url"] == "/crops/dog_0.jpg"


def test_get_photo_404(client):
    resp = client.get("/api/photos/99999")
    assert resp.status_code == 404


def test_static_file_routes_block_path_traversal(client, tmp_path):
    import config

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (config.OUTPUT_DIR / "safe.txt").write_text("ok", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("nope", encoding="utf-8")

    ok = client.get("/organized/safe.txt")
    blocked = client.get("/organized/%2E%2E/secret.txt")

    assert ok.status_code == 200
    assert blocked.status_code == 404


def test_photo_filters_return_distinct_people_and_photo_counts(client_with_photos):
    import db
    import numpy as np

    conn = db.get_db()
    photo_ids = {
        row["filename"]: row["photo_id"]
        for row in conn.execute("SELECT photo_id, filename FROM photos").fetchall()
    }
    emb = np.ones(4, dtype=np.float32).tobytes()

    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (1, 'Alice', 2, 0, 1)"
    )
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (2, 'Alice', 1, 0, 1)"
    )
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (3, 'Bob', 1, 0, 1)"
    )
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (4, 'Noise', 1, 1, 1)"
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.9, 1, 'crops/a1.jpg')""",
        (photo_ids["2020-01-01.jpg"], emb),
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.9, 2, 'crops/a2.jpg')""",
        (photo_ids["2021-06-15.jpg"], emb),
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.9, 3, 'crops/b1.jpg')""",
        (photo_ids["2021-06-15.jpg"], emb),
    )
    conn.execute(
        """INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id, crop_path)
           VALUES (?, '[]', ?, 0.9, 4, 'crops/n1.jpg')""",
        (photo_ids["undated.jpg"], emb),
    )
    conn.execute(
        "INSERT INTO photo_tags (photo_id, tag, source) VALUES (?, 'dog', 'detection')",
        (photo_ids["2020-01-01.jpg"],),
    )
    conn.execute(
        "INSERT INTO photo_tags (photo_id, tag, source) VALUES (?, 'dog', 'detection')",
        (photo_ids["2021-06-15.jpg"],),
    )
    conn.execute(
        "INSERT INTO photo_tags (photo_id, tag, source) VALUES (?, 'cat', 'detection')",
        (photo_ids["undated.jpg"],),
    )
    conn.commit()
    conn.close()

    resp = client_with_photos.get("/api/photo-filters")
    assert resp.status_code == 200
    data = resp.json()

    assert data["people"] == [
        {"person": "Alice", "photo_count": 2},
        {"person": "Bob", "photo_count": 1},
    ]
    assert data["tags"] == [
        {"tag": "cat", "photo_count": 1},
        {"tag": "dog", "photo_count": 2},
    ]
