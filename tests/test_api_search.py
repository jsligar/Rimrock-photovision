"""Tests for the feature-flagged search layer."""

from __future__ import annotations

import importlib
import sqlite3

import numpy as np
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def search_client(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("config.DB_PATH", db_path)
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.OUTPUT_DIR", tmp_path / "organized")
    monkeypatch.setattr("config.UNDATED_DIR", "undated")
    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", True)

    import db as db_module

    db_module.init_db()

    import api.main as main_module

    main_module = importlib.reload(main_module)
    with TestClient(main_module.app) as client:
        yield client


def _seed_search_fixture():
    import db

    conn = db.get_db()
    conn.execute(
        """INSERT INTO photos
           (photo_id, source_path, filename, exif_date, existing_people, processed_at, clip_embedding)
           VALUES (1, 'img/alice.jpg', 'alice.jpg', '2025-01-02T00:00:00+00:00', 'Alice', '2026-01-01T00:00:00+00:00', ?)""",
        (np.ones(4, dtype=np.float32).tobytes(),),
    )
    conn.execute(
        """INSERT INTO photos
           (photo_id, source_path, filename, exif_date, existing_people, processed_at, clip_embedding)
           VALUES (2, 'img/dog_walk.jpg', 'dog_walk.jpg', '2025-01-03T00:00:00+00:00', 'Bob', '2026-01-01T00:00:00+00:00', ?)""",
        (np.full(4, 0.5, dtype=np.float32).tobytes(),),
    )
    conn.execute("INSERT INTO photo_tags (photo_id, tag, source) VALUES (2, 'dog', 'clip')")
    conn.execute("INSERT INTO photo_tags (photo_id, tag, source) VALUES (2, 'dog', 'yolo')")
    conn.execute(
        "INSERT INTO clusters (cluster_id, person_label, face_count, is_noise, approved) VALUES (1, 'Alice', 1, 0, 1)"
    )
    conn.execute(
        "INSERT INTO faces (photo_id, bbox_json, embedding, detection_score, cluster_id) VALUES (1, '[]', ?, 0.9, 1)",
        (np.ones(4, dtype=np.float32).tobytes(),),
    )
    conn.commit()

    db.backfill_fts(conn)
    conn.close()


def test_init_db_backfills_fts_when_search_layer_enabled_later(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("config.DB_PATH", db_path)
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.OUTPUT_DIR", tmp_path / "organized")
    monkeypatch.setattr("config.UNDATED_DIR", "undated")

    import db as db_module

    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", False)
    db_module.init_db()

    conn = db_module.get_db()
    conn.execute(
        "INSERT INTO photos (source_path, filename, existing_people) VALUES ('a/dog.jpg', 'dog.jpg', 'Alice')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", True)
    db_module.init_db()

    conn = db_module.get_db()
    fts_count = conn.execute("SELECT COUNT(*) FROM photos_fts").fetchone()[0]
    conn.close()

    assert fts_count == 1


def test_search_schema_includes_ocr_columns(search_client):
    import db

    conn = db.get_db()
    photo_cols = {row[1] for row in conn.execute("PRAGMA table_info(photos)").fetchall()}
    fts_cols = [row[1] for row in conn.execute("PRAGMA table_info(photos_fts)").fetchall()]
    conn.close()

    assert {"clip_embedding", "ocr_text", "ocr_extracted_at"} <= photo_cols
    assert fts_cols == ["filename", "existing_people", "ocr_text"]


def test_init_db_repairs_stale_fts_before_ocr_updates(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("config.DB_PATH", db_path)
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.OUTPUT_DIR", tmp_path / "organized")
    monkeypatch.setattr("config.UNDATED_DIR", "undated")
    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", True)

    import db as db_module

    db_module.init_db()

    conn = db_module.get_db()
    conn.execute(
        """
        INSERT INTO photos (photo_id, source_path, filename, existing_people, processed_at)
        VALUES (1, 'img/invoice.jpg', 'invoice.jpg', 'Alice', '2026-01-01T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.execute("DELETE FROM photos_fts")
    conn.commit()

    with pytest.raises(sqlite3.DatabaseError, match="database disk image is malformed"):
        conn.execute(
            "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=1",
            ("invoice 8472", "2026-03-25T00:00:00+00:00"),
        )
    conn.close()

    db_module.init_db()

    conn = db_module.get_db()
    conn.execute(
        "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=1",
        ("invoice 8472", "2026-03-25T00:00:00+00:00"),
    )
    conn.commit()

    matches = conn.execute(
        "SELECT rowid FROM photos_fts WHERE photos_fts MATCH ?",
        ("invoice",),
    ).fetchall()
    conn.close()

    assert [row[0] for row in matches] == [1]


def test_semantic_search_hybrid_and_saved_searches(search_client, monkeypatch):
    from api.routes import search as search_module

    _seed_search_fixture()

    monkeypatch.setattr(search_module, "_clip_text_search", lambda query, conn, top_k: [(2, 0.9), (1, 0.2)])

    resp = search_client.get("/api/photos/search", params={"q": "dog", "mode": "hybrid"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"][0]["photo_id"] == 2

    dog_facet = next(item for item in data["facets"]["tags"] if item["tag"] == "dog")
    assert dog_facet["count"] == 1
    assert any(item["person"] == "Alice" for item in data["facets"]["people"])
    assert any(item["year"] == "2025" for item in data["facets"]["years"])

    create = search_client.post("/api/searches", json={"name": "  Dogs  ", "query": {"q": "dog"}})
    assert create.status_code == 200
    assert create.json()["name"] == "Dogs"

    listed = search_client.get("/api/searches")
    assert listed.status_code == 200
    assert listed.json()[0]["name"] == "Dogs"

    delete = search_client.delete(f"/api/searches/{create.json()['search_id']}")
    assert delete.status_code == 200


def test_semantic_search_hybrid_falls_back_when_clip_errors(search_client, monkeypatch):
    from api.routes import search as search_module

    _seed_search_fixture()

    def _boom(query, conn, top_k):
        raise RuntimeError("clip unavailable")

    monkeypatch.setattr(search_module, "_clip_text_search", _boom)

    resp = search_client.get("/api/photos/search", params={"q": "dog", "mode": "hybrid"})
    assert resp.status_code == 200
    assert resp.json()["total"] >= 1


def test_semantic_search_exact_ocr_match_beats_loose_clip_match(search_client, monkeypatch):
    from api.routes import search as search_module
    import db

    _seed_search_fixture()

    conn = db.get_db()
    conn.execute(
        "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=1",
        ("invoice 8472 paid in full", "2026-03-25T00:00:00+00:00"),
    )
    conn.execute(
        "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=2",
        ("park walk", "2026-03-25T00:00:00+00:00"),
    )
    db.backfill_fts(conn)
    conn.close()

    monkeypatch.setattr(search_module, "_clip_text_search", lambda query, conn, top_k: [(2, 0.95), (1, 0.7)])

    resp = search_client.get("/api/photos/search", params={"q": "invoice 8472", "mode": "hybrid"})
    assert resp.status_code == 200
    assert resp.json()["results"][0]["photo_id"] == 1


def test_semantic_search_clip_mode_returns_503_when_clip_errors(search_client, monkeypatch):
    from api.routes import search as search_module

    _seed_search_fixture()

    def _boom(query, conn, top_k):
        raise RuntimeError("clip unavailable")

    monkeypatch.setattr(search_module, "_clip_text_search", _boom)

    resp = search_client.get("/api/photos/search", params={"q": "dog", "mode": "clip"})
    assert resp.status_code == 503


def test_semantic_search_rejects_invalid_mode(search_client):
    resp = search_client.get("/api/photos/search", params={"q": "dog", "mode": "nope"})
    assert resp.status_code == 400


def test_semantic_search_rejects_blank_query(search_client):
    resp = search_client.get("/api/photos/search", params={"q": "   ", "mode": "hybrid"})
    assert resp.status_code == 400


def test_semantic_search_requires_nonempty_saved_search_query(search_client):
    resp = search_client.post("/api/searches", json={"name": "Dogs", "query": {}})
    assert resp.status_code == 400


def test_saved_search_requires_nonempty_query_text(search_client):
    resp = search_client.post("/api/searches", json={"name": "Dogs", "query": {"q": "   "}})
    assert resp.status_code == 400


def test_browse_photos_q_handles_punctuation_with_search_layer(search_client):
    _seed_search_fixture()

    resp = search_client.get("/api/photos", params={"q": 'dog!!! "walk"'})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["photos"][0]["photo_id"] == 2
