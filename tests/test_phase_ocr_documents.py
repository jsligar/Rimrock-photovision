from __future__ import annotations

import importlib
from pathlib import Path

from PIL import Image


def _make_test_image(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (24, 24), color="white").save(path)


def test_run_ocr_documents_copies_document_with_existing_ocr(tmp_path, monkeypatch):
    import db

    monkeypatch.setattr("config.DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.OUTPUT_DIR", tmp_path / "organized")
    monkeypatch.setattr("config.DOCUMENTS_DIR", tmp_path / "documents")
    monkeypatch.setattr("config.LOG_PATH", tmp_path / "rimrock_photos.log")
    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", True)

    db.close_thread_db()
    db.init_db()

    from pipeline import shutdown
    import pipeline.phase_ocr_documents as ocr_module

    ocr_module = importlib.reload(ocr_module)
    shutdown.clear()

    photo_rel = "By-Year/2024/invoice.jpg"
    photo_path = (tmp_path / "originals" / photo_rel)
    _make_test_image(photo_path)

    conn = db.get_db()
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, is_document, ocr_text, processed_at)
        VALUES (?, ?, 1, ?, ?)
        """,
        (photo_rel, "invoice.jpg", "Already indexed", db._now()),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ocr_module, "tesseract_available", lambda: False)

    assert ocr_module.run_ocr_documents() is True

    copied_path = tmp_path / "documents" / "2024" / "invoice.jpg"
    assert copied_path.exists()

    conn = db.get_db()
    status = conn.execute(
        "SELECT status FROM pipeline_state WHERE phase='ocr'"
    ).fetchone()["status"]
    conn.close()
    assert status == "complete"


def test_run_ocr_documents_backfills_missing_text_and_copies_document(tmp_path, monkeypatch):
    import db

    monkeypatch.setattr("config.DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.OUTPUT_DIR", tmp_path / "organized")
    monkeypatch.setattr("config.DOCUMENTS_DIR", tmp_path / "documents")
    monkeypatch.setattr("config.LOG_PATH", tmp_path / "rimrock_photos.log")
    monkeypatch.setattr("config.ENABLE_SEARCH_LAYER", True)

    db.close_thread_db()
    db.init_db()

    from pipeline import shutdown
    import pipeline.phase_ocr_documents as ocr_module

    ocr_module = importlib.reload(ocr_module)
    shutdown.clear()

    photo_rel = "Docs/2025/statement.jpg"
    photo_path = (tmp_path / "originals" / photo_rel)
    _make_test_image(photo_path)

    conn = db.get_db()
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, is_document, ocr_text, processed_at)
        VALUES (?, ?, 1, NULL, ?)
        """,
        (photo_rel, "statement.jpg", db._now()),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ocr_module, "tesseract_available", lambda: True)
    monkeypatch.setattr(
        ocr_module,
        "extract_ocr_text",
        lambda image: "Invoice 8472",
    )

    assert ocr_module.run_ocr_documents() is True

    copied_path = tmp_path / "documents" / "2025" / "statement.jpg"
    assert copied_path.exists()

    conn = db.get_db()
    row = conn.execute(
        "SELECT ocr_text, ocr_extracted_at FROM photos WHERE source_path=?",
        (photo_rel,),
    ).fetchone()
    status = conn.execute(
        "SELECT status FROM pipeline_state WHERE phase='ocr'"
    ).fetchone()["status"]
    conn.close()

    assert row["ocr_text"] == "Invoice 8472"
    assert row["ocr_extracted_at"] is not None
    assert status == "complete"
