"""Backfill OCR text for already-processed photos.

Run after enabling SEARCH_OCR_ENABLED on a database that was processed before
OCR indexing existed. Loads each processed photo without OCR metadata, runs
tesseract against a temporary PNG, and stores the normalized OCR text.

Usage:
    ENABLE_SEARCH_LAYER=1 python -m scripts.backfill_ocr_text
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

import config
import db
from ocr_utils import extract_ocr_text, tesseract_available

log = logging.getLogger(__name__)
_shutdown = False
_JOB_NAME = "ocr_backfill"
_JOB_DETAIL = "Search indexing (OCR)"


def _handle_signal(sig, frame):
    del sig, frame

    global _shutdown
    log.info("Shutdown requested - finishing current batch")
    _shutdown = True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_image(photo_path: Path) -> Image.Image | None:
    try:
        if photo_path.suffix.lower() == ".heic":
            from pillow_heif import register_heif_opener

            register_heif_opener()
        with Image.open(photo_path) as img:
            return img.convert("RGB")
    except Exception as exc:
        log.warning("Unable to load %s: %s", photo_path, exc)
        return None


def backfill_ocr_text(batch_size: int = 25) -> int:
    if not config.ENABLE_SEARCH_LAYER:
        log.error("ENABLE_SEARCH_LAYER is not set - nothing to do")
        db.mark_background_job_error(_JOB_NAME, "ENABLE_SEARCH_LAYER is not set", detail=_JOB_DETAIL)
        return 0
    if not config.SEARCH_OCR_ENABLED:
        log.error("SEARCH_OCR_ENABLED is not set - nothing to do")
        db.mark_background_job_error(_JOB_NAME, "SEARCH_OCR_ENABLED is not set", detail=_JOB_DETAIL)
        return 0
    if not tesseract_available():
        log.error("tesseract is not installed - OCR backfill cannot run")
        db.mark_background_job_error(_JOB_NAME, "tesseract is not installed", detail=_JOB_DETAIL)
        return 0

    conn = db.get_db()
    total = 0
    done = 0
    with_text = 0
    missing = 0
    unreadable = 0
    t0 = time.time()
    try:
        query = """
            SELECT photo_id, source_path FROM photos
             WHERE processed_at IS NOT NULL
               AND ocr_extracted_at IS NULL
        """
        params: list[str] = []
        if config.TEST_YEAR_SCOPE:
            query += " AND source_path LIKE ?"
            params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
            log.info("TEST_YEAR_SCOPE=%s active for OCR backfill.", config.TEST_YEAR_SCOPE)

        rows = conn.execute(query, params).fetchall()
        total = len(rows)
        log.info("%d photos need OCR backfill", total)
        db.mark_background_job_running(_JOB_NAME, total=total, detail=_JOB_DETAIL)

        if total == 0:
            db.mark_background_job_complete(_JOB_NAME, current=0, total=0, detail=_JOB_DETAIL)
            return 0

        for row in rows:
            if _shutdown:
                break

            photo_id = int(row["photo_id"])
            img_path = config.ORIGINALS_DIR / row["source_path"]
            if not img_path.exists():
                log.warning("Missing: %s", img_path)
                missing += 1
                continue

            pil_img = _load_image(img_path)
            if pil_img is None:
                unreadable += 1
                continue

            text = extract_ocr_text(pil_img)
            if text:
                with_text += 1
            conn.execute(
                "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=?",
                (text, _now(), photo_id),
            )
            done += 1

            if done % batch_size == 0:
                conn.commit()
                db.update_background_job_progress(_JOB_NAME, done, total=total, detail=_JOB_DETAIL)

            if done % 100 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                log.info("OCR progress: %d/%d (%.1f/s)", done, total, rate)

        conn.commit()
        db.update_background_job_progress(_JOB_NAME, done, total=total, detail=_JOB_DETAIL)
    except Exception as exc:
        db.mark_background_job_error(_JOB_NAME, str(exc), detail=_JOB_DETAIL)
        raise
    finally:
        conn.close()

    elapsed = time.time() - t0
    if _shutdown:
        db.mark_background_job_error(
            _JOB_NAME,
            f"Interrupted at {done}/{total}",
            detail=_JOB_DETAIL,
        )
    else:
        db.mark_background_job_complete(
            _JOB_NAME,
            current=done,
            total=total,
            detail=_JOB_DETAIL,
        )
    log.info(
        "OCR backfill complete: %d/%d in %.1fs (with_text=%d, missing=%d, unreadable=%d)",
        done,
        total,
        elapsed,
        with_text,
        missing,
        unreadable,
    )
    return done


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    db.init_db()
    backfill_ocr_text()
