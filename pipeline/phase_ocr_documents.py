"""
Phase: OCR Documents

Runs Tesseract only on photos flagged is_document=1 that haven't been OCR'd yet.
Also copies flagged photos into DOCUMENTS_DIR organized by year for easy review.

Triggered manually or via POST /api/pipeline/run/ocr — completely separate from
the main processing loop so normal photos pay zero OCR cost.
"""

import shutil
import time
from pathlib import Path

import config
import db
from ocr_utils import extract_ocr_text, tesseract_available
from pipeline import shutdown
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem
from PIL import Image

log = get_logger("phase_ocr_documents")


def run_ocr_documents() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase: OCR Documents")
    log.info("=" * 60)

    if not tesseract_available():
        msg = "Tesseract not available — cannot run OCR phase"
        log.error(msg)
        db.mark_phase_error("ocr", msg)
        emit_phase_postmortem(log, "ocr", phase_start, False, error=msg)
        return False

    db.mark_phase_running("ocr")
    config.DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)

    conn = db.get_db()
    rows = conn.execute(
        "SELECT photo_id, source_path FROM photos WHERE is_document=1 AND ocr_text IS NULL"
    ).fetchall()
    conn.close()

    total = len(rows)
    log.info("Found %d document photo(s) pending OCR", total)
    db.update_phase_progress("ocr", 0, total)

    if total == 0:
        db.mark_phase_complete("ocr")
        emit_phase_postmortem(log, "ocr", phase_start, True,
                              metrics={"documents_found": 0, "processed": 0})
        return True

    processed = 0
    errors = 0

    for photo_id, source_path in rows:
        if shutdown.is_requested():
            log.info("Graceful shutdown after %d documents", processed)
            break

        photo_path = config.ORIGINALS_DIR / source_path
        if not photo_path.exists():
            log.warning("Source not found: %s", photo_path)
            errors += 1
            continue

        try:
            pil_img = Image.open(photo_path).convert("RGB")
            ocr_text = extract_ocr_text(pil_img)

            conn = db.get_db()
            conn.execute(
                "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=?",
                (ocr_text, db._now(), photo_id),
            )
            conn.commit()
            conn.close()

            # Copy to DOCUMENTS_DIR/YYYY/ for easy browsing
            year = _extract_year(source_path)
            dest_dir = config.DOCUMENTS_DIR / year
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / photo_path.name
            if not dest.exists():
                shutil.copy2(photo_path, dest)

            processed += 1
            chars = len(ocr_text.strip()) if ocr_text else 0
            log.info("[%d/%d] %s — %d chars extracted", processed, total,
                     photo_path.name, chars)

        except Exception as e:
            log.error("OCR failed for %s: %s", source_path, e, exc_info=True)
            errors += 1

        if processed % 10 == 0:
            db.update_phase_progress("ocr", processed, total)

    db.update_phase_progress("ocr", processed, total)

    if shutdown.is_requested():
        db.mark_phase_error("ocr", f"Stopped by user after {processed} documents")
    else:
        db.mark_phase_complete("ocr")

    emit_phase_postmortem(log, "ocr", phase_start, not shutdown.is_requested(),
                          metrics={
                              "documents_found": total,
                              "processed": processed,
                              "errors": errors,
                          })
    return not shutdown.is_requested()


def _extract_year(source_path: str) -> str:
    """Best-effort year extraction from path like By-Year/2022/..."""
    parts = Path(source_path).parts
    for part in parts:
        if len(part) == 4 and part.isdigit() and 1900 <= int(part) <= 2100:
            return part
    return "unknown"
