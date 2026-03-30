"""
Phase: OCR Documents

Builds a document-focused OCR pass on top of the main process phase.

- `process` already flags `is_document=1` when CLIP believes an image is text-heavy
- this phase ensures those document photos are copied into `DOCUMENTS_DIR`
- if OCR text is still missing and Tesseract is available, it backfills `ocr_text`

This makes OCR a meaningful first-class pipeline phase for the web UI instead of
only acting as a narrow backfill path.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from PIL import Image

import config
import db
from ocr_utils import extract_ocr_text, tesseract_available
from pipeline import shutdown
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase_ocr_documents")


def run_ocr_documents() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase: OCR Documents")
    log.info("=" * 60)

    db.mark_phase_running("ocr")
    config.DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)

    conn = db.get_db()
    rows = conn.execute(
        """
        SELECT photo_id, source_path, ocr_text
          FROM photos
         WHERE is_document=1
         ORDER BY source_path
        """
    ).fetchall()
    conn.close()

    total = len(rows)
    missing_ocr = sum(1 for row in rows if not _has_text(row["ocr_text"]))
    tesseract_ready = tesseract_available()

    log.info(
        "Found %d document photo(s); %d still need OCR text",
        total,
        missing_ocr,
    )
    db.update_phase_progress("ocr", 0, total)

    if total == 0:
        db.mark_phase_complete("ocr")
        emit_phase_postmortem(
            log,
            "ocr",
            phase_start,
            True,
            metrics={"documents_found": 0, "processed": 0, "copied": 0, "ocr_updated": 0},
        )
        return True

    if missing_ocr and not tesseract_ready:
        log.warning(
            "Tesseract is unavailable. Document files will still be copied to %s, "
            "but missing OCR text cannot be backfilled.",
            config.DOCUMENTS_DIR,
        )

    processed = 0
    copied = 0
    ocr_updated = 0
    missing_source = 0
    errors = 0

    for row in rows:
        if shutdown.is_requested():
            log.info("Graceful shutdown after %d documents", processed)
            break

        photo_id = int(row["photo_id"])
        source_path = str(row["source_path"])
        ocr_text = row["ocr_text"]
        photo_path = config.ORIGINALS_DIR / source_path

        if not photo_path.exists():
            log.warning("Source not found: %s", photo_path)
            missing_source += 1
            errors += 1
            processed += 1
            db.update_phase_progress("ocr", processed, total)
            continue

        try:
            year = _extract_year(source_path)
            dest_dir = config.DOCUMENTS_DIR / year
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / photo_path.name
            if not dest.exists():
                shutil.copy2(photo_path, dest)
                copied += 1

            if not _has_text(ocr_text) and tesseract_ready:
                pil_img = Image.open(photo_path).convert("RGB")
                ocr_text = extract_ocr_text(pil_img)

                conn = db.get_db()
                conn.execute(
                    "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=?",
                    (ocr_text, db._now(), photo_id),
                )
                conn.commit()
                conn.close()
                ocr_updated += 1

            processed += 1
            chars = len((ocr_text or "").strip())
            log.info(
                "[%d/%d] %s copied to documents/%s - %d chars available",
                processed,
                total,
                photo_path.name,
                year,
                chars,
            )
        except Exception as exc:
            log.error("OCR failed for %s: %s", source_path, exc, exc_info=True)
            errors += 1
            processed += 1

        if processed % 10 == 0 or processed == total:
            db.update_phase_progress("ocr", processed, total)

    db.update_phase_progress("ocr", processed, total)

    message = None
    if shutdown.is_requested():
        message = f"Stopped by user after {processed} documents"
        db.mark_phase_error("ocr", message)
        success = False
    elif missing_ocr and not tesseract_ready:
        message = (
            f"Tesseract unavailable; copied documents but {missing_ocr} document(s) still need OCR text"
        )
        db.mark_phase_error("ocr", message)
        success = False
    elif errors:
        message = f"OCR phase completed with {errors} error(s)"
        db.mark_phase_error("ocr", message)
        success = False
    else:
        db.mark_phase_complete("ocr")
        success = True

    emit_phase_postmortem(
        log,
        "ocr",
        phase_start,
        success,
        metrics={
            "documents_found": total,
            "processed": processed,
            "copied": copied,
            "ocr_updated": ocr_updated,
            "missing_source": missing_source,
            "errors": errors,
        },
        error=message,
    )
    return success


def _has_text(value: str | None) -> bool:
    return bool((value or "").strip())


def _extract_year(source_path: str) -> str:
    """Best-effort year extraction from paths like By-Year/2022/..."""
    parts = Path(source_path).parts
    for part in parts:
        if len(part) == 4 and part.isdigit() and 1900 <= int(part) <= 2100:
            return part
    return "unknown"
