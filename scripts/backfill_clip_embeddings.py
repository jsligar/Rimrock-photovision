"""Backfill CLIP image embeddings for already-processed photos.

Run after enabling ENABLE_SEARCH_LAYER on a database that was processed
before the search layer existed. Loads CLIP once, iterates photos with
clip_embedding IS NULL, encodes each image, and stores the 512-dim vector.

Usage:
    ENABLE_SEARCH_LAYER=1 python -m scripts.backfill_clip_embeddings
"""

from __future__ import annotations

import logging
import signal
import time
from pathlib import Path

import numpy as np
from PIL import Image

import config
import db
from clip_compat import ensure_pkg_resources_packaging

log = logging.getLogger(__name__)
_shutdown = False
_JOB_NAME = "clip_backfill"
_JOB_DETAIL = "Search indexing (CLIP)"


def _handle_signal(sig, frame):
    del sig, frame

    global _shutdown
    log.info("Shutdown requested - finishing current batch")
    _shutdown = True


def _load_image(photo_path: Path) -> Image.Image | None:
    """Load an image as RGB, including HEIC support when available."""
    try:
        if photo_path.suffix.lower() == ".heic":
            from pillow_heif import register_heif_opener

            register_heif_opener()
        with Image.open(photo_path) as img:
            return img.convert("RGB")
    except Exception as exc:
        log.warning("Unable to load %s: %s", photo_path, exc)
        return None


def _clear_cuda_cache(torch) -> None:
    if str(config.CLIP_DEVICE).startswith("cuda") and torch.cuda.is_available():
        torch.cuda.empty_cache()


def backfill_clip_embeddings(batch_size: int = 50) -> int:
    if not config.ENABLE_SEARCH_LAYER:
        log.error("ENABLE_SEARCH_LAYER is not set - nothing to do")
        db.mark_background_job_error(_JOB_NAME, "ENABLE_SEARCH_LAYER is not set", detail=_JOB_DETAIL)
        return 0

    import torch
    import torch.nn.functional as F

    ensure_pkg_resources_packaging()
    import clip as openai_clip

    model, preprocess = openai_clip.load(config.CLIP_MODEL, device=config.CLIP_DEVICE)
    model.eval()
    log.info("CLIP %s loaded on %s", config.CLIP_MODEL, config.CLIP_DEVICE)

    conn = db.get_db()
    total = 0
    filled = 0
    skipped_missing = 0
    skipped_unreadable = 0
    t0 = time.time()
    try:
        rows = conn.execute(
            """SELECT photo_id, source_path FROM photos
               WHERE clip_embedding IS NULL AND processed_at IS NOT NULL"""
        ).fetchall()
        total = len(rows)
        log.info("%d photos need CLIP backfill", total)
        db.mark_background_job_running(_JOB_NAME, total=total, detail=_JOB_DETAIL)

        if total == 0:
            db.mark_background_job_complete(_JOB_NAME, current=0, total=0, detail=_JOB_DETAIL)
            return 0

        for row in rows:
            if _shutdown:
                break

            photo_id = row["photo_id"]
            img_path = config.ORIGINALS_DIR / row["source_path"]

            if not img_path.exists():
                log.warning("Missing: %s", img_path)
                skipped_missing += 1
                continue

            pil_img = _load_image(img_path)
            if pil_img is None:
                skipped_unreadable += 1
                continue

            try:
                image_input = preprocess(pil_img).unsqueeze(0).to(config.CLIP_DEVICE)

                with torch.no_grad():
                    embedding = model.encode_image(image_input).float()
                    embedding = F.normalize(embedding, dim=-1).squeeze(0)

                emb_bytes = embedding.cpu().numpy().astype(np.float32).tobytes()
                conn.execute(
                    "UPDATE photos SET clip_embedding=? WHERE photo_id=?",
                    (emb_bytes, photo_id),
                )
                filled += 1

                if filled % batch_size == 0:
                    conn.commit()
                    db.update_background_job_progress(_JOB_NAME, filled, total=total, detail=_JOB_DETAIL)
                    _clear_cuda_cache(torch)

                if filled % 100 == 0:
                    elapsed = time.time() - t0
                    rate = filled / elapsed if elapsed > 0 else 0
                    log.info("Progress: %d/%d (%.1f/s)", filled, total, rate)
            except Exception as exc:
                log.warning("Failed photo_id=%d: %s", photo_id, exc)

        conn.commit()
        db.update_background_job_progress(_JOB_NAME, filled, total=total, detail=_JOB_DETAIL)
    except Exception as exc:
        db.mark_background_job_error(_JOB_NAME, str(exc), detail=_JOB_DETAIL)
        raise
    finally:
        conn.close()

    elapsed = time.time() - t0
    if _shutdown:
        db.mark_background_job_error(
            _JOB_NAME,
            f"Interrupted at {filled}/{total}",
            detail=_JOB_DETAIL,
        )
    else:
        db.mark_background_job_complete(
            _JOB_NAME,
            current=filled,
            total=total,
            detail=_JOB_DETAIL,
        )
    log.info(
        "Backfill complete: %d/%d in %.1fs (missing=%d, unreadable=%d)",
        filled,
        total,
        elapsed,
        skipped_missing,
        skipped_unreadable,
    )
    return filled


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    db.init_db()
    backfill_clip_embeddings()
