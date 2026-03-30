"""NVIDIA Burst Intelligence routes.

Only registered when NVIDIA_BURST_ENABLED is True.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

import config
import db
import nvidia_burst

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/burst/status")
def burst_status():
    """Return burst feature status and today's usage."""
    return nvidia_burst.get_usage_summary()


@router.post("/burst/cache/prune")
def burst_cache_prune():
    """Delete expired cache entries."""
    deleted = db.burst_cache_prune()
    return {"pruned": deleted}


@router.post("/photos/{photo_id}/caption")
def caption_photo(photo_id: int):
    """Generate a text caption for a photo via NVIDIA vision model."""
    if not nvidia_burst.is_enabled():
        raise HTTPException(503, "NVIDIA Burst is not enabled")

    conn = db.get_db()
    row = conn.execute(
        "SELECT source_path, dest_path FROM photos WHERE photo_id=?",
        (photo_id,),
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "Photo not found")

    # Prefer the organized copy, fall back to original
    image_path = None
    if row["dest_path"]:
        candidate = config.OUTPUT_DIR / row["dest_path"]
        if candidate.exists():
            image_path = candidate
    if image_path is None and row["source_path"]:
        candidate = config.ORIGINALS_DIR / row["source_path"]
        if candidate.exists():
            image_path = candidate

    if image_path is None:
        raise HTTPException(404, "Photo file not found on disk")

    try:
        caption = nvidia_burst.caption_image(image_path)
    except nvidia_burst.BudgetExceededError as e:
        raise HTTPException(429, str(e))

    if caption is None:
        raise HTTPException(502, "Caption generation failed")

    return {
        "photo_id": photo_id,
        "caption": caption,
        "source": "nvidia_burst",
    }
