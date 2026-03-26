"""OCR helpers for the Phase 3 search layer."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageOps

import config

log = logging.getLogger(__name__)

_SPACE_RE = re.compile(r"\s+")
_RESAMPLING = getattr(Image, "Resampling", Image)


def resolve_tesseract_binary() -> str | None:
    """Return the tesseract binary path if present."""
    return shutil.which("tesseract")


def tesseract_available() -> bool:
    return bool(resolve_tesseract_binary())


def normalize_ocr_text(text: str | None) -> str | None:
    """Collapse OCR whitespace/noise into a compact searchable string."""
    if not text:
        return None

    cleaned = _SPACE_RE.sub(" ", str(text)).strip()
    if len(cleaned) < config.SEARCH_OCR_MIN_CHARS:
        return None
    return cleaned[: config.SEARCH_OCR_MAX_CHARS]


def prepare_ocr_image(pil_img: Image.Image) -> Image.Image:
    """Shrink and normalize images before handing them to tesseract."""
    img = ImageOps.exif_transpose(pil_img).convert("L")
    max_dim = max(img.size)
    target_max_dim = max(64, int(config.SEARCH_OCR_MAX_DIM))
    if max_dim > target_max_dim:
        scale = target_max_dim / float(max_dim)
        resized = (
            max(1, int(round(img.width * scale))),
            max(1, int(round(img.height * scale))),
        )
        img = img.resize(resized, _RESAMPLING.LANCZOS)
    return ImageOps.autocontrast(img)


def extract_ocr_text(pil_img: Image.Image) -> str | None:
    """Extract OCR text from an RGB image using the local tesseract binary."""
    binary = resolve_tesseract_binary()
    if not binary:
        return None

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        prepared = prepare_ocr_image(pil_img)
        prepared.save(tmp_path, format="PNG", optimize=False, compress_level=1)

        proc = subprocess.run(
            [
                binary,
                str(tmp_path),
                "stdout",
                "--psm",
                str(config.SEARCH_OCR_PSM),
                "-l",
                config.SEARCH_OCR_LANG,
            ],
            capture_output=True,
            text=True,
            timeout=config.SEARCH_OCR_TIMEOUT_SEC,
        )
        if proc.returncode != 0:
            log.debug("OCR failed (%s): %s", proc.returncode, proc.stderr.strip())
            return None
        return normalize_ocr_text(proc.stdout)
    except subprocess.TimeoutExpired:
        log.debug("OCR timed out after %ss", config.SEARCH_OCR_TIMEOUT_SEC)
        return None
    except Exception as exc:
        log.debug("OCR extraction failed: %s", exc)
        return None
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
