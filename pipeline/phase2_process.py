"""Phase 2 — Process: combined face detection + semantic tagging in one image loop."""

import json
import os
import signal
import subprocess
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Patch onnxruntime → TensorRT FP16 before insightface loads its sessions.
# Falls back to CPU ORT automatically if TRT build fails.
try:
    from trt_ort_session import patch_onnxruntime
    patch_onnxruntime()
except Exception as _trt_err:
    import logging as _log
    _log.getLogger(__name__).warning("TRT patch unavailable, using CPU ORT: %s", _trt_err)

from batch_scope import (
    BatchScopeError,
    load_batch_scope,
    normalize_relative_path,
    resolve_manifest_media_selection,
)
import config
import db
from clip_compat import ensure_pkg_resources_packaging
from ocr_utils import extract_ocr_text, tesseract_available
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem
from pipeline import shutdown

log = get_logger("phase2_process")
_STAGE_KEYS = (
    "load_image",
    "metadata",
    "photo_insert",
    "faces",
    "yolo",
    "ocr",
    "clip",
    "finalize",
    "photo_total",
)


def _handle_signal(signum, frame):
    log.info("Shutdown signal received. Finishing current photo then exiting...")
    shutdown.request()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_stage_timing_summary(stage_totals: dict[str, float], processed_count: int) -> None:
    if processed_count <= 0:
        return

    averages_ms = {
        key: (stage_totals.get(key, 0.0) / processed_count) * 1000.0 for key in _STAGE_KEYS
    }
    log.info(
        "Avg stage timings at %d photos (ms/photo): total=%.0f load=%.0f metadata=%.0f "
        "faces=%.0f yolo=%.0f ocr=%.0f clip=%.0f db=%.0f",
        processed_count,
        averages_ms["photo_total"],
        averages_ms["load_image"],
        averages_ms["metadata"],
        averages_ms["faces"],
        averages_ms["yolo"],
        averages_ms["ocr"],
        averages_ms["clip"],
        averages_ms["photo_insert"] + averages_ms["finalize"],
    )


def _resize_image(img: np.ndarray) -> np.ndarray:
    """Resize image so longest edge <= MAX_INFERENCE_DIM. In-memory only."""
    h, w = img.shape[:2]
    longest = max(h, w)
    if longest <= config.MAX_INFERENCE_DIM:
        return img
    scale = config.MAX_INFERENCE_DIM / longest
    new_w = int(w * scale)
    new_h = int(h * scale)
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)


def _load_image(photo_path: Path) -> np.ndarray | None:
    """Load image to BGR numpy array. Handles HEIC via pillow-heif."""
    ext = photo_path.suffix.lower()
    if ext == ".heic":
        try:
            from pillow_heif import register_heif_opener
            register_heif_opener()
            pil_img = Image.open(photo_path).convert("RGB")
            img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
            return img
        except Exception as e:
            log.error("HEIC load failed for %s: %s", photo_path, e)
            return None
    else:
        img = cv2.imread(str(photo_path))
        if img is None:
            log.error("cv2.imread failed for %s", photo_path)
            return None
        return img


def _run_exiftool(photo_path: Path) -> dict:
    """Return dict of EXIF/XMP fields via exiftool JSON output."""
    try:
        result = subprocess.run(
            [
                "exiftool", "-j",
                "-EXIF:DateTimeOriginal",
                "-EXIF:DateTimeDigitized",
                "-XMP:PersonInImage",
                "-XMP:Subject",
                "-XMP-mwg-rs:RegionPersonDisplayName",
                str(photo_path),
            ],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if data:
                return data[0]
    except Exception as e:
        log.debug("exiftool error for %s: %s", photo_path, e)
    return {}


def _load_prefilter_skiplist() -> set[str]:
    """Load relative source paths flagged by phase1 prefilter."""
    path = config.PREFILTER_REJECTS_PATH
    if not path.exists():
        return set()

    out: set[str] = set()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                rel_path = line.split("\t", 1)[0].strip()
                if rel_path:
                    out.add(rel_path)
    except Exception as e:
        log.warning("Could not load prefilter skip list %s: %s", path, e)
        return set()
    return out


def _parse_google_sidecar(photo_path: Path) -> tuple[str | None, list[str]]:
    """Parse Google Takeout JSON sidecar for date and people."""
    sidecar = photo_path.parent / (photo_path.name + ".json")
    if not sidecar.exists():
        # Try without extension
        sidecar = photo_path.with_suffix(".json")
    if not sidecar.exists():
        return None, []

    try:
        with open(sidecar, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("photoTakenTime", {}).get("timestamp")
        date_str = None
        if ts:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            date_str = dt.isoformat()
        people = [p["name"] for p in data.get("people", []) if "name" in p]
        return date_str, people
    except Exception as e:
        log.debug("Sidecar parse error for %s: %s", sidecar, e)
        return None, []


def _parse_exif_date(date_str: str) -> str | None:
    """Convert EXIF date 'YYYY:MM:DD HH:MM:SS' to ISO8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def _folder_date_hint(photo_path: Path) -> str | None:
    """Try to parse a date from parent folder name like '2019-07' or '2019'."""
    for part in reversed(photo_path.parts):
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            try:
                dt = datetime.strptime(part, "%Y-%m-%d")
                return dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass
        if len(part) == 7 and part[4] == "-":
            try:
                dt = datetime.strptime(part + "-01", "%Y-%m-%d")
                return dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass
        if len(part) == 4 and part.isdigit() and 1900 <= int(part) <= 2100:
            return f"{part}-01-01T00:00:00+00:00"
    return None


def _resolve_date(photo_path: Path, exif: dict) -> tuple[str | None, str | None]:
    """Return (best_date_iso, date_source)."""
    # 1. EXIF original
    d = _parse_exif_date(exif.get("DateTimeOriginal"))
    if d:
        return d, "exif_original"

    # 2. EXIF digitized
    d = _parse_exif_date(exif.get("DateTimeDigitized"))
    if d:
        return d, "exif_digitized"

    # 3. Google Takeout sidecar
    sidecar_date, _ = _parse_google_sidecar(photo_path)
    if sidecar_date:
        return sidecar_date, "sidecar"

    # 4. Folder hint
    d = _folder_date_hint(photo_path)
    if d:
        return d, "folder_hint"

    # 5. File mtime
    mtime = photo_path.stat().st_mtime
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.isoformat(), "file_mtime"


def _extract_existing_people(exif: dict, photo_path: Path) -> list[str]:
    people = set()
    for field in ("PersonInImage", "RegionPersonDisplayName"):
        val = exif.get(field)
        if isinstance(val, list):
            people.update(val)
        elif isinstance(val, str) and val:
            people.add(val)

    _, sidecar_people = _parse_google_sidecar(photo_path)
    people.update(sidecar_people)
    return list(people)


def _save_face_crop(img: np.ndarray, bbox: list, crop_path: Path) -> None:
    x1, y1, x2, y2 = [int(v) for v in bbox]
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(img.shape[1], x2), min(img.shape[0], y2)
    if x2 <= x1 or y2 <= y1:
        return
    crop = img[y1:y2, x1:x2]
    crop_resized = cv2.resize(crop, (112, 112), interpolation=cv2.INTER_AREA)
    crop_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(crop_path), crop_resized)


def _save_obj_crop(img: np.ndarray, bbox: list, crop_path: Path) -> None:
    x1, y1, x2, y2 = [int(v) for v in bbox]
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(img.shape[1], x2), min(img.shape[0], y2)
    if x2 <= x1 or y2 <= y1:
        return
    crop = img[y1:y2, x1:x2]
    crop_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(crop_path), crop)


def load_models():
    """Load all models once. Returns (face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache)."""
    import torch
    import torch.nn.functional as F
    ensure_pkg_resources_packaging()
    import clip as openai_clip
    import insightface
    from insightface.app import FaceAnalysis
    from ultralytics import YOLO

    import onnxruntime as ort
    log.info("ORT providers: %s", ort.get_available_providers())

    log.info("Loading InsightFace %s...", config.INSIGHTFACE_MODEL)
    face_app = FaceAnalysis(name=config.INSIGHTFACE_MODEL, ctx_id=config.CTX_ID)
    face_app.prepare(ctx_id=config.CTX_ID, det_size=config.DET_SIZE, det_thresh=config.DET_THRESH)

    log.info("Loading YOLO model: %s", config.YOLO_MODEL)
    yolo_model = YOLO(config.YOLO_MODEL)
    if not config.YOLO_MODEL.endswith(".engine"):
        yolo_model.to("cuda")

    log.info("Loading CLIP %s...", config.CLIP_MODEL)
    clip_model, clip_preprocess = openai_clip.load(config.CLIP_MODEL, device=config.CLIP_DEVICE)
    clip_model.eval()

    log.info("Pre-encoding CLIP tag prompts...")
    clip_prompt_cache = {}
    for group, tags in config.SEMANTIC_TAG_GROUPS.items():
        clip_prompt_cache[group] = {}
        for tag_name, prompts in tags.items():
            tokens = openai_clip.tokenize(prompts).to(config.CLIP_DEVICE)
            with torch.no_grad():
                embeddings = clip_model.encode_text(tokens).float()
                embeddings = F.normalize(embeddings, dim=-1)
                mean_emb = embeddings.mean(dim=0)
                mean_emb = F.normalize(mean_emb.unsqueeze(0), dim=-1).squeeze(0)
                clip_prompt_cache[group][tag_name] = mean_emb

    # Pre-encode document detection prompts (free dot-product after image embedding).
    doc_text_features = None
    if config.DOCUMENT_DETECTION_ENABLED:
        tokens = openai_clip.tokenize(config.DOCUMENT_CLIP_PROMPTS).to(config.CLIP_DEVICE)
        with torch.no_grad():
            doc_embs = clip_model.encode_text(tokens).float()
            doc_text_features = F.normalize(doc_embs, dim=-1)  # shape (N, D)

    log.info("All models loaded.")
    return face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache, doc_text_features


def run_process() -> bool:
    import torch
    import torch.nn.functional as F
    ensure_pkg_resources_packaging()
    import clip as openai_clip

    phase_start = time.time()
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)
    else:
        log.info("Running in worker thread; skipping OS signal handler registration.")

    log.info("=" * 60)
    log.info("Phase 2 — PROCESS: face + semantic tagging")
    log.info("=" * 60)

    db.mark_phase_running("process")
    db.pipeline_meta_set("clip_model", config.CLIP_MODEL)

    try:
        batch_scope = load_batch_scope()
    except BatchScopeError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("process", msg)
        emit_phase_postmortem(log, "process", phase_start, False, error=msg)
        return False

    if batch_scope:
        log.info(
            "BATCH_MANIFEST_PATH active: %s (%d path(s))",
            batch_scope.manifest_path,
            batch_scope.count,
        )

    face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache, doc_text_features = load_models()

    if config.ENABLE_SEARCH_LAYER and config.SEARCH_OCR_ENABLED and not tesseract_available():
        log.warning("SEARCH_OCR_ENABLED is on, but tesseract is unavailable. OCR indexing will be skipped.")

    # Collect all unprocessed photos
    stage_start = time.perf_counter()
    stage_start = time.perf_counter()
    conn = db.get_db()
    processed_paths = set(
        normalize_relative_path(row[0]) for row in conn.execute(
            "SELECT source_path FROM photos WHERE processed_at IS NOT NULL"
        ).fetchall()
    )
    conn.close()

    manifest_missing = 0
    manifest_unsupported = 0
    if batch_scope:
        manifest_selection = resolve_manifest_media_selection(batch_scope)
        all_photos = list(manifest_selection.image_paths)
        raw_files = list(manifest_selection.raw_paths)
        manifest_missing = len(manifest_selection.missing_relative_paths)
        manifest_unsupported = len(manifest_selection.unsupported_relative_paths)
        if manifest_missing:
            log.warning("Skipped %d manifest path(s) missing from originals.", manifest_missing)
        if manifest_unsupported:
            log.warning(
                "Skipped %d manifest path(s) with unsupported extensions.",
                manifest_unsupported,
            )
    else:
        all_photos = []
        for ext in config.IMAGE_EXTENSIONS:
            all_photos.extend(config.ORIGINALS_DIR.rglob(f"*{ext}"))
            all_photos.extend(config.ORIGINALS_DIR.rglob(f"*{ext.upper()}"))

        raw_files: list[Path] = []
        for ext in config.RAW_EXTENSIONS:
            raw_files.extend(config.ORIGINALS_DIR.rglob(f"*{ext}"))
            raw_files.extend(config.ORIGINALS_DIR.rglob(f"*{ext.upper()}"))

    year_scope_skipped = 0
    if config.TEST_YEAR_SCOPE:
        year_token = str(config.TEST_YEAR_SCOPE).strip()
        filtered_photos = []
        for photo_path in all_photos:
            rel = normalize_relative_path(photo_path.relative_to(config.ORIGINALS_DIR))
            padded = f"/{rel}/"
            if f"/{year_token}/" in padded:
                filtered_photos.append(photo_path)
            else:
                year_scope_skipped += 1
        all_photos = filtered_photos
        log.warning(
            "TEST_YEAR_SCOPE=%s active: skipped %d image(s) outside year scope.",
            year_token,
            year_scope_skipped,
        )

        filtered_raw_files = []
        for raw_path in raw_files:
            rel = normalize_relative_path(raw_path.relative_to(config.ORIGINALS_DIR))
            padded = f"/{rel}/"
            if f"/{year_token}/" in padded:
                filtered_raw_files.append(raw_path)
        raw_files = filtered_raw_files

    screenshot_skipped = 0
    if config.SCREENSHOT_EXCLUDE_PATTERNS:
        filtered_photos = []
        for photo_path in all_photos:
            rel_lower = normalize_relative_path(photo_path.relative_to(config.ORIGINALS_DIR)).lower()
            if any(token in rel_lower for token in config.SCREENSHOT_EXCLUDE_PATTERNS):
                screenshot_skipped += 1
                continue
            filtered_photos.append(photo_path)
        all_photos = filtered_photos
        if screenshot_skipped:
            log.warning(
                "Skipped %d screenshot-like image(s) by pattern filter: %s",
                screenshot_skipped,
                ", ".join(config.SCREENSHOT_EXCLUDE_PATTERNS),
            )

    prefilter_skipped = 0
    prefilter_skip_paths = _load_prefilter_skiplist()
    if prefilter_skip_paths:
        filtered_photos = []
        for photo_path in all_photos:
            rel_path = normalize_relative_path(photo_path.relative_to(config.ORIGINALS_DIR))
            if rel_path in prefilter_skip_paths:
                prefilter_skipped += 1
                continue
            filtered_photos.append(photo_path)
        all_photos = filtered_photos
        if prefilter_skipped:
            log.warning(
                "Skipped %d image(s) from phase1 prefilter mismatch list: %s",
                prefilter_skipped,
                config.PREFILTER_REJECTS_PATH,
            )

    if raw_files:
        log.warning(
            "Found %d RAW file(s) — skipping (not supported). "
            "Convert to JPEG/TIFF first if you want them processed.",
            len(raw_files),
        )

    all_rel_paths = {
        normalize_relative_path(p.relative_to(config.ORIGINALS_DIR)) for p in all_photos
    }
    already_done_in_scope = len(processed_paths & all_rel_paths)
    already_done_out_of_scope = max(0, len(processed_paths) - already_done_in_scope)

    total = len(all_photos)
    log.info("Found %d images to process (%d already done in current scope)", total, already_done_in_scope)
    if already_done_out_of_scope:
        log.info(
            "Ignoring %d previously processed image(s) that are outside current filters "
            "(screenshot/prefilter).",
            already_done_out_of_scope,
        )
    db.update_phase_progress("process", already_done_in_scope, total)

    processed_count = already_done_in_scope
    errors = 0
    stage_totals = {key: 0.0 for key in _STAGE_KEYS}

    for photo_path in all_photos:
        if shutdown.is_requested():
            log.info("Graceful shutdown: stopping after %d photos.", processed_count)
            break

        rel_path = normalize_relative_path(photo_path.relative_to(config.ORIGINALS_DIR))
        if rel_path in processed_paths:
            continue

        try:
            did_process = _process_single_photo(
                photo_path, rel_path,
                face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache,
                doc_text_features,
                torch, F, openai_clip, stage_totals
            )
            if did_process:
                processed_count += 1
            else:
                errors += 1
        except Exception as e:
            log.error("Error processing %s: %s", rel_path, e, exc_info=True)
            errors += 1

        if processed_count % 50 == 0:
            db.update_phase_progress("process", processed_count, total)

        if processed_count % 500 == 0:
            torch.cuda.empty_cache()
            log.info("Cleared CUDA cache at %d photos", processed_count)

        if processed_count > 0 and processed_count % 100 == 0:
            _log_stage_timing_summary(stage_totals, processed_count)

    db.update_phase_progress("process", processed_count, total)
    log.info("Process phase complete. Processed: %d, Errors: %d", processed_count, errors)

    if not shutdown.is_requested():
        db.mark_phase_complete("process")
    else:
        db.mark_phase_error("process", f"Stopped by user after {processed_count} photos")
    success = not shutdown.is_requested()
    emit_phase_postmortem(
        log,
        "process",
        phase_start,
        success,
        metrics={
            "Total discovered": total,
            "Already processed at start (in scope)": already_done_in_scope,
            "Already processed outside scope": already_done_out_of_scope,
            "Processed this run": max(0, processed_count - already_done_in_scope),
            "Processed total": processed_count,
            "Errors": errors,
            "RAW skipped": len(raw_files),
            "Year-scope skipped": year_scope_skipped,
            "Manifest missing": manifest_missing,
            "Manifest unsupported": manifest_unsupported,
            "Screenshots skipped": screenshot_skipped,
            "Prefilter skipped": prefilter_skipped,
            "Avg seconds/photo": f"{(stage_totals['photo_total'] / processed_count):.2f}" if processed_count else "0.00",
            "Avg OCR ms/photo": f"{(stage_totals['ocr'] / processed_count) * 1000.0:.0f}" if processed_count else "0",
            "Avg face ms/photo": f"{(stage_totals['faces'] / processed_count) * 1000.0:.0f}" if processed_count else "0",
            "Avg CLIP ms/photo": f"{(stage_totals['clip'] / processed_count) * 1000.0:.0f}" if processed_count else "0",
            "Avg metadata ms/photo": f"{(stage_totals['metadata'] / processed_count) * 1000.0:.0f}" if processed_count else "0",
        },
        error=None if success else f"Stopped by user after {processed_count} photos",
    )

    return success


def _process_single_photo(
    photo_path: Path,
    rel_path: str,
    face_app,
    yolo_model,
    clip_model,
    clip_preprocess,
    clip_prompt_cache,
    doc_text_features,  # pre-encoded document prompts tensor or None
    torch,
    F,
    openai_clip,
    stage_totals: dict[str, float],
) -> bool:
    import torch as _torch
    photo_start = time.perf_counter()

    # ── Load Image ──
    stage_start = time.perf_counter()
    img = _load_image(photo_path)
    stage_totals["load_image"] += time.perf_counter() - stage_start
    if img is None:
        stage_totals["photo_total"] += time.perf_counter() - photo_start
        return False
    img = _resize_image(img)

    # ── Read Existing Metadata ──
    stage_start = time.perf_counter()
    exif = _run_exiftool(photo_path)
    exif_date, date_source = _resolve_date(photo_path, exif)
    existing_people = _extract_existing_people(exif, photo_path)
    stage_totals["metadata"] += time.perf_counter() - stage_start

    # ── Insert photo record ──
    stage_start = time.perf_counter()
    conn = db.get_db()
    conn.execute(
        """INSERT OR IGNORE INTO photos
           (source_path, filename, exif_date, date_source, existing_people)
           VALUES (?, ?, ?, ?, ?)""",
        (
            rel_path,
            photo_path.name,
            exif_date,
            date_source,
            json.dumps(existing_people) if existing_people else None,
        )
    )
    conn.commit()
    photo_id = conn.execute(
        "SELECT photo_id FROM photos WHERE source_path=?", (rel_path,)
    ).fetchone()[0]
    stage_totals["photo_insert"] += time.perf_counter() - stage_start

    # ── InsightFace Pass ──
    stage_start = time.perf_counter()
    try:
        faces = face_app.get(img)
    except Exception as e:
        log.warning("InsightFace failed for %s: %s", rel_path, e)
        faces = []
    stage_totals["faces"] += time.perf_counter() - stage_start

    auto_gt = (len(existing_people) == 1 and len(faces) == 1)

    for i, face in enumerate(faces):
        embedding = face.embedding.astype(np.float32)
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm

        bbox = face.bbox.tolist()
        crop_path = config.CROPS_DIR / f"face_{photo_id}_{i}.jpg"
        _save_face_crop(img, bbox, crop_path)

        conn.execute(
            """INSERT INTO faces
               (photo_id, bbox_json, embedding, detection_score, is_ground_truth, crop_path)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                photo_id,
                json.dumps(bbox),
                embedding.tobytes(),
                float(face.det_score),
                1 if auto_gt else 0,
                str(crop_path.relative_to(config.LOCAL_BASE)),
            )
        )
        conn.commit()

        if auto_gt:
            face_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            # Mark face's cluster as pre-labeled with person name
            log.debug("Auto-GT: %s → %s", rel_path, existing_people[0])

    # ── YOLO Pass ──
    stage_start = time.perf_counter()
    try:
        results = yolo_model.predict(
            img,
            conf=config.YOLO_CONF_THRESHOLD,
            iou=config.YOLO_IOU_THRESHOLD,
            verbose=False
        )
    except Exception as e:
        log.warning("YOLO failed for %s: %s", rel_path, e)
        results = []
    stage_totals["yolo"] += time.perf_counter() - stage_start

    now = _now()
    if results:
        for result in results:
            boxes = result.boxes
            if boxes is None:
                continue
            for j, box in enumerate(boxes):
                cls_id = int(box.cls[0])
                cls_name = yolo_model.names[cls_id]
                if cls_name not in config.YOLO_DIRECT_TAGS:
                    continue
                tag = config.YOLO_DIRECT_TAGS[cls_name]
                conf = float(box.conf[0])
                xyxy = box.xyxy[0].tolist()

                crop_path = config.CROPS_DIR / f"obj_{photo_id}_{j}.jpg"
                _save_obj_crop(img, xyxy, crop_path)

                conn.execute(
                    """INSERT INTO detections
                       (photo_id, model, tag, tag_group, confidence, bbox_json, crop_path, approved, created_at)
                       VALUES (?, 'yolo', ?, ?, ?, ?, ?, 1, ?)""",
                    (
                        photo_id, tag,
                        _tag_group(tag),
                        conf,
                        json.dumps(xyxy),
                        str(crop_path.relative_to(config.LOCAL_BASE)),
                        now,
                    )
                )
                conn.execute(
                    "INSERT OR IGNORE INTO photo_tags (photo_id, tag, source) VALUES (?, ?, 'yolo')",
                    (photo_id, tag)
                )
                conn.commit()

    # ── CLIP Pass ──
    try:
        pil_img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))

        stage_start = time.perf_counter()
        if config.ENABLE_SEARCH_LAYER and config.SEARCH_OCR_ENABLED and tesseract_available():
            ocr_text = extract_ocr_text(pil_img)
            conn.execute(
                "UPDATE photos SET ocr_text=?, ocr_extracted_at=? WHERE photo_id=?",
                (ocr_text, now, photo_id),
            )
        stage_totals["ocr"] += time.perf_counter() - stage_start

        stage_start = time.perf_counter()
        image_input = clip_preprocess(pil_img).unsqueeze(0).to(config.CLIP_DEVICE)
        with _torch.no_grad():
            image_embedding = clip_model.encode_image(image_input).float()
            image_embedding = F.normalize(image_embedding, dim=-1).squeeze(0)

        if config.ENABLE_SEARCH_LAYER:
            emb_bytes = image_embedding.cpu().numpy().astype(np.float32).tobytes()
            conn.execute("UPDATE photos SET clip_embedding=? WHERE photo_id=?",
                         (emb_bytes, photo_id))

        # Document detection — free dot product against pre-encoded prompts.
        if doc_text_features is not None and config.DOCUMENT_DETECTION_ENABLED:
            with _torch.no_grad():
                doc_scores = (doc_text_features @ image_embedding).cpu()  # (N,)
            doc_score = doc_scores.max().item()
            if doc_score >= config.DOCUMENT_CLIP_THRESHOLD:
                conn.execute("UPDATE photos SET is_document=1 WHERE photo_id=?", (photo_id,))
                log.debug("Document flagged: %s (score=%.3f)", rel_path, doc_score)

        for group, tags in clip_prompt_cache.items():
            for tag_name, prompt_embedding in tags.items():
                score = _torch.dot(image_embedding, prompt_embedding).item()
                if score >= config.CLIP_TAG_THRESHOLD:
                    conn.execute(
                        """INSERT INTO detections
                           (photo_id, model, tag, tag_group, confidence, approved, created_at)
                           VALUES (?, 'clip', ?, ?, ?, 1, ?)""",
                        (photo_id, tag_name, group, score, now)
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO photo_tags (photo_id, tag, source) VALUES (?, ?, 'clip')",
                        (photo_id, tag_name)
                    )
                    conn.commit()
        stage_totals["clip"] += time.perf_counter() - stage_start
    except Exception as e:
        log.warning("CLIP failed for %s: %s", rel_path, e)

    # ── Mark processed ──
    stage_start = time.perf_counter()
    conn.execute(
        "UPDATE photos SET processed_at=? WHERE photo_id=?",
        (_now(), photo_id)
    )
    conn.commit()
    conn.close()
    stage_totals["finalize"] += time.perf_counter() - stage_start
    stage_totals["photo_total"] += time.perf_counter() - photo_start
    return True


def _tag_group(tag: str) -> str | None:
    for group, tags in config.SEMANTIC_TAG_GROUPS.items():
        if tag in tags:
            return group
    for group, direct_tags in [("vehicles", ["car", "pickup truck", "bus", "motorcycle", "bicycle"]),
                                 ("animals", ["dog", "cat", "horse", "cattle", "bird", "livestock"])]:
        if tag in direct_tags:
            return group
    return None


if __name__ == "__main__":
    success = run_process()
    sys.exit(0 if success else 1)
