"""Phase 2 — Process: combined face detection + semantic tagging in one image loop."""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger

log = get_logger("phase2_process")

# Global shutdown flag for graceful stop
shutdown_requested = False


def _handle_signal(signum, frame):
    global shutdown_requested
    log.info("Shutdown signal received. Finishing current photo then exiting...")
    shutdown_requested = True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    import clip as openai_clip
    import insightface
    from insightface.app import FaceAnalysis
    from ultralytics import YOLO

    log.info("Loading InsightFace %s...", config.INSIGHTFACE_MODEL)
    face_app = FaceAnalysis(name=config.INSIGHTFACE_MODEL, ctx_id=config.CTX_ID)
    face_app.prepare(ctx_id=config.CTX_ID, det_size=config.DET_SIZE, det_thresh=config.DET_THRESH)

    log.info("Loading YOLOv8s...")
    yolo_model = YOLO(config.YOLO_MODEL)
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

    log.info("All models loaded.")
    return face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache


def run_process() -> bool:
    import torch
    import torch.nn.functional as F
    import clip as openai_clip

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("=" * 60)
    log.info("Phase 2 — PROCESS: face + semantic tagging")
    log.info("=" * 60)

    db.mark_phase_running("process")

    face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache = load_models()

    # Collect all unprocessed photos
    conn = db.get_db()
    processed_paths = set(
        row[0] for row in conn.execute(
            "SELECT source_path FROM photos WHERE processed_at IS NOT NULL"
        ).fetchall()
    )
    conn.close()

    all_photos = []
    for ext in config.IMAGE_EXTENSIONS:
        all_photos.extend(config.ORIGINALS_DIR.rglob(f"*{ext}"))
        all_photos.extend(config.ORIGINALS_DIR.rglob(f"*{ext.upper()}"))

    total = len(all_photos)
    log.info("Found %d images to process (%d already done)", total, len(processed_paths))
    db.update_phase_progress("process", len(processed_paths), total)

    processed_count = len(processed_paths)
    errors = 0

    for photo_path in all_photos:
        if shutdown_requested:
            log.info("Graceful shutdown: stopping after %d photos.", processed_count)
            break

        rel_path = str(photo_path.relative_to(config.ORIGINALS_DIR))
        if rel_path in processed_paths:
            continue

        # Skip RAW files
        if photo_path.suffix.lower() in config.RAW_EXTENSIONS:
            log.warning("Skipping RAW file: %s", rel_path)
            continue

        try:
            _process_single_photo(
                photo_path, rel_path,
                face_app, yolo_model, clip_model, clip_preprocess, clip_prompt_cache,
                torch, F, openai_clip
            )
            processed_count += 1
        except Exception as e:
            log.error("Error processing %s: %s", rel_path, e, exc_info=True)
            errors += 1

        if processed_count % 50 == 0:
            db.update_phase_progress("process", processed_count, total)

        if processed_count % 500 == 0:
            torch.cuda.empty_cache()
            log.info("Cleared CUDA cache at %d photos", processed_count)

    db.update_phase_progress("process", processed_count, total)
    log.info("Process phase complete. Processed: %d, Errors: %d", processed_count, errors)

    if not shutdown_requested:
        db.mark_phase_complete("process")
    else:
        db.mark_phase_error("process", f"Stopped by user after {processed_count} photos")

    return not shutdown_requested


def _process_single_photo(
    photo_path: Path,
    rel_path: str,
    face_app,
    yolo_model,
    clip_model,
    clip_preprocess,
    clip_prompt_cache,
    torch,
    F,
    openai_clip,
) -> None:
    import torch as _torch

    # ── Load Image ──
    img = _load_image(photo_path)
    if img is None:
        return
    img = _resize_image(img)

    # ── Read Existing Metadata ──
    exif = _run_exiftool(photo_path)
    exif_date, date_source = _resolve_date(photo_path, exif)
    existing_people = _extract_existing_people(exif, photo_path)

    # ── Insert photo record ──
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

    # ── InsightFace Pass ──
    try:
        faces = face_app.get(img)
    except Exception as e:
        log.warning("InsightFace failed for %s: %s", rel_path, e)
        faces = []

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
        image_input = clip_preprocess(pil_img).unsqueeze(0).to(config.CLIP_DEVICE)
        with _torch.no_grad():
            image_embedding = clip_model.encode_image(image_input).float()
            image_embedding = F.normalize(image_embedding, dim=-1).squeeze(0)

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
    except Exception as e:
        log.warning("CLIP failed for %s: %s", rel_path, e)

    # ── Mark processed ──
    conn.execute(
        "UPDATE photos SET processed_at=? WHERE photo_id=?",
        (_now(), photo_id)
    )
    conn.commit()
    conn.close()


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
