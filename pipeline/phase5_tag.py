"""Phase 5 — Tag: write XMP tags to organized copies via exiftool."""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_scope import BatchScopeError, filter_by_batch_scope, load_batch_scope
import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem
from pipeline import shutdown

log = get_logger("phase5_tag")

_WRITE_UNSUPPORTED_EXTS = {".webp"}
_EXPECTED_MAGIC_BY_EXT = {
    ".jpg": {"jpeg"},
    ".jpeg": {"jpeg"},
    ".png": {"png"},
    ".tif": {"tiff"},
    ".tiff": {"tiff"},
    ".webp": {"webp"},
    ".heic": {"heif"},
}
_HEIF_BRANDS = {
    b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1", b"avif",
}


def _is_write_unsupported_ext(path: Path) -> bool:
    return path.suffix.lower() in _WRITE_UNSUPPORTED_EXTS


def _detect_magic(path: Path) -> str | None:
    try:
        with open(path, "rb") as f:
            head = f.read(16)
    except OSError:
        return None

    if head.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if head.startswith((b"II*\x00", b"MM\x00*")):
        return "tiff"
    if len(head) >= 12 and head[0:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "webp"
    if len(head) >= 12 and head[4:8] == b"ftyp" and head[8:12] in _HEIF_BRANDS:
        return "heif"
    return None


def _has_extension_mismatch(path: Path) -> bool:
    expected = _EXPECTED_MAGIC_BY_EXT.get(path.suffix.lower())
    if not expected:
        return False
    magic = _detect_magic(path)
    if not magic:
        return False
    return magic not in expected


def _classify_exiftool_error(stderr: str) -> str:
    msg = (stderr or "").lower()
    if "writing of webp files is not yet supported" in msg:
        return "unsupported_write"
    if "not a valid jpg" in msg or "not a valid jpeg" in msg:
        return "format_mismatch"
    return "error"


def run_tag() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 5 — TAG: write XMP tags to organized photos")
    log.info("=" * 60)

    db.mark_phase_running("tag")

    try:
        batch_scope = load_batch_scope()
    except BatchScopeError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("tag", msg)
        emit_phase_postmortem(log, "tag", phase_start, False, error=msg)
        return False

    conn = db.get_db()

    # Get all verified organized photos
    query = (
        "SELECT photo_id, source_path, dest_path "
        "FROM photos WHERE copy_verified=1 AND dest_path IS NOT NULL"
    )
    params: list = []
    if config.TEST_YEAR_SCOPE:
        query += " AND source_path LIKE ?"
        params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
        log.info("TEST_YEAR_SCOPE=%s active for tag phase.", config.TEST_YEAR_SCOPE)

    photos = conn.execute(query, params).fetchall()

    manifest_skipped = 0
    if batch_scope:
        photos, manifest_skipped = filter_by_batch_scope(
            photos,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
        log.info(
            "BATCH_MANIFEST_PATH active for tag phase: %s (%d queued, %d skipped outside manifest).",
            batch_scope.manifest_path,
            len(photos),
            manifest_skipped,
        )

    total = len(photos)
    log.info("Photos to tag: %d", total)
    db.update_phase_progress("tag", 0, total)

    done = 0
    tagged = 0
    skipped = 0
    skipped_unsupported = 0
    skipped_mismatch = 0
    errors = 0

    for row in photos:
        if shutdown.is_requested():
            log.info("Graceful shutdown: stopping tag after %d photos.", done)
            conn.close()
            db.mark_phase_error("tag", f"Stopped by user after {done} photos")
            emit_phase_postmortem(
                log,
                "tag",
                phase_start,
                False,
                metrics={
                    "Photos to tag": total,
                    "Done": done,
                    "Tagged": tagged,
                    "Skipped (no tags)": skipped,
                    "Skipped (unsupported write)": skipped_unsupported,
                    "Skipped (format mismatch)": skipped_mismatch,
                    "Errors": errors,
                },
                error=f"Stopped by user after {done} photos",
            )
            return False

        photo_id = row["photo_id"]
        dest_rel = row["dest_path"]
        dest_path = config.OUTPUT_DIR / dest_rel

        if not dest_path.exists():
            log.warning("Dest not found: %s", dest_path)
            errors += 1
            continue

        try:
            # ── Collect face tags (approved clusters) ──
            face_rows = conn.execute(
                """SELECT DISTINCT c.person_label
                   FROM faces f
                   JOIN clusters c ON f.cluster_id = c.cluster_id
                   WHERE f.photo_id=?
                     AND c.approved=1
                     AND c.is_noise=0
                     AND c.person_label IS NOT NULL""",
                (photo_id,)
            ).fetchall()
            face_tags = [r[0] for r in face_rows if r[0]]

            # ── Collect object/scene tags (approved detections) ──
            det_rows = conn.execute(
                "SELECT DISTINCT tag FROM detections WHERE photo_id=? AND approved=1",
                (photo_id,)
            ).fetchall()
            object_tags = [r[0] for r in det_rows if r[0]]

            if not face_tags and not object_tags:
                skipped += 1
                done += 1
                continue

            if _is_write_unsupported_ext(dest_path):
                skipped_unsupported += 1
                done += 1
                log.warning("Skipping unsupported tag write format for %s", dest_rel)
                continue

            if _has_extension_mismatch(dest_path):
                skipped_mismatch += 1
                done += 1
                log.warning("Skipping extension/content mismatch file for tag write: %s", dest_rel)
                continue

            person_args = [f"-XMP:PersonInImage={t}" for t in face_tags]
            subject_args = [f"-XMP:Subject+={t}" for t in object_tags]

            cmd = (
                ["exiftool", "-P", "-overwrite_original"]
                + person_args
                + subject_args
                + [str(dest_path)]
            )

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                classification = _classify_exiftool_error(result.stderr)
                if classification == "unsupported_write":
                    skipped_unsupported += 1
                    done += 1
                    log.warning("Skipping unsupported tag write format for %s", dest_rel)
                elif classification == "format_mismatch":
                    skipped_mismatch += 1
                    done += 1
                    log.warning("Skipping extension/content mismatch file for tag write: %s", dest_rel)
                else:
                    log.warning("exiftool error for %s: %s", dest_rel, result.stderr.strip())
                    errors += 1
            else:
                tagged += 1
                done += 1

        except subprocess.TimeoutExpired:
            log.error("exiftool timed out for %s", dest_rel)
            errors += 1
        except Exception as e:
            log.error("Tag error for %s: %s", dest_rel, e)
            errors += 1

        if done % 200 == 0 and done > 0:
            db.update_phase_progress("tag", done, total)
            log.info("Tagged %d / %d", done, total)

    db.update_phase_progress("tag", done, total)
    log.info(
        "Tag phase complete. Tagged: %d, Skipped (no tags): %d, "
        "Skipped (unsupported write): %d, Skipped (format mismatch): %d, Errors: %d",
        tagged, skipped, skipped_unsupported, skipped_mismatch, errors
    )

    conn.close()
    db.mark_phase_complete("tag")
    emit_phase_postmortem(
        log,
        "tag",
        phase_start,
        True,
        metrics={
            "Photos to tag": total,
            "Tagged": tagged,
            "Skipped (no tags)": skipped,
            "Skipped (unsupported write)": skipped_unsupported,
            "Skipped (format mismatch)": skipped_mismatch,
            "Manifest skipped": manifest_skipped,
            "Errors": errors,
        },
    )
    return True


if __name__ == "__main__":
    success = run_tag()
    sys.exit(0 if success else 1)
