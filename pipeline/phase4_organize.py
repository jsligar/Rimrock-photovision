"""Phase 4 — Organize: copy photos to YYYY/YYYY-MM directory structure."""

import hashlib
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_scope import BatchScopeError, filter_by_batch_scope, load_batch_scope
import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem
from pipeline import shutdown

log = get_logger("phase4_organize")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _destination_dir(exif_date: str | None) -> Path:
    if not exif_date:
        return config.OUTPUT_DIR / config.UNDATED_DIR
    try:
        dt = datetime.fromisoformat(exif_date)
        year = dt.strftime("%Y")
        month = dt.strftime("%Y-%m")
        return config.OUTPUT_DIR / year / month
    except ValueError:
        return config.OUTPUT_DIR / config.UNDATED_DIR


def _unique_dest_path(dest_dir: Path, filename: str) -> Path:
    """Return a unique path in dest_dir for filename, appending _{n} if needed."""
    dest = dest_dir / filename
    if not dest.exists():
        return dest
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    n = 1
    while True:
        candidate = dest_dir / f"{stem}_{n}{suffix}"
        if not candidate.exists():
            return candidate
        n += 1


def _check_output_dir_writable() -> str | None:
    """Return an error message if OUTPUT_DIR is not writable, else None."""
    try:
        config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        probe = config.OUTPUT_DIR / ".write_probe"
        probe.touch()
        probe.unlink()
        return None
    except Exception as e:
        return f"OUTPUT_DIR '{config.OUTPUT_DIR}' is not writable: {e}"


def run_organize() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 4 — ORGANIZE: copy to YYYY/YYYY-MM structure")
    log.info("=" * 60)

    db.mark_phase_running("organize")

    writable_err = _check_output_dir_writable()
    if writable_err:
        log.error(writable_err)
        db.mark_phase_error("organize", writable_err)
        emit_phase_postmortem(log, "organize", phase_start, False, error=writable_err)
        return False

    try:
        batch_scope = load_batch_scope()
    except BatchScopeError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("organize", msg)
        emit_phase_postmortem(log, "organize", phase_start, False, error=msg)
        return False

    conn = db.get_db()
    query = (
        "SELECT photo_id, source_path, filename, exif_date, dest_path FROM photos "
        "WHERE dest_path IS NULL OR (dest_path IS NOT NULL AND copy_verified=0)"
    )
    params: list = []
    if config.TEST_YEAR_SCOPE:
        query += " AND source_path LIKE ?"
        params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
        log.info("TEST_YEAR_SCOPE=%s active for organize phase.", config.TEST_YEAR_SCOPE)

    photos = conn.execute(query, params).fetchall()
    conn.close()

    manifest_skipped = 0
    if batch_scope:
        photos, manifest_skipped = filter_by_batch_scope(
            photos,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
        log.info(
            "BATCH_MANIFEST_PATH active for organize phase: %s (%d queued, %d skipped outside manifest).",
            batch_scope.manifest_path,
            len(photos),
            manifest_skipped,
        )

    total = len(photos)
    log.info("Photos to organize: %d", total)
    db.update_phase_progress("organize", 0, total)

    done = 0
    errors = 0

    for row in photos:
        if shutdown.is_requested():
            log.info("Graceful shutdown: stopping organize after %d photos.", done)
            db.mark_phase_error("organize", f"Stopped by user after {done} photos")
            emit_phase_postmortem(
                log,
                "organize",
                phase_start,
                False,
                metrics={"Photos to organize": total, "Done": done, "Errors": errors},
                error=f"Stopped by user after {done} photos",
            )
            return False

        photo_id = row["photo_id"]
        source_rel = row["source_path"]
        filename = row["filename"]
        exif_date = row["exif_date"]

        source_path = config.ORIGINALS_DIR / source_rel

        if not source_path.exists():
            log.warning("Source not found: %s", source_path)
            errors += 1
            continue

        try:
            # Compute source checksum upfront — needed for verification and
            # idempotent retry checks.
            checksum = _sha256(source_path)

            # If a prior run already wrote dest_path (copy_verified=0), reuse
            # it so we don't generate a duplicate _n-suffixed file.
            existing_dest_rel = row["dest_path"]
            if existing_dest_rel is not None:
                dest_path = config.OUTPUT_DIR / existing_dest_rel
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_rel = existing_dest_rel
            else:
                dest_dir = _destination_dir(exif_date)
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_path = _unique_dest_path(dest_dir, filename)
                dest_rel = str(dest_path.relative_to(config.OUTPUT_DIR))

                # Write dest_path to DB BEFORE copying so a crash leaves a
                # tracked (unverified) record instead of an untracked orphan.
                conn2 = db.get_db()
                conn2.execute(
                    "UPDATE photos SET dest_path=?, copy_verified=0 WHERE photo_id=?",
                    (dest_rel, photo_id),
                )
                conn2.commit()
                conn2.close()

            # If dest already exists with matching checksum (idempotent retry),
            # skip the copy entirely.
            if dest_path.exists() and _sha256(dest_path) == checksum:
                log.info(
                    "Dest already matches source for %s — skipping copy, marking verified",
                    source_rel,
                )
            else:
                shutil.copy2(str(source_path), str(dest_path))

                src_size = source_path.stat().st_size
                dst_size = dest_path.stat().st_size
                if src_size != dst_size:
                    log.error(
                        "Copy size mismatch for %s: src=%d bytes, dst=%d bytes — "
                        "copy_verified NOT set",
                        source_rel, src_size, dst_size,
                    )
                    errors += 1
                    continue

                dest_checksum = _sha256(dest_path)
                if checksum != dest_checksum:
                    log.error(
                        "Copy checksum mismatch for %s: src=%s dst=%s — "
                        "copy_verified NOT set",
                        source_rel, checksum, dest_checksum,
                    )
                    errors += 1
                    continue

            conn2 = db.get_db()
            conn2.execute(
                "UPDATE photos SET dest_path=?, checksum=?, copy_verified=1 WHERE photo_id=?",
                (dest_rel, checksum, photo_id),
            )
            conn2.commit()
            conn2.close()

            done += 1

        except Exception as e:
            log.error("Error organizing %s: %s", source_rel, e)
            errors += 1

        if done % 100 == 0 and done > 0:
            db.update_phase_progress("organize", done, total)
            log.info("Organized %d / %d", done, total)

    db.update_phase_progress("organize", done, total)
    log.info("Organize complete. Done: %d, Errors: %d", done, errors)
    db.mark_phase_complete("organize")
    emit_phase_postmortem(
        log,
        "organize",
        phase_start,
        True,
        metrics={
            "Photos to organize": total,
            "Done": done,
            "Errors": errors,
            "Manifest skipped": manifest_skipped,
        },
    )
    return True


if __name__ == "__main__":
    success = run_organize()
    sys.exit(0 if success else 1)
