"""Phase 4 — Organize: copy photos to YYYY/YYYY-MM directory structure."""

import hashlib
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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


def run_organize() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 4 — ORGANIZE: copy to YYYY/YYYY-MM structure")
    log.info("=" * 60)

    db.mark_phase_running("organize")

    conn = db.get_db()
    query = "SELECT photo_id, source_path, filename, exif_date FROM photos WHERE dest_path IS NULL"
    params: list = []
    if config.TEST_YEAR_SCOPE:
        query += " AND source_path LIKE ?"
        params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
        log.info("TEST_YEAR_SCOPE=%s active for organize phase.", config.TEST_YEAR_SCOPE)

    photos = conn.execute(query, params).fetchall()
    conn.close()

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
            dest_dir = _destination_dir(exif_date)
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = _unique_dest_path(dest_dir, filename)

            shutil.copy2(str(source_path), str(dest_path))

            checksum = _sha256(source_path)

            dest_rel = str(dest_path.relative_to(config.OUTPUT_DIR))
            conn2 = db.get_db()
            conn2.execute(
                "UPDATE photos SET dest_path=?, checksum=?, copy_verified=1 WHERE photo_id=?",
                (dest_rel, checksum, photo_id)
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
        metrics={"Photos to organize": total, "Done": done, "Errors": errors},
    )
    return True


if __name__ == "__main__":
    success = run_organize()
    sys.exit(0 if success else 1)
