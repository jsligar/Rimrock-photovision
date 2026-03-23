"""Phase 5 — Tag: write XMP tags to organized copies via exiftool."""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem
from pipeline import shutdown

log = get_logger("phase5_tag")


def run_tag() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 5 — TAG: write XMP tags to organized photos")
    log.info("=" * 60)

    db.mark_phase_running("tag")

    conn = db.get_db()

    # Get all verified organized photos
    photos = conn.execute(
        "SELECT photo_id, dest_path FROM photos WHERE copy_verified=1 AND dest_path IS NOT NULL"
    ).fetchall()

    total = len(photos)
    log.info("Photos to tag: %d", total)
    db.update_phase_progress("tag", 0, total)

    done = 0
    skipped = 0
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
                metrics={"Photos to tag": total, "Done": done, "Skipped": skipped, "Errors": errors},
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
                log.warning("exiftool error for %s: %s", dest_rel, result.stderr.strip())
                errors += 1
            else:
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
    log.info("Tag phase complete. Tagged: %d, Skipped (no tags): %d, Errors: %d",
             done - skipped, skipped, errors)

    conn.close()
    db.mark_phase_complete("tag")
    emit_phase_postmortem(
        log,
        "tag",
        phase_start,
        True,
        metrics={
            "Photos to tag": total,
            "Tagged": done - skipped,
            "Skipped (no tags)": skipped,
            "Errors": errors,
        },
    )
    return True


if __name__ == "__main__":
    success = run_tag()
    sys.exit(0 if success else 1)
