"""Phase 6 - Push: rsync organized photos to NAS."""

import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_scope import BatchScopeError, filter_by_batch_scope, load_batch_scope
import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase6_push")

# NAS destination: sibling 'organized' folder next to the photos source
NAS_DEST_DIR = config.NAS_SOURCE_DIR.parent / "organized"


def run_push() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 6 - PUSH: organized -> NAS")
    log.info("=" * 60)
    log.info("Source: %s", config.OUTPUT_DIR)
    log.info("Dest:   %s", NAS_DEST_DIR)

    if not config.NAS_SOURCE_DIR.exists():
        msg = f"NAS not mounted at {config.NAS_SOURCE_DIR}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        emit_phase_postmortem(log, "push", phase_start, False, error=msg)
        return False

    try:
        NAS_DEST_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        msg = f"Cannot create NAS dest dir {NAS_DEST_DIR}: {e}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        emit_phase_postmortem(log, "push", phase_start, False, error=msg)
        return False

    db.mark_phase_running("push")
    start_time = time.time()

    try:
        batch_scope = load_batch_scope()
    except BatchScopeError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("push", msg)
        emit_phase_postmortem(log, "push", phase_start, False, error=msg)
        return False

    conn = db.get_db()
    query = "SELECT source_path, dest_path FROM photos WHERE copy_verified=1 AND dest_path IS NOT NULL"
    params: list[str] = []
    if config.TEST_YEAR_SCOPE:
        query += " AND source_path LIKE ?"
        params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
        log.info("TEST_YEAR_SCOPE=%s active for push phase.", config.TEST_YEAR_SCOPE)

    push_rows = conn.execute(query, params).fetchall()
    conn.close()

    manifest_skipped = 0
    if batch_scope:
        push_rows, manifest_skipped = filter_by_batch_scope(
            push_rows,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
        log.info(
            "BATCH_MANIFEST_PATH active for push phase: %s (%d queued, %d skipped outside manifest).",
            batch_scope.manifest_path,
            len(push_rows),
            manifest_skipped,
        )

    push_dest_paths = [str(row["dest_path"]) for row in push_rows if row["dest_path"]]
    total = len(push_dest_paths)
    db.update_phase_progress("push", 0, total)

    if total == 0:
        log.info("No organized files matched the current push scope.")
        db.mark_phase_complete("push")
        emit_phase_postmortem(
            log,
            "push",
            phase_start,
            True,
            metrics={"Files queued": 0, "Manifest skipped": manifest_skipped},
        )
        return True

    log.info("Queued %d organized file(s) for push from tracked DB rows.", total)

    files_pushed = 0
    files_from_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            files_from_path = handle.name
            for dest_rel in push_dest_paths:
                handle.write(f"{dest_rel}\n")

        cmd = [
            "rsync",
            "-avh",
            "--progress",
            "--checksum",
            "--ignore-existing",
            f"--log-file={config.RSYNC_PUSH_LOG}",
            f"--files-from={files_from_path}",
            str(config.OUTPUT_DIR) + "/",
            str(NAS_DEST_DIR) + "/",
        ]

        log.info("rsync command: %s", " ".join(cmd))

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            log.debug(line)
            if (
                not line.startswith(" ")
                and not any(
                    line.startswith(prefix)
                    for prefix in [
                        "sending",
                        "sent",
                        "total",
                        "rsync",
                        "building",
                        "delta",
                        "Number",
                        "send",
                        "recv",
                    ]
                )
                and ("/" in line or "." in line)
            ):
                files_pushed += 1
                if files_pushed % 50 == 0:
                    db.update_phase_progress("push", files_pushed, total)

        proc.wait()

        if proc.returncode not in (0, 23, 24):
            msg = f"rsync exited with code {proc.returncode}"
            log.error(msg)
            db.mark_phase_error("push", msg)
            emit_phase_postmortem(
                log,
                "push",
                phase_start,
                False,
                metrics={"Files pushed this run": files_pushed, "Files queued": total},
                error=msg,
            )
            return False

    except FileNotFoundError:
        msg = "rsync not found"
        log.error(msg)
        db.mark_phase_error("push", msg)
        emit_phase_postmortem(log, "push", phase_start, False, error=msg)
        return False
    except Exception as e:
        msg = f"rsync push failed: {e}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        emit_phase_postmortem(
            log,
            "push",
            phase_start,
            False,
            metrics={"Files pushed this run": files_pushed, "Files queued": total},
            error=msg,
        )
        return False
    finally:
        if files_from_path:
            try:
                Path(files_from_path).unlink(missing_ok=True)
            except OSError:
                pass

    elapsed = time.time() - start_time
    db.update_phase_progress("push", files_pushed, total)
    log.info(
        "Push complete. Files pushed: ~%d of %d queued, Elapsed: %.0f seconds",
        files_pushed,
        total,
        elapsed,
    )

    db.mark_phase_complete("push")
    emit_phase_postmortem(
        log,
        "push",
        phase_start,
        True,
        metrics={
            "Files queued": total,
            "Files pushed this run": files_pushed,
            "Manifest skipped": manifest_skipped,
            "Elapsed (seconds)": f"{elapsed:.0f}",
        },
    )
    return True


if __name__ == "__main__":
    success = run_push()
    sys.exit(0 if success else 1)
