"""Phase 6 — Push: rsync organized photos to NAS."""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger

log = get_logger("phase6_push")

# NAS destination: sibling 'organized' folder next to the photos source
NAS_DEST_DIR = config.NAS_SOURCE_DIR.parent / "organized"


def run_push() -> bool:
    log.info("=" * 60)
    log.info("Phase 6 — PUSH: organized → NAS")
    log.info("=" * 60)
    log.info("Source: %s", config.OUTPUT_DIR)
    log.info("Dest:   %s", NAS_DEST_DIR)

    if not config.NAS_SOURCE_DIR.exists():
        msg = f"NAS not mounted at {config.NAS_SOURCE_DIR}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        return False

    # Ensure destination parent exists
    try:
        NAS_DEST_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        msg = f"Cannot create NAS dest dir {NAS_DEST_DIR}: {e}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        return False

    db.mark_phase_running("push")
    start_time = time.time()

    cmd = [
        "rsync",
        "-avh",
        "--progress",
        "--checksum",
        "--ignore-existing",
        f"--log-file={config.RSYNC_PUSH_LOG}",
        str(config.OUTPUT_DIR) + "/",
        str(NAS_DEST_DIR) + "/",
    ]

    log.info("rsync command: %s", " ".join(cmd))

    files_pushed = 0

    try:
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
                and not any(line.startswith(s) for s in
                            ["sending", "sent", "total", "rsync", "building",
                             "delta", "Number", "send", "recv"])
                and ("/" in line or "." in line)
            ):
                files_pushed += 1
                if files_pushed % 50 == 0:
                    db.update_phase_progress("push", files_pushed)

        proc.wait()

        if proc.returncode not in (0, 23, 24):
            msg = f"rsync exited with code {proc.returncode}"
            log.error(msg)
            db.mark_phase_error("push", msg)
            return False

    except FileNotFoundError:
        msg = "rsync not found"
        log.error(msg)
        db.mark_phase_error("push", msg)
        return False
    except Exception as e:
        msg = f"rsync push failed: {e}"
        log.error(msg)
        db.mark_phase_error("push", msg)
        return False

    elapsed = time.time() - start_time
    log.info("Push complete. Files pushed: ~%d, Elapsed: %.0f seconds", files_pushed, elapsed)

    db.mark_phase_complete("push")
    return True


if __name__ == "__main__":
    success = run_push()
    sys.exit(0 if success else 1)
