"""Phase 1 — Pull: rsync photos from NAS to Rimrock NVMe."""

import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger

log = get_logger("phase1_pull")


def _count_files(directory: Path) -> int:
    count = 0
    for ext in config.IMAGE_EXTENSIONS | config.RAW_EXTENSIONS:
        count += len(list(directory.rglob(f"*{ext}")))
        count += len(list(directory.rglob(f"*{ext.upper()}")))
    return count


def run_pull() -> bool:
    log.info("=" * 60)
    log.info("Phase 1 — PULL: NAS → Rimrock NVMe")
    log.info("=" * 60)

    if not config.NAS_SOURCE_DIR.exists():
        msg = f"NAS not mounted at {config.NAS_SOURCE_DIR}"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        return False

    config.ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)

    db.mark_phase_running("pull")
    start_time = time.time()

    cmd = [
        "rsync",
        "-avh",
        "--progress",
        "--checksum",
        "--ignore-existing",
        "--exclude=*.db",
        "--exclude=*.log",
        "--exclude=Thumbs.db",
        "--exclude=.DS_Store",
        "--exclude=*.tmp",
        f"--log-file={config.RSYNC_PULL_LOG}",
        str(config.NAS_SOURCE_DIR) + "/",
        str(config.ORIGINALS_DIR) + "/",
    ]

    log.info("rsync command: %s", " ".join(cmd))

    files_transferred = 0
    # Pattern to detect rsync per-file progress lines: "    123,456  45%   1.23MB/s  0:00:12"
    progress_re = re.compile(r'^\s+[\d,]+\s+\d+%')

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

            # Count files as rsync reports them
            if progress_re.match(line):
                pass  # progress line for current file
            elif (
                not line.startswith(" ")
                and not line.startswith("sending")
                and not line.startswith("sent")
                and not line.startswith("total")
                and not line.startswith("rsync")
                and ("/" in line or "." in line)
            ):
                # Looks like a filename being transferred
                if not any(line.startswith(s) for s in ["building", "delta", "Number", "send", "recv"]):
                    files_transferred += 1
                    if files_transferred % 50 == 0:
                        db.update_phase_progress("pull", files_transferred)
                        log.info("Transferred %d files so far...", files_transferred)

        proc.wait()

        if proc.returncode not in (0, 23, 24):  # 23=partial, 24=vanished OK
            msg = f"rsync exited with code {proc.returncode}"
            log.error(msg)
            db.mark_phase_error("pull", msg)
            return False

    except FileNotFoundError:
        msg = "rsync not found. Install it: sudo apt install rsync"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        return False
    except Exception as e:
        msg = f"rsync failed: {e}"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        return False

    elapsed = time.time() - start_time

    # Count actual files in ORIGINALS_DIR
    total_files = _count_files(config.ORIGINALS_DIR)
    db.update_phase_progress("pull", total_files, total_files)

    # Calculate total size
    total_bytes = sum(
        f.stat().st_size
        for f in config.ORIGINALS_DIR.rglob("*")
        if f.is_file()
    )
    total_gb = total_bytes / 1024**3

    log.info("Pull complete:")
    log.info("  Total files in originals: %d", total_files)
    log.info("  Total size: %.1f GB", total_gb)
    log.info("  Elapsed: %.0f seconds", elapsed)

    db.mark_phase_complete("pull")
    return True


if __name__ == "__main__":
    success = run_pull()
    sys.exit(0 if success else 1)
