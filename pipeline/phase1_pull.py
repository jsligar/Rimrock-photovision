"""Phase 1 — Pull: rsync photos from NAS to Rimrock NVMe."""

import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase1_pull")


def _count_files(directory: Path) -> int:
    count = 0
    for ext in config.IMAGE_EXTENSIONS | config.RAW_EXTENSIONS:
        count += len(list(directory.rglob(f"*{ext}")))
        count += len(list(directory.rglob(f"*{ext.upper()}")))
    return count


def _scan_media_stats(directory: Path) -> tuple[int, int]:
    """Return (media_file_count, total_bytes) for known image/raw extensions."""
    allowed_exts = {e.lower() for e in (config.IMAGE_EXTENSIONS | config.RAW_EXTENSIONS)}
    total_files = 0
    total_bytes = 0

    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in allowed_exts:
            continue
        total_files += 1
        try:
            total_bytes += path.stat().st_size
        except OSError:
            # Skip unreadable/transient files without aborting pre-scan.
            continue

    return total_files, total_bytes


def run_pull() -> bool:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 1 — PULL: NAS → Rimrock NVMe")
    log.info("=" * 60)

    if not config.NAS_SOURCE_DIR.exists():
        msg = f"NAS not mounted at {config.NAS_SOURCE_DIR}"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        emit_phase_postmortem(log, "pull", phase_start, False, error=msg)
        return False

    config.ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)

    db.mark_phase_running("pull")
    start_time = time.time()

    # Pre-pull inventory snapshot for operator visibility.
    source_files, source_bytes = _scan_media_stats(config.NAS_SOURCE_DIR)
    existing_files, existing_bytes = _scan_media_stats(config.ORIGINALS_DIR)
    pending_files_est = max(0, source_files - existing_files)

    db.update_phase_progress("pull", existing_files, source_files)
    log.info("Pre-pull inventory:")
    log.info("  Source media files:   %d", source_files)
    log.info("  Source media size:    %.1f GB", source_bytes / 1024**3)
    log.info("  Existing local files: %d", existing_files)
    log.info("  Existing local size:  %.1f GB", existing_bytes / 1024**3)
    log.info("  Estimated pending:    %d files", pending_files_est)

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
            emit_phase_postmortem(
                log,
                "pull",
                phase_start,
                False,
                metrics={
                    "Source files": source_files,
                    "Existing files before pull": existing_files,
                    "Files transferred this run": files_transferred,
                },
                error=msg,
            )
            return False

    except FileNotFoundError:
        msg = "rsync not found. Install it: sudo apt install rsync"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        emit_phase_postmortem(log, "pull", phase_start, False, error=msg)
        return False
    except Exception as e:
        msg = f"rsync failed: {e}"
        log.error(msg)
        db.mark_phase_error("pull", msg)
        emit_phase_postmortem(
            log,
            "pull",
            phase_start,
            False,
            metrics={"Files transferred this run": files_transferred},
            error=msg,
        )
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
    emit_phase_postmortem(
        log,
        "pull",
        phase_start,
        True,
        metrics={
            "Source files": source_files,
            "Existing files before pull": existing_files,
            "Files transferred this run": files_transferred,
            "Total files in originals": total_files,
            "Total size (GB)": f"{total_gb:.1f}",
            "rsync exit code": proc.returncode if "proc" in locals() else "unknown",
        },
    )
    return True


if __name__ == "__main__":
    success = run_pull()
    sys.exit(0 if success else 1)
