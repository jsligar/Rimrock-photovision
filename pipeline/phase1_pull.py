"""Phase 1 — Pull: rsync photos from NAS to Rimrock NVMe."""

import os
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


def _detect_magic(path: Path) -> str | None:
    """Return a lightweight magic type label from file header bytes."""
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


def _scan_extension_mismatches(directory: Path) -> list[tuple[str, str, str]]:
    """Find files where extension and binary signature disagree."""
    mismatches: list[tuple[str, str, str]] = []
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        expected = _EXPECTED_MAGIC_BY_EXT.get(ext)
        if not expected:
            continue

        detected = _detect_magic(path)
        if detected is None:
            mismatches.append((str(path.relative_to(directory)), ext, "unknown"))
            continue
        if detected not in expected:
            mismatches.append((str(path.relative_to(directory)), ext, detected))
    return mismatches


def _write_prefilter_rejects(entries: list[tuple[str, str, str]]) -> None:
    """Persist phase 1 prefilter results as a small TSV for downstream phases."""
    config.PREFILTER_REJECTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(config.PREFILTER_REJECTS_PATH, "w", encoding="utf-8") as f:
        for rel_path, ext, detected in entries:
            f.write(f"{rel_path}\t{ext}\t{detected}\n")


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


def _resolve_pull_scope() -> tuple[Path, Path, str]:
    """Resolve source/destination scope for pull.

    Returns:
        source_dir: NAS subtree to pull from
        dest_dir: local originals subtree to pull into
        scope_label: human-readable scope description
    """
    if not config.TEST_YEAR_SCOPE:
        return config.NAS_SOURCE_DIR, config.ORIGINALS_DIR, "full"

    year = str(config.TEST_YEAR_SCOPE).strip()
    candidates = [
        config.NAS_SOURCE_DIR / "By-Year" / year,
        config.NAS_SOURCE_DIR / year,
    ]
    for source_dir in candidates:
        if source_dir.exists() and source_dir.is_dir():
            rel = source_dir.relative_to(config.NAS_SOURCE_DIR)
            dest_dir = config.ORIGINALS_DIR / rel
            return source_dir, dest_dir, f"year:{year} ({rel})"

    raise FileNotFoundError(
        f"TEST_YEAR_SCOPE={year} is set, but no matching NAS folder was found under "
        f"{config.NAS_SOURCE_DIR} (checked By-Year/{year} and {year}/)"
    )


def _select_rsync_log_file() -> tuple[str | None, str | None]:
    """Choose a writable rsync log file path for this run.

    Returns:
        (path, note):
            - path: selected log file path to pass to rsync, or None
            - note: optional operator message for fallback/disable situations
    """
    target = config.RSYNC_PULL_LOG
    target_parent = target.parent

    try:
        target_parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None, f"Could not create rsync log directory: {target_parent}"

    if target.exists():
        if os.access(target, os.W_OK):
            return str(target), None

        if not os.access(target_parent, os.W_OK):
            return None, (
                f"rsync log file not writable ({target}) and parent directory "
                f"is not writable ({target_parent}); disabling --log-file"
            )

        fallback = target.with_name(f"{target.stem}.{int(time.time())}{target.suffix}")
        return str(fallback), f"rsync log file not writable ({target}); using fallback {fallback}"

    if os.access(target_parent, os.W_OK):
        return str(target), None

    return None, f"rsync log directory is not writable ({target_parent}); disabling --log-file"


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

    try:
        source_dir, pull_dest_dir, scope_label = _resolve_pull_scope()
    except FileNotFoundError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("pull", msg)
        emit_phase_postmortem(log, "pull", phase_start, False, error=msg)
        return False

    config.ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    pull_dest_dir.mkdir(parents=True, exist_ok=True)

    db.mark_phase_running("pull")
    start_time = time.time()

    # Pre-pull inventory snapshot for operator visibility.
    source_files, source_bytes = _scan_media_stats(source_dir)
    existing_files, existing_bytes = _scan_media_stats(pull_dest_dir)
    pending_files_est = max(0, source_files - existing_files)

    db.update_phase_progress("pull", existing_files, source_files)
    log.info("Pre-pull inventory:")
    log.info("  Scope:                %s", scope_label)
    log.info("  Source subtree:       %s", source_dir)
    log.info("  Dest subtree:         %s", pull_dest_dir)
    log.info("  Source media files:   %d", source_files)
    log.info("  Source media size:    %.1f GB", source_bytes / 1024**3)
    log.info("  Existing local files: %d", existing_files)
    log.info("  Existing local size:  %.1f GB", existing_bytes / 1024**3)
    log.info("  Estimated pending:    %d files", pending_files_est)

    inventory_match = (
        pending_files_est == 0
        and source_files == existing_files
        and source_bytes == existing_bytes
    )
    files_transferred = 0
    rsync_exit_code: int | str = "skipped_noop"

    if config.TEST_YEAR_SCOPE and inventory_match:
        log.info(
            "Inventory matches in scoped test mode (TEST_YEAR_SCOPE=%s); skipping rsync.",
            config.TEST_YEAR_SCOPE,
        )
    else:
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
            str(source_dir) + "/",
            str(pull_dest_dir) + "/",
        ]

        rsync_log_path, rsync_log_note = _select_rsync_log_file()
        if rsync_log_path:
            cmd.insert(-2, f"--log-file={rsync_log_path}")
        if rsync_log_note:
            log.warning(rsync_log_note)

        log.info("rsync command: %s", " ".join(cmd))

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
            rsync_exit_code = proc.returncode

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

    # Count actual files in pull scope
    total_files = _count_files(pull_dest_dir)
    db.update_phase_progress("pull", total_files, total_files)

    # Calculate total size
    total_bytes = sum(
        f.stat().st_size
        for f in pull_dest_dir.rglob("*")
        if f.is_file()
    )
    total_gb = total_bytes / 1024**3

    mismatches = _scan_extension_mismatches(pull_dest_dir)
    if pull_dest_dir != config.ORIGINALS_DIR:
        scope_rel = pull_dest_dir.relative_to(config.ORIGINALS_DIR)
        mismatches = [
            (str((scope_rel / Path(rel_path)).as_posix()), ext, detected)
            for rel_path, ext, detected in mismatches
        ]

    if mismatches:
        _write_prefilter_rejects(mismatches)
        log.warning(
            "Phase 1 prefilter flagged %d extension/signature mismatch file(s). "
            "Phase 2 will skip them. List: %s",
            len(mismatches),
            config.PREFILTER_REJECTS_PATH,
        )
        for rel_path, ext, detected in mismatches[:20]:
            log.warning(
                "Prefilter mismatch: %s (ext=%s detected=%s)",
                rel_path,
                ext,
                detected,
            )
        if len(mismatches) > 20:
            log.warning("Prefilter mismatch list truncated in log output (showing first 20).")
    elif config.PREFILTER_REJECTS_PATH.exists():
        config.PREFILTER_REJECTS_PATH.unlink()

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
            "Scope": scope_label,
            "Total files in pull scope": total_files,
            "Total size (GB)": f"{total_gb:.1f}",
            "Prefilter mismatches": len(mismatches),
            "rsync exit code": rsync_exit_code,
        },
    )
    return True


if __name__ == "__main__":
    success = run_pull()
    sys.exit(0 if success else 1)
