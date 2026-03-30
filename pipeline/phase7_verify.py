"""Phase 7 - Verify: post-push integrity spot-check and report."""

import hashlib
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_scope import BatchScopeError, filter_by_batch_scope, load_batch_scope
import config
import db
from pipeline.logger import get_logger
from pipeline.postmortem import emit_phase_postmortem

log = get_logger("phase7_verify")

NAS_DEST_DIR = config.NAS_SOURCE_DIR.parent / "organized"
SPOT_CHECK_SAMPLE = 50


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def run_verify() -> dict:
    phase_start = time.time()
    log.info("=" * 60)
    log.info("Phase 7 - VERIFY: post-push integrity check")
    log.info("=" * 60)

    db.mark_phase_running("verify")

    try:
        batch_scope = load_batch_scope()
    except BatchScopeError as e:
        msg = str(e)
        log.error(msg)
        db.mark_phase_error("verify", msg)
        emit_phase_postmortem(log, "verify", phase_start, False, error=msg)
        return {}

    conn = db.get_db()
    report: dict = {}

    query = "SELECT photo_id, source_path, dest_path, checksum, exif_date FROM photos WHERE copy_verified=1"
    params: list[str] = []
    if config.TEST_YEAR_SCOPE:
        query += " AND source_path LIKE ?"
        params.append(f"%/{config.TEST_YEAR_SCOPE}/%")
        log.info("TEST_YEAR_SCOPE=%s active for verify phase.", config.TEST_YEAR_SCOPE)

    verified_photos = conn.execute(query, params).fetchall()

    manifest_skipped = 0
    if batch_scope:
        verified_photos, manifest_skipped = filter_by_batch_scope(
            verified_photos,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
        log.info(
            "BATCH_MANIFEST_PATH active for verify phase: %s (%d queued, %d skipped outside manifest).",
            batch_scope.manifest_path,
            len(verified_photos),
            manifest_skipped,
        )

    local_count = sum(
        1
        for row in verified_photos
        if row["dest_path"] and (config.OUTPUT_DIR / row["dest_path"]).exists()
    )

    nas_reachable = NAS_DEST_DIR.exists()
    if not nas_reachable:
        log.warning("NAS dest not reachable: %s", NAS_DEST_DIR)
    nas_count = sum(
        1
        for row in verified_photos
        if nas_reachable and row["dest_path"] and (NAS_DEST_DIR / row["dest_path"]).exists()
    )

    report["local_count"] = local_count
    report["nas_count"] = nas_count
    report["nas_reachable"] = nas_reachable
    log.info("Local organized (tracked scope): %d files", local_count)
    log.info("NAS organized (tracked scope):   %d files", nas_count)

    checksum_candidates = [row for row in verified_photos if row["dest_path"] and row["checksum"]]
    sample_size = min(SPOT_CHECK_SAMPLE, len(checksum_candidates))
    sample = random.sample(checksum_candidates, sample_size) if checksum_candidates else []

    checksum_pass = 0
    checksum_fail = 0
    failed_files: list[str] = []

    for row in sample:
        dest_path = config.OUTPUT_DIR / row["dest_path"]
        if not dest_path.exists():
            checksum_fail += 1
            failed_files.append(str(row["dest_path"]))
            continue
        actual = _sha256(dest_path)
        if actual == row["checksum"]:
            checksum_pass += 1
        else:
            checksum_fail += 1
            failed_files.append(str(row["dest_path"]))
            log.warning("Checksum MISMATCH: %s", row["dest_path"])

    report["spot_check_sample"] = sample_size
    report["checksum_pass"] = checksum_pass
    report["checksum_fail"] = checksum_fail
    report["failed_files"] = failed_files
    log.info("Spot check (%d files): %d pass, %d fail", sample_size, checksum_pass, checksum_fail)

    undated_rows = conn.execute(
        """
        SELECT source_path
          FROM photos
         WHERE copy_verified=1
           AND (dest_path LIKE ? OR exif_date IS NULL)
        """,
        (f"{config.UNDATED_DIR}/%",),
    ).fetchall()
    if batch_scope:
        undated_rows, _ = filter_by_batch_scope(
            undated_rows,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
    undated_list = [r["source_path"] for r in undated_rows]
    report["undated_count"] = len(undated_list)
    report["undated_files"] = undated_list[:100]
    log.info("Undated photos: %d", len(undated_list))

    untagged_rows = conn.execute(
        """
        SELECT p.source_path
          FROM photos p
         WHERE NOT EXISTS (
                   SELECT 1 FROM photo_tags pt WHERE pt.photo_id = p.photo_id
               )
           AND NOT EXISTS (
                   SELECT 1
                     FROM faces f
                     JOIN clusters c ON f.cluster_id = c.cluster_id
                    WHERE f.photo_id = p.photo_id
                      AND c.approved=1
               )
           AND p.copy_verified=1
        """
    ).fetchall()
    if batch_scope:
        untagged_rows, _ = filter_by_batch_scope(
            untagged_rows,
            batch_scope=batch_scope,
            path_getter=lambda row: row["source_path"],
        )
    untagged_list = [r["source_path"] for r in untagged_rows]
    report["untagged_count"] = len(untagged_list)
    report["untagged_files"] = untagged_list[:100]
    log.info("Untagged photos: %d", len(untagged_list))

    conn.close()

    log.info("")
    log.info("VERIFY REPORT")
    log.info("  Local files:      %d", local_count)
    log.info("  NAS files:        %d", nas_count)
    log.info("  NAS reachable:    %s", "YES" if nas_reachable else "NO")
    log.info("  Spot check pass:  %d / %d", checksum_pass, sample_size)
    log.info("  Undated:          %d", len(undated_list))
    log.info("  Untagged:         %d", len(untagged_list))

    db.mark_phase_complete("verify")
    emit_phase_postmortem(
        log,
        "verify",
        phase_start,
        True,
        metrics={
            "Local files": local_count,
            "NAS files": nas_count,
            "NAS reachable": "YES" if nas_reachable else "NO",
            "Spot-check pass": f"{checksum_pass}/{sample_size}",
            "Checksum failures": checksum_fail,
            "Manifest skipped": manifest_skipped,
            "Undated": len(undated_list),
            "Untagged": len(untagged_list),
        },
    )
    return report


if __name__ == "__main__":
    report = run_verify()
    print("\nVERIFY SUMMARY:")
    print(f"  Local files:      {report.get('local_count', 0)}")
    print(f"  NAS files:        {report.get('nas_count', 0)}")
    print(
        f"  Spot check:       {report.get('checksum_pass', 0)}/{report.get('spot_check_sample', 0)} pass"
    )
    print(f"  Undated:          {report.get('undated_count', 0)}")
    print(f"  Untagged:         {report.get('untagged_count', 0)}")
    sys.exit(0)
