"""Phase 7 — Verify: post-push integrity spot-check and report."""

import hashlib
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db
from pipeline.logger import get_logger

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
    log.info("=" * 60)
    log.info("Phase 7 — VERIFY: post-push integrity check")
    log.info("=" * 60)

    db.mark_phase_running("verify")

    conn = db.get_db()
    report = {}

    # ── Count local vs NAS ──
    local_count = sum(
        1 for _ in config.OUTPUT_DIR.rglob("*") if _.is_file()
    )

    nas_count = 0
    nas_reachable = NAS_DEST_DIR.exists()
    if nas_reachable:
        nas_count = sum(1 for _ in NAS_DEST_DIR.rglob("*") if _.is_file())
    else:
        log.warning("NAS dest not reachable: %s", NAS_DEST_DIR)

    report["local_count"] = local_count
    report["nas_count"] = nas_count
    report["nas_reachable"] = nas_reachable
    log.info("Local organized: %d files", local_count)
    log.info("NAS organized:   %d files", nas_count)

    # ── Spot-check SHA-256 ──
    verified_photos = conn.execute(
        "SELECT photo_id, dest_path, checksum FROM photos WHERE copy_verified=1 AND checksum IS NOT NULL"
    ).fetchall()

    sample_size = min(SPOT_CHECK_SAMPLE, len(verified_photos))
    sample = random.sample(verified_photos, sample_size) if verified_photos else []

    checksum_pass = 0
    checksum_fail = 0
    failed_files = []

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

    # ── Undated photos ──
    undated = conn.execute(
        "SELECT source_path FROM photos WHERE dest_path LIKE ? OR exif_date IS NULL",
        (f"{config.UNDATED_DIR}/%",)
    ).fetchall()
    undated_list = [r[0] for r in undated]
    report["undated_count"] = len(undated_list)
    report["undated_files"] = undated_list[:100]  # cap for API response
    log.info("Undated photos: %d", len(undated_list))

    # ── Photos with no tags ──
    untagged = conn.execute(
        """SELECT p.source_path FROM photos p
           WHERE NOT EXISTS (
               SELECT 1 FROM photo_tags pt WHERE pt.photo_id = p.photo_id
           )
           AND NOT EXISTS (
               SELECT 1 FROM faces f
               JOIN clusters c ON f.cluster_id = c.cluster_id
               WHERE f.photo_id = p.photo_id AND c.approved=1
           )
           AND p.copy_verified=1"""
    ).fetchall()
    untagged_list = [r[0] for r in untagged]
    report["untagged_count"] = len(untagged_list)
    report["untagged_files"] = untagged_list[:100]
    log.info("Untagged photos: %d", len(untagged_list))

    conn.close()

    # ── Print summary ──
    log.info("")
    log.info("VERIFY REPORT")
    log.info("  Local files:      %d", local_count)
    log.info("  NAS files:        %d", nas_count)
    log.info("  NAS reachable:    %s", "YES" if nas_reachable else "NO")
    log.info("  Spot check pass:  %d / %d", checksum_pass, sample_size)
    log.info("  Undated:          %d", len(undated_list))
    log.info("  Untagged:         %d", len(untagged_list))

    db.mark_phase_complete("verify")
    return report


if __name__ == "__main__":
    report = run_verify()
    print("\nVERIFY SUMMARY:")
    print(f"  Local files:      {report['local_count']}")
    print(f"  NAS files:        {report['nas_count']}")
    print(f"  Spot check:       {report['checksum_pass']}/{report['spot_check_sample']} pass")
    print(f"  Undated:          {report['undated_count']}")
    print(f"  Untagged:         {report['untagged_count']}")
    sys.exit(0)
