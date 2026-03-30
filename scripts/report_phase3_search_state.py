"""Report Phase 3 search-layer coverage and OCR follow-up items.

Usage:
    python -m scripts.report_phase3_search_state
    python -m scripts.report_phase3_search_state --list-ocr-missing
    python -m scripts.report_phase3_search_state --list-ocr-missing --limit 50
    python -m scripts.report_phase3_search_state --json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from typing import Any

import config
import db


def _to_int(value: Any) -> int:
    return int(value or 0)


def build_report(limit: int = 25) -> dict[str, Any]:
    conn = db.get_db()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS processed,
                   SUM(CASE WHEN clip_embedding IS NOT NULL THEN 1 ELSE 0 END) AS clip_ready,
                   SUM(CASE WHEN ocr_extracted_at IS NOT NULL THEN 1 ELSE 0 END) AS ocr_attempted,
                   SUM(CASE WHEN ocr_text IS NOT NULL AND length(trim(ocr_text)) > 0 THEN 1 ELSE 0 END) AS ocr_with_text
              FROM photos
             WHERE processed_at IS NOT NULL
            """
        ).fetchone()
        if row is None:
            raise sqlite3.OperationalError("No photo rows returned")

        processed = _to_int(row["processed"])
        clip_ready = _to_int(row["clip_ready"])
        ocr_attempted = _to_int(row["ocr_attempted"])
        ocr_with_text = _to_int(row["ocr_with_text"])

        bg_jobs = [
            dict(job)
            for job in conn.execute(
                """
                SELECT job_name, status, progress_current, progress_total,
                       started_at, updated_at, completed_at, error_message, detail
                  FROM background_jobs
                 WHERE job_name IN ('clip_backfill', 'ocr_backfill')
                 ORDER BY job_name
                """
            ).fetchall()
        ]

        missing_rows = conn.execute(
            """
            SELECT photo_id, source_path, filename, dest_path
              FROM photos
             WHERE processed_at IS NOT NULL
               AND ocr_extracted_at IS NULL
             ORDER BY source_path ASC, photo_id ASC
             LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()

        return {
            "api_port": config.API_PORT,
            "test_year_scope": config.TEST_YEAR_SCOPE,
            "processed_photos": processed,
            "clip_ready": clip_ready,
            "ocr_attempted": ocr_attempted,
            "ocr_with_text": ocr_with_text,
            "ocr_missing": processed - ocr_attempted,
            "background_jobs": bg_jobs,
            "ocr_missing_samples": [dict(row) for row in missing_rows],
        }
    finally:
        conn.close()


def _print_text(report: dict[str, Any], show_missing: bool) -> None:
    print("Phase 3 Search State")
    print(f"  API port: {report['api_port']}")
    print(f"  TEST_YEAR_SCOPE: {report['test_year_scope'] or 'off'}")
    print(f"  Processed photos: {report['processed_photos']}")
    print(f"  CLIP ready: {report['clip_ready']} / {report['processed_photos']}")
    print(f"  OCR attempted: {report['ocr_attempted']} / {report['processed_photos']}")
    print(f"  OCR with text: {report['ocr_with_text']} / {report['processed_photos']}")
    print(f"  OCR missing follow-up: {report['ocr_missing']}")

    if report["background_jobs"]:
        print("  Background jobs:")
        for job in report["background_jobs"]:
            progress = f"{job['progress_current']} / {job['progress_total']}" if job["progress_total"] else "-"
            detail = f" ({job['detail']})" if job.get("detail") else ""
            print(f"    - {job['job_name']}: {job['status']} {progress}{detail}")

    if show_missing:
        print("  OCR missing samples:")
        if not report["ocr_missing_samples"]:
            print("    - none")
        else:
            for row in report["ocr_missing_samples"]:
                dest = row["dest_path"] or "-"
                print(f"    - photo_id={row['photo_id']} src={row['source_path']} dest={dest}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Report Phase 3 search-layer state.")
    parser.add_argument(
        "--list-ocr-missing",
        action="store_true",
        help="List photos still missing OCR attempt metadata.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of missing OCR rows to print.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the report as JSON.",
    )
    args = parser.parse_args()

    report = build_report(limit=args.limit)
    if args.json:
        print(json.dumps(report, indent=2))
        return

    _print_text(report, show_missing=args.list_ocr_missing)


if __name__ == "__main__":
    main()
