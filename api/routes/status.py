"""Status routes — pipeline phase status."""

import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter

import config
import db
import nvidia_burst

router = APIRouter()


@router.get("/status")
def get_all_status():
    conn = db.get_db()
    db.reconcile_background_jobs(conn)
    rows = conn.execute("SELECT * FROM pipeline_state ORDER BY ROWID").fetchall()
    bg_rows = conn.execute("SELECT * FROM background_jobs ORDER BY job_name").fetchall()

    phases = []
    for r in rows:
        phases.append({
            "phase": r["phase"],
            "status": r["status"],
            "raw_status": r["status"],
            "progress_current": r["progress_current"],
            "progress_total": r["progress_total"],
            "started_at": r["started_at"],
            "completed_at": r["completed_at"],
            "error_message": r["error_message"],
            "is_stale": False,
            "stale_reason": None,
        })
    _mark_stale_phase_outputs(phases)

    background_jobs = []
    for r in bg_rows:
        background_jobs.append({
            "job_name": r["job_name"],
            "status": r["status"],
            "progress_current": r["progress_current"],
            "progress_total": r["progress_total"],
            "started_at": r["started_at"],
            "updated_at": r["updated_at"],
            "completed_at": r["completed_at"],
            "error_message": r["error_message"],
            "detail": r["detail"],
        })

    # Extra summary counts
    counts = {
        "total_photos": _count(conn, "SELECT COUNT(*) FROM photos"),
        "total_faces": _count(conn, "SELECT COUNT(*) FROM faces"),
        "total_clusters": _count(conn, "SELECT COUNT(*) FROM clusters WHERE is_noise=0"),
        "labeled_clusters": _count(conn, "SELECT COUNT(*) FROM clusters WHERE person_label IS NOT NULL AND is_noise=0"),
        "approved_clusters": _count(conn, "SELECT COUNT(*) FROM clusters WHERE approved=1 AND is_noise=0"),
        "pending_clusters": _count(
            conn,
            """
            SELECT COUNT(*)
              FROM clusters
             WHERE is_noise=0
               AND (person_label IS NULL OR trim(person_label)='')
            """,
        ),
        "noise_clusters": _count(conn, "SELECT COUNT(*) FROM clusters WHERE is_noise=1"),
        "labeled_people": _count(
            conn,
            """
            SELECT COUNT(*)
              FROM (
                    SELECT lower(trim(person_label)) AS person_key
                      FROM clusters
                     WHERE is_noise=0
                       AND person_label IS NOT NULL
                       AND trim(person_label) <> ''
                  GROUP BY lower(trim(person_label))
                   )
            """,
        ),
        "total_detections": _count(conn, "SELECT COUNT(*) FROM detections"),
        "photos_organized": _count(conn, "SELECT COUNT(*) FROM photos WHERE copy_verified=1"),
    }
    if getattr(config, "ENABLE_SEARCH_LAYER", False):
        counts.update(
            {
                "document_photos": _count(conn, "SELECT COUNT(*) FROM photos WHERE is_document=1"),
                "document_photos_ocr_complete": _count(
                    conn,
                    """
                    SELECT COUNT(*)
                      FROM photos
                     WHERE is_document=1
                       AND ocr_text IS NOT NULL
                       AND TRIM(ocr_text) <> ''
                    """,
                ),
                "pending_ocr_documents": _count(
                    conn,
                    """
                    SELECT COUNT(*)
                      FROM photos
                     WHERE is_document=1
                       AND (ocr_text IS NULL OR TRIM(ocr_text) = '')
                    """,
                ),
            }
        )
    else:
        counts.update(
            {
                "document_photos": 0,
                "document_photos_ocr_complete": 0,
                "pending_ocr_documents": 0,
            }
        )
    workflow = _workflow_summary(conn)
    conn.close()

    return {
        "phases": phases,
        "background_jobs": background_jobs,
        "counts": counts,
        "workflow": workflow,
        "nvidia": nvidia_burst.get_status_summary(),
        "sidebar": _sidebar_summary(),
    }


def _count(conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return row[0] if row else 0


def _sidebar_summary() -> dict:
    manifest_path = Path(config.BATCH_MANIFEST_PATH) if config.BATCH_MANIFEST_PATH else None
    return {
        "batch_manifest_active": manifest_path is not None,
        "batch_manifest_path": str(manifest_path) if manifest_path else None,
        "batch_manifest_name": manifest_path.name if manifest_path else None,
        "test_year_scope": config.TEST_YEAR_SCOPE,
        "search_layer_enabled": bool(config.ENABLE_SEARCH_LAYER),
        "search_ocr_enabled": bool(config.SEARCH_OCR_ENABLED),
        "nvidia_feature_enabled": bool(config.NVIDIA_BURST_ENABLED),
    }


def _workflow_summary(conn) -> dict:
    values = dict(
        conn.execute(
            """
            SELECT key, value
              FROM pipeline_meta
             WHERE key IN ('active_workflow_name', 'active_workflow_steps', 'active_workflow_started_at')
            """
        ).fetchall()
    )
    name = (values.get("active_workflow_name") or "").strip()
    raw_steps = values.get("active_workflow_steps") or "[]"
    try:
        steps = json.loads(raw_steps)
    except json.JSONDecodeError:
        steps = []
    return {
        "active": bool(name),
        "name": name or None,
        "steps": steps if isinstance(steps, list) else [],
        "started_at": (values.get("active_workflow_started_at") or "").strip() or None,
    }


def _mark_stale_phase_outputs(phases: list[dict]) -> None:
    phase_map = {phase["phase"]: phase for phase in phases}

    _mark_stale_if_upstream_newer(
        phase_map,
        phase="tag",
        upstream="cluster",
        reason="Cluster changed after tagging. Rerun Tag to refresh XMP metadata.",
    )
    _mark_stale_if_upstream_newer(
        phase_map,
        phase="push",
        upstream="tag",
        reason="Tag output changed after the last push. Rerun Push to sync the latest metadata.",
    )
    _mark_stale_if_upstream_newer(
        phase_map,
        phase="verify",
        upstream="push",
        reason="Push changed after the last verify. Rerun Verify to confirm the latest sync.",
    )


def _mark_stale_if_upstream_newer(
    phase_map: dict[str, dict],
    *,
    phase: str,
    upstream: str,
    reason: str,
) -> None:
    downstream = phase_map.get(phase)
    upstream_phase = phase_map.get(upstream)
    if not downstream or not upstream_phase:
        return

    if downstream["status"] != "complete":
        return

    # An errored or still-running upstream hasn't produced valid output —
    # don't flag downstream as stale based on its completed_at timestamp.
    if upstream_phase.get("status") in ("error", "running"):
        return

    if upstream_phase.get("is_stale"):
        downstream["raw_status"] = downstream["status"]
        downstream["status"] = "pending"
        downstream["is_stale"] = True
        downstream["stale_reason"] = reason
        return

    upstream_completed = _parse_iso8601(upstream_phase.get("completed_at"))
    downstream_completed = _parse_iso8601(downstream.get("completed_at"))
    if not upstream_completed or not downstream_completed:
        return

    if upstream_completed <= downstream_completed:
        return

    downstream["raw_status"] = downstream["status"]
    downstream["status"] = "pending"
    downstream["is_stale"] = True
    downstream["stale_reason"] = reason


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
