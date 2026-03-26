"""Status routes — pipeline phase status."""

from fastapi import APIRouter

import db

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
            "progress_current": r["progress_current"],
            "progress_total": r["progress_total"],
            "started_at": r["started_at"],
            "completed_at": r["completed_at"],
            "error_message": r["error_message"],
        })

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
        "total_detections": _count(conn, "SELECT COUNT(*) FROM detections"),
        "photos_organized": _count(conn, "SELECT COUNT(*) FROM photos WHERE copy_verified=1"),
    }
    conn.close()

    return {"phases": phases, "background_jobs": background_jobs, "counts": counts}


def _count(conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return row[0] if row else 0
