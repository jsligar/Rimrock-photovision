"""Status routes — pipeline phase status."""

from fastapi import APIRouter

import db

router = APIRouter()


@router.get("/status")
def get_all_status():
    conn = db.get_db()
    rows = conn.execute(
        "SELECT * FROM pipeline_state ORDER BY ROWID"
    ).fetchall()
    conn.close()

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

    # Extra summary counts
    conn = db.get_db()
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

    return {"phases": phases, "counts": counts}


def _count(conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return row[0] if row else 0
