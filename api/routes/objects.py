"""Objects & Pets routes — YOLO/CLIP detections and vocabulary management."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

import db
from api.models import VocabEntry

router = APIRouter()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sync_photo_tag_from_detection(conn, photo_id: int, tag: str, source: str) -> None:
    """Mirror detection approval state into photo_tags for this source."""
    approved_count = conn.execute(
        """SELECT COUNT(*) FROM detections
           WHERE photo_id=? AND tag=? AND model=? AND approved=1""",
        (photo_id, tag, source),
    ).fetchone()[0]

    if approved_count > 0:
        conn.execute(
            "INSERT OR IGNORE INTO photo_tags (photo_id, tag, source) VALUES (?, ?, ?)",
            (photo_id, tag, source),
        )
    else:
        conn.execute(
            "DELETE FROM photo_tags WHERE photo_id=? AND tag=? AND source=?",
            (photo_id, tag, source),
        )


@router.get("/objects/tags")
def list_tags():
    conn = db.get_db()
    rows = conn.execute(
        """SELECT d.tag, d.tag_group,
                  COUNT(DISTINCT d.photo_id) as photo_count,
                  GROUP_CONCAT(DISTINCT d.model) as sources
           FROM detections d
           WHERE d.approved=1
           GROUP BY d.tag, d.tag_group
           ORDER BY d.tag_group, photo_count DESC"""
    ).fetchall()
    conn.close()

    grouped: dict = {}
    for r in rows:
        group = r["tag_group"] or "other"
        if group not in grouped:
            grouped[group] = []
        grouped[group].append({
            "tag": r["tag"],
            "tag_group": r["tag_group"],
            "photo_count": r["photo_count"],
            "sources": r["sources"].split(",") if r["sources"] else [],
        })
    return grouped


@router.get("/objects/tags/{tag}")
def get_photos_by_tag(tag: str, page: int = 1, per_page: int = 50):
    conn = db.get_db()
    offset = (page - 1) * per_page

    total_row = conn.execute(
        "SELECT COUNT(DISTINCT photo_id) FROM detections WHERE tag=? AND approved=1",
        (tag,)
    ).fetchone()
    total = total_row[0] if total_row else 0

    photos = conn.execute(
        """SELECT DISTINCT p.photo_id, p.source_path, p.filename, p.exif_date, p.dest_path
           FROM detections d
           JOIN photos p ON d.photo_id = p.photo_id
           WHERE d.tag=? AND d.approved=1
           ORDER BY p.exif_date
           LIMIT ? OFFSET ?""",
        (tag, per_page, offset)
    ).fetchall()
    conn.close()

    return {
        "tag": tag,
        "total": total,
        "page": page,
        "per_page": per_page,
        "photos": [dict(r) for r in photos],
    }


@router.post("/objects/detections/{detection_id}/reject")
def reject_detection(detection_id: int):
    conn = db.get_db()
    row = conn.execute(
        "SELECT detection_id, photo_id, tag, model FROM detections WHERE detection_id=?",
        (detection_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Detection not found")
    conn.execute("UPDATE detections SET approved=0 WHERE detection_id=?", (detection_id,))
    _sync_photo_tag_from_detection(conn, int(row["photo_id"]), row["tag"], row["model"])
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/objects/detections/{detection_id}/approve")
def approve_detection(detection_id: int):
    conn = db.get_db()
    row = conn.execute(
        "SELECT detection_id, photo_id, tag, model FROM detections WHERE detection_id=?",
        (detection_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Detection not found")
    conn.execute("UPDATE detections SET approved=1 WHERE detection_id=?", (detection_id,))
    _sync_photo_tag_from_detection(conn, int(row["photo_id"]), row["tag"], row["model"])
    conn.commit()
    conn.close()
    return {"ok": True}


@router.get("/objects/vocabulary")
def get_vocabulary():
    conn = db.get_db()
    rows = conn.execute(
        "SELECT vocab_id, tag_group, tag_name, prompts_json, enabled, created_at FROM tag_vocabulary ORDER BY tag_group, tag_name"
    ).fetchall()
    conn.close()

    vocab = []
    for r in rows:
        vocab.append({
            "vocab_id": r["vocab_id"],
            "tag_group": r["tag_group"],
            "tag_name": r["tag_name"],
            "prompts": json.loads(r["prompts_json"]),
            "enabled": bool(r["enabled"]),
            "created_at": r["created_at"],
        })
    return vocab


@router.post("/objects/vocabulary")
def add_vocabulary(entry: VocabEntry):
    conn = db.get_db()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO tag_vocabulary
               (tag_group, tag_name, prompts_json, enabled, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (entry.tag_group, entry.tag_name, json.dumps(entry.prompts),
             1 if entry.enabled else 0, _now())
        )
        conn.commit()
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))
    conn.close()
    return {"ok": True}


@router.delete("/objects/vocabulary/{vocab_id}")
def delete_vocabulary(vocab_id: int):
    conn = db.get_db()
    row = conn.execute("SELECT vocab_id FROM tag_vocabulary WHERE vocab_id=?", (vocab_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Vocabulary entry not found")
    conn.execute("DELETE FROM tag_vocabulary WHERE vocab_id=?", (vocab_id,))
    conn.commit()
    conn.close()
    return {"ok": True}
