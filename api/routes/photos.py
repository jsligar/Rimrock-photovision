"""Photo browser routes."""

import json

from fastapi import APIRouter, HTTPException

import config
import db

router = APIRouter()


@router.get("/photos")
def browse_photos(
    person: str | None = None,
    tag: str | None = None,
    year: str | None = None,
    month: str | None = None,
    undated: bool = False,
    page: int = 1,
    per_page: int = 60,
):
    page = max(1, page)
    per_page = min(max(1, per_page), 500)
    conn = db.get_db()
    offset = (page - 1) * per_page

    where_clauses = []
    params: list = []

    if undated:
        where_clauses.append("(p.dest_path LIKE ? OR (p.exif_date IS NULL AND p.dest_path IS NOT NULL))")
        params.append(f"{config.UNDATED_DIR}/%")
    elif year:
        where_clauses.append("p.exif_date LIKE ?")
        if month:
            params.append(f"{year}-{month}%")
        else:
            params.append(f"{year}%")

    if person:
        where_clauses.append(
            """EXISTS (
                SELECT 1 FROM faces f2
                JOIN clusters c2 ON f2.cluster_id = c2.cluster_id
                WHERE f2.photo_id = p.photo_id
                  AND c2.person_label = ?
                  AND c2.approved = 1
            )"""
        )
        params.append(person)

    if tag:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM photo_tags pt WHERE pt.photo_id = p.photo_id AND pt.tag = ?)"
        )
        params.append(tag)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    count_sql = f"SELECT COUNT(*) FROM photos p {where_sql}"
    total = conn.execute(count_sql, params).fetchone()[0]

    data_sql = f"""
        SELECT p.photo_id, p.source_path, p.filename, p.exif_date,
               p.date_source, p.dest_path, p.copy_verified, p.processed_at
        FROM photos p
        {where_sql}
        ORDER BY p.exif_date ASC, p.filename ASC
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(data_sql, params + [per_page, offset]).fetchall()
    conn.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "photos": [dict(r) for r in rows],
    }


@router.get("/photos/{photo_id}")
def get_photo(photo_id: int):
    conn = db.get_db()
    photo = conn.execute(
        "SELECT * FROM photos WHERE photo_id=?", (photo_id,)
    ).fetchone()
    if not photo:
        conn.close()
        raise HTTPException(status_code=404, detail="Photo not found")

    photo_dict = dict(photo)

    # Face annotations
    faces = conn.execute(
        """SELECT f.face_id, f.bbox_json, f.detection_score, f.crop_path,
                  f.is_ground_truth, f.cluster_id,
                  c.person_label, c.approved
           FROM faces f
           LEFT JOIN clusters c ON f.cluster_id = c.cluster_id
           WHERE f.photo_id=?""",
        (photo_id,)
    ).fetchall()

    face_list = []
    for f in faces:
        face_list.append({
            "face_id": f["face_id"],
            "bbox": json.loads(f["bbox_json"]) if f["bbox_json"] else None,
            "detection_score": f["detection_score"],
            "crop_url": f"/crops/{f['crop_path']}" if f["crop_path"] else None,
            "is_ground_truth": bool(f["is_ground_truth"]),
            "cluster_id": f["cluster_id"],
            "person_label": f["person_label"],
            "cluster_approved": bool(f["approved"]) if f["approved"] is not None else False,
        })

    # Object detections
    detections = conn.execute(
        """SELECT detection_id, model, tag, tag_group, confidence, bbox_json, crop_path, approved
           FROM detections WHERE photo_id=?""",
        (photo_id,)
    ).fetchall()
    det_list = []
    for d in detections:
        det_list.append({
            "detection_id": d["detection_id"],
            "model": d["model"],
            "tag": d["tag"],
            "tag_group": d["tag_group"],
            "confidence": d["confidence"],
            "bbox": json.loads(d["bbox_json"]) if d["bbox_json"] else None,
            "crop_url": f"/crops/{d['crop_path']}" if d["crop_path"] else None,
            "approved": bool(d["approved"]),
        })

    # All tags
    tags = conn.execute(
        "SELECT tag, source FROM photo_tags WHERE photo_id=?", (photo_id,)
    ).fetchall()
    tag_list = [{"tag": t["tag"], "source": t["source"]} for t in tags]

    photo_dict["faces"] = face_list
    photo_dict["detections"] = det_list
    photo_dict["tags"] = tag_list

    # Build preview URL
    if photo_dict.get("dest_path"):
        photo_dict["preview_url"] = f"/organized/{photo_dict['dest_path']}"
    else:
        photo_dict["preview_url"] = f"/originals/{photo_dict['source_path']}"

    conn.close()
    return photo_dict
