"""Photo browser routes."""

from __future__ import annotations

import json
from urllib.parse import quote

from fastapi import APIRouter, HTTPException

import config
import db
from api.search_utils import build_fts_match_query

router = APIRouter()


def _asset_url(prefix: str, relative_path: str | None) -> str | None:
    if not relative_path:
        return None

    raw = str(relative_path).strip().replace("\\", "/")
    if not raw:
        return None

    parts = [part for part in raw.split("/") if part]
    if not parts or any(part in (".", "..") for part in parts):
        return None

    encoded = "/".join(quote(part) for part in parts)
    return f"/{prefix}/{encoded}"


def _crop_url_from_path(crop_path: str | None) -> str | None:
    if not crop_path:
        return None

    raw = str(crop_path).strip().replace("\\", "/")
    if not raw:
        return None

    marker = "/crops/"
    if marker in raw:
        raw = raw.split(marker, 1)[1]
    raw = raw.lstrip("/")
    if raw.startswith("crops/"):
        raw = raw[len("crops/"):]

    parts = [part for part in raw.split("/") if part and part not in (".", "..")]
    if not parts:
        return None

    encoded = "/".join(quote(part) for part in parts)
    return f"/crops/{encoded}"


@router.get("/photos")
def browse_photos(
    person: str | None = None,
    tag: str | None = None,
    tags: str | None = None,
    tags_any: str | None = None,
    year: str | None = None,
    month: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    q: str | None = None,
    min_confidence: float | None = None,
    has_faces: bool | None = None,
    undated: bool = False,
    page: int = 1,
    per_page: int = 60,
):
    page = max(1, page)
    per_page = min(max(1, per_page), 500)
    offset = (page - 1) * per_page

    conn = db.get_db()
    try:
        where_clauses: list[str] = []
        params: list = []

        if undated:
            where_clauses.append("(p.dest_path LIKE ? OR (p.exif_date IS NULL AND p.dest_path IS NOT NULL))")
            params.append(f"{config.UNDATED_DIR}/%")
        elif year:
            where_clauses.append("p.exif_date LIKE ?")
            params.append(f"{year}-{month}%" if month else f"{year}%")

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

        if config.ENABLE_SEARCH_LAYER:
            if tags:
                for item in (item.strip() for item in tags.split(",") if item.strip()):
                    where_clauses.append(
                        "EXISTS (SELECT 1 FROM photo_tags pt WHERE pt.photo_id = p.photo_id AND pt.tag = ?)"
                    )
                    params.append(item)

            if tags_any:
                tag_list = [item.strip() for item in tags_any.split(",") if item.strip()]
                if tag_list:
                    placeholders = ",".join("?" * len(tag_list))
                    where_clauses.append(
                        f"EXISTS (SELECT 1 FROM photo_tags pt WHERE pt.photo_id = p.photo_id AND pt.tag IN ({placeholders}))"
                    )
                    params.extend(tag_list)

            if date_from:
                where_clauses.append("p.exif_date >= ?")
                params.append(date_from)

            if date_to:
                where_clauses.append("p.exif_date <= ?")
                params.append(date_to + "T23:59:59")

            if q:
                match_query = build_fts_match_query(q)
                if match_query:
                    where_clauses.append(
                        "p.photo_id IN (SELECT rowid FROM photos_fts WHERE photos_fts MATCH ?)"
                    )
                    params.append(match_query)
                else:
                    where_clauses.append("1=0")

            if min_confidence is not None:
                where_clauses.append(
                    "EXISTS (SELECT 1 FROM detections d WHERE d.photo_id = p.photo_id AND d.confidence >= ?)"
                )
                params.append(min_confidence)

            if has_faces is not None:
                if has_faces:
                    where_clauses.append("EXISTS (SELECT 1 FROM faces f WHERE f.photo_id = p.photo_id)")
                else:
                    where_clauses.append("NOT EXISTS (SELECT 1 FROM faces f WHERE f.photo_id = p.photo_id)")

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

        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "photos": [dict(row) for row in rows],
        }
    finally:
        conn.close()


@router.get("/photo-filters")
def get_photo_filters():
    conn = db.get_db()
    try:
        people_rows = conn.execute(
            """
            SELECT c.person_label AS person,
                   COUNT(DISTINCT f.photo_id) AS photo_count
            FROM faces f
            JOIN clusters c ON f.cluster_id = c.cluster_id
            WHERE c.approved = 1
              AND c.is_noise = 0
              AND c.person_label IS NOT NULL
              AND TRIM(c.person_label) <> ''
            GROUP BY c.person_label
            ORDER BY LOWER(c.person_label) ASC
            """
        ).fetchall()

        tag_rows = conn.execute(
            """
            SELECT pt.tag,
                   COUNT(DISTINCT pt.photo_id) AS photo_count
            FROM photo_tags pt
            WHERE TRIM(pt.tag) <> ''
            GROUP BY pt.tag
            ORDER BY LOWER(pt.tag) ASC
            """
        ).fetchall()

        return {
            "people": [
                {"person": row["person"], "photo_count": row["photo_count"]}
                for row in people_rows
            ],
            "tags": [
                {"tag": row["tag"], "photo_count": row["photo_count"]}
                for row in tag_rows
            ],
        }
    finally:
        conn.close()


@router.get("/photos/{photo_id}")
def get_photo(photo_id: int):
    conn = db.get_db()
    try:
        photo = conn.execute(
            "SELECT * FROM photos WHERE photo_id=?",
            (photo_id,),
        ).fetchone()
        if not photo:
            raise HTTPException(status_code=404, detail="Photo not found")

        photo_dict = dict(photo)
        photo_dict.pop("clip_embedding", None)

        faces = conn.execute(
            """SELECT f.face_id, f.bbox_json, f.detection_score, f.crop_path,
                      f.is_ground_truth, f.cluster_id,
                      c.person_label, c.approved
               FROM faces f
               LEFT JOIN clusters c ON f.cluster_id = c.cluster_id
               WHERE f.photo_id=?""",
            (photo_id,),
        ).fetchall()

        face_list = []
        for face in faces:
            face_list.append({
                "face_id": face["face_id"],
                "bbox": json.loads(face["bbox_json"]) if face["bbox_json"] else None,
                "detection_score": face["detection_score"],
                "crop_url": _crop_url_from_path(face["crop_path"]),
                "is_ground_truth": bool(face["is_ground_truth"]),
                "cluster_id": face["cluster_id"],
                "person_label": face["person_label"],
                "cluster_approved": bool(face["approved"]) if face["approved"] is not None else False,
            })

        detections = conn.execute(
            """SELECT detection_id, model, tag, tag_group, confidence, bbox_json, crop_path, approved
               FROM detections WHERE photo_id=?""",
            (photo_id,),
        ).fetchall()
        detection_list = []
        for detection in detections:
            detection_list.append({
                "detection_id": detection["detection_id"],
                "model": detection["model"],
                "tag": detection["tag"],
                "tag_group": detection["tag_group"],
                "confidence": detection["confidence"],
                "bbox": json.loads(detection["bbox_json"]) if detection["bbox_json"] else None,
                "crop_url": _crop_url_from_path(detection["crop_path"]),
                "approved": bool(detection["approved"]),
            })

        tags = conn.execute(
            "SELECT tag, source FROM photo_tags WHERE photo_id=?",
            (photo_id,),
        ).fetchall()

        photo_dict["faces"] = face_list
        photo_dict["detections"] = detection_list
        photo_dict["tags"] = [{"tag": row["tag"], "source": row["source"]} for row in tags]
        if photo_dict.get("dest_path"):
            photo_dict["preview_url"] = _asset_url("organized", photo_dict["dest_path"])
        else:
            photo_dict["preview_url"] = _asset_url("originals", photo_dict["source_path"])

        return photo_dict
    finally:
        conn.close()
