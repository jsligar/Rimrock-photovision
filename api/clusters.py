"""Cluster review routes."""

import json
from datetime import datetime, timezone
from urllib.parse import quote

import numpy as np
from fastapi import APIRouter, HTTPException

import db
from api.models import (
    AcceptSuggestionRequest,
    ClusterLabel,
    FaceSelectionRequest,
    FaceReassignRequest,
    MergeRequest,
)

router = APIRouter()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_cluster_count(conn, cluster_id: int) -> int:
    count = conn.execute(
        "SELECT COUNT(*) FROM faces WHERE cluster_id=?",
        (cluster_id,),
    ).fetchone()[0]
    conn.execute(
        "UPDATE clusters SET face_count=?, updated_at=? WHERE cluster_id=?",
        (count, _now(), cluster_id),
    )
    return count


def _normalize_face_ids(face_ids: list[int]) -> list[int]:
    return sorted(set(int(fid) for fid in face_ids if fid is not None))


def _target_cluster_label(conn, cluster_id: int) -> str | None:
    row = conn.execute(
        "SELECT person_label FROM clusters WHERE cluster_id=?",
        (cluster_id,),
    ).fetchone()
    if not row:
        return None
    return row["person_label"]


def _decode_embedding(blob: bytes | None) -> np.ndarray | None:
    if not blob:
        return None
    vec = np.frombuffer(blob, dtype=np.float32)
    if vec.size == 0:
        return None
    return vec


def _crop_url_from_path(crop_path: str | None) -> str | None:
    """Normalize DB crop_path values into stable /crops URLs.

    Supports legacy values like:
      - crops/face_123_0.jpg
      - /local/rimrock/photos/crops/face_123_0.jpg
      - face_123_0.jpg
    """
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


def _normalized_centroid(vectors: list[np.ndarray]) -> np.ndarray | None:
    if not vectors:
        return None
    centroid = np.mean(np.vstack(vectors), axis=0)
    norm = float(np.linalg.norm(centroid))
    if norm <= 0:
        return None
    return centroid / norm


def _cluster_centroid(conn, cluster_id: int) -> tuple[np.ndarray | None, int]:
    rows = conn.execute(
        "SELECT embedding FROM faces WHERE cluster_id=?",
        (cluster_id,),
    ).fetchall()
    vectors: list[np.ndarray] = []
    for row in rows:
        vec = _decode_embedding(row["embedding"])
        if vec is not None:
            vectors.append(vec)
    return _normalized_centroid(vectors), len(vectors)


def _labeled_source_centroids(
    conn,
    exclude_cluster_id: int,
    *,
    include_unapproved: bool,
) -> list[dict]:
    approved_clause = "" if include_unapproved else "AND c.approved = 1"
    rows = conn.execute(
        f"""SELECT f.cluster_id, f.embedding, c.person_label, c.approved
           FROM faces f
           JOIN clusters c ON c.cluster_id = f.cluster_id
           WHERE c.person_label IS NOT NULL
             {approved_clause}
             AND c.is_noise = 0
             AND c.cluster_id != ?
           ORDER BY f.cluster_id""",
        (exclude_cluster_id,),
    ).fetchall()
    grouped: dict[int, dict] = {}
    for row in rows:
        cid = int(row["cluster_id"])
        if cid not in grouped:
            grouped[cid] = {
                "cluster_id": cid,
                "person_label": row["person_label"],
                "source_approved": bool(row["approved"]),
                "vectors": [],
            }
        vec = _decode_embedding(row["embedding"])
        if vec is not None:
            grouped[cid]["vectors"].append(vec)

    out = []
    for item in grouped.values():
        centroid = _normalized_centroid(item["vectors"])
        if centroid is None:
            continue
        out.append({
            "cluster_id": item["cluster_id"],
            "person_label": item["person_label"],
            "source_approved": item["source_approved"],
            "centroid": centroid,
            "support_faces": len(item["vectors"]),
        })
    return out


def _dedupe_suggestions_by_person(scored: list[dict]) -> list[dict]:
    """Keep only the best match per person label."""
    best_by_person: dict[str, dict] = {}
    for item in scored:
        key = str(item["person_label"]).strip().lower()
        if not key:
            continue
        prev = best_by_person.get(key)
        if prev is None:
            best_by_person[key] = item
            continue
        if item["score"] > prev["score"]:
            best_by_person[key] = item
            continue
        if item["score"] == prev["score"] and item["source_approved"] and not prev["source_approved"]:
            best_by_person[key] = item

    return sorted(
        best_by_person.values(),
        key=lambda x: (x["score"], x["source_approved"], x["support_faces"]),
        reverse=True,
    )


@router.get("/clusters")
def list_clusters():
    conn = db.get_db()
    rows = conn.execute(
        """SELECT cluster_id, person_label, face_count, is_noise, approved, updated_at
           FROM clusters
           ORDER BY face_count DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/clusters/{cluster_id}/crops")
def get_cluster_crops(cluster_id: int):
    conn = db.get_db()
    faces = conn.execute(
        """SELECT f.face_id, f.crop_path, f.detection_score, f.photo_id, p.filename
           FROM faces f
           JOIN photos p ON f.photo_id = p.photo_id
           WHERE f.cluster_id=?
           ORDER BY f.detection_score DESC
           LIMIT 100""",
        (cluster_id,)
    ).fetchall()
    conn.close()

    crops = []
    for f in faces:
        crops.append({
            "face_id": f["face_id"],
            "crop_url": _crop_url_from_path(f["crop_path"]),
            "detection_score": f["detection_score"],
            "photo_id": f["photo_id"],
            "filename": f["filename"],
        })
    return crops


@router.post("/clusters/{cluster_id}/label")
def label_cluster(cluster_id: int, body: ClusterLabel):
    conn = db.get_db()
    row = conn.execute("SELECT cluster_id FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    conn.execute(
        "UPDATE clusters SET person_label=?, updated_at=? WHERE cluster_id=?",
        (body.person_label, _now(), cluster_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/clusters/{cluster_id}/approve")
def approve_cluster(cluster_id: int):
    conn = db.get_db()
    row = conn.execute("SELECT cluster_id FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    conn.execute(
        "UPDATE clusters SET approved=1, updated_at=? WHERE cluster_id=?",
        (_now(), cluster_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/clusters/{cluster_id}/noise")
def mark_noise(cluster_id: int):
    conn = db.get_db()
    row = conn.execute("SELECT cluster_id FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    conn.execute(
        "UPDATE clusters SET is_noise=1, approved=0, updated_at=? WHERE cluster_id=?",
        (_now(), cluster_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/clusters/{cluster_id}/untag")
def untag_cluster(cluster_id: int):
    conn = db.get_db()
    row = conn.execute("SELECT cluster_id FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    conn.execute(
        "UPDATE clusters SET person_label=NULL, approved=0, updated_at=? WHERE cluster_id=?",
        (_now(), cluster_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/clusters/{cluster_id}/untag-faces")
def untag_selected_faces(cluster_id: int, body: FaceSelectionRequest):
    """Move selected faces into a new unlabeled cluster."""
    conn = db.get_db()
    row = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (cluster_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")

    face_ids = _normalize_face_ids(body.face_ids)
    if not face_ids:
        conn.close()
        raise HTTPException(status_code=400, detail="No face IDs provided")

    placeholders = ",".join("?" for _ in face_ids)
    found_rows = conn.execute(
        f"""SELECT face_id FROM faces
            WHERE cluster_id=?
              AND face_id IN ({placeholders})""",
        [cluster_id, *face_ids],
    ).fetchall()
    if len(found_rows) != len(face_ids):
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="One or more selected faces are not in this cluster",
        )

    max_id = conn.execute("SELECT COALESCE(MAX(cluster_id), 0) FROM clusters").fetchone()[0]
    target_cluster_id = int(max_id) + 1
    conn.execute(
        """INSERT INTO clusters
           (cluster_id, person_label, face_count, is_noise, approved, updated_at)
           VALUES (?, NULL, 0, 0, 0, ?)""",
        (target_cluster_id, _now()),
    )

    moved = conn.execute(
        f"""UPDATE faces
            SET cluster_id=?
            WHERE cluster_id=?
              AND face_id IN ({placeholders})""",
        [target_cluster_id, cluster_id, *face_ids],
    ).rowcount
    if moved != len(face_ids):
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to move all selected faces")

    conn.execute(
        """INSERT INTO face_move_history
           (created_at, source_cluster_id, target_cluster_id, face_ids_json, undone_at)
           VALUES (?, ?, ?, ?, NULL)""",
        (_now(), cluster_id, target_cluster_id, json.dumps(face_ids)),
    )
    move_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    source_count = _update_cluster_count(conn, cluster_id)
    target_count = _update_cluster_count(conn, target_cluster_id)
    if source_count == 0:
        conn.execute("DELETE FROM clusters WHERE cluster_id=?", (cluster_id,))

    conn.commit()
    conn.close()
    return {
        "ok": True,
        "move_id": move_id,
        "moved_faces": moved,
        "source_cluster_id": cluster_id,
        "target_cluster_id": target_cluster_id,
        "target_face_count": target_count,
    }


@router.get("/clusters/{cluster_id}/suggestions")
def cluster_suggestions(cluster_id: int, top_k: int = 3):
    conn = db.get_db()
    cluster = conn.execute(
        """SELECT cluster_id, person_label, approved, is_noise
           FROM clusters
           WHERE cluster_id=?""",
        (cluster_id,),
    ).fetchone()
    if not cluster:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster["is_noise"]:
        conn.close()
        return {"cluster_id": cluster_id, "suggestions": [], "reason": "noise_cluster"}

    target_centroid, target_faces = _cluster_centroid(conn, cluster_id)
    if target_centroid is None:
        conn.close()
        return {"cluster_id": cluster_id, "suggestions": [], "reason": "no_embeddings"}

    top_k = max(1, min(int(top_k), 10))

    approved_sources = _labeled_source_centroids(
        conn,
        exclude_cluster_id=cluster_id,
        include_unapproved=False,
    )
    sources = list(approved_sources)
    source_pool = "approved_only"

    # If approved labels are too narrow, include labeled (pending) sources for diversity.
    approved_labels = {
        str(s["person_label"]).strip().lower() for s in approved_sources if s.get("person_label")
    }
    if not sources or len(approved_labels) < min(2, top_k):
        all_labeled_sources = _labeled_source_centroids(
            conn,
            exclude_cluster_id=cluster_id,
            include_unapproved=True,
        )
        if not all_labeled_sources:
            conn.close()
            return {"cluster_id": cluster_id, "suggestions": [], "reason": "no_labeled_labels"}

        existing_cluster_ids = {int(s["cluster_id"]) for s in sources}
        for item in all_labeled_sources:
            if int(item["cluster_id"]) in existing_cluster_ids:
                continue
            # Only backfill labels not already represented by approved suggestions.
            label_key = str(item["person_label"]).strip().lower()
            if label_key in approved_labels:
                continue
            sources.append(item)
            existing_cluster_ids.add(int(item["cluster_id"]))
        if approved_sources:
            source_pool = "approved_plus_labeled"
        else:
            source_pool = "labeled_only"

    if not sources:
        conn.close()
        return {"cluster_id": cluster_id, "suggestions": [], "reason": "no_approved_labels"}

    scored = []
    for source in sources:
        score = float(np.dot(target_centroid, source["centroid"]))
        scored.append({
            "person_label": source["person_label"],
            "source_cluster_id": source["cluster_id"],
            "score": score,
            "support_faces": source["support_faces"],
            "source_approved": bool(source["source_approved"]),
        })
    top = _dedupe_suggestions_by_person(scored)[:top_k]
    if not top:
        conn.close()
        return {"cluster_id": cluster_id, "suggestions": [], "reason": "no_labeled_embeddings"}

    second_score = top[1]["score"] if len(top) > 1 else -1.0
    for i, item in enumerate(top):
        item["margin"] = round(item["score"] - (second_score if i == 0 else top[0]["score"]), 4)
        item["score"] = round(item["score"], 4)
        item["recommended"] = bool(
            i == 0
            and item["source_approved"]
            and item["score"] >= 0.45
            and item["margin"] >= 0.03
        )

    conn.close()
    return {
        "cluster_id": cluster_id,
        "target_face_count": target_faces,
        "cluster_labeled": bool(cluster["person_label"]),
        "source_pool": source_pool,
        "suggestions": top,
    }


@router.post("/clusters/{cluster_id}/accept-suggestion")
def accept_cluster_suggestion(cluster_id: int, body: AcceptSuggestionRequest):
    person_label = (body.person_label or "").strip()
    if not person_label:
        raise HTTPException(status_code=400, detail="person_label is required")

    conn = db.get_db()
    cluster = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (cluster_id,),
    ).fetchone()
    if not cluster:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")

    conn.execute(
        "UPDATE clusters SET person_label=?, approved=0, is_noise=0, updated_at=? WHERE cluster_id=?",
        (person_label, _now(), cluster_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "cluster_id": cluster_id, "person_label": person_label}


@router.post("/clusters/reassign-faces")
def reassign_faces(body: FaceReassignRequest):
    conn = db.get_db()

    source = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (body.source_cluster_id,),
    ).fetchone()
    if not source:
        conn.close()
        raise HTTPException(status_code=404, detail="Source cluster not found")

    face_ids = _normalize_face_ids(body.face_ids)
    if not face_ids:
        conn.close()
        raise HTTPException(status_code=400, detail="No face IDs provided")

    placeholders = ",".join("?" for _ in face_ids)
    rows = conn.execute(
        f"""SELECT face_id FROM faces
            WHERE cluster_id=?
              AND face_id IN ({placeholders})""",
        [body.source_cluster_id, *face_ids],
    ).fetchall()
    if len(rows) != len(face_ids):
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="One or more selected faces are not in the source cluster",
        )

    target_cluster_id = body.target_cluster_id
    target_person_label = (body.target_person_label or "").strip()

    has_target_cluster = target_cluster_id is not None
    has_target_label = bool(target_person_label)
    if has_target_cluster and has_target_label:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Provide only one destination: target_cluster_id or target_person_label",
        )

    if has_target_cluster:
        target = conn.execute(
            "SELECT cluster_id FROM clusters WHERE cluster_id=?",
            (target_cluster_id,),
        ).fetchone()
        if not target:
            conn.close()
            raise HTTPException(status_code=404, detail="Target cluster not found")
    elif has_target_label:
        existing = conn.execute(
            """SELECT cluster_id
               FROM clusters
               WHERE person_label IS NOT NULL
                 AND lower(person_label)=lower(?)
                 AND is_noise=0
               ORDER BY approved DESC, face_count DESC
               LIMIT 1""",
            (target_person_label,),
        ).fetchone()
        if existing:
            target_cluster_id = existing["cluster_id"]
        else:
            max_id = conn.execute(
                "SELECT COALESCE(MAX(cluster_id), 0) FROM clusters",
            ).fetchone()[0]
            target_cluster_id = int(max_id) + 1
            conn.execute(
                """INSERT INTO clusters
                   (cluster_id, person_label, face_count, is_noise, approved, updated_at)
                   VALUES (?, ?, 0, 0, 0, ?)""",
                (target_cluster_id, target_person_label, _now()),
            )
    else:
        conn.close()
        raise HTTPException(
            status_code=400,
            detail="Provide target_cluster_id or target_person_label",
        )

    if target_cluster_id == body.source_cluster_id:
        conn.close()
        raise HTTPException(status_code=400, detail="Target cannot be the source cluster")

    moved = conn.execute(
        f"""UPDATE faces
            SET cluster_id=?
            WHERE cluster_id=?
              AND face_id IN ({placeholders})""",
        [target_cluster_id, body.source_cluster_id, *face_ids],
    ).rowcount
    if moved != len(face_ids):
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to move all selected faces")

    # Keep destination cluster as active/non-noise after manual reassignment.
    conn.execute(
        "UPDATE clusters SET is_noise=0, updated_at=? WHERE cluster_id=?",
        (_now(), target_cluster_id),
    )
    conn.execute(
        """INSERT INTO face_move_history
           (created_at, source_cluster_id, target_cluster_id, face_ids_json, undone_at)
           VALUES (?, ?, ?, ?, NULL)""",
        (_now(), body.source_cluster_id, target_cluster_id, json.dumps(face_ids)),
    )
    move_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    target_count = _update_cluster_count(conn, target_cluster_id)
    source_count = conn.execute(
        "SELECT COUNT(*) FROM faces WHERE cluster_id=?",
        (body.source_cluster_id,),
    ).fetchone()[0]
    if source_count == 0:
        conn.execute("DELETE FROM clusters WHERE cluster_id=?", (body.source_cluster_id,))
    else:
        _update_cluster_count(conn, body.source_cluster_id)

    conn.commit()
    conn.close()
    return {
        "ok": True,
        "move_id": move_id,
        "moved_faces": moved,
        "source_cluster_id": body.source_cluster_id,
        "target_cluster_id": target_cluster_id,
        "target_person_label": target_person_label or None,
        "target_face_count": target_count,
    }


@router.post("/clusters/reassign-faces/undo-last")
def undo_last_face_reassign():
    conn = db.get_db()
    move = conn.execute(
        """SELECT move_id, source_cluster_id, target_cluster_id, face_ids_json
           FROM face_move_history
           WHERE undone_at IS NULL
           ORDER BY move_id DESC
           LIMIT 1"""
    ).fetchone()
    if not move:
        conn.close()
        raise HTTPException(status_code=404, detail="No face move available to undo")

    source_cluster_id = int(move["source_cluster_id"])
    target_cluster_id = int(move["target_cluster_id"])
    face_ids = _normalize_face_ids(json.loads(move["face_ids_json"] or "[]"))
    if not face_ids:
        conn.close()
        raise HTTPException(status_code=400, detail="Move history is empty")

    source_row = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (source_cluster_id,),
    ).fetchone()
    if not source_row:
        conn.execute(
            """INSERT INTO clusters
               (cluster_id, person_label, face_count, is_noise, approved, updated_at)
               VALUES (?, NULL, 0, 0, 0, ?)""",
            (source_cluster_id, _now()),
        )

    target_row = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (target_cluster_id,),
    ).fetchone()
    if not target_row:
        conn.close()
        raise HTTPException(status_code=409, detail="Target cluster no longer exists")

    placeholders = ",".join("?" for _ in face_ids)
    moved_back = conn.execute(
        f"""UPDATE faces
            SET cluster_id=?
            WHERE cluster_id=?
              AND face_id IN ({placeholders})""",
        [source_cluster_id, target_cluster_id, *face_ids],
    ).rowcount
    if moved_back == 0:
        conn.close()
        raise HTTPException(status_code=409, detail="Selected faces are no longer in target cluster")

    _update_cluster_count(conn, source_cluster_id)
    target_count = conn.execute(
        "SELECT COUNT(*) FROM faces WHERE cluster_id=?",
        (target_cluster_id,),
    ).fetchone()[0]
    if target_count == 0:
        conn.execute("DELETE FROM clusters WHERE cluster_id=?", (target_cluster_id,))
    else:
        _update_cluster_count(conn, target_cluster_id)

    conn.execute(
        "UPDATE face_move_history SET undone_at=? WHERE move_id=?",
        (_now(), move["move_id"]),
    )
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "move_id": move["move_id"],
        "moved_faces": moved_back,
        "source_cluster_id": source_cluster_id,
        "target_cluster_id": target_cluster_id,
    }


@router.post("/clusters/merge")
def merge_clusters(body: MergeRequest):
    """Merge source cluster into target cluster."""
    conn = db.get_db()

    target = conn.execute(
        "SELECT cluster_id, person_label FROM clusters WHERE cluster_id=?",
        (body.target_cluster_id,)
    ).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="Target cluster not found")

    source = conn.execute(
        "SELECT cluster_id FROM clusters WHERE cluster_id=?",
        (body.source_cluster_id,)
    ).fetchone()
    if not source:
        conn.close()
        raise HTTPException(status_code=404, detail="Source cluster not found")

    # Re-assign all faces from source to target
    conn.execute(
        "UPDATE faces SET cluster_id=? WHERE cluster_id=?",
        (body.target_cluster_id, body.source_cluster_id)
    )

    # Update target face count
    new_count = conn.execute(
        "SELECT COUNT(*) FROM faces WHERE cluster_id=?",
        (body.target_cluster_id,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE clusters SET face_count=?, updated_at=? WHERE cluster_id=?",
        (new_count, _now(), body.target_cluster_id)
    )

    # Remove source cluster
    conn.execute("DELETE FROM clusters WHERE cluster_id=?", (body.source_cluster_id,))

    conn.commit()
    conn.close()
    return {"ok": True, "merged_into": body.target_cluster_id, "new_face_count": new_count}
