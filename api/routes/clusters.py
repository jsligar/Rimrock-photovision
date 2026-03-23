"""Cluster review routes."""

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException

import config
import db
from api.models import ClusterLabel, MergeRequest

router = APIRouter()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
            "crop_url": f"/crops/{f['crop_path']}" if f["crop_path"] else None,
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
