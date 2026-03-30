"""Persistent person prototype memory stored outside the working photo DB."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

import config

log = logging.getLogger(__name__)


def _memory_path(path: Path | None = None) -> Path:
    return Path(path or config.PERSON_MEMORY_PATH)


def _person_label_key(label: str | None) -> str:
    return str(label or "").strip().lower()


def _decode_embedding(blob: bytes | None) -> np.ndarray | None:
    if not blob:
        return None
    vec = np.frombuffer(blob, dtype=np.float32)
    if vec.size == 0:
        return None
    return vec


def _normalized_centroid(vectors: list[np.ndarray]) -> np.ndarray | None:
    if not vectors:
        return None
    centroid = np.mean(np.vstack(vectors), axis=0)
    norm = float(np.linalg.norm(centroid))
    if norm <= 0:
        return None
    return centroid / norm


def load_person_memory(path: Path | None = None) -> list[dict]:
    memory_path = _memory_path(path)
    if not memory_path.exists():
        return []

    try:
        payload = json.loads(memory_path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Failed to read person memory from %s: %s", memory_path, exc)
        return []

    prototypes = []
    for item in payload if isinstance(payload, list) else []:
        centroid_data = item.get("centroid")
        if not isinstance(centroid_data, list) or not centroid_data:
            continue
        try:
            centroid = np.asarray(centroid_data, dtype=np.float32)
        except Exception:
            continue
        prototypes.append({
            "person_label": item.get("person_label"),
            "cluster_id": item.get("cluster_id"),
            "centroid": centroid,
            "support_faces": int(item.get("support_faces") or 0),
            "support_clusters": int(item.get("support_clusters") or 0),
            "clean_approved_faces": int(item.get("clean_approved_faces") or 0),
            "usable_label": bool(item.get("usable_label", True)),
            "source_approved": True,
            "prototype_source": "memory",
            "updated_at": item.get("updated_at"),
        })
    return prototypes


def _merge_person_memory(existing: list[dict], current: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for item in existing:
        key = _person_label_key(item.get("person_label"))
        if not key:
            continue
        merged[key] = item

    for item in current:
        key = _person_label_key(item.get("person_label"))
        if not key:
            continue
        merged[key] = item

    return sorted(
        merged.values(),
        key=lambda item: str(item.get("person_label") or "").lower(),
    )


def save_person_memory(prototypes: list[dict], path: Path | None = None) -> Path:
    memory_path = _memory_path(path)
    memory_path.parent.mkdir(parents=True, exist_ok=True)

    payload = []
    for item in prototypes:
        centroid = item.get("centroid")
        if centroid is None:
            continue
        payload.append({
            "person_label": item.get("person_label"),
            "centroid": np.asarray(centroid, dtype=np.float32).tolist(),
            "support_faces": int(item.get("support_faces") or 0),
            "support_clusters": int(item.get("support_clusters") or 0),
            "clean_approved_faces": int(item.get("clean_approved_faces") or 0),
            "usable_label": bool(item.get("usable_label", True)),
            "updated_at": item.get("updated_at"),
        })

    tmp_path = memory_path.with_suffix(memory_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(memory_path)
    return memory_path


def sync_person_memory_from_db(
    conn,
    *,
    min_approved_faces: int,
    min_clean_face_score: float,
    preserve_existing_on_empty: bool = True,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT f.cluster_id, f.embedding, f.detection_score, c.person_label, c.updated_at
        FROM faces f
        JOIN clusters c ON c.cluster_id = f.cluster_id
        WHERE c.approved = 1
          AND c.is_noise = 0
          AND c.person_label IS NOT NULL
          AND TRIM(c.person_label) <> ''
        ORDER BY c.person_label, f.cluster_id
        """
    ).fetchall()

    grouped: dict[str, dict] = {}
    for row in rows:
        score = float(row["detection_score"] or 0.0)
        if score < float(min_clean_face_score):
            continue

        label = str(row["person_label"]).strip()
        if not label:
            continue

        vec = _decode_embedding(row["embedding"])
        if vec is None:
            continue

        key = _person_label_key(label)
        if key not in grouped:
            grouped[key] = {
                "person_label": label,
                "vectors": [],
                "cluster_ids": set(),
                "updated_at": row["updated_at"],
            }

        grouped[key]["vectors"].append(vec)
        grouped[key]["cluster_ids"].add(int(row["cluster_id"]))
        if row["updated_at"] and (
            not grouped[key]["updated_at"] or str(row["updated_at"]) > str(grouped[key]["updated_at"])
        ):
            grouped[key]["updated_at"] = row["updated_at"]

    prototypes = []
    for item in grouped.values():
        clean_approved_faces = len(item["vectors"])
        if clean_approved_faces < int(min_approved_faces):
            continue
        centroid = _normalized_centroid(item["vectors"])
        if centroid is None:
            continue
        prototypes.append({
            "person_label": item["person_label"],
            "cluster_id": None,
            "centroid": centroid,
            "support_faces": clean_approved_faces,
            "support_clusters": len(item["cluster_ids"]),
            "clean_approved_faces": clean_approved_faces,
            "usable_label": True,
            "source_approved": True,
            "prototype_source": "memory",
            "updated_at": item["updated_at"],
        })

    if prototypes:
        existing = load_person_memory() if preserve_existing_on_empty else []
        save_person_memory(_merge_person_memory(existing, prototypes))
    elif not preserve_existing_on_empty:
        save_person_memory([])

    return prototypes
