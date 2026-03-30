"""Cluster review routes."""

import json
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import quote

import numpy as np
from fastapi import APIRouter, HTTPException

import config
import db
import person_memory
from api.models import (
    AcceptSuggestionRequest,
    ClusterLabel,
    FaceSelectionRequest,
    FaceReassignRequest,
    MergeRequest,
)

router = APIRouter()
log = logging.getLogger(__name__)
MEGA_CLUSTER_FACE_COUNT = 25
_REVIEW_STATE_RANK = {
    "unlabeled": 3,
    "labeled_pending": 2,
    "approved": 1,
    "noise": 0,
}
_REVIEW_CACHE_LOCK = threading.RLock()
_REVIEW_CACHE_MAX_ENTRIES = 256
_REVIEW_CACHE_STAMP: tuple[str, int, int] | None = None
_REVIEW_CACHE: dict[str, dict] = {
    "cluster_list": {},
    "cluster_suggestions": {},
    "by_person_prototype": {},
    "noise_crops": {},
    "person_review": {},
    "review_match_sources": {},
    "review_stats": {},
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _review_cache_db_stamp(conn) -> tuple[str, int, int]:
    row = conn.execute(
        """
        SELECT
            COALESCE((SELECT MAX(updated_at) FROM clusters), '') AS max_cluster_updated_at,
            (SELECT COUNT(*) FROM clusters) AS cluster_count,
            (SELECT COUNT(*) FROM faces) AS face_count
        """
    ).fetchone()
    return (
        str(row["max_cluster_updated_at"] or ""),
        int(row["cluster_count"] or 0),
        int(row["face_count"] or 0),
    )


def _clear_review_caches_for_tests() -> None:
    _invalidate_review_cache()


def _invalidate_review_cache(conn=None) -> None:
    global _REVIEW_CACHE_STAMP
    with _REVIEW_CACHE_LOCK:
        for bucket in _REVIEW_CACHE.values():
            bucket.clear()
        _REVIEW_CACHE_STAMP = _review_cache_db_stamp(conn) if conn is not None else None


def _sync_review_cache(conn) -> None:
    global _REVIEW_CACHE_STAMP
    stamp = _review_cache_db_stamp(conn)
    with _REVIEW_CACHE_LOCK:
        if _REVIEW_CACHE_STAMP == stamp:
            return
        for bucket in _REVIEW_CACHE.values():
            bucket.clear()
        _REVIEW_CACHE_STAMP = stamp


def _review_cache_get(bucket_name: str, key):
    with _REVIEW_CACHE_LOCK:
        return _REVIEW_CACHE[bucket_name].get(key)


def _review_cache_set(bucket_name: str, key, value):
    with _REVIEW_CACHE_LOCK:
        bucket = _REVIEW_CACHE[bucket_name]
        if len(bucket) >= _REVIEW_CACHE_MAX_ENTRIES:
            bucket.pop(next(iter(bucket)))
        bucket[key] = value
    return value


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


def _normalized_vector(vec: np.ndarray | None) -> np.ndarray | None:
    if vec is None:
        return None
    norm = float(np.linalg.norm(vec))
    if norm <= 0:
        return None
    return vec / norm


def _centroid_from_sum(vec_sum: np.ndarray | None, count: int) -> np.ndarray | None:
    if vec_sum is None or count <= 0:
        return None
    norm = float(np.linalg.norm(vec_sum))
    if norm <= 0:
        return None
    return vec_sum / norm


def _new_stat_bucket() -> dict:
    return {
        "all_sum": None,
        "all_count": 0,
        "clean_sum": None,
        "clean_count": 0,
        "clean_approved_sum": None,
        "clean_approved_count": 0,
    }


def _accumulate_stat(bucket: dict, prefix: str, vec: np.ndarray) -> None:
    sum_key = f"{prefix}_sum"
    count_key = f"{prefix}_count"
    current = bucket.get(sum_key)
    bucket[sum_key] = vec.copy() if current is None else (current + vec)
    bucket[count_key] = int(bucket.get(count_key) or 0) + 1


def _merge_sum(existing: np.ndarray | None, incoming: np.ndarray | None) -> np.ndarray | None:
    if incoming is None:
        return existing
    return incoming.copy() if existing is None else (existing + incoming)


def _build_review_stats(conn) -> dict:
    rows = conn.execute(
        """SELECT f.cluster_id,
                  f.embedding,
                  f.detection_score,
                  c.person_label,
                  c.approved,
                  c.is_noise
           FROM faces f
           JOIN clusters c ON c.cluster_id = f.cluster_id
           ORDER BY c.person_label, f.cluster_id, f.face_id"""
    ).fetchall()

    cluster_stats: dict[int, dict] = {}
    label_stats: dict[str, dict] = {}

    for row in rows:
        vec = _normalized_vector(_decode_embedding(row["embedding"]))
        if vec is None:
            continue

        cluster_id = int(row["cluster_id"])
        cluster_bucket = cluster_stats.setdefault(cluster_id, _new_stat_bucket())
        _accumulate_stat(cluster_bucket, "all", vec)
        is_clean = _is_clean_face(row)
        if is_clean:
            _accumulate_stat(cluster_bucket, "clean", vec)
            if bool(row["approved"]):
                _accumulate_stat(cluster_bucket, "clean_approved", vec)

        if bool(row["is_noise"]):
            continue

        label = str(row["person_label"] or "").strip()
        if not label:
            continue

        label_bucket = label_stats.setdefault(
            label.lower(),
            {
                "person_label": label,
                "clusters": {},
            },
        )
        label_cluster_bucket = label_bucket["clusters"].setdefault(cluster_id, _new_stat_bucket())
        _accumulate_stat(label_cluster_bucket, "all", vec)
        if is_clean:
            _accumulate_stat(label_cluster_bucket, "clean", vec)
            if bool(row["approved"]):
                _accumulate_stat(label_cluster_bucket, "clean_approved", vec)

    return {
        "clusters": cluster_stats,
        "labels": label_stats,
    }


def _review_stats(conn) -> dict:
    _sync_review_cache(conn)
    cached = _review_cache_get("review_stats", "all")
    if cached is not None:
        return cached
    return _review_cache_set("review_stats", "all", _build_review_stats(conn))


def _cluster_centroid(conn, cluster_id: int) -> tuple[np.ndarray | None, int]:
    stats = _review_stats(conn)
    bucket = stats["clusters"].get(int(cluster_id))
    if not bucket:
        return None, 0
    return _centroid_from_sum(bucket.get("all_sum"), int(bucket.get("all_count") or 0)), int(
        bucket.get("all_count") or 0
    )


def _is_clean_face(row) -> bool:
    score = float(row["detection_score"] or 0.0)
    return score >= config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE


def _person_label_prototypes(
    conn,
    exclude_cluster_id: int,
    *,
    include_unapproved: bool,
) -> list[dict]:
    stats = _review_stats(conn)
    out = []

    for item in stats["labels"].values():
        cluster_stats = item["clusters"]
        approved_sum = None
        approved_count = 0
        approved_cluster_ids: set[int] = set()
        clean_sum = None
        clean_count = 0
        clean_cluster_ids: set[int] = set()

        for cluster_id, cluster_bucket in cluster_stats.items():
            if int(cluster_id) == int(exclude_cluster_id):
                continue

            cluster_approved_count = int(cluster_bucket.get("clean_approved_count") or 0)
            cluster_clean_count = int(cluster_bucket.get("clean_count") or 0)

            if cluster_approved_count > 0:
                approved_cluster_ids.add(int(cluster_id))
                approved_count += cluster_approved_count
                approved_sum = _merge_sum(approved_sum, cluster_bucket.get("clean_approved_sum"))

            if include_unapproved and cluster_clean_count > 0:
                clean_cluster_ids.add(int(cluster_id))
                clean_count += cluster_clean_count
                clean_sum = _merge_sum(clean_sum, cluster_bucket.get("clean_sum"))

        usable_label = approved_count >= config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES
        if include_unapproved:
            prototype_sum = approved_sum if usable_label and approved_count > 0 else clean_sum
            support_faces = approved_count if usable_label and approved_count > 0 else clean_count
            support_cluster_ids = approved_cluster_ids if usable_label and approved_count > 0 else clean_cluster_ids
        else:
            prototype_sum = approved_sum
            support_faces = approved_count
            support_cluster_ids = approved_cluster_ids

        centroid = _centroid_from_sum(prototype_sum, support_faces)
        if centroid is None or support_faces <= 0:
            continue

        out.append({
            "cluster_id": next(iter(support_cluster_ids)) if len(support_cluster_ids) == 1 else None,
            "person_label": item["person_label"],
            "source_approved": usable_label,
            "usable_label": usable_label,
            "prototype_source": "current",
            "centroid": centroid,
            "support_faces": support_faces,
            "support_clusters": len(support_cluster_ids),
            "clean_approved_faces": approved_count,
        })
    return out


def _sync_person_memory(conn) -> None:
    try:
        person_memory.sync_person_memory_from_db(
            conn,
            min_approved_faces=config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
            min_clean_face_score=config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
            preserve_existing_on_empty=True,
        )
    except Exception as exc:
        log.warning("Failed to sync person memory: %s", exc)


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
        if item.get("usable_label") and not prev.get("usable_label"):
            best_by_person[key] = item
            continue
        if item["score"] > prev["score"]:
            best_by_person[key] = item
            continue
        if item["score"] == prev["score"] and item["source_approved"] and not prev["source_approved"]:
            best_by_person[key] = item

    return sorted(
        best_by_person.values(),
        key=lambda x: (x.get("usable_label", False), x["score"], x["source_approved"], x["support_faces"]),
        reverse=True,
    )


def _review_match_sources(
    conn,
    *,
    exclude_cluster_id: int,
    min_labels_hint: int = 3,
) -> tuple[list[dict], str]:
    _sync_review_cache(conn)
    cache_key = (int(exclude_cluster_id), int(min_labels_hint))
    cached = _review_cache_get("review_match_sources", cache_key)
    if cached is not None:
        return cached

    approved_sources = _person_label_prototypes(
        conn,
        exclude_cluster_id=exclude_cluster_id,
        include_unapproved=False,
    )
    memory_sources = person_memory.load_person_memory()
    sources = list(approved_sources) + list(memory_sources)
    source_pool = "approved_plus_memory" if approved_sources and memory_sources else (
        "memory_only" if memory_sources else "approved_only"
    )

    approved_labels = {
        str(s["person_label"]).strip().lower() for s in approved_sources if s.get("person_label")
    }
    if not sources or len(approved_labels) < min(2, min_labels_hint):
        all_labeled_sources = _person_label_prototypes(
            conn,
            exclude_cluster_id=exclude_cluster_id,
            include_unapproved=True,
        )
        if not all_labeled_sources and not sources:
            return [], "no_labeled_labels"

        existing_labels = {str(s["person_label"]).strip().lower() for s in sources if s.get("person_label")}
        for item in all_labeled_sources:
            label_key = str(item["person_label"]).strip().lower()
            if label_key in approved_labels or label_key in existing_labels:
                continue
            sources.append(item)
            existing_labels.add(label_key)
        if approved_sources and memory_sources:
            source_pool = "approved_plus_memory_plus_labeled"
        elif approved_sources:
            source_pool = "approved_plus_labeled"
        elif memory_sources:
            source_pool = "memory_plus_labeled"
        else:
            source_pool = "labeled_only"

    return _review_cache_set("review_match_sources", cache_key, (sources, source_pool))


def _prototype_match_threshold() -> float:
    return max(-1.0, min(1.0, float(config.PERSON_PROTOTYPE_MATCH_THRESHOLD)))


def _prototype_person_key(person_label: str | None) -> str:
    return str(person_label or "").strip().lower()


def _prototype_source_rank(item: dict) -> tuple:
    return (
        int(bool(item.get("usable_label"))),
        int(bool(item.get("source_approved"))),
        int(item.get("support_faces") or 0),
        int(item.get("clean_approved_faces") or 0),
        int(item.get("support_clusters") or 0),
        int(str(item.get("prototype_source") or "") == "current"),
    )


def _dedupe_person_prototype_sources(sources: list[dict]) -> list[dict]:
    best_by_person: dict[str, dict] = {}
    for source in sources:
        person_label = str(source.get("person_label") or "").strip()
        raw_centroid = source.get("centroid")
        if not person_label or raw_centroid is None:
            continue

        centroid = _normalized_vector(np.asarray(raw_centroid, dtype=np.float32))
        if centroid is None:
            continue

        normalized = {
            **source,
            "person_label": person_label,
            "centroid": centroid,
        }
        key = _prototype_person_key(person_label)
        prev = best_by_person.get(key)
        if prev is None or _prototype_source_rank(normalized) > _prototype_source_rank(prev):
            best_by_person[key] = normalized

    return sorted(
        best_by_person.values(),
        key=lambda item: str(item.get("person_label") or "").lower(),
    )


def _build_by_person_prototype(conn, cluster_id: int | None = None) -> dict:
    _sync_review_cache(conn)
    threshold = _prototype_match_threshold()
    threshold_key = round(threshold, 6)
    scope_cluster_id = int(cluster_id) if cluster_id is not None else None
    scoped_cluster_mode = scope_cluster_id is not None
    cache_key = ("summary", threshold_key, scope_cluster_id)
    cached = _review_cache_get("by_person_prototype", cache_key)
    if cached is not None:
        return cached

    raw_sources, _ = _review_match_sources(
        conn,
        exclude_cluster_id=scope_cluster_id if scope_cluster_id is not None else -1,
        min_labels_hint=3,
    )
    prototype_sources = _dedupe_person_prototype_sources(raw_sources)
    row_query = [
        """SELECT f.face_id,
                  f.crop_path,
                  f.detection_score,
                  f.photo_id,
                  f.embedding,
                  f.cluster_id,
                  p.filename
           FROM faces f
           JOIN clusters c ON c.cluster_id = f.cluster_id
           JOIN photos p ON p.photo_id = f.photo_id
           WHERE (c.person_label IS NULL OR trim(c.person_label) = '')"""
    ]
    params: list[int] = []
    if scope_cluster_id is not None:
        row_query.append("AND f.cluster_id = ?")
        params.append(scope_cluster_id)
    row_query.append("ORDER BY f.cluster_id ASC, f.face_id ASC")
    rows = conn.execute("\n".join(row_query), tuple(params)).fetchall()

    prototype_dim = None
    usable_sources: list[dict] = []
    for source in prototype_sources:
        centroid = source.get("centroid")
        if centroid is None:
            continue
        if prototype_dim is None:
            prototype_dim = int(centroid.size)
        if int(centroid.size) != prototype_dim:
            continue
        usable_sources.append(source)

    prototype_matrix = None
    if usable_sources and prototype_dim:
        prototype_matrix = np.vstack([source["centroid"] for source in usable_sources]).astype(np.float32, copy=False)

    face_records: list[dict] = []
    embedded_indices: list[int] = []
    embedded_vectors: list[np.ndarray] = []
    for row in rows:
        record = {
            "face_id": int(row["face_id"]),
            "cluster_id": int(row["cluster_id"]),
            "crop_url": _crop_url_from_path(row["crop_path"]),
            "detection_score": round(float(row["detection_score"] or 0.0), 4),
            "photo_id": int(row["photo_id"]),
            "filename": row["filename"],
        }
        face_records.append(record)

        vec = _normalized_vector(_decode_embedding(row["embedding"]))
        if vec is None or prototype_dim is None or int(vec.size) != prototype_dim:
            continue
        embedded_indices.append(len(face_records) - 1)
        embedded_vectors.append(vec)

    best_matches: dict[int, tuple[dict, float]] = {}
    if prototype_matrix is not None and embedded_vectors:
        face_matrix = np.vstack(embedded_vectors).astype(np.float32, copy=False)
        similarities = face_matrix @ prototype_matrix.T
        best_indices = np.argmax(similarities, axis=1)
        best_scores = similarities[np.arange(similarities.shape[0]), best_indices]
        for record_idx, source_idx, best_similarity in zip(
            embedded_indices,
            best_indices.tolist(),
            best_scores.tolist(),
        ):
            best_matches[int(record_idx)] = (
                usable_sources[int(source_idx)],
                float(best_similarity),
            )

    assignments_by_label: dict[str, list[dict]] = {}
    groups: dict[str, dict] = {}
    total_matched_faces = 0
    for idx, record in enumerate(face_records):
        source, best_similarity = best_matches.get(idx, (None, None))
        matched_person = str(source["person_label"]).strip() if source else None
        similarity_value = round(float(best_similarity), 4) if best_similarity is not None else 0.0
        match_score = (
            round(_clamp((float(best_similarity) + 1.0) / 2.0), 4)
            if best_similarity is not None
            else 0.0
        )
        is_match = bool(matched_person) and best_similarity is not None and float(best_similarity) >= threshold
        is_bucketed_match = bool(matched_person) and best_similarity is not None and (
            scoped_cluster_mode or float(best_similarity) >= threshold
        )
        group_label = matched_person if is_bucketed_match else "__unknown__"
        group_key = _prototype_person_key(group_label)

        face_item = {
            **record,
            "similarity": similarity_value,
            "match_score": match_score,
            "matched_person": matched_person,
        }
        assignments_by_label.setdefault(group_key, []).append(face_item)

        group = groups.get(group_key)
        if group is None:
            if is_bucketed_match and source is not None:
                group = {
                    "person_label": matched_person,
                    "display_label": matched_person,
                    "face_count": 0,
                    "similarity_sum": 0.0,
                    "prototype_support_faces": int(source.get("support_faces") or 0),
                    "usable_label": bool(source.get("usable_label")),
                }
            else:
                group = {
                    "person_label": "__unknown__",
                    "display_label": "Unknown",
                    "face_count": 0,
                    "similarity_sum": 0.0,
                }
            groups[group_key] = group

        group["face_count"] += 1
        if is_bucketed_match and best_similarity is not None:
            total_matched_faces += 1
            group["similarity_sum"] += float(best_similarity)

    summary_groups = []
    for group in groups.values():
        person_label = str(group["person_label"])
        face_count = int(group["face_count"] or 0)
        item = {
            "person_label": person_label,
            "display_label": group["display_label"],
            "face_count": face_count,
            "avg_similarity": (
                round(float(group["similarity_sum"]) / face_count, 4)
                if person_label != "__unknown__" and face_count > 0
                else 0.0
            ),
        }
        if person_label != "__unknown__":
            item["prototype_support_faces"] = int(group.get("prototype_support_faces") or 0)
            item["usable_label"] = bool(group.get("usable_label"))
        summary_groups.append(item)

    summary_groups.sort(
        key=lambda item: (
            str(item.get("person_label")) == "__unknown__",
            str(item.get("display_label") or "").lower(),
        )
    )

    _review_cache_set(
        "by_person_prototype",
        ("assignments", threshold_key, scope_cluster_id),
        assignments_by_label,
    )
    return _review_cache_set("by_person_prototype", cache_key, {
        "groups": summary_groups,
        "total_untagged_faces": len(face_records),
        "total_matched_faces": total_matched_faces,
        "threshold": round(threshold, 4),
        "prototype_count": len(usable_sources),
        "scope_cluster_id": scope_cluster_id,
        "scoped_cluster_mode": scoped_cluster_mode,
    })


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _cluster_review_state(person_label: str | None, approved: bool, is_noise: bool) -> str:
    if is_noise:
        return "noise"
    if person_label and approved:
        return "approved"
    if person_label:
        return "labeled_pending"
    return "unlabeled"


def _cluster_review_reasons(
    review_state: str,
    face_count: int,
    avg_detection_score: float,
    photo_count: int,
) -> list[str]:
    reasons: list[str] = []
    if review_state == "unlabeled":
        reasons.append("needs label")
    elif review_state == "labeled_pending":
        reasons.append("awaiting approval")
    elif review_state == "approved":
        reasons.append("approved")
    else:
        reasons.append("noise")

    if face_count >= MEGA_CLUSTER_FACE_COUNT:
        reasons.append(f"mega cluster ({face_count} faces)")
    elif face_count >= 8:
        reasons.append(f"{face_count} faces")

    if avg_detection_score >= 0.88:
        reasons.append("high-confidence faces")
    elif avg_detection_score >= 0.72:
        reasons.append("solid-confidence faces")
    elif avg_detection_score > 0:
        reasons.append("lower-confidence faces")

    if photo_count >= 8:
        reasons.append(f"{photo_count} photos")
    return reasons


def _serialize_cluster_row(row) -> dict:
    item = dict(row)
    face_count = int(item.get("face_count") or 0)
    avg_detection_score = _clamp(item.get("avg_detection_score") or 0.0)
    photo_count = int(item.get("photo_count") or 0)
    review_state = _cluster_review_state(
        item.get("person_label"),
        bool(item.get("approved")),
        bool(item.get("is_noise")),
    )

    size_score = min(face_count / 30.0, 1.0)
    if review_state == "unlabeled":
        uncertainty_score = 1.0
    elif review_state == "labeled_pending":
        uncertainty_score = 0.55
    elif review_state == "approved":
        uncertainty_score = 0.05
    else:
        uncertainty_score = 0.0

    is_mega_cluster = bool(
        face_count >= MEGA_CLUSTER_FACE_COUNT
        and review_state in {"unlabeled", "labeled_pending"}
    )
    mega_boost = 0.15 if is_mega_cluster else 0.0
    review_priority_score = min(
        1.0,
        (size_score * 0.50)
        + (avg_detection_score * 0.25)
        + (uncertainty_score * 0.25)
        + mega_boost,
    )

    if review_state in {"approved", "noise"}:
        review_priority_bucket = "low"
    elif review_priority_score >= 0.75 or is_mega_cluster:
        review_priority_bucket = "high"
    elif review_priority_score >= 0.45:
        review_priority_bucket = "medium"
    else:
        review_priority_bucket = "low"

    item["avg_detection_score"] = round(avg_detection_score, 4)
    item["photo_count"] = photo_count
    item["review_state"] = review_state
    item["is_mega_cluster"] = is_mega_cluster
    item["review_priority_score"] = round(review_priority_score, 4)
    item["review_priority_bucket"] = review_priority_bucket
    item["review_priority_reasons"] = _cluster_review_reasons(
        review_state,
        face_count,
        avg_detection_score,
        photo_count,
    )
    return item


def _person_cluster_quality(conn) -> dict[int, dict]:
    stats = _review_stats(conn)
    quality_by_cluster: dict[int, dict] = {}

    for item in stats["labels"].values():
        cluster_stats = item["clusters"]
        approved_sum = None
        approved_count = 0
        clean_sum = None
        clean_count = 0
        all_sum = None
        all_count = 0

        for cluster_bucket in cluster_stats.values():
            cluster_all_sum = cluster_bucket.get("all_sum")
            cluster_clean_sum = cluster_bucket.get("clean_sum")
            cluster_approved_sum = cluster_bucket.get("clean_approved_sum")
            cluster_all_count = int(cluster_bucket.get("all_count") or 0)
            cluster_clean_count = int(cluster_bucket.get("clean_count") or 0)
            cluster_approved_count = int(cluster_bucket.get("clean_approved_count") or 0)

            if cluster_all_sum is not None and cluster_all_count > 0:
                all_sum = cluster_all_sum.copy() if all_sum is None else (all_sum + cluster_all_sum)
                all_count += cluster_all_count
            if cluster_clean_sum is not None and cluster_clean_count > 0:
                clean_sum = cluster_clean_sum.copy() if clean_sum is None else (clean_sum + cluster_clean_sum)
                clean_count += cluster_clean_count
            if cluster_approved_sum is not None and cluster_approved_count > 0:
                approved_sum = (
                    cluster_approved_sum.copy() if approved_sum is None else (approved_sum + cluster_approved_sum)
                )
                approved_count += cluster_approved_count

        usable_label = approved_count >= config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES
        prototype_sum = approved_sum if usable_label and approved_count > 0 else clean_sum
        prototype_count = approved_count if usable_label and approved_count > 0 else clean_count
        if prototype_sum is None or prototype_count <= 0:
            prototype_sum = all_sum
            prototype_count = all_count

        prototype = _centroid_from_sum(prototype_sum, prototype_count)
        if prototype is None:
            continue

        ranked_clusters: list[dict] = []
        for cluster_id, cluster_bucket in cluster_stats.items():
            if usable_label and int(cluster_bucket.get("clean_approved_count") or 0) > 0:
                cluster_sum = cluster_bucket.get("clean_approved_sum")
                support_faces = int(cluster_bucket.get("clean_approved_count") or 0)
            elif int(cluster_bucket.get("clean_count") or 0) > 0:
                cluster_sum = cluster_bucket.get("clean_sum")
                support_faces = int(cluster_bucket.get("clean_count") or 0)
            else:
                cluster_sum = cluster_bucket.get("all_sum")
                support_faces = int(cluster_bucket.get("all_count") or 0)

            if cluster_sum is None or support_faces <= 0:
                continue

            avg_similarity = float(np.dot(cluster_sum, prototype) / support_faces)
            person_match_score = _clamp((avg_similarity + 1.0) / 2.0)
            ranked_clusters.append({
                "cluster_id": int(cluster_id),
                "person_match_score": round(person_match_score, 4),
                "support_faces": support_faces,
            })

        ranked_clusters.sort(
            key=lambda item: (
                -float(item["person_match_score"]),
                -int(item["support_faces"]),
                int(item["cluster_id"]),
            )
        )
        cluster_count = len(ranked_clusters)
        for idx, ranked_item in enumerate(ranked_clusters, start=1):
            quality_by_cluster[int(ranked_item["cluster_id"])] = {
                "person_match_score": ranked_item["person_match_score"],
                "person_cluster_rank": idx,
                "person_cluster_count": cluster_count,
            }

    return quality_by_cluster


def _cluster_review_sort_key(item: dict) -> tuple:
    return (
        -_REVIEW_STATE_RANK.get(str(item.get("review_state")), 0),
        -int(bool(item.get("is_mega_cluster"))),
        -float(item.get("review_priority_score") or 0.0),
        -int(item.get("face_count") or 0),
        -float(item.get("avg_detection_score") or 0.0),
        int(item.get("cluster_id") or 0),
    )


def _build_cluster_list_response(conn, sort: str) -> list[dict]:
    rows = conn.execute(
        """SELECT c.cluster_id,
                  c.person_label,
                  c.face_count,
                  c.is_noise,
                  c.approved,
                  c.updated_at,
                  COALESCE(AVG(f.detection_score), 0) AS avg_detection_score,
                  COUNT(DISTINCT f.photo_id) AS photo_count
           FROM clusters c
           LEFT JOIN faces f ON f.cluster_id = c.cluster_id
           GROUP BY c.cluster_id, c.person_label, c.face_count, c.is_noise, c.approved, c.updated_at
           ORDER BY c.face_count DESC, c.cluster_id ASC"""
    ).fetchall()
    quality_by_cluster = _person_cluster_quality(conn)
    items = [_serialize_cluster_row(row) for row in rows]
    for item in items:
        quality = quality_by_cluster.get(int(item["cluster_id"]))
        if quality:
            item.update(quality)

    ranked = sorted(items, key=_cluster_review_sort_key)
    for idx, item in enumerate(ranked, start=1):
        item["review_priority_rank"] = idx

    if sort == "review":
        return ranked
    return items


@router.get("/clusters")
def list_clusters(sort: str = "face_count"):
    sort = (sort or "face_count").strip().lower()
    if sort not in {"face_count", "review"}:
        raise HTTPException(status_code=400, detail="sort must be 'face_count' or 'review'")

    conn = db.get_db()
    _sync_review_cache(conn)
    cache_key = sort
    cached = _review_cache_get("cluster_list", cache_key)
    if cached is not None:
        conn.close()
        return cached
    response = _build_cluster_list_response(conn, sort)
    conn.close()
    return _review_cache_set("cluster_list", cache_key, response)


@router.get("/clusters/by-person-prototype")
def list_by_person_prototype(cluster_id: int | None = None):
    conn = db.get_db()
    response = _build_by_person_prototype(conn, cluster_id=cluster_id)
    conn.close()
    return response


@router.get("/clusters/by-person-prototype/{person_label}/faces")
def get_by_person_prototype_faces(person_label: str, cluster_id: int | None = None):
    conn = db.get_db()
    _sync_review_cache(conn)
    threshold_key = round(_prototype_match_threshold(), 6)
    scope_cluster_id = int(cluster_id) if cluster_id is not None else None
    normalized_label = "__unknown__" if _prototype_person_key(person_label) == "__unknown__" else person_label
    group_key = _prototype_person_key(normalized_label)
    cache_key = ("faces", threshold_key, scope_cluster_id, group_key)
    cached = _review_cache_get("by_person_prototype", cache_key)
    if cached is not None:
        conn.close()
        return cached

    assignments = _review_cache_get("by_person_prototype", ("assignments", threshold_key, scope_cluster_id))
    if assignments is None:
        _build_by_person_prototype(conn, cluster_id=scope_cluster_id)
        assignments = _review_cache_get("by_person_prototype", ("assignments", threshold_key, scope_cluster_id))

    faces = list((assignments or {}).get(group_key, []))
    if not faces and group_key != "__unknown__":
        conn.close()
        raise HTTPException(status_code=404, detail="Prototype group not found")

    faces.sort(
        key=lambda item: (
            -(float(item["similarity"]) if item.get("similarity") is not None else -1.0),
            -float(item.get("detection_score") or 0.0),
            int(item.get("face_id") or 0),
        )
    )
    conn.close()
    return _review_cache_set("by_person_prototype", cache_key, faces)


@router.get("/clusters/{cluster_id}/crops")
def get_cluster_crops(cluster_id: int):
    conn = db.get_db()
    _sync_review_cache(conn)
    cluster = conn.execute(
        """SELECT cluster_id, is_noise
           FROM clusters
           WHERE cluster_id=?""",
        (cluster_id,),
    ).fetchone()
    faces = conn.execute(
        """SELECT f.face_id, f.crop_path, f.detection_score, f.photo_id, p.filename, f.embedding
           FROM faces f
           JOIN photos p ON f.photo_id = p.photo_id
           WHERE f.cluster_id=?
           ORDER BY f.detection_score DESC
           LIMIT 250""",
        (cluster_id,)
    ).fetchall()

    crops = []
    if cluster and bool(cluster["is_noise"]):
        cached = _review_cache_get("noise_crops", int(cluster_id))
        if cached is not None:
            conn.close()
            return cached
        sources, _ = _review_match_sources(
            conn,
            exclude_cluster_id=cluster_id,
            min_labels_hint=10,
        )
        for f in faces:
            best_label = None
            best_similarity = None
            best_match_score = None
            prototype_source = None
            vec = _normalized_vector(_decode_embedding(f["embedding"]))
            if vec is not None and sources:
                best_source = max(sources, key=lambda source: float(np.dot(vec, source["centroid"])))
                best_similarity = float(np.dot(vec, best_source["centroid"]))
                best_match_score = _clamp((best_similarity + 1.0) / 2.0)
                best_label = best_source["person_label"]
                prototype_source = best_source.get("prototype_source", "current")
            crops.append({
                "face_id": f["face_id"],
                "crop_url": _crop_url_from_path(f["crop_path"]),
                "detection_score": f["detection_score"],
                "photo_id": f["photo_id"],
                "filename": f["filename"],
                "predicted_label": best_label,
                "best_similarity": round(best_similarity, 4) if best_similarity is not None else None,
                "best_match_score": round(best_match_score, 4) if best_match_score is not None else None,
                "prototype_source": prototype_source,
            })
        crops.sort(
            key=lambda item: (
                -float(item["best_match_score"] or -1.0),
                -float(item["detection_score"] or 0.0),
                int(item["face_id"]),
            )
        )
        conn.close()
        return _review_cache_set("noise_crops", int(cluster_id), crops)

    for f in faces:
        crops.append({
            "face_id": f["face_id"],
            "crop_url": _crop_url_from_path(f["crop_path"]),
            "detection_score": f["detection_score"],
            "photo_id": f["photo_id"],
            "filename": f["filename"],
        })
    conn.close()
    return crops


def _person_review_payload(conn, person_label: str) -> dict:
    label = (person_label or "").strip()
    _sync_review_cache(conn)
    cache_key = label.lower()
    cached = _review_cache_get("person_review", cache_key)
    if cached is not None:
        return cached

    rows = conn.execute(
        """SELECT f.face_id,
                  f.crop_path,
                  f.detection_score,
                  f.photo_id,
                  p.filename,
                  f.embedding,
                  f.cluster_id,
                  c.approved
           FROM faces f
           JOIN clusters c ON c.cluster_id = f.cluster_id
           JOIN photos p ON p.photo_id = f.photo_id
           WHERE c.person_label IS NOT NULL
             AND lower(trim(c.person_label)) = lower(?)
             AND c.is_noise = 0
           ORDER BY c.approved DESC, f.detection_score ASC, f.face_id ASC""",
        (label,),
    ).fetchall()

    face_rows: list[tuple] = []
    clean_vectors: list[np.ndarray] = []
    clean_cluster_ids: set[int] = set()
    clean_approved_vectors: list[np.ndarray] = []
    clean_approved_cluster_ids: set[int] = set()
    all_cluster_ids: set[int] = set()

    for row in rows:
        cluster_id = int(row["cluster_id"])
        all_cluster_ids.add(cluster_id)
        vec = _normalized_vector(_decode_embedding(row["embedding"]))
        face_rows.append((row, vec))
        if vec is None or not _is_clean_face(row):
            continue
        clean_vectors.append(vec)
        clean_cluster_ids.add(cluster_id)
        if bool(row["approved"]):
            clean_approved_vectors.append(vec)
            clean_approved_cluster_ids.add(cluster_id)

    usable_label = len(clean_approved_vectors) >= config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES
    prototype_vectors = clean_approved_vectors if usable_label else clean_vectors
    prototype_cluster_ids = (
        clean_approved_cluster_ids if usable_label else clean_cluster_ids
    )
    prototype = _normalized_centroid(prototype_vectors)
    if prototype is None:
        fallback_vectors = [vec for _, vec in face_rows if vec is not None]
        prototype = _normalized_centroid(fallback_vectors)
        if not prototype_cluster_ids:
            prototype_cluster_ids = {
                int(row["cluster_id"]) for row, vec in face_rows if vec is not None
            }

    faces = []
    for row, vec in face_rows:
        det_score = _clamp(row["detection_score"] or 0.0)
        similarity_score = None
        if prototype is not None and vec is not None:
            similarity_score = float(np.dot(vec, prototype))
        match_score = (
            _clamp((similarity_score + 1.0) / 2.0)
            if similarity_score is not None
            else 0.0
        )
        review_score = round((match_score * 0.75) + (det_score * 0.25), 4)
        faces.append({
            "face_id": int(row["face_id"]),
            "cluster_id": int(row["cluster_id"]),
            "crop_url": _crop_url_from_path(row["crop_path"]),
            "detection_score": round(det_score, 4),
            "photo_id": int(row["photo_id"]),
            "filename": row["filename"],
            "cluster_approved": bool(row["approved"]),
            "match_score": round(match_score, 4),
            "similarity_score": round(similarity_score, 4) if similarity_score is not None else None,
            "review_score": review_score,
        })

    faces.sort(
        key=lambda item: (
            float(item["review_score"]),
            float(item["detection_score"]),
            int(item["face_id"]),
        )
    )
    for idx, item in enumerate(faces, start=1):
        item["review_rank"] = idx

    return _review_cache_set("person_review", cache_key, {
        "person_label": label,
        "sort_mode": "worst_first",
        "face_count": len(faces),
        "cluster_count": len(all_cluster_ids),
        "prototype_support_faces": len(prototype_vectors),
        "prototype_support_clusters": len(prototype_cluster_ids),
        "clean_approved_faces": len(clean_approved_vectors),
        "usable_label": usable_label,
        "faces": faces,
    })


@router.get("/clusters/{cluster_id}/person-review")
def get_person_review(cluster_id: int):
    conn = db.get_db()
    cluster = conn.execute(
        """SELECT cluster_id, person_label, is_noise
           FROM clusters
           WHERE cluster_id=?""",
        (cluster_id,),
    ).fetchone()
    if not cluster:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster["is_noise"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Noise clusters do not have a person review file")

    person_label = (cluster["person_label"] or "").strip()
    if not person_label:
        conn.close()
        raise HTTPException(status_code=400, detail="Cluster must be labeled before reviewing a full person file")

    payload = _person_review_payload(conn, person_label)
    conn.close()
    return {
        "cluster_id": cluster_id,
        **payload,
    }


@router.post("/clusters/{cluster_id}/person-review/remove-face")
def remove_face_from_person_review(cluster_id: int, body: FaceSelectionRequest):
    conn = db.get_db()
    cluster = conn.execute(
        """SELECT cluster_id, person_label, is_noise
           FROM clusters
           WHERE cluster_id=?""",
        (cluster_id,),
    ).fetchone()
    if not cluster:
        conn.close()
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster["is_noise"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Noise clusters do not have a person review file")

    person_label = (cluster["person_label"] or "").strip()
    if not person_label:
        conn.close()
        raise HTTPException(status_code=400, detail="Cluster must be labeled before removing faces from a person review file")

    face_ids = _normalize_face_ids(body.face_ids)
    if len(face_ids) != 1:
        conn.close()
        raise HTTPException(status_code=400, detail="Remove exactly one face at a time from a person review file")

    face_id = face_ids[0]
    face_row = conn.execute(
        """SELECT f.face_id, f.cluster_id
           FROM faces f
           JOIN clusters c ON c.cluster_id = f.cluster_id
           WHERE f.face_id=?
             AND c.person_label IS NOT NULL
             AND lower(trim(c.person_label)) = lower(?)
             AND c.is_noise = 0""",
        (face_id, person_label),
    ).fetchone()
    if not face_row:
        conn.close()
        raise HTTPException(status_code=400, detail="Selected face is not part of this person's labeled faces")

    source_cluster_id = int(face_row["cluster_id"])
    max_id = conn.execute("SELECT COALESCE(MAX(cluster_id), 0) FROM clusters").fetchone()[0]
    target_cluster_id = int(max_id) + 1
    conn.execute(
        """INSERT INTO clusters
           (cluster_id, person_label, face_count, is_noise, approved, updated_at)
           VALUES (?, NULL, 0, 0, 0, ?)""",
        (target_cluster_id, _now()),
    )

    moved = conn.execute(
        """UPDATE faces
           SET cluster_id=?
           WHERE face_id=?
             AND cluster_id=?""",
        (target_cluster_id, face_id, source_cluster_id),
    ).rowcount
    if moved != 1:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to remove the selected face")

    conn.execute(
        """INSERT INTO face_move_history
           (created_at, source_cluster_id, target_cluster_id, face_ids_json, undone_at)
           VALUES (?, ?, ?, ?, NULL)""",
        (_now(), source_cluster_id, target_cluster_id, json.dumps([face_id])),
    )
    move_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    source_count = _update_cluster_count(conn, source_cluster_id)
    target_count = _update_cluster_count(conn, target_cluster_id)
    if source_count == 0:
        conn.execute("DELETE FROM clusters WHERE cluster_id=?", (source_cluster_id,))

    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
    conn.close()
    return {
        "ok": True,
        "move_id": move_id,
        "moved_faces": 1,
        "removed_face_id": face_id,
        "person_label": person_label,
        "source_cluster_id": source_cluster_id,
        "target_cluster_id": target_cluster_id,
        "target_face_count": target_count,
    }


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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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

    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
    _sync_review_cache(conn)
    top_k = max(1, min(int(top_k), 10))
    cache_key = (int(cluster_id), int(top_k))
    cached = _review_cache_get("cluster_suggestions", cache_key)
    if cached is not None:
        conn.close()
        return cached

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

    sources, source_pool = _review_match_sources(
        conn,
        exclude_cluster_id=cluster_id,
        min_labels_hint=top_k,
    )
    if not sources and source_pool == "no_labeled_labels":
        conn.close()
        return {"cluster_id": cluster_id, "suggestions": [], "reason": "no_labeled_labels"}

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
            "support_clusters": source["support_clusters"],
            "clean_approved_faces": source["clean_approved_faces"],
            "usable_label": bool(source.get("usable_label")),
            "source_approved": bool(source["source_approved"]),
            "prototype_source": source.get("prototype_source", "current"),
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

    response = {
        "cluster_id": cluster_id,
        "target_face_count": target_faces,
        "cluster_labeled": bool(cluster["person_label"]),
        "source_pool": source_pool,
        "usable_min_approved_faces": config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
        "clean_face_min_score": config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
        "suggestions": top,
    }
    conn.close()
    return _review_cache_set("cluster_suggestions", cache_key, response)


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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
        # Preserve cluster boundaries when assigning by person name.
        # We group multiple labeled clusters into one person review file, so
        # a name-based move should create a new labeled slice instead of
        # silently merging into an arbitrary existing cluster for that person.
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

    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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
    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
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

    _sync_person_memory(conn)
    conn.commit()
    _invalidate_review_cache(conn)
    conn.close()
    return {"ok": True, "merged_into": body.target_cluster_id, "new_face_count": new_count}
