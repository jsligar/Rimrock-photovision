"""Search routes: semantic, face similarity, hybrid ranking, saved searches.

Only registered when ENABLE_SEARCH_LAYER is True.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import numpy as np
from fastapi import APIRouter, HTTPException

import config
import db
from api.models import SavedSearchCreate
from api.search_utils import fts_ranked_photo_ids, search_tokens
from clip_compat import ensure_pkg_resources_packaging

log = logging.getLogger(__name__)
router = APIRouter()

_clip_model = None
_clip_preprocess = None
_clip_tokenize = None
_SPACE_RE = re.compile(r"\s+")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_query_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_saved_query(query: dict) -> dict:
    normalized = dict(query or {})

    q = _clean_query_text(normalized.get("q"))
    if q is None:
        normalized.pop("q", None)
    else:
        normalized["q"] = q

    if "similar_to_face" in normalized:
        try:
            normalized["similar_to_face"] = int(normalized["similar_to_face"])
        except (TypeError, ValueError):
            normalized.pop("similar_to_face", None)

    if "q" not in normalized and "similar_to_face" not in normalized:
        raise HTTPException(400, "query must include a non-empty q or similar_to_face")

    return normalized


def _normalized_vec(vec: np.ndarray | None) -> np.ndarray | None:
    if vec is None or vec.size == 0:
        return None
    if not np.isfinite(vec).all():
        return None
    norm = float(np.linalg.norm(vec))
    if norm <= 0:
        return None
    return (vec / norm).astype(np.float32, copy=False)


def _blob_to_vec(blob: bytes | None) -> np.ndarray | None:
    if not blob:
        return None
    return np.frombuffer(blob, dtype=np.float32).copy()


def _normalized_text(value: str | None) -> str:
    if not value:
        return ""
    return _SPACE_RE.sub(" ", str(value).lower()).strip()


def _ranked_position_scores(photo_ids: list[int]) -> dict[int, float]:
    if not photo_ids:
        return {}
    return {photo_id: 1.0 / (1.0 + (idx * 0.35)) for idx, photo_id in enumerate(photo_ids)}


def _get_clip():
    global _clip_model, _clip_preprocess, _clip_tokenize
    if _clip_model is None:
        ensure_pkg_resources_packaging()
        import clip as openai_clip

        _clip_model, _clip_preprocess = openai_clip.load(
            config.CLIP_MODEL,
            device=config.CLIP_DEVICE,
        )
        _clip_model.eval()
        _clip_tokenize = openai_clip.tokenize
        log.info("Search: CLIP %s loaded on %s", config.CLIP_MODEL, config.CLIP_DEVICE)
    return _clip_model, _clip_preprocess, _clip_tokenize


def _photo_rows_for_ids(conn, photo_ids: list[int]) -> list[dict]:
    if not photo_ids:
        return []
    placeholders = ",".join("?" * len(photo_ids))
    rows = conn.execute(
        f"""SELECT photo_id, source_path, filename, exif_date,
                   date_source, dest_path, copy_verified, processed_at
            FROM photos
            WHERE photo_id IN ({placeholders})""",
        photo_ids,
    ).fetchall()
    lookup = {int(row["photo_id"]): dict(row) for row in rows}
    return [lookup[pid] for pid in photo_ids if pid in lookup]


def _compute_facets(conn, photo_ids: list[int]) -> dict:
    if not photo_ids:
        return {"tags": [], "people": [], "years": []}
    placeholders = ",".join("?" * len(photo_ids))

    tags = conn.execute(
        f"""SELECT tag, COUNT(DISTINCT photo_id) AS cnt
            FROM photo_tags
            WHERE photo_id IN ({placeholders})
            GROUP BY tag
            ORDER BY cnt DESC, tag ASC
            LIMIT 20""",
        photo_ids,
    ).fetchall()

    people = conn.execute(
        f"""SELECT c.person_label, COUNT(DISTINCT f.photo_id) AS cnt
            FROM faces f
            JOIN clusters c ON f.cluster_id = c.cluster_id
            WHERE f.photo_id IN ({placeholders})
              AND c.approved = 1
              AND c.person_label IS NOT NULL
            GROUP BY c.person_label
            ORDER BY cnt DESC, c.person_label ASC
            LIMIT 20""",
        photo_ids,
    ).fetchall()

    years = conn.execute(
        f"""SELECT SUBSTR(exif_date, 1, 4) AS year, COUNT(*) AS cnt
            FROM photos
            WHERE photo_id IN ({placeholders})
              AND exif_date IS NOT NULL
            GROUP BY year
            ORDER BY year DESC""",
        photo_ids,
    ).fetchall()

    return {
        "tags": [{"tag": row["tag"], "count": row["cnt"]} for row in tags],
        "people": [{"person": row["person_label"], "count": row["cnt"]} for row in people],
        "years": [{"year": row["year"], "count": row["cnt"]} for row in years],
    }


def _clip_text_search(query: str, conn, top_k: int) -> list[tuple[int, float]]:
    import torch
    import torch.nn.functional as F

    clip_model, _, tokenize = _get_clip()

    tokens = tokenize([query]).to(config.CLIP_DEVICE)
    with torch.no_grad():
        text_emb = clip_model.encode_text(tokens).float()
        text_emb = F.normalize(text_emb, dim=-1).squeeze(0)
    query_vec = _normalized_vec(text_emb.cpu().numpy().astype(np.float32))
    if query_vec is None:
        return []

    rows = conn.execute(
        "SELECT photo_id, clip_embedding FROM photos WHERE clip_embedding IS NOT NULL"
    ).fetchall()
    if not rows:
        return []

    photo_ids: list[int] = []
    vectors: list[np.ndarray] = []
    for row in rows:
        vec = _normalized_vec(_blob_to_vec(row["clip_embedding"]))
        if vec is None or vec.size != query_vec.size:
            continue
        photo_ids.append(int(row["photo_id"]))
        vectors.append(vec)

    if not vectors:
        return []

    matrix = np.stack(vectors)
    scores = matrix @ query_vec
    ranked = sorted(zip(photo_ids, scores.tolist()), key=lambda item: -item[1])
    return ranked[:top_k]


def _safe_clip_text_search(query: str, conn, top_k: int) -> list[tuple[int, float]]:
    try:
        return _clip_text_search(query, conn, top_k)
    except Exception as exc:
        log.warning("Search CLIP unavailable for %r: %s", query, exc)
        return []


def _tag_overlap_search(query: str, conn) -> dict[int, float]:
    tokens = search_tokens(query)
    if not tokens:
        return {}

    placeholders = ",".join("?" * len(tokens))
    rows = conn.execute(
        f"""SELECT photo_id, COUNT(DISTINCT tag) AS matched
            FROM photo_tags
            WHERE LOWER(tag) IN ({placeholders})
            GROUP BY photo_id""",
        tokens,
    ).fetchall()
    token_count = len(tokens)
    return {int(row["photo_id"]): row["matched"] / token_count for row in rows}


def _keyword_signal_scores(conn, photo_ids: list[int], query: str) -> dict[int, float]:
    tokens = search_tokens(query)
    if not photo_ids or not tokens:
        return {}

    phrase = _normalized_text(query)
    placeholders = ",".join("?" * len(photo_ids))

    photo_rows = conn.execute(
        f"""
        SELECT photo_id, filename, existing_people, ocr_text
          FROM photos
         WHERE photo_id IN ({placeholders})
        """,
        photo_ids,
    ).fetchall()
    tag_rows = conn.execute(
        f"""
        SELECT photo_id, tag
          FROM photo_tags
         WHERE photo_id IN ({placeholders})
        """,
        photo_ids,
    ).fetchall()

    tags_by_photo: dict[int, set[str]] = {}
    for row in tag_rows:
        tags_by_photo.setdefault(int(row["photo_id"]), set()).add(_normalized_text(row["tag"]))

    token_count = len(tokens)
    scores: dict[int, float] = {}
    for row in photo_rows:
        photo_id = int(row["photo_id"])
        filename_text = _normalized_text(row["filename"])
        people_text = _normalized_text(row["existing_people"])
        ocr_text = _normalized_text(row["ocr_text"])
        tag_texts = tags_by_photo.get(photo_id, set())

        score = 0.0

        def add_field_score(text: str, phrase_boost: float, token_boost: float) -> None:
            nonlocal score
            if not text:
                return
            if phrase and phrase in text:
                score += phrase_boost
            matched = sum(1 for token in tokens if token in text)
            if matched:
                score += token_boost * (matched / token_count)

        add_field_score(filename_text, 0.8, 0.25)
        add_field_score(people_text, 1.1, 0.4)
        add_field_score(ocr_text, 1.35, 0.55)

        if phrase and phrase in tag_texts:
            score += 0.95
        if tag_texts:
            matched_tags = sum(1 for token in tokens if token in tag_texts)
            if matched_tags:
                score += 0.35 * (matched_tags / token_count)

        if score > 0:
            scores[photo_id] = score

    return scores


def _hybrid_rank(query: str, conn, top_k: int) -> list[tuple[int, float]]:
    clip_results = _safe_clip_text_search(query, conn, top_k * 2)
    clip_scores = {int(photo_id): score for photo_id, score in clip_results}

    fts_ranked = fts_ranked_photo_ids(conn, query, limit=top_k * 6)
    fts_scores = _ranked_position_scores(fts_ranked)
    tag_scores = _tag_overlap_search(query, conn)
    keyword_scores = _keyword_signal_scores(
        conn,
        list(set(clip_scores) | set(fts_scores) | set(tag_scores)),
        query,
    )

    all_ids = set(clip_scores) | set(fts_scores) | set(tag_scores) | set(keyword_scores)
    ranked: list[tuple[int, float]] = []
    for photo_id in all_ids:
        score = (
            config.SEARCH_CLIP_WEIGHT * clip_scores.get(photo_id, 0.0)
            + config.SEARCH_TEXT_WEIGHT * fts_scores.get(photo_id, 0.0)
            + config.SEARCH_TAG_WEIGHT * tag_scores.get(photo_id, 0.0)
            + config.SEARCH_KEYWORD_WEIGHT * keyword_scores.get(photo_id, 0.0)
        )
        ranked.append((photo_id, score))

    ranked.sort(key=lambda item: -item[1])
    return ranked[:top_k]


def _face_similarity(conn, face_id: int, top_k: int) -> dict:
    ref_row = conn.execute(
        "SELECT embedding FROM faces WHERE face_id=?",
        (face_id,),
    ).fetchone()
    if not ref_row:
        raise HTTPException(404, "Face not found")

    ref_vec = _normalized_vec(_blob_to_vec(ref_row["embedding"]))
    if ref_vec is None:
        raise HTTPException(409, "Reference face embedding is unavailable")

    all_faces = conn.execute(
        "SELECT face_id, photo_id, embedding FROM faces"
    ).fetchall()

    photo_scores: dict[int, float] = {}
    for row in all_faces:
        current_face_id = int(row["face_id"])
        if current_face_id == face_id:
            continue

        vec = _normalized_vec(_blob_to_vec(row["embedding"]))
        if vec is None or vec.size != ref_vec.size:
            continue

        score = float(np.dot(ref_vec, vec))
        photo_id = int(row["photo_id"])
        if photo_id not in photo_scores or score > photo_scores[photo_id]:
            photo_scores[photo_id] = score

    ranked = sorted(photo_scores.items(), key=lambda item: -item[1])[:top_k]
    photo_ids = [photo_id for photo_id, _ in ranked]
    score_map = dict(ranked)
    photos = _photo_rows_for_ids(conn, photo_ids)
    for photo in photos:
        photo["score"] = round(score_map.get(photo["photo_id"], 0.0), 4)

    return {
        "query": f"similar_to_face:{face_id}",
        "mode": "face_similarity",
        "total": len(photos),
        "results": photos,
        "facets": _compute_facets(conn, photo_ids),
    }


@router.get("/photos/search")
def semantic_search(
    q: str | None = None,
    similar_to_face: int | None = None,
    mode: str = "auto",
    top_k: int | None = None,
):
    top_k = min(max(1, top_k or config.SEARCH_TOP_K), 200)
    mode = (mode or "auto").strip().lower()
    q = _clean_query_text(q)
    if mode not in {"auto", "clip", "hybrid"}:
        raise HTTPException(400, "mode must be one of: auto, clip, hybrid")

    conn = db.get_db()
    try:
        if similar_to_face is not None:
            return _face_similarity(conn, similar_to_face, top_k)

        if not q:
            raise HTTPException(400, "Provide q (text query) or similar_to_face (face ID)")

        if mode == "auto":
            mode = "hybrid"

        if mode == "clip":
            try:
                ranked = _clip_text_search(q, conn, top_k)
            except Exception as exc:
                log.error("Explicit CLIP search failed for %r: %s", q, exc)
                raise HTTPException(503, "CLIP search unavailable")
        elif mode == "hybrid":
            ranked = _hybrid_rank(q, conn, top_k)
        else:
            try:
                ranked = _clip_text_search(q, conn, top_k)
            except Exception as exc:
                log.error("Fallback CLIP search failed for %r: %s", q, exc)
                raise HTTPException(503, "CLIP search unavailable")

        photo_ids = [photo_id for photo_id, _ in ranked]
        score_map = {photo_id: score for photo_id, score in ranked}
        photos = _photo_rows_for_ids(conn, photo_ids)
        for photo in photos:
            photo["score"] = round(score_map.get(photo["photo_id"], 0.0), 4)

        return {
            "query": q,
            "mode": mode,
            "total": len(photos),
            "results": photos,
            "facets": _compute_facets(conn, photo_ids),
        }
    finally:
        conn.close()


@router.get("/searches")
def list_saved_searches():
    conn = db.get_db()
    rows = conn.execute(
        "SELECT search_id, name, query_json, created_at, updated_at FROM saved_searches ORDER BY created_at DESC"
    ).fetchall()
    conn.close()

    out = []
    for row in rows:
        try:
            query = _normalize_saved_query(json.loads(row["query_json"]))
        except Exception as exc:
            log.warning("Skipping invalid saved search %s: %s", row["search_id"], exc)
            continue
        out.append({
            "search_id": row["search_id"],
            "name": row["name"],
            "query": query,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        })
    return out


@router.post("/searches")
def create_saved_search(body: SavedSearchCreate):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "name is required")
    query = _normalize_saved_query(body.query)

    conn = db.get_db()
    now = _now()
    cur = conn.execute(
        "INSERT INTO saved_searches (name, query_json, created_at) VALUES (?, ?, ?)",
        (name, json.dumps(query), now),
    )
    conn.commit()
    search_id = cur.lastrowid
    conn.close()
    return {
        "search_id": search_id,
        "name": name,
        "query": query,
        "created_at": now,
    }


@router.delete("/searches/{search_id}")
def delete_saved_search(search_id: int):
    conn = db.get_db()
    cur = conn.execute("DELETE FROM saved_searches WHERE search_id=?", (search_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise HTTPException(404, "Saved search not found")
    return {"deleted": search_id}
