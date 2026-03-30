"""NVIDIA Burst Intelligence — hosted API adapter for expensive tasks.

Gated behind NVIDIA_BURST_ENABLED. When disabled, all public functions
return None or no-op so callers don't need guard checks everywhere.

Capabilities:
  - query_rewrite(): expand a user search query into richer terms
  - rerank(): reorder search results using a hosted reranker
  - caption_image(): generate a text description of a photo

All calls are:
  - budget-checked (daily request + token caps)
  - content-hash cached (avoid repeat API calls)
  - privacy-aware (strip EXIF, resize before sending)
"""

import base64
import hashlib
import io
import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

import config
import db

log = logging.getLogger(__name__)
_HEALTH_CACHE_LOCK = threading.RLock()
_HEALTH_CACHE_TTL_SEC = 45
_HEALTH_CACHE_RESULT: dict | None = None
_HEALTH_CACHE_EXPIRES_AT = 0.0


# ── Guard ────────────────────────────────────────────────────────────────────

def is_enabled() -> bool:
    return bool(config.NVIDIA_BURST_ENABLED and config.NVIDIA_API_KEY)


def _require_enabled() -> None:
    if not is_enabled():
        raise RuntimeError("NVIDIA Burst is not enabled or API key is missing")


# ── Budget ───────────────────────────────────────────────────────────────────

class BudgetExceededError(Exception):
    pass


def _health_check_url() -> str:
    return f"{config.NVIDIA_API_BASE_URL.rstrip('/')}/models"


def _health_timeout_sec() -> int:
    return max(2, min(int(config.NVIDIA_BURST_TIMEOUT_SEC or 5), 5))


def _base_status_summary() -> dict:
    feature_enabled = bool(config.NVIDIA_BURST_ENABLED)
    api_key_present = bool(config.NVIDIA_API_KEY)
    configured = bool(feature_enabled and api_key_present)
    if not feature_enabled:
        status = "disabled"
        label = "NVIDIA Off"
        tone = "off"
    elif not api_key_present:
        status = "missing_key"
        label = "No API Key"
        tone = "warn"
    else:
        status = "configured"
        label = "Checking"
        tone = "neutral"

    return {
        "feature_enabled": feature_enabled,
        "api_key_present": api_key_present,
        "enabled": configured,
        "configured": configured,
        "reachable": False,
        "healthy": False,
        "status": status,
        "label": label,
        "tone": tone,
        "server_url": config.NVIDIA_API_BASE_URL,
        "health_url": _health_check_url(),
        "last_checked_at": None,
        "last_error": None,
        "requests_used": 0,
        "requests_cap": int(config.NVIDIA_BURST_DAILY_REQUEST_CAP),
        "tokens_used": 0,
        "tokens_cap": int(config.NVIDIA_BURST_DAILY_TOKEN_CAP),
        "requests_remaining": int(config.NVIDIA_BURST_DAILY_REQUEST_CAP),
        "tokens_remaining": int(config.NVIDIA_BURST_DAILY_TOKEN_CAP),
        "by_type": [],
        "llm_model": config.NVIDIA_LLM_MODEL,
        "vision_model": config.NVIDIA_VISION_MODEL,
        "rerank_model": config.NVIDIA_RERANK_MODEL,
    }


def _merge_usage(summary: dict) -> dict:
    if not summary.get("enabled"):
        return summary

    usage = db.burst_usage_today()
    requests_used = int(usage["request_count"])
    tokens_used = int(usage["token_count"])
    requests_cap = int(config.NVIDIA_BURST_DAILY_REQUEST_CAP)
    tokens_cap = int(config.NVIDIA_BURST_DAILY_TOKEN_CAP)
    summary.update({
        "date": usage["date"],
        "requests_used": requests_used,
        "requests_cap": requests_cap,
        "tokens_used": tokens_used,
        "tokens_cap": tokens_cap,
        "requests_remaining": max(0, requests_cap - requests_used),
        "tokens_remaining": max(0, tokens_cap - tokens_used),
        "by_type": usage["by_type"],
    })
    return summary


def clear_health_cache() -> None:
    global _HEALTH_CACHE_RESULT, _HEALTH_CACHE_EXPIRES_AT
    with _HEALTH_CACHE_LOCK:
        _HEALTH_CACHE_RESULT = None
        _HEALTH_CACHE_EXPIRES_AT = 0.0


def _probe_server_health() -> dict:
    summary = _base_status_summary()
    if not summary["configured"]:
        return _merge_usage(summary)

    import requests

    headers = {
        "Authorization": f"Bearer {config.NVIDIA_API_KEY}",
        "Accept": "application/json",
    }
    checked_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        response = requests.get(
            _health_check_url(),
            headers=headers,
            timeout=_health_timeout_sec(),
        )
        summary["reachable"] = True
        summary["last_checked_at"] = checked_at

        if 200 <= response.status_code < 300:
            summary.update({
                "healthy": True,
                "status": "connected",
                "label": "NVIDIA Live",
                "tone": "ok",
                "last_error": None,
            })
        elif response.status_code in (401, 403):
            summary.update({
                "healthy": False,
                "status": "auth_error",
                "label": "Auth Error",
                "tone": "bad",
                "last_error": f"HTTP {response.status_code}",
            })
        else:
            summary.update({
                "healthy": False,
                "status": "http_error",
                "label": "Server Error",
                "tone": "warn",
                "last_error": f"HTTP {response.status_code}",
            })
    except Exception as exc:
        summary.update({
            "reachable": False,
            "healthy": False,
            "status": "unreachable",
            "label": "Offline",
            "tone": "bad",
            "last_checked_at": checked_at,
            "last_error": str(exc),
        })

    return _merge_usage(summary)


def get_status_summary(*, force_refresh: bool = False) -> dict:
    global _HEALTH_CACHE_RESULT, _HEALTH_CACHE_EXPIRES_AT
    now = time.time()
    with _HEALTH_CACHE_LOCK:
        if not force_refresh and _HEALTH_CACHE_RESULT is not None and now < _HEALTH_CACHE_EXPIRES_AT:
            return dict(_HEALTH_CACHE_RESULT)

    summary = _probe_server_health()

    with _HEALTH_CACHE_LOCK:
        _HEALTH_CACHE_RESULT = dict(summary)
        _HEALTH_CACHE_EXPIRES_AT = now + _HEALTH_CACHE_TTL_SEC

    return dict(summary)


def _check_budget(request_type: str) -> None:
    usage = db.burst_usage_today()
    if usage["request_count"] >= config.NVIDIA_BURST_DAILY_REQUEST_CAP:
        raise BudgetExceededError(
            f"Daily request cap reached ({config.NVIDIA_BURST_DAILY_REQUEST_CAP})"
        )
    if usage["token_count"] >= config.NVIDIA_BURST_DAILY_TOKEN_CAP:
        raise BudgetExceededError(
            f"Daily token cap reached ({config.NVIDIA_BURST_DAILY_TOKEN_CAP})"
        )


def get_usage_summary() -> dict:
    return get_status_summary()


# ── Cache ────────────────────────────────────────────────────────────────────

def _content_hash(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _cache_get(content_hash: str, request_type: str) -> Optional[dict]:
    if not config.NVIDIA_BURST_CACHE_ENABLED:
        return None
    raw = db.burst_cache_get(content_hash, request_type)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _cache_put(content_hash: str, request_type: str, request_data: dict, response_data: dict) -> None:
    if not config.NVIDIA_BURST_CACHE_ENABLED:
        return
    db.burst_cache_put(
        content_hash=content_hash,
        request_type=request_type,
        request_json=json.dumps(request_data),
        response_json=json.dumps(response_data),
        ttl_hours=config.NVIDIA_BURST_CACHE_TTL_HOURS,
    )


# ── Privacy ──────────────────────────────────────────────────────────────────

_PRIVACY_MAX_DIM = 1024


def _prepare_image_bytes(image_path: Path) -> bytes:
    """Load image, optionally strip EXIF and resize for privacy, return JPEG bytes."""
    from PIL import Image

    img = Image.open(image_path)

    if config.NVIDIA_BURST_PRIVACY_MODE:
        # Strip all EXIF by re-creating without metadata
        clean = Image.new(img.mode, img.size)
        clean.paste(img)
        img = clean

        # Resize if too large
        w, h = img.size
        if max(w, h) > _PRIVACY_MAX_DIM:
            scale = _PRIVACY_MAX_DIM / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    if img.mode == "RGBA":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _image_to_data_url(image_path: Path) -> str:
    """Return a base64 data URL suitable for vision model payloads."""
    img_bytes = _prepare_image_bytes(image_path)
    b64 = base64.b64encode(img_bytes).decode("ascii")
    return f"data:image/jpeg;base64,{b64}"


# ── HTTP client ──────────────────────────────────────────────────────────────

def _api_post(endpoint: str, payload: dict, *, timeout: int | None = None) -> dict:
    """POST to an NVIDIA NIM endpoint. Returns parsed JSON response."""
    import requests

    timeout = timeout or config.NVIDIA_BURST_TIMEOUT_SEC
    url = f"{config.NVIDIA_API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {config.NVIDIA_API_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _chat_completion(messages: list[dict], *, model: str | None = None, max_tokens: int = 512) -> dict:
    """Call the chat/completions endpoint (OpenAI-compatible)."""
    payload = {
        "model": model or config.NVIDIA_LLM_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    return _api_post("chat/completions", payload)


def _extract_chat_text(response: dict) -> str:
    """Pull the assistant message text from a chat completion response."""
    choices = response.get("choices", [])
    if not choices:
        return ""
    return (choices[0].get("message", {}).get("content", "") or "").strip()


def _extract_usage_tokens(response: dict) -> int:
    """Pull total token count from a completion response."""
    usage = response.get("usage", {})
    return int(usage.get("total_tokens", 0))


# ── Public API: Query Rewrite ────────────────────────────────────────────────

_REWRITE_SYSTEM_PROMPT = (
    "You are a search query expansion assistant for a family photo library. "
    "Given a user's search query, produce an expanded version that includes "
    "synonyms, related terms, and likely intent. Keep the expansion concise "
    "(under 60 words). Return ONLY the expanded query, no explanation."
)


def query_rewrite(original_query: str) -> Optional[str]:
    """Expand a search query via LLM. Returns expanded query or None if disabled/error."""
    if not is_enabled() or not config.NVIDIA_BURST_QUERY_REWRITE:
        return None

    cache_key = _content_hash(f"rewrite:{original_query}")
    cached = _cache_get(cache_key, "query_rewrite")
    if cached:
        return cached.get("expanded_query")

    try:
        _check_budget("query_rewrite")
    except BudgetExceededError as e:
        log.warning("Query rewrite skipped: %s", e)
        return None

    try:
        response = _chat_completion(
            [
                {"role": "system", "content": _REWRITE_SYSTEM_PROMPT},
                {"role": "user", "content": original_query},
            ],
            max_tokens=128,
        )
        expanded = _extract_chat_text(response)
        tokens = _extract_usage_tokens(response)
        db.burst_usage_increment("query_rewrite", tokens)

        result = {"expanded_query": expanded, "original": original_query}
        _cache_put(cache_key, "query_rewrite", {"query": original_query}, result)

        log.info("Burst query rewrite: %r -> %r (%d tokens)", original_query, expanded, tokens)
        return expanded
    except Exception as e:
        log.warning("Burst query rewrite failed for %r: %s", original_query, e)
        return None


# ── Public API: Rerank ───────────────────────────────────────────────────────

def rerank(
    query: str,
    documents: list[dict],
    *,
    top_n: int | None = None,
) -> Optional[list[dict]]:
    """Rerank search results via hosted reranker.

    Args:
        query: the search query
        documents: list of dicts, each must have a "text" key
        top_n: how many to return (default from config)

    Returns list of dicts with original data + "rerank_score", or None if disabled.
    """
    if not is_enabled() or not config.NVIDIA_BURST_RERANK:
        return None
    if not documents:
        return None

    top_n = top_n or config.NVIDIA_BURST_RERANK_TOP_N
    doc_texts = [d.get("text", "") for d in documents]

    cache_key = _content_hash(f"rerank:{query}:{json.dumps(doc_texts)}")
    cached = _cache_get(cache_key, "rerank")
    if cached:
        return cached.get("results")

    try:
        _check_budget("rerank")
    except BudgetExceededError as e:
        log.warning("Rerank skipped: %s", e)
        return None

    try:
        payload = {
            "model": config.NVIDIA_RERANK_MODEL,
            "query": {"text": query},
            "passages": [{"text": t} for t in doc_texts],
            "top_n": min(top_n, len(documents)),
        }
        response = _api_post("ranking", payload)
        rankings = response.get("rankings", [])

        tokens = response.get("usage", {}).get("total_tokens", len(doc_texts) * 50)
        db.burst_usage_increment("rerank", int(tokens))

        results = []
        for rank_entry in rankings:
            idx = rank_entry["index"]
            if 0 <= idx < len(documents):
                doc = dict(documents[idx])
                doc["rerank_score"] = rank_entry.get("logit", 0.0)
                results.append(doc)

        cache_result = {"results": results}
        _cache_put(cache_key, "rerank", {"query": query, "doc_count": len(documents)}, cache_result)

        log.info("Burst rerank: %r over %d docs -> %d results", query, len(documents), len(results))
        return results
    except Exception as e:
        log.warning("Burst rerank failed for %r: %s", query, e)
        return None


# ── Public API: Image Captioning ─────────────────────────────────────────────

_CAPTION_SYSTEM_PROMPT = (
    "Describe this photo concisely in 1-3 sentences. Focus on the people, "
    "objects, setting, and any notable activities. Be factual and specific."
)


def caption_image(image_path: Path) -> Optional[str]:
    """Generate a text caption for a photo via vision model. Returns caption or None."""
    if not is_enabled():
        return None

    path = Path(image_path)
    if not path.exists():
        log.warning("Caption requested for missing file: %s", path)
        return None

    # Cache by file content hash
    file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    cache_key = _content_hash(f"caption:{file_hash}")
    cached = _cache_get(cache_key, "caption")
    if cached:
        return cached.get("caption")

    try:
        _check_budget("caption")
    except BudgetExceededError as e:
        log.warning("Caption skipped: %s", e)
        return None

    try:
        data_url = _image_to_data_url(path)
        response = _chat_completion(
            [
                {"role": "system", "content": _CAPTION_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": data_url}},
                        {"type": "text", "text": "Describe this photo."},
                    ],
                },
            ],
            model=config.NVIDIA_VISION_MODEL,
            max_tokens=256,
        )
        caption = _extract_chat_text(response)
        tokens = _extract_usage_tokens(response)
        db.burst_usage_increment("caption", tokens)

        _cache_put(cache_key, "caption", {"file_hash": file_hash}, {"caption": caption})

        log.info("Burst caption for %s: %s (%d tokens)", path.name, caption[:80], tokens)
        return caption
    except Exception as e:
        log.warning("Burst caption failed for %s: %s", path, e)
        return None
