"""Shared helpers for the Phase 3 search layer."""

from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:['._-][A-Za-z0-9]+)*")
_MAX_SEARCH_TOKENS = 12
_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "image",
    "in",
    "of",
    "on",
    "or",
    "photo",
    "picture",
    "the",
    "to",
    "with",
}


def search_tokens(query: str | None) -> list[str]:
    """Return stable, bounded search tokens with light stopword removal."""
    raw_tokens = [m.group(0).lower() for m in _TOKEN_RE.finditer(query or "")]
    if not raw_tokens:
        return []

    filtered = [token for token in raw_tokens if token not in _STOPWORDS]
    tokens = filtered or raw_tokens

    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= _MAX_SEARCH_TOKENS:
            break
    return out


def build_fts_match_query(query: str | None) -> str | None:
    """Build a conservative FTS5 query from user text.

    We quote each token to avoid syntax errors from punctuation and treat the
    query as an AND across meaningful terms.
    """
    tokens = search_tokens(query)
    if not tokens:
        return None
    quoted_tokens: list[str] = []
    for token in tokens:
        safe_token = token.replace('"', "")
        if not safe_token:
            continue
        quoted_tokens.append(f'"{safe_token}"')
    return " AND ".join(quoted_tokens) or None


def fts_photo_ids(conn, query: str | None) -> set[int]:
    """Return matching photo IDs for a user query, swallowing FTS syntax issues."""
    return set(fts_ranked_photo_ids(conn, query))


def fts_ranked_photo_ids(conn, query: str | None, limit: int = 200) -> list[int]:
    """Return FTS-ranked photo IDs for a user query."""
    match_query = build_fts_match_query(query)
    if not match_query:
        return []
    try:
        rows = conn.execute(
            """
            SELECT rowid
              FROM photos_fts
             WHERE photos_fts MATCH ?
             ORDER BY bm25(photos_fts, 8.0, 6.0, 2.5)
             LIMIT ?
            """,
            (match_query, int(limit)),
        ).fetchall()
        return [int(r[0]) for r in rows]
    except Exception as exc:
        log.debug("FTS query failed for %r via %r: %s", query, match_query, exc)
        return []
