import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

import config

log = logging.getLogger(__name__)

_PHOTOS_FTS_COLUMNS = ["filename", "existing_people", "ocr_text"]
_BACKGROUND_JOB_STALE_SECONDS = 10 * 60


_thread_local = threading.local()


def get_db() -> sqlite3.Connection:
    """Return a thread-local SQLite connection, creating if missing or closed.

    Each OS thread gets its own connection.  Callers may still call
    conn.close() when done; the next get_db() call on that thread will
    transparently open a fresh connection.
    """
    conn: sqlite3.Connection | None = getattr(_thread_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
        except Exception:
            conn = None
            _thread_local.conn = None
    if conn is None:
        conn = sqlite3.connect(str(config.DB_PATH), check_same_thread=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        _thread_local.conn = conn
    return conn


def close_thread_db() -> None:
    """Close and evict the thread-local connection. Safe to call if none exists."""
    conn: sqlite3.Connection | None = getattr(_thread_local, "conn", None)
    if conn is not None:
        _thread_local.conn = None
        try:
            conn.close()
        except Exception:
            pass


def init_db() -> None:
    """Create all tables if they don't exist. Idempotent."""
    conn = get_db()
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS photos (
            photo_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            source_path     TEXT UNIQUE NOT NULL,
            filename        TEXT NOT NULL,
            exif_date       TEXT,
            date_source     TEXT,
            existing_people TEXT,
            dest_path       TEXT,
            copy_verified   INTEGER DEFAULT 0,
            checksum        TEXT,
            processed_at    TEXT,
            process_error   TEXT
        );

        CREATE TABLE IF NOT EXISTS faces (
            face_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            photo_id         INTEGER REFERENCES photos(photo_id),
            bbox_json        TEXT,
            embedding        BLOB NOT NULL,
            detection_score  REAL,
            cluster_id       INTEGER,
            is_ground_truth  INTEGER DEFAULT 0,
            crop_path        TEXT
        );

        CREATE TABLE IF NOT EXISTS clusters (
            cluster_id   INTEGER PRIMARY KEY,
            person_label TEXT,
            face_count   INTEGER DEFAULT 0,
            is_noise     INTEGER DEFAULT 0,
            approved     INTEGER DEFAULT 0,
            updated_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS detections (
            detection_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            photo_id      INTEGER REFERENCES photos(photo_id),
            model         TEXT NOT NULL,
            tag           TEXT NOT NULL,
            tag_group     TEXT,
            confidence    REAL,
            bbox_json     TEXT,
            crop_path     TEXT,
            approved      INTEGER DEFAULT 1,
            created_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS tag_vocabulary (
            vocab_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            tag_group    TEXT NOT NULL,
            tag_name     TEXT NOT NULL,
            prompts_json TEXT NOT NULL,
            enabled      INTEGER DEFAULT 1,
            created_at   TEXT,
            UNIQUE(tag_group, tag_name)
        );

        CREATE TABLE IF NOT EXISTS photo_tags (
            photo_id INTEGER REFERENCES photos(photo_id),
            tag      TEXT NOT NULL,
            source   TEXT NOT NULL,
            PRIMARY KEY (photo_id, tag, source)
        );

        CREATE TABLE IF NOT EXISTS face_move_history (
            move_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at         TEXT NOT NULL,
            source_cluster_id  INTEGER NOT NULL,
            target_cluster_id  INTEGER NOT NULL,
            face_ids_json      TEXT NOT NULL,
            undone_at          TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_state (
            phase             TEXT PRIMARY KEY,
            status            TEXT DEFAULT 'pending',
            progress_current  INTEGER DEFAULT 0,
            progress_total    INTEGER DEFAULT 0,
            started_at        TEXT,
            completed_at      TEXT,
            error_message     TEXT
        );

        CREATE TABLE IF NOT EXISTS background_jobs (
            job_name          TEXT PRIMARY KEY,
            status            TEXT DEFAULT 'pending',
            progress_current  INTEGER DEFAULT 0,
            progress_total    INTEGER DEFAULT 0,
            started_at        TEXT,
            updated_at        TEXT,
            completed_at      TEXT,
            error_message     TEXT,
            detail            TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    phases = ["preflight", "pull", "process", "cluster", "organize", "tag", "push", "verify"]
    for phase in phases:
        cur.execute(
            "INSERT OR IGNORE INTO pipeline_state (phase, status) VALUES (?, 'pending')",
            (phase,),
        )

    _ensure_base_photo_columns(cur)

    now = _now()
    for group, tags in config.SEMANTIC_TAG_GROUPS.items():
        for tag_name, prompts in tags.items():
            cur.execute(
                """
                INSERT OR IGNORE INTO tag_vocabulary
                    (tag_group, tag_name, prompts_json, enabled, created_at)
                VALUES (?, ?, ?, 1, ?)
                """,
                (group, tag_name, json.dumps(prompts), now),
            )

    if config.ENABLE_SEARCH_LAYER:
        _init_search_schema(conn)
        _ensure_search_backfill(conn)

    if config.NVIDIA_BURST_ENABLED:
        _init_burst_schema(conn)

    conn.commit()
    conn.close()


def _ensure_base_photo_columns(cur: sqlite3.Cursor) -> None:
    """Migrate base photo columns added after the initial schema release."""
    cols = {row[1] for row in cur.execute("PRAGMA table_info(photos)").fetchall()}
    if "process_error" not in cols:
        cur.execute("ALTER TABLE photos ADD COLUMN process_error TEXT")


def _ensure_photo_search_columns(cur: sqlite3.Cursor) -> None:
    cols = {row[1] for row in cur.execute("PRAGMA table_info(photos)").fetchall()}
    if "clip_embedding" not in cols:
        cur.execute("ALTER TABLE photos ADD COLUMN clip_embedding BLOB")
    if "ocr_text" not in cols:
        cur.execute("ALTER TABLE photos ADD COLUMN ocr_text TEXT")
    if "ocr_extracted_at" not in cols:
        cur.execute("ALTER TABLE photos ADD COLUMN ocr_extracted_at TEXT")
    if "is_document" not in cols:
        cur.execute("ALTER TABLE photos ADD COLUMN is_document INTEGER NOT NULL DEFAULT 0")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_photos_is_document ON photos(is_document)")


def _photos_fts_columns(conn: sqlite3.Connection) -> list[str]:
    try:
        rows = conn.execute("PRAGMA table_info(photos_fts)").fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row[1]) for row in rows]


def _photos_fts_indexed_doc_count(conn: sqlite3.Connection) -> Optional[int]:
    """Return the number of rows in the FTS5 index, or None on error.

    Uses a direct COUNT on photos_fts rather than a fts5vocab virtual table,
    which is simpler, faster, and not susceptible to partial-state issues from
    interrupted vocab table creation.
    """
    try:
        row = conn.execute("SELECT COUNT(*) FROM photos_fts").fetchone()
        return int(row[0] or 0)
    except sqlite3.OperationalError as exc:
        log.warning("Unable to count photos_fts rows: %s", exc)
        return None


def _drop_photos_fts(cur: sqlite3.Cursor) -> None:
    cur.executescript(
        """
        DROP TRIGGER IF EXISTS photos_fts_insert;
        DROP TRIGGER IF EXISTS photos_fts_update;
        DROP TRIGGER IF EXISTS photos_fts_delete;
        DROP TABLE IF EXISTS photos_fts;
        """
    )


def _create_photos_fts(cur: sqlite3.Cursor) -> None:
    cur.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS photos_fts USING fts5(
            filename,
            existing_people,
            ocr_text,
            content='photos',
            content_rowid='photo_id'
        );

        CREATE TRIGGER IF NOT EXISTS photos_fts_insert
        AFTER INSERT ON photos BEGIN
            INSERT INTO photos_fts(rowid, filename, existing_people, ocr_text)
            VALUES (
                new.photo_id,
                new.filename,
                COALESCE(new.existing_people, ''),
                COALESCE(new.ocr_text, '')
            );
        END;

        CREATE TRIGGER IF NOT EXISTS photos_fts_update
        AFTER UPDATE OF filename, existing_people, ocr_text ON photos BEGIN
            INSERT INTO photos_fts(photos_fts, rowid, filename, existing_people, ocr_text)
            VALUES (
                'delete',
                old.photo_id,
                old.filename,
                COALESCE(old.existing_people, ''),
                COALESCE(old.ocr_text, '')
            );
            INSERT INTO photos_fts(rowid, filename, existing_people, ocr_text)
            VALUES (
                new.photo_id,
                new.filename,
                COALESCE(new.existing_people, ''),
                COALESCE(new.ocr_text, '')
            );
        END;

        CREATE TRIGGER IF NOT EXISTS photos_fts_delete
        AFTER DELETE ON photos BEGIN
            INSERT INTO photos_fts(photos_fts, rowid, filename, existing_people, ocr_text)
            VALUES (
                'delete',
                old.photo_id,
                old.filename,
                COALESCE(old.existing_people, ''),
                COALESCE(old.ocr_text, '')
            );
        END;
        """
    )


def _init_search_schema(conn: sqlite3.Connection) -> None:
    """Create search indexes, FTS5 table, and OCR/CLIP columns."""
    cur = conn.cursor()

    _ensure_photo_search_columns(cur)

    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_photos_exif_date         ON photos(exif_date);
        CREATE INDEX IF NOT EXISTS idx_photos_filename          ON photos(filename);
        CREATE INDEX IF NOT EXISTS idx_photos_ocr_extracted_at  ON photos(ocr_extracted_at);
        CREATE INDEX IF NOT EXISTS idx_faces_photo_id           ON faces(photo_id);
        CREATE INDEX IF NOT EXISTS idx_faces_cluster_id         ON faces(cluster_id);
        CREATE INDEX IF NOT EXISTS idx_detections_photo_id      ON detections(photo_id);
        CREATE INDEX IF NOT EXISTS idx_detections_tag           ON detections(tag);
        CREATE INDEX IF NOT EXISTS idx_detections_confidence    ON detections(confidence);
        CREATE INDEX IF NOT EXISTS idx_photo_tags_tag           ON photo_tags(tag);
        CREATE INDEX IF NOT EXISTS idx_photo_tags_photo_id      ON photo_tags(photo_id);
        """
    )

    try:
        existing_fts_cols = _photos_fts_columns(conn)
        if existing_fts_cols and existing_fts_cols != _PHOTOS_FTS_COLUMNS:
            log.info(
                "Recreating photos_fts for updated search schema: %s -> %s",
                existing_fts_cols,
                _PHOTOS_FTS_COLUMNS,
            )
            _drop_photos_fts(cur)
        _create_photos_fts(cur)
    except Exception as exc:
        log.warning("FTS5 not available (%s) - text search disabled", exc)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_searches (
            search_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            query_json  TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            updated_at  TEXT
        )
        """
    )

    conn.commit()


def _ensure_search_backfill(conn: sqlite3.Connection) -> None:
    """Rebuild FTS when the search layer is enabled on an existing database."""
    try:
        photo_count = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
    except Exception as exc:
        log.warning("Search schema verification skipped: %s", exc)
        return

    # Detect a previously interrupted rebuild.
    try:
        started = conn.execute(
            "SELECT value FROM pipeline_meta WHERE key='fts_rebuild_started_at'"
        ).fetchone()
        completed = conn.execute(
            "SELECT value FROM pipeline_meta WHERE key='fts_rebuild_completed_at'"
        ).fetchone()
        if started and not completed:
            log.warning(
                "FTS rebuild started at %s but never completed (process was likely killed). "
                "Re-running now.", started["value"],
            )
    except Exception:
        pass  # pipeline_meta may not exist yet on very first init

    indexed_count = _photos_fts_indexed_doc_count(conn)
    if indexed_count is None:
        return

    if photo_count != indexed_count:
        log.info(
            "Rebuilding photos_fts index (%d photos, %d indexed docs)", photo_count, indexed_count
        )
        backfill_fts(conn)
    else:
        log.debug("photos_fts index is current (%d docs).", indexed_count)


def backfill_fts(conn: sqlite3.Connection | None = None) -> None:
    """Rebuild FTS index from existing photos. Idempotent and atomic.

    The FTS5 'rebuild' command executes as a single atomic SQL write.
    Timestamps are recorded in pipeline_meta so the startup check can
    detect a previously interrupted rebuild attempt.
    """
    if not config.ENABLE_SEARCH_LAYER:
        return

    close = conn is None
    if conn is None:
        conn = get_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES ('fts_rebuild_started_at', ?)",
            (_now(),),
        )
        conn.commit()
        conn.execute("INSERT INTO photos_fts(photos_fts) VALUES('rebuild')")
        conn.commit()
        conn.execute(
            "INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES ('fts_rebuild_completed_at', ?)",
            (_now(),),
        )
        conn.commit()
    except Exception as exc:
        log.warning("FTS backfill failed: %s", exc)
    finally:
        if close:
            conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_phase_running(phase: str) -> None:
    conn = get_db()
    conn.execute(
        """
        UPDATE pipeline_state
           SET status='running', started_at=?, completed_at=NULL, error_message=NULL,
               progress_current=0
         WHERE phase=?
        """,
        (_now(), phase),
    )
    conn.commit()
    conn.close()


def mark_phase_complete(phase: str) -> None:
    conn = get_db()
    conn.execute(
        "UPDATE pipeline_state SET status='complete', completed_at=? WHERE phase=?",
        (_now(), phase),
    )
    conn.commit()
    conn.close()


def update_phase_progress(phase: str, current: int, total: Optional[int] = None) -> None:
    conn = get_db()
    if total is not None:
        conn.execute(
            "UPDATE pipeline_state SET progress_current=?, progress_total=? WHERE phase=?",
            (current, total, phase),
        )
    else:
        conn.execute(
            "UPDATE pipeline_state SET progress_current=? WHERE phase=?",
            (current, phase),
        )
    conn.commit()
    conn.close()


def mark_phase_error(phase: str, message: str) -> None:
    conn = get_db()
    conn.execute(
        "UPDATE pipeline_state SET status='error', error_message=?, completed_at=? WHERE phase=?",
        (message, _now(), phase),
    )
    conn.commit()
    conn.close()


def reset_phase_state(phases: list[str]) -> None:
    """Reset downstream phases when upstream outputs change."""
    if not phases:
        return

    conn = get_db()
    placeholders = ",".join("?" for _ in phases)
    conn.execute(
        f"""
        UPDATE pipeline_state
           SET status='pending',
               progress_current=0,
               progress_total=0,
               started_at=NULL,
               completed_at=NULL,
               error_message=NULL
         WHERE phase IN ({placeholders})
        """,
        phases,
    )
    conn.commit()
    conn.close()


def _ensure_background_job(
    conn: sqlite3.Connection,
    job_name: str,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO background_jobs
            (job_name, status, progress_current, progress_total)
        VALUES (?, 'pending', 0, 0)
        """,
        (job_name,),
    )


def mark_background_job_running(
    job_name: str,
    *,
    total: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    conn = get_db()
    _ensure_background_job(conn, job_name)
    now = _now()
    conn.execute(
        """
        UPDATE background_jobs
           SET status='running',
               progress_current=0,
               progress_total=COALESCE(?, progress_total),
               started_at=?,
               updated_at=?,
               completed_at=NULL,
               error_message=NULL,
               detail=COALESCE(?, detail)
         WHERE job_name=?
        """,
        (total, now, now, detail, job_name),
    )
    conn.commit()
    conn.close()


def update_background_job_progress(
    job_name: str,
    current: int,
    *,
    total: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    conn = get_db()
    _ensure_background_job(conn, job_name)
    if total is None:
        conn.execute(
            """
            UPDATE background_jobs
               SET status='running',
                   progress_current=?,
                   updated_at=?,
                   detail=COALESCE(?, detail)
             WHERE job_name=?
            """,
            (current, _now(), detail, job_name),
        )
    else:
        conn.execute(
            """
            UPDATE background_jobs
               SET status='running',
                   progress_current=?,
                   progress_total=?,
                   updated_at=?,
                   detail=COALESCE(?, detail)
             WHERE job_name=?
            """,
            (current, total, _now(), detail, job_name),
        )
    conn.commit()
    conn.close()


def mark_background_job_complete(
    job_name: str,
    *,
    current: Optional[int] = None,
    total: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    conn = get_db()
    _ensure_background_job(conn, job_name)
    progress_current = current if current is not None else 0
    progress_total = total if total is not None else progress_current
    conn.execute(
        """
        UPDATE background_jobs
           SET status='complete',
               progress_current=?,
               progress_total=?,
               updated_at=?,
               completed_at=?,
               error_message=NULL,
               detail=COALESCE(?, detail)
         WHERE job_name=?
        """,
        (progress_current, progress_total, _now(), _now(), detail, job_name),
    )
    conn.commit()
    conn.close()


def mark_background_job_error(job_name: str, message: str, *, detail: Optional[str] = None) -> None:
    conn = get_db()
    _ensure_background_job(conn, job_name)
    conn.execute(
        """
        UPDATE background_jobs
           SET status='error',
               updated_at=?,
               completed_at=?,
               error_message=?,
               detail=COALESCE(?, detail)
         WHERE job_name=?
        """,
        (_now(), _now(), message, detail, job_name),
    )
    conn.commit()
    conn.close()


def reconcile_background_jobs(conn: sqlite3.Connection | None = None) -> int:
    close = conn is None
    if conn is None:
        conn = get_db()

    assert conn is not None
    stale_rows = conn.execute(
        """
        SELECT job_name, started_at, updated_at
          FROM background_jobs
         WHERE status='running'
        """
    ).fetchall()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    stale_job_names: list[str] = []

    for row in stale_rows:
        last_seen = row["updated_at"] or row["started_at"]
        if not last_seen:
            continue
        try:
            stamp = datetime.fromisoformat(str(last_seen))
        except ValueError:
            continue
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        if (now - stamp).total_seconds() > _BACKGROUND_JOB_STALE_SECONDS:
            stale_job_names.append(str(row["job_name"]))

    if stale_job_names:
        message = "Background job appears stale or was interrupted."
        conn.executemany(
            """
            UPDATE background_jobs
               SET status='error',
                   updated_at=?,
                   completed_at=?,
                   error_message=COALESCE(error_message, ?)
             WHERE job_name=?
            """,
            [(now_iso, now_iso, message, job_name) for job_name in stale_job_names],
        )
        conn.commit()
        log.warning("Marked stale background jobs as error: %s", ", ".join(stale_job_names))

    if close:
        conn.close()

    return len(stale_job_names)


# ── NVIDIA Burst Intelligence (Phase 4, gated) ──────────────────────────────

def _init_burst_schema(conn: sqlite3.Connection) -> None:
    """Create burst cache and usage tables. Only called when NVIDIA_BURST_ENABLED."""
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS burst_cache (
            cache_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            content_hash  TEXT NOT NULL,
            request_type  TEXT NOT NULL,
            request_json  TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            expires_at    TEXT NOT NULL,
            UNIQUE(content_hash, request_type)
        );

        CREATE INDEX IF NOT EXISTS idx_burst_cache_hash
            ON burst_cache(content_hash, request_type);
        CREATE INDEX IF NOT EXISTS idx_burst_cache_expires
            ON burst_cache(expires_at);

        CREATE TABLE IF NOT EXISTS burst_usage (
            usage_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            usage_date     TEXT NOT NULL,
            request_type   TEXT NOT NULL,
            request_count  INTEGER DEFAULT 0,
            token_count    INTEGER DEFAULT 0,
            UNIQUE(usage_date, request_type)
        );

        CREATE INDEX IF NOT EXISTS idx_burst_usage_date
            ON burst_usage(usage_date);
        """
    )
    conn.commit()


def burst_cache_get(content_hash: str, request_type: str) -> Optional[str]:
    """Return cached response JSON if present and not expired, else None."""
    conn = get_db()
    row = conn.execute(
        """SELECT response_json FROM burst_cache
           WHERE content_hash=? AND request_type=?
             AND expires_at > ?""",
        (content_hash, request_type, _now()),
    ).fetchone()
    return row["response_json"] if row else None


def burst_cache_put(
    content_hash: str,
    request_type: str,
    request_json: str,
    response_json: str,
    ttl_hours: int,
) -> None:
    """Insert or replace a burst cache entry."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=ttl_hours)
    conn = get_db()
    conn.execute(
        """INSERT OR REPLACE INTO burst_cache
               (content_hash, request_type, request_json, response_json, created_at, expires_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_hash, request_type, request_json, response_json,
         now.isoformat(), expires.isoformat()),
    )
    conn.commit()


def burst_cache_prune() -> int:
    """Delete expired cache entries. Returns count deleted."""
    conn = get_db()
    cur = conn.execute(
        "DELETE FROM burst_cache WHERE expires_at <= ?", (_now(),)
    )
    conn.commit()
    deleted = cur.rowcount
    return deleted


def burst_usage_increment(request_type: str, tokens: int = 0) -> None:
    """Increment daily usage counters for a request type."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_db()
    conn.execute(
        """INSERT INTO burst_usage (usage_date, request_type, request_count, token_count)
           VALUES (?, ?, 1, ?)
           ON CONFLICT(usage_date, request_type) DO UPDATE SET
               request_count = request_count + 1,
               token_count = token_count + excluded.token_count""",
        (today, request_type, tokens),
    )
    conn.commit()


def burst_usage_today() -> dict:
    """Return today's aggregate usage: {request_count, token_count, by_type: [...]}."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_db()
    rows = conn.execute(
        "SELECT request_type, request_count, token_count FROM burst_usage WHERE usage_date=?",
        (today,),
    ).fetchall()
    total_requests = sum(r["request_count"] for r in rows)
    total_tokens = sum(r["token_count"] for r in rows)
    return {
        "date": today,
        "request_count": total_requests,
        "token_count": total_tokens,
        "by_type": [
            {"type": r["request_type"], "requests": r["request_count"], "tokens": r["token_count"]}
            for r in rows
        ],
    }


# ── Pipeline metadata key-value store ────────────────────────────────────────

def pipeline_meta_get(key: str) -> Optional[str]:
    """Return the value for *key* from pipeline_meta, or None if absent."""
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM pipeline_meta WHERE key=?", (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def pipeline_meta_set(key: str, value: str) -> None:
    """Upsert a key-value pair in pipeline_meta."""
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()


# ── Detection → photo_tags sync ───────────────────────────────────────────────

def sync_photo_tag_from_detection(
    conn: sqlite3.Connection,
    photo_id: int,
    tag: str,
    source: str,
) -> None:
    """Reconcile photo_tags with the current detection approval state.

    Queries all detections for *photo_id* + *tag* + *source* and adds or
    removes the photo_tags row accordingly.  Call this after toggling a
    detection's ``approved`` flag so photo_tags stays in sync without a
    full re-tag pass.
    """
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
    conn.commit()
