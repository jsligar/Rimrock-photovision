import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import config


def get_db() -> sqlite3.Connection:
    """Return a thread-local SQLite connection with row_factory set."""
    conn = sqlite3.connect(str(config.DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create all tables if they don't exist. Idempotent."""
    conn = get_db()
    cur = conn.cursor()

    cur.executescript("""
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
            processed_at    TEXT
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

        CREATE TABLE IF NOT EXISTS pipeline_state (
            phase             TEXT PRIMARY KEY,
            status            TEXT DEFAULT 'pending',
            progress_current  INTEGER DEFAULT 0,
            progress_total    INTEGER DEFAULT 0,
            started_at        TEXT,
            completed_at      TEXT,
            error_message     TEXT
        );
    """)

    # Seed pipeline_state rows for all phases
    phases = ['preflight', 'pull', 'process', 'cluster', 'organize', 'tag', 'push', 'verify']
    for phase in phases:
        cur.execute(
            "INSERT OR IGNORE INTO pipeline_state (phase, status) VALUES (?, 'pending')",
            (phase,)
        )

    # Seed tag_vocabulary from config
    now = _now()
    for group, tags in config.SEMANTIC_TAG_GROUPS.items():
        for tag_name, prompts in tags.items():
            cur.execute(
                """INSERT OR IGNORE INTO tag_vocabulary
                   (tag_group, tag_name, prompts_json, enabled, created_at)
                   VALUES (?, ?, ?, 1, ?)""",
                (group, tag_name, json.dumps(prompts), now)
            )

    conn.commit()
    conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_phase_running(phase: str) -> None:
    conn = get_db()
    conn.execute(
        """UPDATE pipeline_state
           SET status='running', started_at=?, completed_at=NULL, error_message=NULL,
               progress_current=0
           WHERE phase=?""",
        (_now(), phase)
    )
    conn.commit()
    conn.close()


def mark_phase_complete(phase: str) -> None:
    conn = get_db()
    conn.execute(
        "UPDATE pipeline_state SET status='complete', completed_at=? WHERE phase=?",
        (_now(), phase)
    )
    conn.commit()
    conn.close()


def update_phase_progress(phase: str, current: int, total: Optional[int] = None) -> None:
    conn = get_db()
    if total is not None:
        conn.execute(
            "UPDATE pipeline_state SET progress_current=?, progress_total=? WHERE phase=?",
            (current, total, phase)
        )
    else:
        conn.execute(
            "UPDATE pipeline_state SET progress_current=? WHERE phase=?",
            (current, phase)
        )
    conn.commit()
    conn.close()


def mark_phase_error(phase: str, message: str) -> None:
    conn = get_db()
    conn.execute(
        "UPDATE pipeline_state SET status='error', error_message=?, completed_at=? WHERE phase=?",
        (message, _now(), phase)
    )
    conn.commit()
    conn.close()
