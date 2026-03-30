"""Tests for db.py — schema init and phase state transitions."""

import db


def test_init_db_creates_tables(tmp_db):
    c = db.get_db()
    tables = {
        r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    c.close()
    assert "photos" in tables
    assert "faces" in tables
    assert "clusters" in tables
    assert "detections" in tables
    assert "tag_vocabulary" in tables
    assert "photo_tags" in tables
    assert "pipeline_state" in tables


def test_all_phases_seeded(tmp_db):
    c = db.get_db()
    phases = {r[0] for r in c.execute("SELECT phase FROM pipeline_state").fetchall()}
    c.close()
    expected = {"preflight", "pull", "process", "cluster", "organize", "tag", "push", "verify"}
    assert phases == expected


def test_all_phases_start_pending(tmp_db):
    c = db.get_db()
    statuses = {r[0] for r in c.execute("SELECT status FROM pipeline_state").fetchall()}
    c.close()
    assert statuses == {"pending"}


def test_mark_phase_running(tmp_db):
    db.mark_phase_running("pull")
    c = db.get_db()
    row = c.execute("SELECT status, started_at FROM pipeline_state WHERE phase='pull'").fetchone()
    c.close()
    assert row["status"] == "running"
    assert row["started_at"] is not None


def test_mark_phase_complete(tmp_db):
    db.mark_phase_running("pull")
    db.mark_phase_complete("pull")
    c = db.get_db()
    row = c.execute("SELECT status, completed_at FROM pipeline_state WHERE phase='pull'").fetchone()
    c.close()
    assert row["status"] == "complete"
    assert row["completed_at"] is not None


def test_mark_phase_error(tmp_db):
    db.mark_phase_error("process", "model load failed")
    c = db.get_db()
    row = c.execute("SELECT status, error_message FROM pipeline_state WHERE phase='process'").fetchone()
    c.close()
    assert row["status"] == "error"
    assert row["error_message"] == "model load failed"


def test_update_phase_progress_with_total(tmp_db):
    db.update_phase_progress("process", 42, 100)
    c = db.get_db()
    row = c.execute(
        "SELECT progress_current, progress_total FROM pipeline_state WHERE phase='process'"
    ).fetchone()
    c.close()
    assert row["progress_current"] == 42
    assert row["progress_total"] == 100


def test_update_phase_progress_without_total(tmp_db):
    db.update_phase_progress("process", 10, 100)
    db.update_phase_progress("process", 20)
    c = db.get_db()
    row = c.execute(
        "SELECT progress_current, progress_total FROM pipeline_state WHERE phase='process'"
    ).fetchone()
    c.close()
    assert row["progress_current"] == 20
    assert row["progress_total"] == 100  # unchanged


def test_init_db_idempotent(tmp_db):
    """Calling init_db twice should not raise or duplicate data."""
    db.init_db()
    c = db.get_db()
    count = c.execute("SELECT COUNT(*) FROM pipeline_state").fetchone()[0]
    c.close()
    assert count == 8


def test_reset_phase_state_clears_completion_metadata(tmp_db):
    db.mark_phase_running("tag")
    db.update_phase_progress("tag", 42, 100)
    db.mark_phase_complete("tag")

    db.reset_phase_state(["tag"])

    c = db.get_db()
    row = c.execute(
        """
        SELECT status, progress_current, progress_total, started_at, completed_at, error_message
        FROM pipeline_state
        WHERE phase='tag'
        """
    ).fetchone()
    c.close()

    assert row["status"] == "pending"
    assert row["progress_current"] == 0
    assert row["progress_total"] == 0
    assert row["started_at"] is None
    assert row["completed_at"] is None
    assert row["error_message"] is None
