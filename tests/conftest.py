"""Shared pytest fixtures."""

import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure repo root is on path so imports work without hardware deps
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """In-memory SQLite DB with schema initialized; patches config.DB_PATH."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("config.DB_PATH", db_path)
    monkeypatch.setattr("config.LOCAL_BASE", tmp_path)
    monkeypatch.setattr("config.ORIGINALS_DIR", tmp_path / "originals")
    monkeypatch.setattr("config.CROPS_DIR", tmp_path / "crops")
    monkeypatch.setattr("config.UNDATED_DIR", "undated")

    import db as db_module
    db_module.init_db()
    return db_path


@pytest.fixture
def conn(tmp_db):
    """Open SQLite connection to the test DB."""
    import db as db_module
    c = db_module.get_db()
    yield c
    c.close()
