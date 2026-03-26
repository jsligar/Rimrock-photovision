"""Tests for pull scope resolution (full vs year-scoped)."""

from pathlib import Path

import pytest

import config
from pipeline.phase1_pull import _resolve_pull_scope


def test_resolve_pull_scope_full(monkeypatch, tmp_path):
    nas = tmp_path / "nas"
    originals = tmp_path / "originals"
    nas.mkdir()
    originals.mkdir()

    monkeypatch.setattr(config, "NAS_SOURCE_DIR", nas)
    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "TEST_YEAR_SCOPE", None)

    src, dst, label = _resolve_pull_scope()
    assert src == nas
    assert dst == originals
    assert label == "full"


def test_resolve_pull_scope_year_by_year_folder(monkeypatch, tmp_path):
    nas = tmp_path / "nas"
    originals = tmp_path / "originals"
    year_dir = nas / "By-Year" / "2025"
    year_dir.mkdir(parents=True)
    originals.mkdir()

    monkeypatch.setattr(config, "NAS_SOURCE_DIR", nas)
    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "TEST_YEAR_SCOPE", "2025")

    src, dst, label = _resolve_pull_scope()
    assert src == year_dir
    assert dst == originals / "By-Year" / "2025"
    assert "year:2025" in label


def test_resolve_pull_scope_year_root_folder(monkeypatch, tmp_path):
    nas = tmp_path / "nas"
    originals = tmp_path / "originals"
    year_dir = nas / "2025"
    year_dir.mkdir(parents=True)
    originals.mkdir()

    monkeypatch.setattr(config, "NAS_SOURCE_DIR", nas)
    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "TEST_YEAR_SCOPE", "2025")

    src, dst, _ = _resolve_pull_scope()
    assert src == year_dir
    assert dst == originals / "2025"


def test_resolve_pull_scope_missing_year_raises(monkeypatch, tmp_path):
    nas = tmp_path / "nas"
    originals = tmp_path / "originals"
    nas.mkdir()
    originals.mkdir()

    monkeypatch.setattr(config, "NAS_SOURCE_DIR", nas)
    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "TEST_YEAR_SCOPE", "2025")

    with pytest.raises(FileNotFoundError):
        _resolve_pull_scope()
