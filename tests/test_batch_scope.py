from pathlib import Path

import pytest

import config
from batch_scope import (
    BatchScopeError,
    filter_by_batch_scope,
    load_batch_scope,
    resolve_manifest_media_selection,
)


def test_load_batch_scope_skips_comments_and_dedupes(tmp_path, monkeypatch):
    originals = tmp_path / "originals"
    originals.mkdir()
    manifest = tmp_path / "batch.txt"
    manifest.write_text(
        "# comment\n"
        "By-Year/2025/a.jpg\n"
        "\n"
        "By-Year/2025/a.jpg\n"
        "By-Year/2025/b.jpg\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)

    scope = load_batch_scope()
    assert scope is not None
    assert scope.relative_paths == ("By-Year/2025/a.jpg", "By-Year/2025/b.jpg")
    assert scope.contains("By-Year\\2025\\a.jpg")


def test_load_batch_scope_rejects_parent_traversal(tmp_path, monkeypatch):
    manifest = tmp_path / "batch.txt"
    manifest.write_text("../escape.jpg\n", encoding="utf-8")

    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)

    with pytest.raises(BatchScopeError):
        load_batch_scope()


def test_resolve_manifest_media_selection_tracks_images_raw_and_missing(tmp_path, monkeypatch):
    originals = tmp_path / "originals"
    target_dir = originals / "By-Year" / "2025"
    target_dir.mkdir(parents=True)
    (target_dir / "good.jpg").write_bytes(b"jpg")
    (target_dir / "raw.cr3").write_bytes(b"raw")
    (target_dir / "note.txt").write_text("skip", encoding="utf-8")

    manifest = tmp_path / "batch.txt"
    manifest.write_text(
        "By-Year/2025/good.jpg\n"
        "By-Year/2025/raw.cr3\n"
        "By-Year/2025/missing.jpg\n"
        "By-Year/2025/note.txt\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(config, "ORIGINALS_DIR", originals)
    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)

    scope = load_batch_scope()
    selection = resolve_manifest_media_selection(scope, base_dir=originals)

    assert [p.name for p in selection.image_paths] == ["good.jpg"]
    assert [p.name for p in selection.raw_paths] == ["raw.cr3"]
    assert list(selection.missing_relative_paths) == ["By-Year/2025/missing.jpg"]
    assert list(selection.unsupported_relative_paths) == ["By-Year/2025/note.txt"]


def test_filter_by_batch_scope_filters_rows_by_source_path(tmp_path, monkeypatch):
    manifest = tmp_path / "batch.txt"
    manifest.write_text("By-Year/2025/keep.jpg\n", encoding="utf-8")

    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)
    scope = load_batch_scope()

    rows = [
        {"source_path": "By-Year/2025/keep.jpg"},
        {"source_path": "By-Year/2025/drop.jpg"},
    ]
    filtered, skipped = filter_by_batch_scope(
        rows,
        batch_scope=scope,
        path_getter=lambda row: row["source_path"],
    )

    assert [row["source_path"] for row in filtered] == ["By-Year/2025/keep.jpg"]
    assert skipped == 1
