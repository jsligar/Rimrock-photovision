"""Tests for Phase 5 tag format handling helpers."""

from pathlib import Path

from pipeline.phase5_tag import (
    _classify_exiftool_error,
    _detect_magic,
    _has_extension_mismatch,
    _is_write_unsupported_ext,
)


def _write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_is_write_unsupported_ext_webp(tmp_path):
    p = tmp_path / "x.webp"
    _write(p, b"RIFF" + b"\x10\x00\x00\x00" + b"WEBP" + b"VP8 ")
    assert _is_write_unsupported_ext(p) is True


def test_detect_magic_png(tmp_path):
    p = tmp_path / "x.png"
    _write(p, b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
    assert _detect_magic(p) == "png"


def test_has_extension_mismatch_jpg_with_png_content(tmp_path):
    p = tmp_path / "bad.jpg"
    _write(p, b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
    assert _has_extension_mismatch(p) is True


def test_has_extension_mismatch_false_for_valid_jpeg(tmp_path):
    p = tmp_path / "ok.jpeg"
    _write(p, b"\xff\xd8\xff\xe0" + b"\x00" * 32)
    assert _has_extension_mismatch(p) is False


def test_classify_exiftool_error_webp():
    stderr = "Error: Writing of WEBP files is not yet supported - /tmp/a.webp"
    assert _classify_exiftool_error(stderr) == "unsupported_write"


def test_classify_exiftool_error_mismatch():
    stderr = "Error: Not a valid JPG (looks more like a PNG) - /tmp/a.jpg"
    assert _classify_exiftool_error(stderr) == "format_mismatch"


def test_classify_exiftool_error_other():
    stderr = "Error: Something else happened"
    assert _classify_exiftool_error(stderr) == "error"
