"""Tests for phase1 prefilter extension/signature mismatch detection."""

from pathlib import Path

from pipeline.phase1_pull import _detect_magic, _scan_extension_mismatches


def _write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_detect_magic_jpeg(tmp_path):
    p = tmp_path / "a.jpg"
    _write(p, b"\xff\xd8\xff\xe0" + b"\x00" * 32)
    assert _detect_magic(p) == "jpeg"


def test_detect_magic_webp(tmp_path):
    p = tmp_path / "a.webp"
    _write(p, b"RIFF" + b"\x10\x00\x00\x00" + b"WEBP" + b"VP8 ")
    assert _detect_magic(p) == "webp"


def test_scan_extension_mismatch_detects_webp_named_jpeg(tmp_path):
    bad = tmp_path / "nested" / "wrong.jpeg"
    _write(bad, b"RIFF" + b"\x10\x00\x00\x00" + b"WEBP" + b"VP8 ")

    mismatches = _scan_extension_mismatches(tmp_path)
    assert len(mismatches) == 1
    rel_path, ext, detected = mismatches[0]
    assert rel_path.replace("\\", "/") == "nested/wrong.jpeg"
    assert ext == ".jpeg"
    assert detected == "webp"


def test_scan_extension_mismatch_marks_unknown_signature(tmp_path):
    bad = tmp_path / "mystery.jpg"
    _write(bad, b"NOT_AN_IMAGE_HEADER")

    mismatches = _scan_extension_mismatches(tmp_path)
    assert len(mismatches) == 1
    assert mismatches[0][2] == "unknown"


def test_scan_extension_mismatch_ignores_correct_types(tmp_path):
    good_jpeg = tmp_path / "ok.jpg"
    good_png = tmp_path / "ok.png"
    _write(good_jpeg, b"\xff\xd8\xff\xe0" + b"\x00" * 32)
    _write(good_png, b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

    assert _scan_extension_mismatches(tmp_path) == []
