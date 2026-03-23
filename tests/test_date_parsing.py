"""Tests for pure date-parsing functions in phase2_process."""

import json
import tempfile
from pathlib import Path

import pytest

# Import only the pure functions — no GPU/model imports triggered
from pipeline.phase2_process import (
    _parse_exif_date,
    _folder_date_hint,
)


class TestParseExifDate:
    def test_valid_exif_date(self):
        result = _parse_exif_date("2021:07:15 14:30:00")
        assert result == "2021-07-15T14:30:00+00:00"

    def test_empty_string(self):
        assert _parse_exif_date("") is None

    def test_none_equivalent_falsy(self):
        assert _parse_exif_date(None) is None

    def test_invalid_format(self):
        assert _parse_exif_date("not-a-date") is None

    def test_partial_date(self):
        assert _parse_exif_date("2021:07") is None

    def test_zero_date(self):
        # Some cameras write 0000:00:00 00:00:00
        assert _parse_exif_date("0000:00:00 00:00:00") is None

    def test_boundary_year(self):
        result = _parse_exif_date("1970:01:01 00:00:00")
        assert result is not None
        assert result.startswith("1970-01-01")


class TestFolderDateHint:
    def test_full_date_in_path(self):
        result = _folder_date_hint(Path("/photos/2019-07-04/img.jpg"))
        assert result == "2019-07-04T00:00:00+00:00"

    def test_year_month_in_path(self):
        result = _folder_date_hint(Path("/photos/2019-07/img.jpg"))
        assert result == "2019-07-01T00:00:00+00:00"

    def test_year_only_in_path(self):
        result = _folder_date_hint(Path("/photos/2019/img.jpg"))
        assert result == "2019-01-01T00:00:00+00:00"

    def test_no_date_in_path(self):
        assert _folder_date_hint(Path("/no/date/here.jpg")) is None

    def test_year_too_old(self):
        assert _folder_date_hint(Path("/photos/1800/img.jpg")) is None

    def test_year_too_far_future(self):
        assert _folder_date_hint(Path("/photos/2200/img.jpg")) is None

    def test_prefers_deepest_date(self):
        # Most specific (deepest) folder should win since we reverse parts
        result = _folder_date_hint(Path("/2018/2019-06-15/img.jpg"))
        assert result is not None
        assert "2019-06-15" in result

    def test_invalid_date_string_ignored(self):
        # "2019-13-01" is invalid month — should not match full date, fall back
        result = _folder_date_hint(Path("/photos/2019-13-01/img.jpg"))
        # Falls back to year-only from "2019-13-01"? No — "2019-13-01" is 10 chars,
        # tries strptime which fails, then tries 7-char (nope, it's 10), then 4-char year
        # but "2019" is not a standalone part here. Should return None.
        assert result is None

    def test_nested_path_with_no_date_parts(self):
        assert _folder_date_hint(Path("/home/user/photos/vacation/beach.jpg")) is None


class TestParseGoogleSidecar:
    def test_missing_sidecar_returns_none(self, tmp_path):
        from pipeline.phase2_process import _parse_google_sidecar
        result = _parse_google_sidecar(tmp_path / "photo.jpg")
        assert result == (None, [])

    def test_valid_sidecar_with_timestamp_and_people(self, tmp_path):
        from pipeline.phase2_process import _parse_google_sidecar
        sidecar = tmp_path / "photo.jpg.json"
        sidecar.write_text(json.dumps({
            "photoTakenTime": {"timestamp": "1561939200"},  # 2019-07-01
            "people": [{"name": "Alice"}, {"name": "Bob"}]
        }))
        date_str, people = _parse_google_sidecar(tmp_path / "photo.jpg")
        assert date_str is not None
        assert "2019-07-01" in date_str
        assert set(people) == {"Alice", "Bob"}

    def test_sidecar_without_timestamp(self, tmp_path):
        from pipeline.phase2_process import _parse_google_sidecar
        sidecar = tmp_path / "photo.jpg.json"
        sidecar.write_text(json.dumps({"people": [{"name": "Alice"}]}))
        date_str, people = _parse_google_sidecar(tmp_path / "photo.jpg")
        assert date_str is None
        assert people == ["Alice"]

    def test_malformed_sidecar_returns_none(self, tmp_path):
        from pipeline.phase2_process import _parse_google_sidecar
        sidecar = tmp_path / "photo.jpg.json"
        sidecar.write_text("not valid json {{{")
        result = _parse_google_sidecar(tmp_path / "photo.jpg")
        assert result == (None, [])
