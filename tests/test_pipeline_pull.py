"""Tests for rsync line parsing logic in phase1_pull — covers the operator precedence fix."""

import re


# Replicate the detection logic from phase1_pull.py so we can unit test it in isolation
progress_re = re.compile(r'^\s+[\d,]+\s+\d+%')

def _looks_like_transfer(line: str) -> bool:
    """Mirrors the elif condition in phase1_pull.run_pull() after the precedence fix."""
    if progress_re.match(line):
        return False
    if (
        not line.startswith(" ")
        and not line.startswith("sending")
        and not line.startswith("sent")
        and not line.startswith("total")
        and not line.startswith("rsync")
        and ("/" in line or "." in line)  # ← fixed: explicit parentheses
    ):
        if not any(line.startswith(s) for s in ["building", "delta", "Number", "send", "recv"]):
            return True
    return False


class TestRsyncLineParsing:
    # Lines that should be counted as file transfers
    def test_relative_path_with_slash(self):
        assert _looks_like_transfer("photos/2019/IMG_001.jpg") is True

    def test_filename_with_dot(self):
        assert _looks_like_transfer("IMG_001.jpg") is True

    def test_nested_path(self):
        assert _looks_like_transfer("./2019/vacation/beach.jpg") is True

    # Lines that should NOT be counted
    def test_progress_line_ignored(self):
        assert _looks_like_transfer("    123,456  45%   1.23MB/s  0:00:12") is False

    def test_sent_summary_ignored(self):
        assert _looks_like_transfer("sent 12.4K bytes  received 234 bytes") is False

    def test_total_summary_ignored(self):
        assert _looks_like_transfer("total size is 1.2GB  speedup is 1.00") is False

    def test_rsync_error_line_ignored(self):
        assert _looks_like_transfer("rsync: [sender] write error: Broken pipe") is False

    def test_sending_ignored(self):
        assert _looks_like_transfer("sending incremental file list") is False

    def test_building_ignored(self):
        assert _looks_like_transfer("building file list ... done") is False

    def test_delta_ignored(self):
        assert _looks_like_transfer("delta-transmission enabled") is False

    def test_number_of_files_ignored(self):
        assert _looks_like_transfer("Number of files: 1,234") is False

    def test_blank_line_ignored(self):
        assert _looks_like_transfer("") is False

    def test_indented_line_ignored(self):
        assert _looks_like_transfer("  some.indented.line") is False

    def test_recv_ignored(self):
        assert _looks_like_transfer("recv 1.2K bytes") is False


class TestPrecedenceBugWouldHaveFailed:
    """Demonstrate that the OLD (unfixed) logic would have been wrong."""

    def _old_logic(self, line: str) -> bool:
        """The BUGGY version with broken precedence."""
        if progress_re.match(line):
            return False
        if (
            not line.startswith(" ")
            and not line.startswith("sending")
            and not line.startswith("sent")
            and not line.startswith("total")
            and not line.startswith("rsync")
            and "/" in line or "." in line  # ← no parentheses = bug
        ):
            if not any(line.startswith(s) for s in ["building", "delta", "Number", "send", "recv"]):
                return True
        return False

    def test_old_logic_incorrectly_counts_dot_lines(self):
        # "total size is 1.2GB" — old buggy logic returns True due to "." in line bypassing all guards
        assert self._old_logic("total size is 1.2GB  speedup is 1.00") is True

    def test_new_logic_correctly_rejects_dot_lines(self):
        # Fixed logic respects the startswith("total") guard
        assert _looks_like_transfer("total size is 1.2GB  speedup is 1.00") is False
