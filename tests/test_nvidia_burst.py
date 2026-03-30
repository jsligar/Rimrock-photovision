"""Tests for nvidia_burst module — all API calls mocked, no real NVIDIA access."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(autouse=True)
def _burst_enabled(monkeypatch):
    """Enable burst with a fake API key for all tests."""
    monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", True)
    monkeypatch.setattr("config.NVIDIA_API_KEY", "nvapi-test-key")
    monkeypatch.setattr("config.NVIDIA_API_BASE_URL", "https://test.nvidia.com/v1")
    monkeypatch.setattr("config.NVIDIA_LLM_MODEL", "test/llm")
    monkeypatch.setattr("config.NVIDIA_VISION_MODEL", "test/vision")
    monkeypatch.setattr("config.NVIDIA_RERANK_MODEL", "test/rerank")
    monkeypatch.setattr("config.NVIDIA_BURST_DAILY_REQUEST_CAP", 100)
    monkeypatch.setattr("config.NVIDIA_BURST_DAILY_TOKEN_CAP", 50000)
    monkeypatch.setattr("config.NVIDIA_BURST_PRIVACY_MODE", True)
    monkeypatch.setattr("config.NVIDIA_BURST_CACHE_ENABLED", False)
    monkeypatch.setattr("config.NVIDIA_BURST_CACHE_TTL_HOURS", 168)
    monkeypatch.setattr("config.NVIDIA_BURST_TIMEOUT_SEC", 5)
    monkeypatch.setattr("config.NVIDIA_BURST_QUERY_REWRITE", True)
    monkeypatch.setattr("config.NVIDIA_BURST_RERANK", True)
    monkeypatch.setattr("config.NVIDIA_BURST_RERANK_TOP_N", 10)


class TestIsEnabled:
    def test_enabled_with_key(self):
        import nvidia_burst
        assert nvidia_burst.is_enabled() is True

    def test_disabled_without_key(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_API_KEY", "")
        import nvidia_burst
        assert nvidia_burst.is_enabled() is False

    def test_disabled_when_flag_off(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import nvidia_burst
        assert nvidia_burst.is_enabled() is False


class TestBudget:
    def test_check_budget_passes_under_cap(self, monkeypatch):
        import nvidia_burst
        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 0, "by_type": [],
        })
        nvidia_burst._check_budget("test")  # should not raise

    def test_check_budget_request_cap(self, monkeypatch):
        import nvidia_burst
        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 100, "token_count": 0, "by_type": [],
        })
        with pytest.raises(nvidia_burst.BudgetExceededError, match="request cap"):
            nvidia_burst._check_budget("test")

    def test_check_budget_token_cap(self, monkeypatch):
        import nvidia_burst
        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 50000, "by_type": [],
        })
        with pytest.raises(nvidia_burst.BudgetExceededError, match="token cap"):
            nvidia_burst._check_budget("test")

    def test_usage_summary_when_disabled(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import nvidia_burst
        summary = nvidia_burst.get_usage_summary()
        assert summary == {"enabled": False}


class TestQueryRewrite:
    def test_returns_none_when_disabled(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import nvidia_burst
        assert nvidia_burst.query_rewrite("dog photos") is None

    def test_returns_none_when_rewrite_flag_off(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_QUERY_REWRITE", False)
        import nvidia_burst
        assert nvidia_burst.query_rewrite("dog photos") is None

    def test_successful_rewrite(self, monkeypatch):
        import nvidia_burst

        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 0, "by_type": [],
        })
        monkeypatch.setattr("db.burst_usage_increment", lambda *a, **kw: None)

        mock_response = {
            "choices": [{"message": {"content": "dogs puppies canine pets outdoor photos"}}],
            "usage": {"total_tokens": 42},
        }
        monkeypatch.setattr("nvidia_burst._api_post", lambda *a, **kw: mock_response)

        result = nvidia_burst.query_rewrite("dog photos")
        assert result == "dogs puppies canine pets outdoor photos"

    def test_rewrite_budget_exceeded(self, monkeypatch):
        import nvidia_burst

        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 100, "token_count": 0, "by_type": [],
        })

        result = nvidia_burst.query_rewrite("dog photos")
        assert result is None

    def test_rewrite_api_error_returns_none(self, monkeypatch):
        import nvidia_burst

        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 0, "by_type": [],
        })

        def _raise(*a, **kw):
            raise ConnectionError("network down")

        monkeypatch.setattr("nvidia_burst._api_post", _raise)

        result = nvidia_burst.query_rewrite("dog photos")
        assert result is None


class TestRerank:
    def test_returns_none_when_disabled(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import nvidia_burst
        assert nvidia_burst.rerank("query", [{"text": "a"}]) is None

    def test_returns_none_on_empty_docs(self):
        import nvidia_burst
        assert nvidia_burst.rerank("query", []) is None

    def test_successful_rerank(self, monkeypatch):
        import nvidia_burst

        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 0, "by_type": [],
        })
        monkeypatch.setattr("db.burst_usage_increment", lambda *a, **kw: None)

        docs = [
            {"text": "birthday party", "photo_id": 1},
            {"text": "dog in yard", "photo_id": 2},
            {"text": "birthday cake candles", "photo_id": 3},
        ]
        mock_response = {
            "rankings": [
                {"index": 2, "logit": 0.95},
                {"index": 0, "logit": 0.80},
                {"index": 1, "logit": 0.30},
            ],
            "usage": {"total_tokens": 100},
        }
        monkeypatch.setattr("nvidia_burst._api_post", lambda *a, **kw: mock_response)

        result = nvidia_burst.rerank("birthday", docs)
        assert result is not None
        assert len(result) == 3
        assert result[0]["photo_id"] == 3
        assert result[0]["rerank_score"] == 0.95


class TestCaptionImage:
    def test_returns_none_when_disabled(self, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import nvidia_burst
        assert nvidia_burst.caption_image(Path("/fake/photo.jpg")) is None

    def test_returns_none_for_missing_file(self):
        import nvidia_burst
        assert nvidia_burst.caption_image(Path("/nonexistent/photo.jpg")) is None

    def test_successful_caption(self, monkeypatch, tmp_path):
        import nvidia_burst

        # Create a tiny valid JPEG
        img_path = tmp_path / "test.jpg"
        try:
            from PIL import Image
            img = Image.new("RGB", (100, 100), color="red")
            img.save(str(img_path), "JPEG")
        except ImportError:
            pytest.skip("Pillow not installed")

        monkeypatch.setattr("db.burst_usage_today", lambda: {
            "date": "2026-01-01", "request_count": 0, "token_count": 0, "by_type": [],
        })
        monkeypatch.setattr("db.burst_usage_increment", lambda *a, **kw: None)

        mock_response = {
            "choices": [{"message": {"content": "A red square image."}}],
            "usage": {"total_tokens": 30},
        }
        monkeypatch.setattr("nvidia_burst._api_post", lambda *a, **kw: mock_response)

        result = nvidia_burst.caption_image(img_path)
        assert result == "A red square image."


class TestContentHash:
    def test_deterministic(self):
        import nvidia_burst
        a = nvidia_burst._content_hash("hello")
        b = nvidia_burst._content_hash("hello")
        assert a == b
        assert len(a) == 64

    def test_different_inputs(self):
        import nvidia_burst
        a = nvidia_burst._content_hash("hello")
        b = nvidia_burst._content_hash("world")
        assert a != b


class TestDbBurstSchema:
    def test_burst_tables_created(self, tmp_db, monkeypatch):
        """When burst is enabled, init_db creates burst_cache and burst_usage tables."""
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", True)
        import db as db_module
        db_module.init_db()
        conn = db_module.get_db()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()
        assert "burst_cache" in tables
        assert "burst_usage" in tables

    def test_burst_tables_not_created_when_disabled(self, tmp_db, monkeypatch):
        """When burst is disabled, burst tables are not created."""
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", False)
        import db as db_module
        # Re-init without burst
        Path(tmp_db).unlink(missing_ok=True)
        db_module.init_db()
        conn = db_module.get_db()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()
        assert "burst_cache" not in tables
        assert "burst_usage" not in tables

    def test_usage_increment_and_today(self, tmp_db, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", True)
        import db as db_module
        db_module.init_db()

        db_module.burst_usage_increment("query_rewrite", 42)
        db_module.burst_usage_increment("query_rewrite", 10)
        db_module.burst_usage_increment("caption", 100)

        usage = db_module.burst_usage_today()
        assert usage["request_count"] == 3
        assert usage["token_count"] == 152
        assert len(usage["by_type"]) == 2

    def test_cache_put_get(self, tmp_db, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", True)
        import db as db_module
        db_module.init_db()

        db_module.burst_cache_put("abc123", "rewrite", '{"q":"test"}', '{"result":"ok"}', 24)
        result = db_module.burst_cache_get("abc123", "rewrite")
        assert result == '{"result":"ok"}'

        # Miss on different type
        assert db_module.burst_cache_get("abc123", "caption") is None

    def test_cache_prune(self, tmp_db, monkeypatch):
        monkeypatch.setattr("config.NVIDIA_BURST_ENABLED", True)
        import db as db_module
        db_module.init_db()

        # Insert an already-expired entry
        conn = db_module.get_db()
        conn.execute(
            """INSERT INTO burst_cache
                   (content_hash, request_type, request_json, response_json, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("old", "test", "{}", "{}", "2020-01-01T00:00:00", "2020-01-02T00:00:00"),
        )
        conn.commit()
        conn.close()

        deleted = db_module.burst_cache_prune()
        assert deleted == 1
