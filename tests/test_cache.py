import json
import sys
from pathlib import Path
import types
import logging

import fakeredis
import redis

sys.path.append(str(Path(__file__).resolve().parents[1]))

# Create a minimal stub for auto_reviews_parser module required by ParserService
dummy_module = types.ModuleType("auto_reviews_parser")

class _Config:
    DB_PATH = ":memory:"
    TARGET_BRANDS = {}


class _Parser:
    def __init__(self, db_path, queue_service=None):
        self.db = types.SimpleNamespace(get_parsing_stats=lambda: {})


dummy_module.Config = _Config
dummy_module.AutoReviewsParser = _Parser
sys.modules["auto_reviews_parser"] = dummy_module

from src.utils.cache import RedisCache
from src.services.parser_service import ParserService


def _fake_from_url(url: str, decode_responses: bool = True):
    return fakeredis.FakeRedis(decode_responses=decode_responses)


def test_redis_cache_get_set(monkeypatch):
    monkeypatch.setattr(redis, "from_url", _fake_from_url)
    cache = RedisCache("redis://localhost:6379/0")

    assert cache.get("key") is None
    cache.set("key", "value")
    assert cache.get("key") == "value"


def test_parser_service_uses_cache(monkeypatch):
    monkeypatch.setattr(redis, "from_url", _fake_from_url)
    cache = RedisCache("redis://localhost:6379/0")
    service = ParserService(cache=cache)

    calls = {"count": 0}

    def fake_stats():
        calls["count"] += 1
        return {
            "total_reviews": 0,
            "unique_brands": 0,
            "unique_models": 0,
            "by_source": {},
            "by_type": {},
        }

    monkeypatch.setattr(service.parser.db, "get_parsing_stats", fake_stats)
    monkeypatch.setattr(service.queue_service, "get_queue_stats", lambda: {})

    # First call - cache miss
    data1 = service.get_status_data()
    assert calls["count"] == 1

    # Second call - should hit cache
    data2 = service.get_status_data()
    assert calls["count"] == 1
    assert data1 == data2


def test_parser_service_show_status_logs(monkeypatch, caplog):
    monkeypatch.setattr(redis, "from_url", _fake_from_url)
    cache = RedisCache("redis://localhost:6379/0")
    service = ParserService(cache=cache)

    def fake_get_status_data():
        return {
            "stats": {
                "total_reviews": 1,
                "unique_brands": 1,
                "unique_models": 1,
                "by_source": {"src": 1},
                "by_type": {"type": 1},
            },
            "queue_stats": {"pending": 1},
        }

    monkeypatch.setattr(service, "get_status_data", fake_get_status_data)

    with caplog.at_level(logging.INFO):
        service.show_status()

    assert "СТАТУС БАЗЫ ДАННЫХ" in caplog.text
    assert "СТАТУС ОЧЕРЕДИ" in caplog.text
