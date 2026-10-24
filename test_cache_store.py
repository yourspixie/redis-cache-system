"""Automated test suite for Redis-Based In-Memory Cache System.

Covers:
- Setting and retrieving values (strings, integers, floats, dicts, lists, booleans).
- Missing keys (cache misses).
- Time-to-Live (TTL) expiration behavior.
- Key deletion and existence checks.
- Invalid TTL handling (negative, zero, NaN, infinity, boolean, strings).
- Invalid key validation.
- Namespace prefix isolation and safe clearing.
- Cache-Aside pattern logic and hit/miss counts.
- RedisCacheStore unit tests (using fakeredis / mocking).
- Live Redis integration tests (automatically skipped if no Redis server is running).
- Factory function validation.
"""

from __future__ import annotations

import math
import time
from unittest.mock import MagicMock, patch

import pytest

from cache_store import (
    BaseCacheStore,
    CacheConnectionError,
    CacheError,
    InvalidTTLError,
    MemoryCacheStore,
    RedisCacheStore,
    create_cache_store,
)

try:
    import fakeredis
except ImportError:
    fakeredis = None

try:
    import redis
except ImportError:
    redis = None


# ============================================================================
# MemoryCacheStore Tests
# ============================================================================

class TestMemoryCacheStore:
    """Unit tests for the thread-safe in-memory cache store."""

    def test_set_and_get_primitives(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        assert cache.set("str_key", "hello world") is True
        assert cache.get("str_key") == "hello world"

        assert cache.set("int_key", 1042) is True
        assert cache.get("int_key") == 1042

        assert cache.set("float_key", 3.14159) is True
        assert cache.get("float_key") == 3.14159

        assert cache.set("bool_key", True) is True
        assert cache.get("bool_key") is True

    def test_set_and_get_complex_structures(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        payload = {
            "user_id": 42,
            "roles": ["admin", "developer"],
            "metadata": {"verified": True, "score": 98.6},
        }
        assert cache.set("complex_payload", payload) is True
        retrieved = cache.get("complex_payload")
        assert retrieved == payload
        assert retrieved["roles"] == ["admin", "developer"]

    def test_get_nonexistent_key_returns_none(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        assert cache.get("nonexistent_key_12345") is None

    def test_delete_existing_and_nonexistent(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        cache.set("to_delete", "val")
        assert cache.exists("to_delete") is True
        assert cache.delete("to_delete") is True
        assert cache.exists("to_delete") is False
        assert cache.get("to_delete") is None
        # Deleting again should return False
        assert cache.delete("to_delete") is False

    def test_ttl_expiration(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        cache.set("short_lived", "ephemeral", ttl_seconds=0.1)
        assert cache.get("short_lived") == "ephemeral"
        time.sleep(0.15)
        # Value must have expired
        assert cache.get("short_lived") is None
        assert cache.exists("short_lived") is False

    def test_purge_expired(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        cache.set("perm_key", "persistent", ttl_seconds=None)
        cache.set("exp_1", "v1", ttl_seconds=0.05)
        cache.set("exp_2", "v2", ttl_seconds=0.05)
        time.sleep(0.08)

        purged_count = cache.purge_expired()
        assert purged_count == 2
        assert cache.get("perm_key") == "persistent"
        assert cache.get("exp_1") is None
        assert cache.get("exp_2") is None

    @pytest.mark.parametrize(
        "invalid_ttl",
        [
            0,
            -1,
            -10.5,
            "10",
            True,
            False,
            float("nan"),
            float("inf"),
            float("-inf"),
            [10],
            {"ttl": 5},
        ],
    )
    def test_invalid_ttl_values(self, invalid_ttl: any) -> None:
        cache = MemoryCacheStore(prefix="test:")
        with pytest.raises(InvalidTTLError):
            cache.set("key", "value", ttl_seconds=invalid_ttl)

    @pytest.mark.parametrize("invalid_key", ["", "   ", None, 123, ["key"]])
    def test_invalid_keys(self, invalid_key: any) -> None:
        cache = MemoryCacheStore(prefix="test:")
        with pytest.raises(ValueError):
            cache.set(invalid_key, "value")
        with pytest.raises(ValueError):
            cache.get(invalid_key)
        with pytest.raises(ValueError):
            cache.delete(invalid_key)

    def test_safe_clear_isolated_by_prefix(self) -> None:
        store1 = MemoryCacheStore(prefix="store1:")
        store2 = MemoryCacheStore(prefix="store2:")

        # Inject into same underlying dictionary structure or separate instances
        store1.set("key1", "val1")
        store1.set("key2", "val2")
        store2.set("keyA", "valA")

        cleared1 = store1.clear()
        assert cleared1 == 2
        assert store1.get("key1") is None
        assert store1.get("key2") is None
        # store2 must remain intact
        assert store2.get("keyA") == "valA"

    def test_cache_statistics_and_reset(self) -> None:
        cache = MemoryCacheStore(prefix="test:")
        cache.set("k1", "v1")

        # 3 hits
        cache.get("k1")
        cache.get("k1")
        cache.get("k1")
        # 2 misses
        cache.get("missing_1")
        cache.get("missing_2")

        stats = cache.get_stats()
        assert stats["hits"] == 3
        assert stats["misses"] == 2
        assert stats["total_requests"] == 5
        assert stats["hit_ratio"] == 0.6
        assert stats["hit_percentage"] == "60.0%"

        cache.reset_stats()
        reset_stats = cache.get_stats()
        assert reset_stats["hits"] == 0
        assert reset_stats["misses"] == 0
        assert reset_stats["total_requests"] == 0
        assert reset_stats["hit_ratio"] == 0.0

    def test_context_manager(self) -> None:
        with MemoryCacheStore(prefix="ctx:") as cache:
            cache.set("k", "v")
            assert cache.get("k") == "v"


# ============================================================================
# Cache-Aside Pattern Tests
# ============================================================================

class TestCacheAsidePattern:
    """Tests for the Cache-Aside helper implementation."""

    def test_cache_aside_fetches_once_on_miss(self) -> None:
        cache = MemoryCacheStore(prefix="aside:")
        loader_calls = 0

        def data_loader() -> str:
            nonlocal loader_calls
            loader_calls += 1
            return f"computed_value_{loader_calls}"

        # First call: cache miss -> loader executed
        val1, hit1 = cache.get_or_set("item:100", data_loader, ttl_seconds=60)
        assert val1 == "computed_value_1"
        assert hit1 is False
        assert loader_calls == 1

        # Second call: cache hit -> loader NOT executed
        val2, hit2 = cache.get_or_set("item:100", data_loader, ttl_seconds=60)
        assert val2 == "computed_value_1"
        assert hit2 is True
        assert loader_calls == 1  # Still 1

    def test_cache_aside_with_ttl_expiration(self) -> None:
        cache = MemoryCacheStore(prefix="aside:")
        calls = 0

        def fetcher() -> int:
            nonlocal calls
            calls += 1
            return calls

        # 1st call: miss
        val1, hit1 = cache.get_or_set("temp_item", fetcher, ttl_seconds=0.1)
        assert val1 == 1
        assert hit1 is False

        # Immediate: hit
        val2, hit2 = cache.get_or_set("temp_item", fetcher, ttl_seconds=0.1)
        assert val2 == 1
        assert hit2 is True

        # Sleep past TTL
        time.sleep(0.15)

        # After TTL: miss -> reloaded
        val3, hit3 = cache.get_or_set("temp_item", fetcher, ttl_seconds=0.1)
        assert val3 == 2
        assert hit3 is False
        assert calls == 2


# ============================================================================
# RedisCacheStore Unit Tests (Fakeredis / Mocks)
# ============================================================================

class TestRedisCacheStoreUnit:
    """Unit tests for RedisCacheStore using fakeredis or unittest.mock."""

    @pytest.fixture
    def fake_redis_cache(self) -> RedisCacheStore:
        if fakeredis is None:
            pytest.skip("fakeredis is not installed")
        fake_client = fakeredis.FakeRedis()
        return RedisCacheStore(prefix="test_app:", client=fake_client)

    def test_redis_set_and_get(self, fake_redis_cache: RedisCacheStore) -> None:
        cache = fake_redis_cache
        assert cache.set("my_str", "hello_redis") is True
        assert cache.get("my_str") == "hello_redis"

        complex_data = {"id": 99, "tags": ["python", "redis"], "active": True}
        assert cache.set("my_json", complex_data) is True
        assert cache.get("my_json") == complex_data

    def test_redis_get_miss_and_stats(self, fake_redis_cache: RedisCacheStore) -> None:
        cache = fake_redis_cache
        assert cache.get("missing_key") is None
        stats = cache.get_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0

        cache.set("exists", "yes")
        assert cache.get("exists") == "yes"
        stats = cache.get_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_ratio"] == 0.5

    def test_redis_delete(self, fake_redis_cache: RedisCacheStore) -> None:
        cache = fake_redis_cache
        cache.set("delete_me", "bye")
        assert cache.exists("delete_me") is True
        assert cache.delete("delete_me") is True
        assert cache.exists("delete_me") is False
        assert cache.delete("delete_me") is False

    def test_redis_ttl_expiration(self, fake_redis_cache: RedisCacheStore) -> None:
        cache = fake_redis_cache
        cache.set("quick_key", "temporary", ttl_seconds=0.1)
        assert cache.get("quick_key") == "temporary"
        time.sleep(0.15)
        assert cache.get("quick_key") is None

    def test_redis_safe_clear_preserves_external_keys(
        self, fake_redis_cache: RedisCacheStore
    ) -> None:
        cache = fake_redis_cache
        raw_client = cache.client

        # Application keys (with prefix 'test_app:')
        cache.set("k1", "val1")
        cache.set("k2", "val2")

        # External / unmanaged key belonging to another service on the same Redis DB
        raw_client.set("external_service:unrelated_key", b"do_not_delete")

        # Execute safe clear
        cleared_count = cache.clear()
        assert cleared_count == 2

        # Application keys are deleted
        assert cache.get("k1") is None
        assert cache.get("k2") is None

        # CRITICAL SAFETY CHECK: External key MUST NOT be deleted!
        external_val = raw_client.get("external_service:unrelated_key")
        assert external_val == b"do_not_delete"

    def test_redis_invalid_ttl(self, fake_redis_cache: RedisCacheStore) -> None:
        cache = fake_redis_cache
        with pytest.raises(InvalidTTLError):
            cache.set("k", "v", ttl_seconds=-5)
        with pytest.raises(InvalidTTLError):
            cache.set("k", "v", ttl_seconds=0)
        with pytest.raises(InvalidTTLError):
            cache.set("k", "v", ttl_seconds=True)

    def test_redis_connection_error_handling(self) -> None:
        mock_client = MagicMock()
        mock_client.ping.side_effect = redis.exceptions.ConnectionError("Connection refused")
        mock_client.get.side_effect = redis.exceptions.ConnectionError("Connection refused")

        cache = RedisCacheStore(prefix="mock:", client=mock_client)
        with pytest.raises(CacheConnectionError):
            cache.ping()

        with pytest.raises(CacheConnectionError):
            cache.get("any_key")


# ============================================================================
# Factory Function Tests
# ============================================================================

class TestFactoryFunction:
    """Tests for create_cache_store factory."""

    def test_factory_creates_memory_backend(self) -> None:
        store = create_cache_store("memory", prefix="fact_mem:")
        assert isinstance(store, MemoryCacheStore)
        assert store.prefix == "fact_mem:"

    def test_factory_creates_redis_backend_with_client(self) -> None:
        if fakeredis is None:
            pytest.skip("fakeredis required")
        fake_client = fakeredis.FakeRedis()
        store = create_cache_store("redis", prefix="fact_red:", client=fake_client)
        assert isinstance(store, RedisCacheStore)
        assert store.prefix == "fact_red:"

    def test_factory_rejects_unknown_backend(self) -> None:
        with pytest.raises(ValueError, match="Unsupported cache backend"):
            create_cache_store("memcached")


# ============================================================================
# Live Redis Integration Tests (Conditional)
# ============================================================================

def is_redis_server_running(host: str = "localhost", port: int = 6379) -> bool:
    """Helper to detect if a live Redis server is running locally."""
    if redis is None:
        return False
    try:
        r = redis.Redis(host=host, port=port, socket_timeout=0.5, socket_connect_timeout=0.5)
        return bool(r.ping())
    except Exception:
        return False


@pytest.mark.integration
class TestLiveRedisIntegration:
    """Live integration tests against an actual running Redis server.

    These tests will automatically execute if a Redis server is listening on
    localhost:6379, or skip cleanly if the server is offline.
    """

    @pytest.fixture(autouse=True)
    def check_redis_availability(self) -> None:
        if not is_redis_server_running():
            pytest.skip("Live Redis server is not running on localhost:6379")

    @pytest.fixture
    def live_redis_cache(self) -> RedisCacheStore:
        cache = RedisCacheStore(
            host="localhost",
            port=6379,
            db=15,  # Use isolated high-numbered test DB
            prefix="pytest_integ:",
        )
        yield cache
        # Teardown: safely clear test prefix
        cache.clear()
        cache.close()

    def test_live_roundtrip(self, live_redis_cache: RedisCacheStore) -> None:
        cache = live_redis_cache
        key = "live_key"
        payload = {"system": "integration", "timestamp": time.time()}

        assert cache.set(key, payload, ttl_seconds=10) is True
        assert cache.get(key) == payload
        assert cache.exists(key) is True
        assert cache.delete(key) is True
        assert cache.get(key) is None
