"""Redis-Based In-Memory Cache System - Core Cache Store Module.

This module provides a unified cache abstraction with two backend implementations:
1. MemoryCacheStore: An in-memory, thread-safe cache with TTL support for development and testing.
2. RedisCacheStore: A production-ready Redis cache store using redis-py with connection pooling,
   JSON serialization, Redis-native TTL expiration, and safe namespace-isolated key operations.

Also includes Cache-Aside pattern helpers and factory functions.
"""

from __future__ import annotations

import abc
import json
import math
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union

try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisError, TimeoutError as RedisTimeoutError
except ImportError:  # pragma: no cover
    redis = None  # type: ignore
    RedisConnectionError = ConnectionError  # type: ignore
    RedisTimeoutError = TimeoutError  # type: ignore
    RedisError = Exception  # type: ignore


# ============================================================================
# Custom Exceptions
# ============================================================================

class CacheError(Exception):
    """Base exception class for all cache-related errors."""
    pass


class InvalidTTLError(CacheError, ValueError):
    """Raised when an invalid TTL (Time-To-Live) value is provided."""
    pass


class CacheConnectionError(CacheError):
    """Raised when communication with the cache server fails."""
    pass


class SerializationError(CacheError):
    """Raised when serialization or deserialization of cached data fails."""
    pass


# ============================================================================
# Base Abstract Cache Store
# ============================================================================

class BaseCacheStore(abc.ABC):
    """Abstract Base Class defining the contract for cache implementations."""

    @abc.abstractmethod
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[Union[int, float]] = None,
    ) -> bool:
        """Store a key-value pair with an optional Time-To-Live (TTL) in seconds.

        Args:
            key: The cache key identifier.
            value: The data to store.
            ttl_seconds: Expiration time in seconds. If None, the key persists indefinitely.

        Returns:
            bool: True if the operation succeeded, False otherwise.

        Raises:
            InvalidTTLError: If ttl_seconds is non-numeric, <= 0, or not finite.
            ValueError: If key is not a non-empty string.
            CacheError: If storage fails.
        """
        pass

    @abc.abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Retrieve the value associated with key, or None if missing or expired.

        Args:
            key: The cache key identifier.

        Returns:
            The stored value if found and not expired, otherwise None.

        Raises:
            ValueError: If key is not a non-empty string.
            CacheError: If retrieval fails.
        """
        pass

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        """Remove a key from the cache.

        Args:
            key: The cache key identifier.

        Returns:
            bool: True if key was deleted, False if key did not exist.

        Raises:
            ValueError: If key is not a non-empty string.
            CacheError: If deletion fails.
        """
        pass

    @abc.abstractmethod
    def clear(self) -> int:
        """Safely remove only application-owned keys from the cache.

        Never flushes an entire shared Redis instance or database.

        Returns:
            int: The number of application keys removed.
        """
        pass

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists in cache and is not expired.

        Args:
            key: The cache key identifier.

        Returns:
            bool: True if the key exists and is unexpired, False otherwise.
        """
        pass

    @abc.abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Return cache runtime metrics (hits, misses, hit ratio, total keys).

        Returns:
            Dictionary containing cache metrics.
        """
        pass

    @abc.abstractmethod
    def reset_stats(self) -> None:
        """Reset the runtime hits and misses counters."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Close any active network connections or background resources."""
        pass

    def __enter__(self) -> BaseCacheStore:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def get_or_set(
        self,
        key: str,
        loader_fn: Callable[[], Any],
        ttl_seconds: Optional[Union[int, float]] = None,
    ) -> Tuple[Any, bool]:
        """Implement the Cache-Aside pattern.

        Checks the cache first. If hit, returns (value, True).
        If miss, invokes loader_fn(), caches the result, and returns (value, False).

        Args:
            key: Cache key.
            loader_fn: Callable to fetch data if cache misses.
            ttl_seconds: Expiration TTL for cached value.

        Returns:
            Tuple of (value, is_cache_hit).
        """
        cached_value = self.get(key)
        if cached_value is not None:
            return cached_value, True

        fresh_value = loader_fn()
        self.set(key, fresh_value, ttl_seconds=ttl_seconds)
        return fresh_value, False

    @staticmethod
    def _validate_key(key: str) -> None:
        """Ensure key is a non-empty string."""
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"Cache key must be a non-empty string, got {key!r}")

    @staticmethod
    def _validate_ttl(ttl_seconds: Optional[Union[int, float]]) -> None:
        """Validate TTL argument.

        Must be None or a strictly positive finite number (int or float).
        Booleans are explicitly rejected (since bool is a subclass of int in Python).
        """
        if ttl_seconds is None:
            return
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
            raise InvalidTTLError(
                f"TTL must be a numeric integer or float, got {type(ttl_seconds).__name__} ({ttl_seconds!r})"
            )
        if not math.isfinite(ttl_seconds):
            raise InvalidTTLError(f"TTL must be a finite number, got {ttl_seconds}")
        if ttl_seconds <= 0:
            raise InvalidTTLError(
                f"TTL must be strictly greater than 0 seconds, got {ttl_seconds}"
            )


# ============================================================================
# In-Memory Cache Store Implementation
# ============================================================================

class _MemoryCacheEntry:
    """Internal container for in-memory cache items."""

    __slots__ = ("value", "expires_at", "created_at")

    def __init__(self, value: Any, expires_at: Optional[float]) -> None:
        self.value = value
        self.expires_at = expires_at  # Monotonic timestamp
        self.created_at = time.monotonic()

    def is_expired(self, current_time: float) -> bool:
        if self.expires_at is None:
            return False
        return current_time >= self.expires_at


class MemoryCacheStore(BaseCacheStore):
    """Thread-safe, in-memory key-value cache with TTL expiration.

    Designed for local development, rapid unit testing, and benchmarking against
    network-based backends. Uses Python's time.monotonic() to prevent TTL drift
    caused by system clock adjustments.
    """

    def __init__(self, prefix: str = "app:") -> None:
        """Initialize the in-memory cache store.

        Args:
            prefix: Key prefix for namespace isolation.
        """
        self.prefix = prefix
        self._store: Dict[str, _MemoryCacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[Union[int, float]] = None,
    ) -> bool:
        self._validate_key(key)
        self._validate_ttl(ttl_seconds)

        expires_at = (
            time.monotonic() + float(ttl_seconds)
            if ttl_seconds is not None
            else None
        )
        entry = _MemoryCacheEntry(value=value, expires_at=expires_at)
        full_k = self._full_key(key)

        with self._lock:
            self._store[full_k] = entry
        return True

    def get(self, key: str) -> Optional[Any]:
        self._validate_key(key)
        full_k = self._full_key(key)
        now = time.monotonic()

        with self._lock:
            entry = self._store.get(full_k)
            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired(now):
                # Lazy eviction on read
                del self._store[full_k]
                self._misses += 1
                return None

            self._hits += 1
            return entry.value

    def delete(self, key: str) -> bool:
        self._validate_key(key)
        full_k = self._full_key(key)
        with self._lock:
            if full_k in self._store:
                del self._store[full_k]
                return True
            return False

    def exists(self, key: str) -> bool:
        self._validate_key(key)
        full_k = self._full_key(key)
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(full_k)
            if entry is None:
                return False
            if entry.is_expired(now):
                del self._store[full_k]
                return False
            return True

    def clear(self) -> int:
        """Clear only keys belonging to this store's namespace prefix."""
        with self._lock:
            keys_to_remove = [k for k in self._store.keys() if k.startswith(self.prefix)]
            for k in keys_to_remove:
                del self._store[k]
            return len(keys_to_remove)

    def purge_expired(self) -> int:
        """Proactively purge all expired keys across the store."""
        now = time.monotonic()
        purged = 0
        with self._lock:
            expired_keys = [
                k for k, entry in self._store.items() if entry.is_expired(now)
            ]
            for k in expired_keys:
                del self._store[k]
                purged += 1
        return purged

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total_requests = self._hits + self._misses
            hit_ratio = (self._hits / total_requests) if total_requests > 0 else 0.0
            return {
                "backend": "memory",
                "prefix": self.prefix,
                "hits": self._hits,
                "misses": self._misses,
                "total_requests": total_requests,
                "hit_ratio": round(hit_ratio, 4),
                "hit_percentage": f"{hit_ratio * 100:.1f}%",
                "key_count": len(self._store),
            }

    def reset_stats(self) -> None:
        with self._lock:
            self._hits = 0
            self._misses = 0

    def close(self) -> None:
        """No persistent resources to release for memory backend."""
        pass


# ============================================================================
# Redis Cache Store Implementation
# ============================================================================

class RedisCacheStore(BaseCacheStore):
    """Production Redis cache implementation using redis-py.

    Features:
    - Connection pooling and configurable socket timeouts.
    - Safe JSON serialization supporting primitives, lists, dictionaries, etc.
    - Redis-native TTL expiration using PX (millisecond precision).
    - Namespace isolation: all keys are prefixed (default: 'app_cache:').
    - Safe clear(): Uses SCAN iteration with chunked pipeline deletions to clear
      only application keys. Never calls FLUSHDB or FLUSHALL.
    - In-app metrics tracking (hits, misses, hit ratio).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        prefix: str = "app_cache:",
        socket_timeout: float = 2.0,
        socket_connect_timeout: float = 2.0,
        client: Optional[Any] = None,
    ) -> None:
        """Initialize the Redis cache store.

        Args:
            host: Redis server hostname or IP address.
            port: Redis server port.
            db: Redis database index (0-15).
            password: Optional authentication password.
            prefix: Key namespace prefix to prevent collisions on shared instances.
            socket_timeout: Timeout in seconds for socket read/write operations.
            socket_connect_timeout: Timeout in seconds for connecting to Redis.
            client: Optional pre-configured Redis client (used for mocks / fakeredis).

        Raises:
            CacheError: If the 'redis' Python package is not installed.
        """
        if redis is None and client is None:
            raise CacheError(
                "The 'redis' package is not installed. Install it via 'pip install redis'."
            )

        self.host = host
        self.port = port
        self.db = db
        self.prefix = prefix
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

        if client is not None:
            self._client = client
            self._pool = None
        else:
            self._pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=password,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=False,
            )
            self._client = redis.Redis(connection_pool=self._pool)

    @property
    def client(self) -> Any:
        return self._client

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def ping(self) -> bool:
        """Verify connectivity to the Redis server."""
        try:
            return bool(self._client.ping())
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(
                f"Failed to connect to Redis at {self.host}:{self.port} (db={self.db}): {exc}"
            ) from exc

    def _serialize(self, value: Any) -> bytes:
        try:
            payload = json.dumps({"data": value}, ensure_ascii=False)
            return payload.encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise SerializationError(f"Failed to serialize value for cache: {exc}") from exc

    def _deserialize(self, raw_bytes: bytes) -> Any:
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
            return payload["data"]
        except (KeyError, ValueError, UnicodeDecodeError) as exc:
            raise SerializationError(f"Failed to deserialize cache payload: {exc}") from exc

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[Union[int, float]] = None,
    ) -> bool:
        self._validate_key(key)
        self._validate_ttl(ttl_seconds)

        payload = self._serialize(value)
        full_k = self._full_key(key)

        try:
            if ttl_seconds is not None:
                # Millisecond precision via PX
                ttl_ms = max(1, int(round(ttl_seconds * 1000)))
                result = self._client.set(full_k, payload, px=ttl_ms)
            else:
                result = self._client.set(full_k, payload)
            return bool(result)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(
                f"Error writing key '{key}' to Redis: {exc}"
            ) from exc
        except RedisError as exc:
            raise CacheError(f"Redis error setting key '{key}': {exc}") from exc

    def get(self, key: str) -> Optional[Any]:
        self._validate_key(key)
        full_k = self._full_key(key)

        try:
            raw = self._client.get(full_k)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(
                f"Error reading key '{key}' from Redis: {exc}"
            ) from exc
        except RedisError as exc:
            raise CacheError(f"Redis error getting key '{key}': {exc}") from exc

        with self._lock:
            if raw is None:
                self._misses += 1
                return None
            self._hits += 1

        return self._deserialize(raw)

    def delete(self, key: str) -> bool:
        self._validate_key(key)
        full_k = self._full_key(key)

        try:
            count = self._client.delete(full_k)
            return bool(count > 0)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(
                f"Error deleting key '{key}' from Redis: {exc}"
            ) from exc
        except RedisError as exc:
            raise CacheError(f"Redis error deleting key '{key}': {exc}") from exc

    def exists(self, key: str) -> bool:
        self._validate_key(key)
        full_k = self._full_key(key)

        try:
            return bool(self._client.exists(full_k) > 0)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(
                f"Error checking key '{key}' in Redis: {exc}"
            ) from exc
        except RedisError as exc:
            raise CacheError(f"Redis error checking key '{key}': {exc}") from exc

    def clear(self) -> int:
        """Safely clear only application keys matching this store's namespace prefix.

        Uses non-blocking SCAN iteration and chunked pipeline deletes.
        NEVER calls FLUSHDB or FLUSHALL, ensuring safety on shared Redis servers.
        """
        match_pattern = f"{self.prefix}*"
        deleted_count = 0
        batch_size = 500

        try:
            keys_batch = []
            for key in self._client.scan_iter(match=match_pattern, count=100):
                keys_batch.append(key)
                if len(keys_batch) >= batch_size:
                    pipe = self._client.pipeline(transaction=False)
                    for k in keys_batch:
                        pipe.delete(k)
                    results = pipe.execute()
                    deleted_count += sum(results)
                    keys_batch.clear()

            if keys_batch:
                pipe = self._client.pipeline(transaction=False)
                for k in keys_batch:
                    pipe.delete(k)
                results = pipe.execute()
                deleted_count += sum(results)

            return deleted_count
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise CacheConnectionError(f"Error clearing cache prefix: {exc}") from exc
        except RedisError as exc:
            raise CacheError(f"Redis error during safe clear: {exc}") from exc

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total_requests = self._hits + self._misses
            hit_ratio = (self._hits / total_requests) if total_requests > 0 else 0.0

            stats: Dict[str, Any] = {
                "backend": "redis",
                "host": self.host,
                "port": self.port,
                "db": self.db,
                "prefix": self.prefix,
                "hits": self._hits,
                "misses": self._misses,
                "total_requests": total_requests,
                "hit_ratio": round(hit_ratio, 4),
                "hit_percentage": f"{hit_ratio * 100:.1f}%",
            }

        # Attempt to count active namespace keys safely
        try:
            key_count = sum(1 for _ in self._client.scan_iter(match=f"{self.prefix}*", count=100))
            stats["key_count"] = key_count
        except Exception:
            stats["key_count"] = "unknown"

        return stats

    def reset_stats(self) -> None:
        with self._lock:
            self._hits = 0
            self._misses = 0

    def close(self) -> None:
        """Close connection pool and socket connections."""
        try:
            if hasattr(self._client, "close"):
                self._client.close()
            if self._pool is not None and hasattr(self._pool, "disconnect"):
                self._pool.disconnect()
        except Exception:
            pass


# ============================================================================
# Factory Function
# ============================================================================

def create_cache_store(backend: str = "memory", **kwargs: Any) -> BaseCacheStore:
    """Factory function to instantiate the requested cache backend.

    Args:
        backend: Backend selector ('memory' or 'redis').
        **kwargs: Additional parameters forwarded to the store constructor.

    Returns:
        An instance of BaseCacheStore (MemoryCacheStore or RedisCacheStore).

    Raises:
        ValueError: If an unknown backend is requested.
    """
    backend_normalized = backend.strip().lower()
    if backend_normalized == "memory":
        return MemoryCacheStore(**kwargs)
    elif backend_normalized == "redis":
        return RedisCacheStore(**kwargs)
    else:
        raise ValueError(
            f"Unsupported cache backend: {backend!r}. Allowed backends are: 'memory', 'redis'."
        )
