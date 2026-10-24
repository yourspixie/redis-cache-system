"""Redis-Based In-Memory Cache System - Application Entry Point & CLI.

This module provides an interactive CLI demonstrating:
1. Cache write and read operations.
2. Cache miss detection.
3. Time-to-Live (TTL) expiration.
4. Cache-Aside pattern with simulated database latency.
5. High-throughput read benchmarking.
6. Comprehensive cache hit/miss statistics.

Handles unavailable Redis servers and missing dependencies gracefully with
actionable diagnostics.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Dict

from cache_store import (
    BaseCacheStore,
    CacheConnectionError,
    CacheError,
    MemoryCacheStore,
    RedisCacheStore,
    create_cache_store,
)


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    """Format a clean plain-text ASCII table."""
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, val in enumerate(row):
            widths[idx] = max(widths[idx], len(str(val)))

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    header_str = "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |"

    body_lines = []
    for row in rows:
        body_lines.append(
            "| " + " | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row)) + " |"
        )

    return "\n".join([sep, header_str, sep, *body_lines, sep])


def demo_basic_operations(cache: BaseCacheStore) -> None:
    """Demonstrate basic set, get, miss, and delete operations."""
    print("\n" + "=" * 60)
    print("DEMO 1: Basic Cache Operations (Write, Read, Miss, Delete)")
    print("=" * 60)

    test_key = "user:session:42"
    test_val = {
        "username": "alex_developer",
        "role": "engineer",
        "preferences": {"theme": "dark", "notifications": True},
    }

    # 1. Set value
    print(f"[*] Storing key '{test_key}' with complex dictionary value...")
    cache.set(test_key, test_val, ttl_seconds=30)
    print("    [+] Set successful.")

    # 2. Read value (Hit)
    print(f"[*] Retrieving key '{test_key}'...")
    retrieved = cache.get(test_key)
    print(f"    [+] Cache Hit: {retrieved}")
    assert retrieved == test_val, "Retrieved value mismatch!"

    # 3. Read non-existent key (Miss)
    missing_key = "user:session:999_nonexistent"
    print(f"[*] Querying non-existent key '{missing_key}'...")
    miss_val = cache.get(missing_key)
    print(f"    [-] Cache Miss confirmed: {miss_val} (as expected)")

    # 4. Deletion
    print(f"[*] Deleting key '{test_key}'...")
    deleted = cache.delete(test_key)
    print(f"    [+] Deleted: {deleted}")
    print(f"[*] Verifying deletion: cache.get('{test_key}') -> {cache.get(test_key)}")


def demo_ttl_expiration(cache: BaseCacheStore, ttl_seconds: float = 2.0) -> None:
    """Demonstrate Time-To-Live (TTL) automatic expiration."""
    print("\n" + "=" * 60)
    print(f"DEMO 2: Time-To-Live (TTL) Expiration ({ttl_seconds}s)")
    print("=" * 60)

    ttl_key = "auth:temp_token"
    ttl_value = "token_xyz_987654"

    print(f"[*] Setting key '{ttl_key}' with TTL = {ttl_seconds} seconds...")
    cache.set(ttl_key, ttl_value, ttl_seconds=ttl_seconds)

    # Immediate read
    val_immediate = cache.get(ttl_key)
    print(f"[*] Immediate read (within TTL): '{val_immediate}' -> HIT")
    assert val_immediate == ttl_value, "Expected immediate read to succeed."

    # Sleep slightly past expiration
    sleep_time = ttl_seconds + 0.3
    print(f"[*] Sleeping for {sleep_time:.1f}s to allow TTL expiration...")
    time.sleep(sleep_time)

    # Post-expiration read
    val_after = cache.get(ttl_key)
    print(f"[*] Read after expiration: {val_after} -> EXPIRED (Cache Miss)")
    if val_after is None:
        print("    [+] TTL expiration verified successfully!")
    else:
        print("    [!] Warning: Value did not expire as expected.")


def demo_cache_aside(cache: BaseCacheStore) -> None:
    """Demonstrate the Cache-Aside pattern with simulated database latency."""
    print("\n" + "=" * 60)
    print("DEMO 3: Cache-Aside Pattern with Simulated Database Latency")
    print("=" * 60)

    # Simulated slow database
    db_calls = 0

    def query_user_from_db(user_id: int) -> Dict[str, Any]:
        nonlocal db_calls
        db_calls += 1
        print(f"    [DB] Querying database for user_id={user_id} (simulated 150ms latency)...")
        time.sleep(0.15)  # 150ms database latency
        return {
            "id": user_id,
            "name": f"User {user_id}",
            "email": f"user{user_id}@example.com",
            "tier": "enterprise",
        }

    user_id = 1001
    cache_key = f"profile:user:{user_id}"

    # Clean up key beforehand
    cache.delete(cache_key)

    # First request: Cache Miss -> Slow DB Query
    print("[*] Request 1 (Cold Cache):")
    t0 = time.perf_counter()
    data1, hit1 = cache.get_or_set(
        cache_key,
        loader_fn=lambda: query_user_from_db(user_id),
        ttl_seconds=60,
    )
    t1 = time.perf_counter()
    latency1_ms = (t1 - t0) * 1000
    print(f"    Result: {data1['name']} | Cache Hit: {hit1} | Elapsed: {latency1_ms:.2f} ms")

    # Second request: Cache Hit -> Instant Return
    print("\n[*] Request 2 (Warm Cache):")
    t2 = time.perf_counter()
    data2, hit2 = cache.get_or_set(
        cache_key,
        loader_fn=lambda: query_user_from_db(user_id),
        ttl_seconds=60,
    )
    t3 = time.perf_counter()
    latency2_ms = (t3 - t2) * 1000
    print(f"    Result: {data2['name']} | Cache Hit: {hit2} | Elapsed: {latency2_ms:.2f} ms")

    speedup = latency1_ms / latency2_ms if latency2_ms > 0 else float("inf")
    print(f"\n    [+] Database queries executed: {db_calls} (only 1 call for 2 requests)")
    print(f"    [+] Cache Speedup Factor: {speedup:.1f}x faster on hit!")


def run_benchmark(cache: BaseCacheStore, read_ops: int = 10000) -> Dict[str, Any]:
    """Execute repeated read benchmark against the cache store."""
    print("\n" + "=" * 60)
    print(f"PERFORMANCE BENCHMARK: Repeated Reads ({read_ops:,} operations)")
    print("=" * 60)

    bench_key = "benchmark:read_target"
    bench_val = {
        "id": 12345,
        "uuid": "4b6a9e10-c1e1-4c70-98b4-ef2918e90a9b",
        "status": "active",
        "payload": "Benchmark payload for high-throughput read testing",
    }

    # Warmup write
    cache.set(bench_key, bench_val, ttl_seconds=300)
    initial_check = cache.get(bench_key)
    if initial_check != bench_val:
        raise RuntimeError("Benchmark target key failed to initialize.")

    # Reset stats so benchmark measures cleanly
    cache.reset_stats()

    print(f"[*] Starting benchmark with {read_ops:,} repeated reads on key '{bench_key}'...")
    start_time = time.perf_counter()

    for _ in range(read_ops):
        _ = cache.get(bench_key)

    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    ops_per_second = read_ops / elapsed_seconds if elapsed_seconds > 0 else 0.0
    avg_latency_us = (elapsed_seconds / read_ops) * 1_000_000

    backend_name = "In-Memory (Local RAM)" if isinstance(cache, MemoryCacheStore) else "Redis (Network/Loopback)"

    results = {
        "backend": backend_name,
        "operations": read_ops,
        "elapsed_seconds": round(elapsed_seconds, 4),
        "ops_per_second": round(ops_per_second, 2),
        "avg_latency_us": round(avg_latency_us, 2),
    }

    table_data = [
        ["Backend", backend_name],
        ["Total Read Operations", f"{read_ops:,}"],
        ["Total Elapsed Time", f"{elapsed_seconds:.4f} s"],
        ["Throughput", f"{ops_per_second:,.2f} ops/sec"],
        ["Average Latency", f"{avg_latency_us:.2f} us/op"],
    ]
    print("\n" + format_table(["Metric", "Measurement"], table_data))

    if isinstance(cache, MemoryCacheStore):
        print("\n[NOTE] In-memory benchmark measures in-process RAM dictionary access.")
        print("       It represents zero network overhead and pure CPU execution.")
    else:
        print("\n[NOTE] Redis benchmark measures client-server IPC/loopback socket communication,")
        print("       serialization/deserialization, and Redis event-loop processing.")

    return results


def print_stats_summary(cache: BaseCacheStore) -> None:
    """Print current cache statistics summary."""
    print("\n" + "=" * 60)
    print("CACHE METRICS SUMMARY")
    print("=" * 60)
    stats = cache.get_stats()
    table_data = [[k, str(v)] for k, v in stats.items()]
    print(format_table(["Statistic", "Value"], table_data))


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Redis-Based In-Memory Cache System CLI Demonstration & Benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        choices=["memory", "redis"],
        default="memory",
        help="Select caching backend: 'memory' (in-process) or 'redis' (Redis server)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Redis host address (used when backend is 'redis')",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6379,
        help="Redis port (used when backend is 'redis')",
    )
    parser.add_argument(
        "--db",
        type=int,
        default=0,
        help="Redis database index (used when backend is 'redis')",
    )
    parser.add_argument(
        "--prefix",
        default="demo_cache:",
        help="Namespace prefix for application keys",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run repeated read benchmark",
    )
    parser.add_argument(
        "--reads",
        type=int,
        default=10000,
        help="Number of read operations for benchmark",
    )
    parser.add_argument(
        "--ttl",
        type=float,
        default=2.0,
        help="TTL duration in seconds for expiration demo",
    )
    parser.add_argument(
        "--no-demos",
        action="store_true",
        help="Skip interactive demos and only run benchmark",
    )
    return parser


def main() -> int:
    """Main entry point for CLI application."""
    parser = build_parser()
    args = parser.parse_args()

    print("=" * 60)
    print("    REDIS-BASED IN-MEMORY CACHE SYSTEM")
    print("=" * 60)
    print(f"[*] Selected Backend: {args.backend.upper()}")

    # Initialize Cache Store
    try:
        if args.backend == "memory":
            cache = create_cache_store("memory", prefix=args.prefix)
        else:
            print(f"[*] Connecting to Redis at {args.host}:{args.port} (db={args.db})...")
            cache = create_cache_store(
                "redis",
                host=args.host,
                port=args.port,
                db=args.db,
                prefix=args.prefix,
                socket_timeout=3.0,
                socket_connect_timeout=3.0,
            )
            # Verify connectivity
            assert isinstance(cache, RedisCacheStore)
            cache.ping()
            print("    [+] Successfully connected to Redis server!")
    except CacheConnectionError as exc:
        print("\n" + "!" * 60)
        print("ERROR: Could not establish connection to Redis server.")
        print("!" * 60)
        print(f"Details: {exc}\n")
        print("Troubleshooting steps:")
        print("  1. Verify Redis is running locally on port 6379:")
        print("     - On Windows WSL: 'sudo service redis-server start'")
        print("     - Via Docker:     'docker run -d -p 6379:6379 --name redis redis:7-alpine'")
        print("  2. If running locally without a Redis server, use the memory backend:")
        print("     python app.py --backend memory\n")
        return 1
    except CacheError as exc:
        print(f"\n[!] Cache Initialization Error: {exc}")
        return 1
    except Exception as exc:
        print(f"\n[!] Unexpected Error during initialization: {exc}")
        return 1

    try:
        # Run demonstrations unless --no-demos is set
        if not args.no_demos:
            demo_basic_operations(cache)
            demo_ttl_expiration(cache, ttl_seconds=args.ttl)
            demo_cache_aside(cache)

        # Run benchmark if requested or as part of full run
        if args.benchmark or not args.no_demos:
            run_benchmark(cache, read_ops=args.reads)

        # Display final statistics
        print_stats_summary(cache)

        # Clean up application-owned keys
        print("\n[*] Cleaning up demonstration keys safely...")
        deleted = cache.clear()
        print(f"    [+] Cleared {deleted} keys belonging to prefix '{args.prefix}'")
        print("\n[OK] Demonstration completed successfully.")
        return 0

    finally:
        cache.close()


if __name__ == "__main__":
    sys.exit(main())
