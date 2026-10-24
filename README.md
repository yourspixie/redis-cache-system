
Redis-Based In-Memory Cache System
A modular, production-ready key-value caching system implemented in Python, featuring dual backends: an in-process thread-safe in-memory cache and a distributed Redis cache using the official redis-py client.

This repository demonstrates enterprise caching patterns including Cache-Aside (Lazy Loading), Time-To-Live (TTL) automatic expiration, cache hit/miss tracking, safe namespace isolation, and high-throughput performance benchmarking.
1. Features
Unified Caching Interface (BaseCacheStore): Abstract base class defining consistent contracts for set, get, delete, clear, exists, and get_stats.

Dual Backends:

  - MemoryCacheStore: Thread-safe (threading.RLock), in-process RAM cache using monotonic clock timestamps (time.monotonic()) immune to system clock shifts.

  - RedisCacheStore: Production Redis backend with connection pooling, automatic JSON serialization/deserialization, and native millisecond-precision TTL (PX).

Cache-Aside Pattern: Built-in get_or_set helper demonstrating real-world database query bypass with measurable latency reduction.

Safe Namespace Isolation: Keys are prefixed (e.g., app_cache:). Safe clear() uses Redis non-blocking SCAN iteration with pipelined deletions, never flushing the entire shared Redis database.

Robust Input Validation: Strict validation rejecting non-numeric, negative, zero, boolean, or infinite TTL values with custom InvalidTTLError.

Runtime Metrics: Tracks total requests, hits, misses, hit ratio, and active key counts.

Graceful Error Handling: Actionable diagnostic guidance when the Redis server is offline or packages are missing.

Automated Testing Suite: 37 unit tests covering edge cases, invalid inputs, expiration, and safe prefix isolation using pytest and fakeredis, plus conditional live Redis integration tests.

2. Architecture Overview
For comprehensive architectural design, sequence diagrams, and backend comparisons, refer to architecture.md.
3. Technologies Used
Python 3.10+ (Developed and tested with Python 3.12.7)

Redis Server (compatible with Redis 6.x and 7.x)

redis-py (v8.1.0): Official Python Redis client

pytest & pytest-cov: Test execution and coverage reporting

fakeredis: Pure-Python in-memory Redis emulator for reproducible unit tests without external services

Git & GitHub: Version control and source publication

4. Repository Structure

redis-cache-system/

├── app.py                   # Application entry point, CLI, demos & benchmarks

├── cache_store.py           # Core cache store abstraction & implementations

├── requirements.txt         # Production runtime dependencies

├── requirements-dev.txt     # Development and testing dependencies

├── pytest.ini               # Pytest configuration and marker definitions

├── README.md                # Project documentation and user guide

├── .gitignore               # Git exclusion rules for venv, cache, and secrets

├── tests/

│   └── test_cache_store.py  # Unit, edge-case, and integration test suite

└── docs/

    ├── architecture.md      # Detailed system architecture and sequence flows

    └── benchmark_results.md # Empirical benchmark data and analysis
5. Prerequisites
Python 3.10 or newer. Check your installed version:
  ```bash

  python --version

  ```

Git. Check your version:
  ```bash

  git --version

  ```

(Optional, for Redis backend) Redis Server 6.x+ running locally or in a container.
6. Installation
Windows (PowerShell)

\# 1. Clone repository (or navigate to workspace)

cd d:\Saci\redis

\# 2. Create Python virtual environment

python -m venv .venv

\# 3. Activate virtual environment

.\\.venv\Scripts\Activate.ps1

\# 4. Install production and development dependencies

pip install -r requirements-dev.txt
Linux / macOS (Bash / Zsh)

\# 1. Create Python virtual environment

*python3* -m venv .venv

\# 2. Activate virtual environment

*source* .venv/bin/activate

\# 3. Install dependencies

*pip* install -r requirements-dev.txt
7. How to Start Redis
If you wish to test the live Redis backend, choose one of the following methods to start Redis:

Option A: Using Docker (Recommended for Windows & Mac)

*docker* run -d -p 6379:6379 --name redis-server redis:7-alpine
Option B: Using Windows Subsystem for Linux (WSL)

*wsl* sudo apt update && *wsl* sudo apt install -y redis-server

*wsl* sudo service redis-server start
Option C: Using Native Windows Service (Memurai)
Download and run the Memurai Developer Edition installer (Redis 7 compatible for Windows).

Option D: Using Homebrew (macOS)

*brew* install redis

*brew* services start redis
8. Running the Application
The CLI application (app.py) demonstrates all cache behaviors and performance benchmarks.

Run with In-Memory Backend (Zero Setup Required)

*python* app.py --backend memory
Run with Redis Backend

*python* app.py --backend redis
Run with Custom Redis Host / Port / Prefix

*python* app.py --backend redis --host 127.0.0.1 --port 6379 --db 0 --prefix myapp:
Run High-Throughput Read Benchmark

*python* app.py --backend memory --benchmark --reads 100000 --no-demos
9. Executing Tests
The test suite contains 38 tests covering both backends, input validation, and expiration.


\# Run all tests with coverage report

*pytest* -v --cov=cache_store tests/

\# Run only unit tests

*pytest* -v -m "not integration" tests/

\# Run integration tests against a live Redis instance

*pytest* -v -m "integration" tests/
Note: Live integration tests automatically skip if no Redis server is listening on port 6379, ensuring the test suite remains 100% green and deterministic.
10. Performance Benchmarking
Empirical benchmarks run on an Intel Core Ultra 5 125H (14-core) processor running Windows 11 with Python 3.12.7:

| Backend | Operations | Total Time | Throughput | Average Latency |

| :--- | :--- | :--- | :--- | :--- |

| In-Memory (RAM) | 10,000 reads | 0.0053 s | 1,875,398 ops/sec | 0.53 µs/op |

| In-Memory (RAM) | 100,000 reads | 0.0633 s | 1,579,092 ops/sec | 0.63 µs/op |

| Redis (Loopback) | Unrun locally* | -- | ~35,000 ops/sec (typ.) | ~28 µs/op (typ.) |

\ Note: As documented in benchmark_results.md, an active Redis daemon was not present on the test host. The application's connection failure handler was validated instead.*
11. Sample Output (Captured from Live Run)

\============================================================

    REDIS-BASED IN-MEMORY CACHE SYSTEM

\============================================================

[*] Selected Backend: MEMORY

\============================================================

DEMO 1: Basic Cache Operations (Write, Read, Miss, Delete)

\============================================================

[*] Storing key 'user:session:42' with complex dictionary value...

    [+] Set successful.

[*] Retrieving key 'user:session:42'...

    [+] Cache Hit: {'username': 'alex_developer', 'role': 'engineer', 'preferences': {'theme': 'dark', 'notifications': True}}

[*] Querying non-existent key 'user:session:999_nonexistent'...

    [-] Cache Miss confirmed: None (as expected)

[*] Deleting key 'user:session:42'...

    [+] Deleted: True

[*] Verifying deletion: cache.get('user:session:42') -> None

\============================================================

DEMO 2: Time-To-Live (TTL) Expiration (2.0s)

\============================================================

[*] Setting key 'auth:temp_token' with TTL = 2.0 seconds...

[*] Immediate read (within TTL): 'token_xyz_987654' -> HIT

[*] Sleeping for 2.3s to allow TTL expiration...

[*] Read after expiration: None -> EXPIRED (Cache Miss)

    [+] TTL expiration verified successfully!

\============================================================

DEMO 3: Cache-Aside Pattern with Simulated Database Latency

\============================================================

[*] Request 1 (Cold Cache):

    [DB] Querying database for user_id=1001 (simulated 150ms latency)...

    Result: User 1001 | Cache Hit: False | Elapsed: 150.21 ms

[*] Request 2 (Warm Cache):

    Result: User 1001 | Cache Hit: True | Elapsed: 0.01 ms

    [+] Database queries executed: 1 (only 1 call for 2 requests)

    [+] Cache Speedup Factor: 12211.8x faster on hit!

\============================================================

PERFORMANCE BENCHMARK: Repeated Reads (10,000 operations)

\============================================================

[*] Starting benchmark with 10,000 repeated reads on key 'benchmark:read_target'...

+-----------------------+-----------------------+

| Metric                | Measurement           |

+-----------------------+-----------------------+

| Backend               | In-Memory (Local RAM) |

| Total Read Operations | 10,000                |

| Total Elapsed Time    | 0.0053 s              |

| Throughput            | 1,875,398.49 ops/sec  |

| Average Latency       | 0.53 us/op            |

+-----------------------+-----------------------+

\============================================================

CACHE METRICS SUMMARY

\============================================================

+----------------+-------------+

| Statistic      | Value       |

+----------------+-------------+

| backend        | memory      |

| prefix         | demo_cache: |

| hits           | 10000       |

| misses         | 0           |

| total_requests | 10000       |

| hit_ratio      | 1.0         |

| hit_percentage | 100.0%      |

| key_count      | 1           |

+----------------+-------------+

[*] Cleaning up demonstration keys safely...

    [+] Cleared 1 keys belonging to prefix 'demo_cache:'

[OK] Demonstration completed successfully.
12. Known Limitations
Process Scope: The MemoryCacheStore is strictly confined to the local Python process; it cannot share state between separate processes or worker threads across distinct processes.

Serialization Format: RedisCacheStore uses JSON serialization. Python objects with custom classes, sets, or binary buffers must be converted to JSON-serializable types prior to caching, or custom codecs must be supplied.

Single Redis Node: Does not currently configure Redis Sentinel or Redis Cluster topologies (designed for standalone Redis instances).

13. Future Improvements

Redis Sentinel & Cluster Support: Add failover-aware client configuration using redis.sentinel.Sentinel and redis.cluster.RedisCluster.


Multi-Level Caching (L1/L2): Implement a hybrid tiered store where L1 is local memory (MemoryCacheStore) and L2 is Redis, synchronizing evictions via Redis Pub/Sub.


Advanced Serialization: Add support for MsgPack and Protocol Buffers for more compact payloads and faster serialization.


Proactive Memory Eviction: Implement LRU (Least Recently Used) max-entry caps for MemoryCacheStore to prevent unbounded memory growth in long-running processes.

14. License
Distributed under the MIT License. See LICENSE for details.

⋮⋮⋮⋮
