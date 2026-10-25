---
sidebar_position: 4
title: Benchmarks
description: QueryFlux benchmarks — proxy overhead against mock backends (~0.1 ms p50) and per feature, and the capacity admission benchmark for shared maxRunningQueries leases.
image: img/queryflux-hero-banner.png
---
# Benchmarks

## Proxy overhead

End-to-end overhead is measured by `queryflux-bench` (`cargo build --release --bin queryflux --bin queryflux-bench`, then `target/release/queryflux-bench`). It uses **mock** backends (a Trino HTTP coordinator and a StarRocks MySQL-wire FE) that answer instantly, **50** warmup queries per path, then **500** timed iterations — direct to the mock vs the same request through QueryFlux's Trino HTTP frontend. Overhead is the difference. Numbers below: median of 3 runs on an Apple M1 Max (10 cores, 32 GB); they vary by machine, so compare runs on the same one.

### Trino (mock HTTP coordinator), `SELECT 1`

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct | 0.09 ms | 0.13 ms | 0.15 ms |
| Via QueryFlux | 0.20 ms | 0.25 ms | 0.28 ms |
| **Overhead** | **0.12 ms** | **0.13 ms** | **0.13 ms** |

### StarRocks path (mock MySQL FE), `SELECT 1`

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct (one reused MySQL connection) | 0.03 ms | 0.09 ms | 0.12 ms |
| Via QueryFlux | 0.19 ms | 0.26 ms | 0.32 ms |
| **Overhead** | **0.16 ms** | **0.17 ms** | **0.20 ms** |

### Overhead per feature (Trino path)

Each feature scenario runs QueryFlux with one feature turned on; the dashboard-style query is the baseline, with none (`QUERYFLUX_BENCH_SCENARIOS` picks a subset):

| Scenario | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Dashboard-style query (CTE, join, filters, `GROUP BY`, `LIMIT`) | 0.09 ms | 0.12 ms | 0.12 ms |
| Translation, cache hit (same SQL repeated) | 0.11 ms | 0.12 ms | 0.12 ms |
| Translation, cache miss (new SQL every query) | 1.91 ms | 2.26 ms | 2.59 ms |
| Built-in guards (`read_only`, `row_limit`, `require_predicate`) | 0.19 ms | 0.22 ms | 0.26 ms |
| Python `check(ctx)` guard | 0.19 ms | 0.23 ms | 0.29 ms |
| Router chain (header, tags, regex, Python), none matching | 0.15 ms | 0.18 ms | 0.21 ms |
| Access control, OPA allows | 0.22 ms | 0.28 ms | 0.35 ms |
| Access control, OPA adds a row filter (query rewritten) | 2.47 ms | 2.92 ms | 3.67 ms |
| Result cache hit (no backend call) | 0.37 ms | 0.51 ms | 1.06 ms |

The two multi-millisecond rows are sqlglot running on every query: a translation-cache miss (repeated SQL is cached), and the row-filter rewrite, which is not cached yet.

## Capacity benchmark

`maxRunningQueries` is enforced fleet-wide through capacity leases in Postgres (see [Worker mode: shared capacity](architecture/worker-mode.md#shared-capacity)). Every query acquires a lease before it runs and releases it when it ends, so lease throughput and latency bound how many queries per second a fleet can admit. `queryflux-bench capacity` measures that path in isolation, with no engines involved.

### What it does

Simulated workers run queries that only acquire a lease, hold it for a configurable time, and release it. A denied acquire is handled as dispatch handles it: the query's lease id is released (dispatch drops its slot guard on every denial, so a denial costs two calls), then the acquire is retried with the same backoff (100 ms doubling to a 2 s cap, configurable).

- **Transports.** `--transport store` calls `CapacityStore` on a `PostgresStore` in process: what the control plane does for each worker request, minus HTTP. `--transport http` goes through the control plane's `/worker/capacity/*` API (the real `worker_api` router, served in process on an ephemeral port and backed by Postgres) with the JSON a worker sends; each simulated worker has its own HTTP client. `--worker-api-url` points it at a running control plane instead. `both` runs each scenario twice.
- **Load.** Closed loop (`--concurrency C`: C queries always in flight) or open loop (`--rate R`: Poisson arrivals at R/s, or evenly spaced with `--uniform-arrivals`; arrivals beyond `--max-outstanding` are shed and counted). Queries spread round-robin over `--clusters`, each capped at `--cap`. `--query-duration` is `0`, `fixed:MS` or `exp:MEAN_MS`.
- **Cap-violation checker, every scenario.** The harness counts admitted, unreleased queries per cluster (up on grant, down *before* release, so it can never run ahead of the leases really held). Any grant that takes a cluster above its cap is a violation. It also samples `active_lease_count` per cluster every `--sample-ms` on a separate connection pool, and checks no lease is left once every query has released. Any violation makes the command exit with status 1.
- **Postgres load.** Transactions/s is the change in `xact_commit + xact_rollback` from `pg_stat_database` over the measured window; statements/s comes from `pg_stat_statements` when the extension is loaded, otherwise it is skipped with a note. Both include the benchmark's own sampling (a few queries per second).

Admission backends sit behind a small `AdmissionBackend` / `WorkerAdmission` trait pair in `crates/queryflux-bench/src/capacity/backend.rs`, so a new admission design (counter rows, quotas, a shared reserve) is one more implementation measured by the same scenarios and the same checker.

### Running it

Use a disposable database: migrations run first, and the benchmark writes and deletes lease rows under unique `bench-…` names.

```bash
# Predefined scenarios, both transports (release build)
make bench-capacity DATABASE_URL=postgres://queryflux@localhost:5432/queryflux_bench

# Longer runs and a JSON report
make bench-capacity DATABASE_URL=… BENCH_ARGS="--duration-secs 30 --json capacity.json"

# A custom scenario: 8 workers, 500 queries/s open loop, 4 clusters capped at 20, 40 ms queries
cargo run --release -p queryflux-bench -- capacity --database-url "$DATABASE_URL" \
  --transport http --workers 8 --rate 500 --clusters 4 --cap 20 --query-duration exp:40
```

`queryflux-bench capacity --help` lists every flag. For statements/s, start Postgres with `shared_preload_libraries=pg_stat_statements` and run `CREATE EXTENSION pg_stat_statements` in the benchmark database. Measure on a Postgres with default durability (`fsync` and `synchronous_commit` on): a test instance with `fsync=off` hides the commit cost that dominates lease latency. On macOS, `fsync` does not flush the drive cache, so even a durable local instance commits faster than Postgres on a Linux server with a real disk.

Predefined scenarios (`--scenario NAME`, repeatable, or `all`). The first four reproduce the shape of the table in the high-throughput plan: `maxRunningQueries` 10,000 so every acquire is granted, zero-length queries (acquire then release), 4 workers.

| Scenario | Load | Clusters | Cap | `coordinationPoolSize` |
|---|---|---|---|---|
| `serial` | 1 at a time, 1 worker | 1 | 10,000 | 32 |
| `c32-one-cluster` | 32 concurrent | 1 | 10,000 | 32 |
| `c32-eight-clusters` | 32 concurrent | 8 | 10,000 | 32 |
| `c32-eight-clusters-pool2` | 32 concurrent | 8 | 10,000 | 2 (the default) |
| `contended` | 32 concurrent, 20 ms queries | 1 | 8 | 32 |
| `contended-open` | 300/s Poisson arrivals, 20 ms queries (75% of capacity) | 1 | 8 | 32 |

`--duration-secs` (default 10) and `--warmup-secs` (default 2) apply to every scenario.

### Reading the output

| Column | Meaning |
|---|---|
| `adm/s` | Grants per second in the measured window |
| `acquire p50/p99/p999` | One acquire call, request to answer (granted or not) |
| `admit p99` | Query arrival to grant, including denials and backoff |
| `waited (n, p50/p99)` | How many queries were denied at least once, and their arrival-to-grant time |
| `release p50/p99` | One release call |
| `denied` | Acquire calls answered "full" (each one is followed by a backoff and a retry) |
| `errors` | Failed acquire or release calls (timeouts, pool exhaustion); samples are printed below the table |
| `hand-off p50/p99` | A query ending while others wait on the same cluster, to the next grant there: how long a freed slot sits idle. Approximate: the next grant consumes the oldest pending release |
| `max in-flight` | Highest harness count of admitted, unreleased queries on any cluster, against the cap |
| `violations` | Grants above the cap plus lease-count samples above the cap. Must be 0 |
| `pg xact/s`, `pg stmt/s` | Postgres transactions and statements per second during the window |

The JSON report (`--json FILE`, or `--json -` for stdout with the table on stderr) has the same numbers plus mean and max latencies, retries, shed arrivals, leaked leases, the scenario parameters, and the machine.

### Baseline

Measured 2026-10-19 on an Apple M1 Max (10 cores, 32 GB) against a local Postgres 17.6 with default durability (`fsync` and `synchronous_commit` on, `pg_stat_statements` loaded), 15 s per scenario after 3 s warmup, median of three runs with the range of admissions/s. The machine was busy with other builds during the runs, so the spread is wide. Indicative only: compare runs on the same machine, not against these numbers.

| Scenario | Transport | Admissions/s (median, range) | Acquire p50 / p99 ms | Release p50 ms | Denied | Hand-off p50 / p99 ms | Waited p99 ms | Violations | PG xact/s | PG stmt/s |
|---|---|---|---|---|---|---|---|---|---|---|
| `serial` | store | 2,360 (1,979-2,394) | 0.24 / 0.6 | 0.14 | 0/s | - | - | 0 | 4,701 | 11,764 |
| `serial` | http | 1,349 (945-1,376) | 0.35 / 2.3 | 0.23 | 0/s | - | - | 0 | 2,590 | 6,748 |
| `c32-one-cluster` | store | 3,267 (2,481-3,407) | 8.47 / 24.9 | 0.22 | 0/s | - | - | 0 | 6,734 | 16,341 |
| `c32-one-cluster` | http | 3,121 (2,508-3,411) | 9.05 / 21.9 | 0.26 | 0/s | - | - | 0 | 6,318 | 15,611 |
| `c32-eight-clusters` | store | 6,793 (6,391-7,713) | 2.80 / 14.4 | 1.09 | 0/s | - | - | 0 | 13,318 | 33,978 |
| `c32-eight-clusters` | http | 6,874 (5,122-7,375) | 2.90 / 10.0 | 1.20 | 0/s | - | - | 0 | 13,701 | 34,400 |
| `c32-eight-clusters-pool2` | store | 1,763 (1,252-3,908) | 7.11 / 35.1 | 7.07 | 0/s | - | - | 0 | 3,720 | 8,843 |
| `c32-eight-clusters-pool2` | http | 1,396 (1,274-3,339) | 9.38 / 40.8 | 9.39 | 0/s | - | - | 0 | 2,844 | 7,011 |
| `contended` | store | 324 (299-343) | 1.73 / 23.8 | 0.64 | 30/s | 2.6 / 24.5 | 12821 | 0 | 730 | 1,774 |
| `contended` | http | 325 (291-336) | 1.95 / 15.8 | 0.71 | 31/s | 2.9 / 20.6 | 17611 | 0 | 721 | 1,758 |
| `contended-open` | store | 303 (302-304) | 0.70 / 9.3 | 0.31 | 210/s | 3.8 / 22.8 | 6318 | 0 | 1,037 | 2,571 |
| `contended-open` | http | 301 (300-301) | 0.85 / 9.6 | 0.46 | 195/s | 4.1 / 23.8 | 4714 | 0 | 997 | 2,483 |

What it shows:

- **One hot cluster tops out around 3,000 admissions/s** however many workers ask (the per-cluster advisory lock, `COUNT(*)` and `INSERT`), while 8 clusters reach about 6,800/s.
- **The default `coordinationPoolSize` of 2 caps the whole process** at roughly 1,400 to 1,800/s for any number of clusters, with release waiting on the pool as long as acquire.
- **Every admission costs Postgres 2 transactions and 5 statements** (`BEGIN`, advisory lock, count-and-insert, `COMMIT`, then the release `DELETE`), plus another release per denial: about 10,000 transactions/s at 5,000 admissions/s.
- **Waiting is polling.** In `contended`, queries that are denied once wait seconds (p99 13 to 18 s) because the 100 ms to 2 s backoff keeps losing to queries that arrive just as a slot frees. In `contended-open`, at 75% of capacity, about a third of queries are denied at least once and wait up to 3 to 6 s (p99), although a freed slot is reused within about 25 ms (hand-off p99).
- No cap violations in any run.

These differ from the plan's table (measured through a control plane with Postgres in Docker on macOS, which adds network latency to every call): absolute numbers here are higher, but the shape is the same.

### Not covered yet

Full-stack mode (a control plane plus real workers, DuckDB `SELECT 1` queries over Trino HTTP, from an open-loop load generator) is not implemented. It would add per-query worker CPU (translation, fingerprinting) and history ingest to the picture; until then, combine this benchmark with the proxy-overhead numbers above.
