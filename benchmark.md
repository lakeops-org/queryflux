# QueryFlux overhead benchmark

This document describes the **queryflux-bench** micro-benchmark: what it measures, how to run it, and how to read the output.

## What it measures

The benchmark compares **client-observed latency** for a trivial query in two setups:

1. **Trino** — A full Trino HTTP client flow (`POST /v1/statement`, follow `nextUri` until the query is finished) against a **local mock Trino**, vs the **same mock** reached **through QueryFlux** (Trino HTTP frontend → configured cluster → backend).
2. **StarRocks** — **`SELECT 1`** over **MySQL** to a **local mock MySQL** (baseline), vs the same work via **Trino HTTP to QueryFlux**, which routes to a **StarRocks (MySQL protocol)** cluster pointing at that mock.

Backends are **in-process mocks on localhost**, so timings reflect mostly **proxy and protocol overhead**, not real warehouse I/O or query planning. Results are useful for **regression tracking** and **rough comparison of code paths** (Trino passthrough vs StarRocks translation), not for production query performance.

## How to run

**Prerequisites**

- Rust toolchain and a Python venv as for the rest of the repo (`make setup` creates `.venv` and installs dependencies needed because other workspace crates link PyO3).

**Recommended (release build + same flags as CI/Makefile):**

```bash
make benchmark
```

This runs:

1. `cargo build --release --bin queryflux` (the bench spawns this binary next to the bench artifact).
2. `cargo run --release -p queryflux-bench`

**Manual equivalent:**

```bash
PYO3_PYTHON="$(pwd)/.venv/bin/python3" \
PYTHONPATH="$(pwd)/.venv/lib/python3.13/site-packages" \
cargo build --release --bin queryflux

PYO3_PYTHON="$(pwd)/.venv/bin/python3" \
PYTHONPATH="$(pwd)/.venv/lib/python3.13/site-packages" \
cargo run --release -p queryflux-bench
```

**Optional environment variables** (defaults in parentheses):

| Variable | Meaning |
|----------|---------|
| `QUERYFLUX_BENCH_WARMUP` | Warmup iterations per path (50) |
| `QUERYFLUX_BENCH_ITERATIONS` | Timed iterations per path (500) |
| `QUERYFLUX_BENCH_TRINO_POLL` | If `1`/`true`, mock Trino returns `RUNNING` + `nextUri` so the client performs a poll `GET` (exercises QueryFlux poll forwarding). Default `0` (single response, `FINISHED`). |

**Output**

- Human-readable tables are printed to **stderr**.
- A single **JSON array** is printed to **stdout** (`[{"name", "value", "unit"}]`, values in ms with µs precision), consumed by `scripts/bench/compare.py`.

| Variable | Meaning |
|----------|---------|
| `QUERYFLUX_BENCH_BIN` | Path of the `queryflux` binary to benchmark (default: the one built next to the harness). |

## In CI

Merges to `main` run this benchmark (`.github/workflows/proxy-bench.yml`, called from `ci.yml`) and record results on the `benchmark-data` branch, which feeds [queryflux.dev/benchmarks](https://queryflux.dev/benchmarks). `scripts/bench/publish.py` warns and comments on the merged PR when a run jumps against the last 10 `main` runs. On a PR, adding the `benchmark` label builds base and head in release mode and alternates them on the same runner (`scripts/bench/proxy-ab.sh`). `scripts/bench/compare.py` turns the runs into a sticky PR comment (warning only). Hot-path micro-benchmarks (`make bench-micro`) run under CodSpeed. Full description: [website/docs/benchmarks.md](website/docs/benchmarks.md).

## Sample results

Numbers below are **one representative run** on a developer machine; your absolute milliseconds will vary. Relative gaps (direct vs via QueryFlux) are the interesting part for regressions.

### Trino (mock backend)

```
── QueryFlux overhead — Trino ─────────────────────────────────────
                  p50       p95       p99       mean      stddev
  Direct          0.08 ms    0.14 ms    0.25 ms    0.09 ms    0.11 ms
  Via QueryFlux   0.13 ms    0.21 ms    0.27 ms    0.14 ms    0.04 ms
  Overhead        0.05 ms    0.07 ms    0.01 ms    0.05 ms        —
  Min / max     direct   0.04 /   2.31 ms   Proxy   0.08 /   0.39 ms
  Overhead vs direct p50: +67.0%
────────────────────────────────────────────────────────────────────────
```

### StarRocks (mock MySQL backend, client via Trino HTTP → QueryFlux)

```
── QueryFlux overhead — StarRocks ─────────────────────────────────────
                  p50       p95       p99       mean      stddev
  Direct          0.12 ms    0.34 ms    0.39 ms    0.15 ms    0.08 ms
  Via QueryFlux   0.34 ms    0.64 ms    1.16 ms    0.38 ms    0.16 ms
  Overhead        0.22 ms    0.29 ms    0.77 ms    0.23 ms        —
  Min / max     direct   0.10 /   0.96 ms   Proxy   0.24 /   1.89 ms
  Overhead vs direct p50: +185.2%
────────────────────────────────────────────────────────────────────────
```

### JSON metrics (same run)

The program also emits named series for dashboards, for example:

| Name | Unit | Example value |
|------|------|----------------|
| Trino — Direct p50 (ms) | ms | 0.08 |
| Trino — Via QueryFlux p50 (ms) | ms | 0.13 |
| Trino — Overhead vs direct p50 (%) | % | 67.01 |
| StarRocks — Direct p50 (ms) | ms | 0.12 |
| StarRocks — Via QueryFlux p50 (ms) | ms | 0.34 |
| StarRocks — Overhead vs direct p50 (%) | % | 185.23 |

(Full output includes p95, p99, mean, stddev, and overhead columns for each engine.)

## How to read the table

- **Direct** — Baseline latency (HTTP to mock Trino, or MySQL `SELECT 1` to mock MySQL).
- **Via QueryFlux** — Same logical workload through the proxy.
- **p50 / p95 / p99** — Percentiles of the **timed** iterations for that column (500 samples by default).
- **mean / stddev** — Average and spread of those samples.
- **Overhead vs direct p50** — \((\text{proxy p50} / \text{direct p50}) - 1\), expressed as a percentage: how much **higher the median** is when using QueryFlux.

**Overhead row (last numeric row before min/max)** — This is **not** “the p99 of (proxy − direct) per request.” Direct and proxy are **separate runs**. The tool reports:

- Overhead p50 = p50(proxy) − p50(direct)  
- Overhead p99 = p99(proxy) − p99(direct)  
- Overhead mean = mean(proxy) − mean(direct)

So **Overhead p99** can be **smaller than Overhead p50** if the **direct** path had a heavier tail in its **own** sample (e.g. a rare slow direct outlier inflates direct p99 more than proxy p99). In the sample Trino run, **direct max** was much higher than **proxy max**, which illustrates that tail behavior.

**Takeaway from the sample numbers**

- **Trino path:** In this harness, median extra cost through QueryFlux is on the order of **~0.05 ms** (~**67%** relative median increase on an **~0.08 ms** baseline — large percentage, tiny absolute time).
- **StarRocks path:** More work (Trino HTTP in, MySQL out); median extra cost is roughly **~0.22 ms** here (**~185%** vs a **~0.12 ms** direct MySQL baseline in the mock).

## Limitations

- **Localhost mocks** — No real network WAN, disk, or engine CPU for query execution.
- **Tiny queries** — End-to-end warehouse benchmarks (e.g. TPC-H at scale) are dominated by the engine; they complement but do not replace this overhead-focused bench.
- **Machine noise** — Compare trends or A/B branches on the same host where possible; absolute ms will differ across machines.
