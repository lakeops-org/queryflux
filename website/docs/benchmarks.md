---
sidebar_position: 4
title: Benchmarks
description: How QueryFlux performance is measured — proxy overhead vs direct backends, hot-path micro-benchmarks, and per-PR regression checks in CI.
image: img/queryflux-hero-banner.png
---
# Benchmarks

QueryFlux tracks performance at two levels. They run at different times because one is precise and cheap, and the other is noisy and expensive:

| | What | How it's measured | When it runs | Where results go |
| --- | --- | --- | --- | --- |
| **Micro-benchmarks** | Per-function cost of the per-query hot paths | CPU instruction counts (CodSpeed simulation) | Every PR that touches QueryFlux code | PR comment, [CodSpeed dashboard](https://codspeed.io/lakeops-org/queryflux) |
| **Proxy overhead** | End-to-end latency a client sees through QueryFlux vs going direct | Wall-clock | Every merge to `main`, and PRs labelled `benchmark` | [**Live results**](/benchmarks), PR comment |

Website and docs-only changes never trigger either.

**Latest numbers from `main`: [/benchmarks](/benchmarks).**

## Proxy overhead

`crates/queryflux-bench` starts in-process **mock** backends — a Trino HTTP coordinator and a MySQL-wire server standing in for StarRocks — and sends `SELECT 1`:

- **Direct**: client → mock backend.
- **Via QueryFlux**: client → QueryFlux (Trino HTTP frontend) → mock backend.

Mocks answer instantly, so the difference is what QueryFlux itself adds (protocol handling, routing, dispatch, result streaming) and nothing else: no warehouse I/O, no planning. Use these numbers to track regressions and compare code paths, not as production query latency.

**Reading the results**

- **Overhead p50** = p50(via QueryFlux) − p50(direct). Direct and proxy are separate samples, so overhead at p99 can be smaller than at p50 when the direct path had a heavier tail.
- The StarRocks path does more work than Trino passthrough (Trino HTTP in, MySQL wire out), so expect higher overhead there.

## Micro-benchmarks

Criterion benchmarks in `crates/*/benches`, all over the same SQL corpus (`benches/sql_corpus.rs`: a point lookup, TPC-H Q1, and a CTE + join + window query):

| Crate | Benchmarks |
| --- | --- |
| `queryflux-fingerprint` | `fast_hash`, `rich_fingerprint` (polyglot parse) |
| `queryflux-routing` | full router chain (header → regex → tags → fallback), with and without trace |
| `queryflux-guardrails` | built-in guard chain (read-only, row limit, require predicate) incl. the shared parse |
| `queryflux-translation` | sqlglot translation: Trino → StarRocks, Trino → DuckDB, MySQL → Trino |

In CI they run under [CodSpeed](https://codspeed.io)'s simulation mode, which counts executed instructions instead of timing, so a 2% change is real rather than runner noise.

## In CI

This follows the usual split for performance testing. Deterministic benchmarks run on every PR. Wall-clock benchmarks are too noisy and costly for that, so they run after merge to watch the trend, and before merge only on request.

### Every PR: micro-benchmarks

The `CI` workflow runs the CodSpeed suite once a maintainer approves the run (the same gate as the Rust test job), and only when `crates/`, `benches/`, `Cargo.toml`/`Cargo.lock`, `requirements.txt` or the bench scripts change. CodSpeed comments with per-benchmark changes against `main`. Instruction counts don't depend on runner load, so even a 2–5% change is a real signal.

### Opt-in per PR: proxy overhead A/B

For PRs that touch the request path (frontends, dispatch, engine adapters, routing), a maintainer adds the **`benchmark`** label. Only people with triage or write access can add labels, so the label is also the approval to spend the runner time. The `Proxy benchmark` workflow then:

- builds the PR's merge commit and its base in release mode, and runs them alternately (ABBA, 6 rounds × 2,000 queries) on the same runner, so both sides see the same machine;
- treats the *Direct* rows, identical mock code on both sides, as the measure of runner noise;
- gates only on *Via QueryFlux p50*: it's flagged 🔴 when its median per-round change exceeds **10% + measured runner noise** *and* head was slower in all but at most one round. p95, p99 and mean are shown but are too tail-sensitive on shared runners. (Identical base and head binaries pass with no flags; that's how the rule was tuned.)
- posts the table as a sticky PR comment and in the job summary.

To re-run after pushing more commits, remove the label and add it again.

### Every merge to `main`: trend and jump check

On merge, the proxy benchmark runs again and its result goes to the `benchmark-data` branch, which the website build turns into [/benchmarks](/benchmarks). This is the safety net for PRs that didn't get the A/B. Each `main` run lands on a different shared runner, so it's compared against the **last 10 `main` runs**, not just the previous one. If *Via QueryFlux p50* is both ≥15% above their median **and** higher than all ten, the job warns and comments on the merged PR.

All of these are **warnings, not merge blockers**. A flagged PR gets a maintainer's attention, and it can still merge if the slowdown is expected (for example, a new feature on the hot path).

## Running locally

Prerequisites: `make setup` (creates `.venv` with sqlglot, needed by PyO3 crates).

```bash
# Proxy overhead: release build, then one run (tables on stderr, JSON on stdout)
make benchmark

# Micro-benchmarks (plain Criterion locally; HTML reports in target/criterion)
make bench-micro
# or a single crate
cargo bench -p queryflux-routing

# A/B two builds yourself, the way CI does
cargo build --release --bin queryflux --bin queryflux-bench
cp target/release/queryflux /tmp/queryflux-head
git switch main && cargo build --release --bin queryflux && cp target/release/queryflux /tmp/queryflux-base
git switch -
HARNESS=target/release/queryflux-bench scripts/bench/proxy-ab.sh /tmp/queryflux-base /tmp/queryflux-head 6 /tmp/bench
python3 scripts/bench/compare.py /tmp/bench
```

Proxy benchmark knobs (environment): `QUERYFLUX_BENCH_WARMUP` (default 50), `QUERYFLUX_BENCH_ITERATIONS` (500), `QUERYFLUX_BENCH_TRINO_POLL` (`1` adds a `nextUri` poll round trip), `QUERYFLUX_BENCH_BIN` (the `queryflux` binary to benchmark).

## Adding a benchmark

- **New hot path**: add a `[[bench]]` target with `harness = false` and `criterion = { workspace = true }` under `[dev-dependencies]`, and include the shared corpus with `#[path = "../../../benches/sql_corpus.rs"] mod sql_corpus;`. Then add the crate to the `cargo codspeed build` line in `.github/workflows/ci.yml` and to `make bench-micro`.
- **New query shape**: append to `benches/sql_corpus.rs`. Prefer adding over editing: an edited query starts a new baseline in every benchmark that uses it.
