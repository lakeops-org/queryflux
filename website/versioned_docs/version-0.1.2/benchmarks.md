---
sidebar_position: 4
title: Proxy Benchmarks
description: QueryFlux proxy overhead benchmarks — mock-backend latency for Trino HTTP and MySQL wire paths (~0.35 ms p50).
image: img/queryflux-hero-banner.png
---
# Benchmark (proxy overhead)

End-to-end overhead is measured by `queryflux-bench` (`cargo run --bin queryflux-bench` after `cargo build --bin queryflux`). It uses **mock** backends (Trino HTTP + MySQL wire for StarRocks), **50** warmup queries per path, then **500** timed iterations of `SELECT 1` — direct to the mock vs the same request through QueryFlux (Trino HTTP frontend). Numbers vary by machine.

## Trino (mock HTTP coordinator)

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct | 0.21 ms | 0.29 ms | 0.35 ms |
| Via QueryFlux | 0.57 ms | 0.81 ms | 1.21 ms |
| **Overhead** | **0.36 ms** | **0.52 ms** | **0.86 ms** |

## StarRocks path (mock MySQL FE)

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct (MySQL `SELECT 1`) | 0.36 ms | 0.54 ms | 1.20 ms |
| Via QueryFlux | 0.70 ms | 1.21 ms | 4.20 ms |
| **Overhead** | **0.34 ms** | **0.67 ms** | **3.01 ms** |
