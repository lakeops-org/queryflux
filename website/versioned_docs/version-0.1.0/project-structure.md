---
sidebar_position: 3
description: High-level layout of the QueryFlux repository — crate responsibilities, directories, and runnable examples.
---

# Project structure

High-level layout of the QueryFlux repository. Crate responsibilities also appear in **[Development](/docs/development)**; runnable stacks are in **[Getting started](/docs/getting-started)**.

## Layout

```
queryflux/
├── Cargo.toml · Cargo.lock · rust-toolchain.toml
├── Makefile                      # dev, test, compose helpers
├── requirements.txt              # Python deps (sqlglot) for translation
├── config.example.yaml           # Full config reference
├── config.local.yaml             # Local overrides (compose-oriented; optional in clones)
├── README.md · LICENSE
├── development.md · contribute.md · benchmark.md
│
├── crates/                       # Rust workspace (see below)
├── queryflux-studio/           # QueryFlux Studio — Next.js admin UI
├── examples/                     # Docker Compose examples (minimal, observability, full stack)
│   ├── minimal-trino/
│   ├── minimal-inmemory/
│   ├── quickstart/
│   ├── with-prometheus-grafana/
│   ├── full-stack/
│   └── README.md
├── docker/
│   ├── docker-compose.yml        # Stack used by `make env`
│   ├── test/                     # E2E stack (`docker-compose.test.yml`, fakesnow)
│   ├── fixtures/                 # SQL init, TPCH helpers, test data seeds
│   ├── queryflux/                # QueryFlux container build
│   └── queryflux-studio/         # Studio container build
├── website/                      # Docusaurus documentation site
├── grafana/                      # Dashboards & provisioning
├── prometheus/                   # Example scrape config (host-oriented)
└── .github/workflows/            # CI (tests, benchmarks, images, etc.)
```

## Rust workspace (`crates/`)

| Crate | Role |
| --- | --- |
| `queryflux` | Main binary: config, wiring, admin HTTP, engine registration |
| `queryflux-core` | Shared types, config structs, session & engine registry |
| `queryflux-config` | Loading YAML / env into proxy config |
| `queryflux-frontend` | Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL, Snowflake, dispatch |
| `queryflux-engine-adapters` | Trino, DuckDB, StarRocks, Athena, ADBC, … |
| `queryflux-cluster-manager` | Cluster groups, load balancing, queueing |
| `queryflux-routing` | Router chain, `routingFallback`, script routing |
| `queryflux-persistence` | In-memory & PostgreSQL stores, migrations |
| `queryflux-translation` | sqlglot via PyO3 |
| `queryflux-metrics` | Prometheus instrumentation |
| `queryflux-auth` | Auth providers & authorization plumbing |
| `queryflux-fingerprint` | Query fingerprinting (AST-based deduplication) |
| `queryflux-bench` | Proxy overhead benchmarks (mock backends) |
| `queryflux-e2e-tests` | Integration tests behind Docker |

Authoritative workspace membership is **`Cargo.toml`** `[workspace] members`.

## UI and ops

| Path | Purpose |
| --- | --- |
| `queryflux-studio/` | Studio SPA: clusters, queries, routing — talks to QueryFlux **admin API** |
| `examples/` | **Self-contained** compose files; run from each subdirectory |
| `docker/` | Compose for **repo development** (`make env` / `make test-e2e`) and **Dockerfile** trees |
| `grafana/` · `prometheus/` | Dashboards and sample Prometheus config |
| `website/` | Docusaurus documentation site; edit **`website/docs/`** for published content |
