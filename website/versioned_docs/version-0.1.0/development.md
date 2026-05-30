---
sidebar_position: 5
description: Set up the Rust workspace, Python/sqlglot sidecar, and Docker dependencies for local QueryFlux development.
---

# Development guide

This guide is for working on the QueryFlux **Rust workspace** and its local dependencies (Python/sqlglot, Docker).

## Prerequisites

- **Rust** (stable toolchain), `cargo`, `rustfmt`, `clippy`
- **Python 3.10+** (3.13 is used in some Makefile `PYTHONPATH` examples; adjust paths if your venv uses another minor version)
- **Docker** and **Docker Compose** (for `make env`, `make test-e2e`, and the full stack)

## First-time setup

```bash
make setup
```

This creates `.venv/` and installs Python packages from `requirements.txt` (including **sqlglot** for dialect translation via PyO3).

Point PyO3 at that interpreter when building or running (required whenever translation is enabled):

```bash
export PYO3_PYTHON="$(pwd)/.venv/bin/python3"
# Optional: helps some environments resolve the venv site-packages
export PYTHONPATH="$(pwd)/.venv/lib/python3.13/site-packages"
```

Adjust `PYTHONPATH` if your venv’s `lib/pythonX.Y` differs.

## Daily commands

| Goal | Command |
|------|---------|
| Format (if you use rustfmt manually) | `cargo fmt --all` |
| Clippy lints (no Docker) | `make lint` |
| Unit tests (no Docker) | `make test` |
| Release build | `make build` |
| E2E tests (Docker: Trino, StarRocks, Lakekeeper) | `make test-e2e` |
| Start backing services (Docker) | `make env` |
| Run QueryFlux locally | `make server` |
| Stop Docker stack + QueryFlux | `make stop` |
| Follow container logs | `make logs` |
| Clean build + Docker volumes | `make clean` |

## Running QueryFlux locally

`make env` brings up dependencies via `docker compose` (see `Makefile` for the exact services). After containers are healthy, run the binary from the repo root with your config (or use `make server`), for example:

```bash
export PYO3_PYTHON="$(pwd)/.venv/bin/python3"
export PYTHONPATH="$(pwd)/.venv/lib/python3.13/site-packages"  # version may vary
cargo run --bin queryflux -- --config config.local.yaml
```

Use `config.local.yaml` for the compose-oriented stack, or copy and edit `config.example.yaml` for your own layout.

**Note:** If `sqlglot` is not importable, the process starts but **translation is disabled** and logs a warning; dialect mismatches may then fail on the backend.

## Workspace layout

The Cargo workspace lives under `crates/`. Common places to touch code:

| Crate | Responsibility |
|-------|----------------|
| `queryflux` | Binary entrypoint, wiring config → routers → cluster manager → adapters |
| `queryflux-core` | Shared types (`SessionContext`, query IDs, config structs) |
| `queryflux-config` | Loading YAML into `ProxyConfig` |
| `queryflux-frontend` | HTTP/frontends, dispatch, Trino handlers |
| `queryflux-routing` | `RouterTrait` implementations and `RouterChain` |
| `queryflux-cluster-manager` | Group capacity, strategies, `acquire_cluster` / `release_cluster` |
| `queryflux-translation` | sqlglot / `TranslationService` |
| `queryflux-engine-adapters` | Trino, DuckDB, StarRocks, Athena, ADBC, … |
| `queryflux-persistence` | In-memory and PostgreSQL stores |
| `queryflux-metrics` | Prometheus instrumentation, buffered metrics |
| `queryflux-auth` | Auth providers, authorization, identity resolution |
| `queryflux-fingerprint` | Query fingerprinting (AST-based deduplication) |
| `queryflux-bench` | Proxy overhead benchmarks (mock backends) |
| `queryflux-e2e-tests` | Integration tests behind Docker |

Architecture narrative: **[System map](/docs/architecture/system-map)** and the **[architecture documentation overview](/docs/architecture/overview)**.

## Configuration reference

Authoritative shapes are the serde types in `queryflux-core` (`config.rs`) and working examples such as `config.local.yaml`. The snippet in the root `README.md` may lag; prefer `config.local.yaml` and **[Routing and clusters](/docs/architecture/routing-and-clusters)** for group `members` and top-level `clusters`.

## Troubleshooting

- **PyO3 / Python not found:** Set `PYO3_PYTHON` to the venv’s `python3` and ensure `make setup` completed.
- **Port conflicts:** Adjust ports in `docker/docker-compose.yml` or disable conflicting local services.
- **E2E failures:** Bring up `docker/test/docker-compose.test.yml` (Trino, StarRocks, Iceberg stack); see `make test-e2e`.

For contribution expectations (PRs, tests, docs), see **[Contributing](/docs/contribute)**.

The same content is maintained in the repository as [`development.md`](https://github.com/lakeops-org/queryflux/blob/main/development.md).
