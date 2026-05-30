---
sidebar_position: 1
sidebar_label: Getting started
title: Getting Started
description: Run QueryFlux with Docker Compose, connect SQL clients, smoke-test Trino HTTP routing, and open Studio in minutes.
image: img/queryflux-hero-banner.png
---
# Getting started

The fastest path is a **Docker Compose** example under [`examples/`](https://github.com/lakeops-org/queryflux/tree/main/examples). Run commands **from that example directory** so paths like `./config.yaml` resolve.

See the **[examples README](https://github.com/lakeops-org/queryflux/blob/main/examples/README.md)** for a comparison table, image/registry notes, and environment variables (`RUST_LOG`, `TPCH_SCALE` for the full stack).

:::tip Same ports in some examples

[`minimal-trino`](https://github.com/lakeops-org/queryflux/tree/main/examples/minimal-trino) and [`minimal-inmemory`](https://github.com/lakeops-org/queryflux/tree/main/examples/minimal-inmemory) both publish **8080**, **8081**, **3000**, and **9000**. Only run one at a time, or change published ports in `docker-compose.yml`.

[`with-prometheus-grafana`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-prometheus-grafana) uses **3000 for Grafana** (not Studio). Do not run it alongside an example that uses 3000 for Studio unless you remap a port.

:::

## Prerequisites

- **Docker** and **Docker Compose** (for the examples below)
- **Rust**, **Python 3.10+**, and **Make** — only if you use **[Develop from source](#develop-from-source)** or build the binary locally

## Example: minimal (Postgres + Trino + QueryFlux + Studio)

**Best for:** production-like **Postgres** persistence, full **Studio** (query history, clusters/groups/routing via API once seeded).

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux/examples/minimal-trino
docker compose up -d --wait
```

| Service | URL / connection |
| --- | --- |
| SQL (Trino **via** QueryFlux) | http://localhost:8080 |
| Trino (direct, bypasses QueryFlux) | http://localhost:8081 |
| Admin API | http://localhost:9000 |
| Studio | http://localhost:3000 (login **admin** / **admin**) |
| Postgres (from host) | `localhost:5433` — user `queryflux`, password `queryflux`, database `queryflux` |

**Next steps:** Trino CLI from the host or from inside the `trino` container, verifying traffic goes through QueryFlux — full walkthrough in **[`examples/minimal-trino/README.md`](https://github.com/lakeops-org/queryflux/blob/main/examples/minimal-trino/README.md)** (including Studio **Queries** and the port/hostname cheat sheet).

## Example: minimal in-memory

**Best for:** fastest local tryout; **no Postgres**. Routing/clusters come from [`config.yaml`](https://github.com/lakeops-org/queryflux/blob/main/examples/minimal-inmemory/config.yaml); **restart QueryFlux** after edits. Studio pages that need Postgres may **503** — see **[`examples/minimal-inmemory/README.md`](https://github.com/lakeops-org/queryflux/blob/main/examples/minimal-inmemory/README.md)**.

```bash
cd queryflux/examples/minimal-inmemory
docker compose up -d --wait
```

Ports match **minimal** (8080, 8081, 3000, 9000) — stop the other stack first.

## Example: Prometheus + Grafana

**Best for:** same workload as **minimal** (Postgres + Trino + QueryFlux) plus **Prometheus** and **Grafana** (dashboards from repo `grafana/`). **No Studio** in this stack; Grafana is on **3000**.

```bash
cd queryflux/examples/with-prometheus-grafana
docker compose up -d --wait
```

| Service | URL |
| --- | --- |
| Trino via QueryFlux | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin and `/metrics` | http://localhost:9000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (login **admin** / **admin**) |

Details: **[`examples/with-prometheus-grafana/README.md`](https://github.com/lakeops-org/queryflux/blob/main/examples/with-prometheus-grafana/README.md)**.

## Example: full stack (Trino + StarRocks + Iceberg)

**Best for:** multi-engine demos — **Trino**, **StarRocks**, **Lakekeeper**, **MinIO**, **QueryFlux**, **Studio**; optional TPCH load into Iceberg.

```bash
cd queryflux/examples/full-stack
docker compose up -d --wait
docker compose --profile loader run --rm -T data-loader
docker compose --profile loader run --rm -T starrocks-catalog-setup
```

| Service | URL / connection |
| --- | --- |
| Trino via QueryFlux | http://localhost:8080 |
| MySQL wire (e.g. StarRocks via QueryFlux) | `mysql` client to **localhost:3306** |
| Studio | http://localhost:3000 (login **admin** / **admin**) |
| Trino (direct) | http://localhost:8081 |
| MinIO console | http://localhost:19001 |
| Lakekeeper REST | http://localhost:8181 |
| QueryFlux Postgres | **localhost:5433** |

More detail on loader scripts and fixtures: **[examples README — Full stack](https://github.com/lakeops-org/queryflux/blob/main/examples/README.md#full-stack-full-stack)**.

## Quick smoke test (any stack with Trino HTTP on 8080)

After `docker compose up -d --wait`:

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

You should see QueryFlux handle the Trino HTTP request; follow logs with `docker compose logs -f queryflux` if needed.

## Develop from source

For the full Rust workspace, Python/sqlglot, and the compose stack under `docker/` (not the `examples/` images), use the repository **Makefile** from the **repo root**:

```bash
make setup    # Python venv + sqlglot (for translation)
make env      # backing services (Docker)
make server   # run QueryFlux per Makefile
```

Typical URLs (see your `Makefile` / `docker/docker-compose.yml` if you customize ports):

| Service | URL |
| --- | --- |
| QueryFlux (Trino HTTP) | http://localhost:8080 |
| Admin / metrics | http://localhost:9000/metrics |
| Trino (direct) | http://localhost:8081 |
| PostgreSQL | localhost:5433 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (login **admin** / **admin**) |

```bash
make stop     # stop services
make logs     # container logs
make lint     # clippy (no Docker)
make test     # unit tests (no Docker)
make clean    # artifacts + volumes
```

See **[Development](/docs/development)** for `PYO3_PYTHON`, `config.local.yaml`, E2E tests, and troubleshooting.

## Build the binary

```bash
make build
# or
cargo build --release
./target/release/queryflux --config config.yaml
```

Use a config file that matches your deployment — **[Configuration](/docs/configuration)** and the per-example **`config.yaml`** files are good references.
