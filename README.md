# <p align="center"><big>QueryFlux</big></p>

<p align="center">
  <a href="https://queryflux.dev/" title="QueryFlux documentation">
    <img
      src="website/static/img/queryflux-hero-banner.svg"
      alt="QueryFlux — One query, any engine. Rust mascot with fiber cables connecting Trino, DuckDB, Snowflake, DataFusion, Databricks, Presto, Spark, and ClickHouse"
      width="800"
    />
  </a>
</p>
<p align="center"><strong>One query. Any engine.</strong></p>

<p align="center">
  <a href="https://github.com/lakeops-org/queryflux/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://img.shields.io/github/actions/workflow/status/lakeops-org/queryflux/ci.yml?branch=main&amp;label=build&amp;style=for-the-badge&amp;logo=github&amp;logoColor=white" alt="CI status on main" /></a>
  <a href="https://github.com/lakeops-org/queryflux/pkgs/container/queryflux"><img src="https://ghcr-badge.elias.eu.org/shield/lakeops-org/queryflux/queryflux" alt="GHCR downloads" /></a>
  <a href="https://github.com/lakeops-org/queryflux/releases/latest"><img src="https://img.shields.io/github/v/release/lakeops-org/queryflux?sort=semver&amp;style=for-the-badge&amp;logo=github&amp;logoColor=white&amp;label=release" alt="Latest release" /></a>
  <a href="https://github.com/lakeops-org/queryflux/commits/main/"><img src="https://img.shields.io/github/last-commit/lakeops-org/queryflux?style=for-the-badge&amp;logo=git&amp;logoColor=white&amp;label=last%20commit" alt="Last commit" /></a>
</p>
<p align="center">
  <a href="https://github.com/lakeops-org/queryflux/blob/main/LICENSE"><img src="https://img.shields.io/github/license/lakeops-org/queryflux?style=for-the-badge&amp;logo=apache&amp;logoColor=white" alt="License" /></a>
  <a href="https://github.com/lakeops-org/queryflux/issues"><img src="https://img.shields.io/github/issues/lakeops-org/queryflux?style=for-the-badge&amp;logo=github&amp;logoColor=white&amp;label=issues" alt="Open issues" /></a>
  <a href="https://github.com/lakeops-org/queryflux/stargazers"><img src="https://img.shields.io/github/stars/lakeops-org/queryflux?style=for-the-badge&amp;logo=github&amp;logoColor=white&amp;label=stars" alt="GitHub stars" /></a>
</p>
<p align="center">
  <a href="https://join.slack.com/t/queryfluxworkspace/shared_invite/zt-3v7qedxj9-o8ElCLGK0UXT8xBU0_bD8w">Slack community</a>
  &nbsp;·&nbsp;
  <a href="https://queryflux.dev/">Documentation</a>
  &nbsp;·&nbsp;
  <a href="https://lakeops.dev/blog/the-future-isnt-one-engine">Why a control plane</a>
</p>

# <p align="center"><big>Universal SQL multi-engine query router and proxy in Rust</big></p>

QueryFlux sits between SQL clients and multiple backend query engines, providing protocol translation, intelligent routing, load balancing, and automatic SQL dialect conversion.

## Overview

QueryFlux lets you connect any SQL client using standard protocols (Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake HTTP wire + SQL API v2, and Arrow Flight SQL) and route queries to the right backend engine — Trino, DuckDB, StarRocks, Athena, or ClickHouse — based on flexible routing rules. SQL dialects are translated automatically when needed via [sqlglot](https://github.com/tobymao/sqlglot).

The future of data platforms is not one engine — it is a control plane over specialized engines on shared data. [The Future Isn't One Engine. It's a Control Plane.](https://lakeops.dev/blog/the-future-isnt-one-engine) explains why QueryFlux sits above the engines, and how it pairs with LakeOps for table maintenance.

```
Client (Trino CLI / psql / mysql / Snowflake connectors)
    ↓ native protocol
QueryFlux
    ↓ routing + dialect translation
Trino / DuckDB / StarRocks / ClickHouse
```

## Features

**Frontend Protocols**
- Trino HTTP (port 8080)
- PostgreSQL wire (port 5432)
- MySQL wire (port 3306)
- Snowflake HTTP wire + SQL REST API v2 on one listener (`snowflakeHttp` in YAML; pick a port, e.g. 8443)
- Arrow Flight SQL (query execution)

### Snowflake HTTP (wire + SQL API v2)

The Snowflake **HTTP wire** (JDBC/ODBC/Python connector, SnowSQL) and **SQL API v2** share the **`snowflakeHttp`** listener; routing distinguishes `snowflakeHttp` vs `snowflakeSqlApi` for `protocolBased` routers. See [`config.example.yaml`](config.example.yaml) for a commented block (`sessionAffinityAcknowledged`, optional session TTL fields). Full reference: [Snowflake frontend](website/docs/architecture/frontends/snowflake.md).

```yaml
queryflux:
  frontends:
    snowflakeHttp:
      enabled: true
      port: 8443
routers:
  - type: protocolBased
    trinoHttp: trino-default
    snowflakeHttp: duckdb-local
    snowflakeSqlApi: duckdb-local
```

**Backend Engines**
- Trino — async HTTP polling
- DuckDB — embedded, in-process execution
- StarRocks — MySQL wire protocol
- Athena — AWS SDK, async polling
- ClickHouse — HTTP interface, Arrow results

**Routing**
- Protocol-based (route by client connection type)
- Header-based (HTTP header values)
- Query regex matching
- Client tags (Trino `X-Trino-Client-Tags`)
- Python script (custom routing logic)
- Compound (multiple conditions with AND/OR)
- Fallback group

**Other**
- SQL dialect translation via sqlglot (31+ dialects)
- Query queuing with per-cluster capacity limits
- In-memory (single-instance) or PostgreSQL-backed state
- Prometheus metrics + Grafana dashboards
- Admin REST API with OpenAPI spec + Basic auth
- QueryFlux Studio — web management UI (cluster monitoring, query history, config management)

## QueryFlux Studio

Studio is the web management UI, served on port `3000`. It connects to the Admin REST API on port `9000`.

**Default login:** username `admin`, password `admin`.

> **Security:** Change the default password immediately after first login. Go to **Security → Change password** in Studio. The new password is stored as a bcrypt hash in Postgres and the default credentials are no longer used.

You can also set bootstrap credentials via YAML or environment variables:

```yaml
queryflux:
  adminApi:
    port: 9000
    username: admin       # override with QUERYFLUX_ADMIN_USER
    password: admin       # override with QUERYFLUX_ADMIN_PASSWORD
```

Once the password has been changed through the UI, YAML/env credentials are ignored and the database record takes precedence. See the [Studio docs](website/docs/studio.md) for the full reference.

## Benchmarks

[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/lakeops-org/queryflux)

- **Hot-path micro-benchmarks**, run on every PR that touches QueryFlux. Fingerprinting, routing, guardrails and SQL translation (`crates/*/benches`) run under [CodSpeed](https://codspeed.io/lakeops-org/queryflux), which counts CPU instructions, so even small regressions show up reliably.
- **Proxy overhead**, run on every merge to `main` and checked against recent history. [`queryflux-bench`](crates/queryflux-bench) sends `SELECT 1` to mock Trino / StarRocks backends, directly and through QueryFlux; the difference is the latency QueryFlux adds. For PRs that touch the request path, add the `benchmark` label to get an A/B comparison against the base. **Latest results: [queryflux.dev/benchmarks](https://queryflux.dev/benchmarks).**

Regressions warn and never block a merge. Run locally with `make benchmark` (proxy) and `make bench-micro` (micro). See the [benchmarks docs](website/docs/benchmarks.md) for methodology.

## Getting Started

### Prerequisites

- Rust (stable)
- Python 3.10+ (for sqlglot SQL translation)
- Docker + Docker Compose (for local development stack)

### Setup

```bash
# Install Python dependencies (sqlglot)
make setup

# Start services (Trino, Postgres, Prometheus, Grafana) and run QueryFlux
make dev
```

This starts:
| Service | URL |
|---|---|
| QueryFlux (Trino HTTP) | http://localhost:8080 |
| Admin / Metrics | http://localhost:9000/metrics |
| Trino (direct) | http://localhost:8081 |
| PostgreSQL | localhost:5433 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

### Test it

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

### CLI Quickstart (No Docker)

You can run QueryFlux natively with an embedded DuckDB engine without any containers:

```bash
# 1. Clone the repository
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux

# 2. Install the binary globally (if linking fails, use `export DUCKDB_DOWNLOAD_LIB=1` before running this)
cargo install --path crates/queryflux

# 3. Automatically create a .venv and install Python dependencies (e.g., sqlglot)
queryflux --install-deps

# 4. Validate the config and environment
queryflux --config examples/cli-quickstart/config.yaml --validate

# 5. Start the proxy
queryflux --config examples/cli-quickstart/config.yaml
```

### Kubernetes

QueryFlux includes a provider-neutral Helm chart:

```bash
helm install queryflux ./charts/queryflux
kubectl port-forward svc/queryflux 3000:3000 8080:8080 9000:9000
```

See [`charts/queryflux/README.md`](charts/queryflux/README.md) for production values, external config, existing secrets, ingress, autoscaling, network policy, and Prometheus ServiceMonitor options.

### Build

```bash
make build
# or
cargo build --release
./target/release/queryflux --config config.yaml
```

## Configuration

Copy `config.example.yaml` and adjust for your environment:

```yaml
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
  persistence:
    type: inMemory  # or: postgres

clusterGroups:
  trino-default:
    engine: trino
    maxRunningQueries: 100
    clusters:
      - name: trino-1
        endpoint: http://trino-host:8080
        auth:
          type: basic
          username: user
          password: pass

  duckdb-local:
    engine: duckDb
    maxRunningQueries: 4
    clusters:
      - name: duckdb-1
        databasePath: /tmp/queryflux.duckdb

routers:
  - type: protocolBased
    trinoHttp: trino-default

  - type: header
    headerName: x-target-engine
    headerValueToGroup:
      duckdb: duckdb-local

routingFallback: trino-default
```

See `config.example.yaml` for the full reference including TLS, auth, query queuing, SQL translation, and Python script routing.

## Project Structure

```
queryflux/
├── crates/
│   ├── queryflux/                  # Main binary
│   ├── queryflux-cli/              # CLI logic and environment setup
│   ├── queryflux-core/             # Shared types and traits
│   ├── queryflux-config/           # Config loading
│   ├── queryflux-frontend/         # Protocol frontends (Trino HTTP, PG/MySQL wire, Snowflake HTTP, …)
│   ├── queryflux-engine-adapters/  # Backend engine adapters
│   ├── queryflux-cluster-manager/  # Load balancing and queueing
│   ├── queryflux-routing/          # Router implementations
│   ├── queryflux-persistence/      # State storage (in-memory / PostgreSQL)
│   ├── queryflux-translation/      # SQL dialect translation (sqlglot via PyO3)
│   ├── queryflux-metrics/          # Prometheus metrics
│   ├── queryflux-auth/             # Authentication and authorization
│   ├── queryflux-bench/            # Proxy overhead benchmarks
│   └── queryflux-e2e-tests/        # Integration tests
├── examples/                       # Docker Compose quickstarts (see examples/README.md)
├── grafana/                        # Grafana dashboards
├── prometheus/                     # Prometheus config
├── config.example.yaml
├── docker/
│   ├── docker-compose.yml          # Local dev stack (`make dev`)
│   ├── fixtures/                   # SQL init, test data (shared with examples)
│   ├── test/                       # E2E stack: docker-compose.test.yml, fakesnow helpers
│   └── queryflux/                  # QueryFlux Dockerfile
├── docs/                           # Architecture markdown
├── website/                        # Docusaurus documentation site
```

QueryFlux Studio is maintained separately in the [queryflux-studio](https://github.com/lakeops-org/queryflux-studio) repository.

## Development

```bash
make dev      # Start all services and run QueryFlux
make stop     # Stop services
make logs     # View logs
make check    # Run tests and linting
make clean    # Remove build artifacts and Docker volumes
```

See [development.md](development.md) for environment variables, workspace layout, and how to run the binary locally. See [contribute.md](contribute.md) for pull request expectations.

## Architecture

See [docs/README.md](docs/README.md) for the full architecture doc set (motivation, query translation, routing and clusters). The high-level overview lives in [docs/architecture.md](docs/architecture.md).

**Docs website:** a Docusaurus mirror of this README and `docs/` lives under [`website/`](website/README.md); run `npm install` and `npm start` there for a local browseable site.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
