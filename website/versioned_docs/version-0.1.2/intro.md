---
sidebar_position: 1
sidebar_label: Overview
title: Documentation Overview
description: QueryFlux docs — multi-engine SQL proxy for Trino, PostgreSQL, MySQL, Snowflake, and Flight. Routing, dialect translation, and observability in one place.
image: img/queryflux-hero-banner.png
keywords:
  - QueryFlux
  - SQL proxy
  - documentation
---
# QueryFlux documentation

QueryFlux is a **high-performance, protocol-aware SQL proxy** for analytical and operational engines. Clients connect with the drivers they already use (Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake HTTP wire and SQL API v2, Arrow Flight). QueryFlux routes each query to the right backend, translates SQL dialects when needed, and exposes a single observability surface — so you stop wiring **N clients × M engines** by hand.

> **One endpoint.** Multiple engines. Native protocols.

The ideas mirror what proxies like [ProxySQL](https://proxysql.com/documentation) do for MySQL/PostgreSQL fleets: place a smart tier in front of your databases, centralize routing and limits, and keep clients simple. QueryFlux applies that pattern to **multi-engine** analytics (Trino, DuckDB, StarRocks, Athena, and more) and **lakehouse** workloads.

---

## Quick guides

| Guide | What you will do |
| --- | --- |
| **[Getting started](/docs/getting-started)** | Run QueryFlux with Docker Compose, connect a SQL client, smoke-test Trino HTTP. |
| **[QueryFlux Studio](/docs/studio)** | Use the web UI for clusters, routing, query history, and admin security. |
| **[Configuration](/docs/configuration)** | Edit `config.yaml` — frontends, cluster groups, routers, persistence, admin API. |

---

## What is QueryFlux?

Modern stacks mix **many query engines** — each with its own wire protocol and SQL dialect. QueryFlux sits **between clients and engines**: it accepts familiar protocols, matches **routing rules** (protocol, headers, SQL patterns, tags, or scripted logic), then dispatches to a **cluster group** with concurrency limits and optional **sqlglot** dialect translation.

**Typical outcomes:**

- **Cost-aware routing** — steer CPU-heavy work to compute-priced clusters and selective scans to pay-per-scan engines.
- **SLA protection** — cap concurrent queries per group; queue at the proxy instead of overloading backends.
- **Transparent migration** — split traffic by weight between engines without client changes.

For a deeper product rationale, see **[Motivation and goals](/docs/architecture/motivation-and-goals)** and **[Benchmarks](/docs/benchmarks)**.

---

## How does it work?

Every query passes through three stages:

### 1. Protocol ingestion

QueryFlux listens on multiple frontends at once:

| Frontend | Default port | Typical clients |
| --- | --- | --- |
| Trino HTTP | 8080 | Trino CLI, JDBC, Python |
| PostgreSQL wire | 5432 | `psql`, Postgres drivers |
| MySQL wire | 3306 | `mysql`, JDBC, BI tools |
| Snowflake HTTP + SQL API v2 | configurable (e.g. 8443) | Snowflake JDBC/ODBC/Python, SnowSQL, REST v2 |
| Arrow Flight SQL | gRPC | Flight-native clients |

Details: **[Frontends](/docs/architecture/frontends/overview)** and **[Snowflake frontend](/docs/architecture/frontends/snowflake)**.

### 2. Routing

Rules are evaluated in order. The first match selects a **cluster group**. You can match on protocol, HTTP headers, SQL text (regex), Trino client tags, compound logic, or **Python** for custom routing. A **fallback** group catches everything else.

Details: **[Routing and clusters](/docs/architecture/routing-and-clusters)**.

### 3. Dispatch and dialect translation

QueryFlux picks a healthy cluster (round-robin, least-loaded, failover, weighted), optionally **rewrites SQL** for the target dialect, and runs the query. If the group is at capacity, queries **queue** at the proxy.

```
Client (psql / Trino CLI / mysql / Snowflake / BI)
        │  native protocol
        ▼
┌──────────────────────────┐
│        QueryFlux         │
│  Frontend → Router → SQL │
│  translation → Group     │
└────────────┬─────────────┘
             ▼
      Trino / DuckDB / …
```

---

## Reference manual

Use these when you already know what you are looking for:

| Topic | Doc |
| --- | --- |
| **YAML reference** | **[Configuration](/docs/configuration)** |
| **System layout** | **[Architecture overview](/docs/architecture/overview)**, **[System map](/docs/architecture/system-map)** |
| **Dialect rewriting** | **[Query translation](/docs/architecture/query-translation)** |
| **Tags and routing** | **[Query tags](/docs/architecture/query-tags)** |
| **Metrics and health** | **[Observability](/docs/architecture/observability)** |
| **Wire protocols** | **[Frontends](/docs/architecture/frontends/overview)** |
| **Extending engines** | **[Adding engine support](/docs/architecture/adding-engine-support)** |
| **Auth model** | **[Auth & authorization design](/docs/architecture/auth-authz-design)** |

---

## Project resources

| Page | Purpose |
| --- | --- |
| **[Development](/docs/development)** | Build from source, venv, `make env`, tests |
| **[Contribute](/docs/contribute)** | PRs, issues, community |
| **[Project structure](/docs/project-structure)** | Repository layout |
| **[Roadmap](/docs/roadmap)** | Shipped vs planned features |
