---
slug: why-queryflux
title: "Why QueryFlux: the missing layer above query engines"
description: Open table formats made data interoperable. Query engines are still islands. Why QueryFlux exists as a neutral SQL layer above the engines.
authors: [amit]
tags: [Vision]
keywords:
  - QueryFlux
  - SQL proxy
  - query router
  - lakehouse
  - Iceberg
  - multi-engine
  - DuckDB
  - Trino
image: img/blog/why-queryflux-hero.jpg
---

For the last few years, the data lake world has been quietly fixing one of data infrastructure's oldest problems: lock-in.

Apache Iceberg and the broader open table format movement changed the contract between storage and compute. Data stopped belonging to a single engine. Instead of copying the same dataset into five systems to support five workloads, teams keep one copy and let many engines read it: Spark for batch, Trino for interactive SQL, DuckDB for local analysis, StarRocks for serving. One dataset, many readers.

> **Data became interoperable. Query engines did not.**

<!-- truncate -->

<figure class="blog-figure">
  <img src="/img/blog/interoperability-layers.svg" alt="Diagram: open table formats unify storage while query engines remain separate until QueryFlux provides one SQL front door" />
  <figcaption>Open formats fixed the storage layer. Clients still face N engines × M protocols unless something sits in between.</figcaption>
</figure>

## We solved storage interoperability. Compute is next.

Most teams still treat query engines as silos, and they pay for it in ways that compound quietly.

Every engine speaks its own wire protocol, has its own SQL dialect quirks, and needs its own connection management. Multiply engines by clients (BI tools, notebooks, application code, CLIs) and you don't get a linear integration problem; you get a combinatorial one. Each pairing needs its own driver, dialect handling, and retry logic. Routing, rate limiting, and observability get reinvented in isolation, over and over.

### Cost and performance follow the same pattern

Cloud analytical systems charge either for compute time or for data scanned, and query shapes skew toward compute-bound or IO-bound work. Each class tends to run cheaper under a different pricing model.

While building QueryFlux we ran the same analytical SQL across engines with different pricing models. A poor match between query shape and billing model inflated cost by **2–5×** against a better-matched engine. In a prototype that steered CPU-heavy work to compute-priced backends and scan-heavy work to byte-priced ones, total cost for one representative suite fell by up to about **56%**, and some individual queries by up to about **90%**.

That was a prototype, not a built-in cost optimizer (today you express that logic as routing rules or a Python router), but the gap it showed is structural.

Performance tells the same story from another angle. Trino is built for federation: a coordinator plans the query, schedules stages across workers, and assembles results. For a query scanning millions of rows across dozens of nodes, that overhead is well spent. For a quick filter or a small aggregation, it isn't. DuckDB runs in-process, with no network round trips and no worker coordination, and the same small query can come back in tens of milliseconds instead of a second.

:::info Prototype, not a productized router
QueryFlux ships the **primitives** — regex routers, protocol routers, Python `route()`, cluster groups, and dialect translation. You decide the policy; the proxy enforces it in one place.
:::

### Route by shape, not by hope

With QueryFlux, you express “small local queries → DuckDB, everything else → Trino” as configuration instead of application code:

<figure class="blog-figure">
  <img src="/img/blog/routing-rules-yaml.svg" alt="YAML routing rules: regex on FROM local.* to duckdb-local cluster group, plus protocol-based Trino HTTP routing" />
  <figcaption>Declarative routing in <code>config.yaml</code> — see <a href="/docs/configuration">configuration</a>.</figcaption>
</figure>

When regex isn't enough, drop to Python for the same decision:

<figure class="blog-figure">
  <img src="/img/blog/routing-rules-python.svg" alt="Python route function sending local non-JOIN queries to duckdb-local" />
  <figcaption>A custom router sees the SQL, protocol, user, and session — <a href="/docs/architecture/routing-and-clusters">routing docs</a>.</figcaption>
</figure>

The client (BI tool, notebook, or CLI) sends queries as it always has. QueryFlux picks the engine and translates the dialect. The application doesn't change.

Without one layer in front of the engines, capturing those savings is hard: there is no single place to observe queries, apply routing logic, and dispatch to the right backend. Teams end up locked into one engine, or maintaining bespoke glue that can't reason about cost or capability in one place.

## What QueryFlux is

QueryFlux is an open-source SQL proxy and router, written in Rust: **one front door**, many engines, explicit routing, automatic dialect translation, and shared capacity limits and observability.

Clients connect over the protocol they already speak:

| Front door | Typical clients |
| --- | --- |
| Trino HTTP, PostgreSQL, MySQL wire | BI, apps, ORMs |
| Snowflake HTTP, Arrow Flight SQL | Warehouses, Arrow stack |
| MCP | AI agents and assistants |

QueryFlux routes each query to a backend, translates SQL when needed, and streams results back in the client's native format. The frontend never knows which engine it is talking to; the engine adapter never knows which protocol the client used.

Native adapters cover Trino, DuckDB, StarRocks, ClickHouse, Athena, and MySQL. The Apache Arrow ADBC driver manager reaches Snowflake, BigQuery, Databricks, Redshift, PostgreSQL, SQL Server, and more. Adding an engine or client protocol means implementing one well-defined Rust trait.

## How routing works

Routing is a two-stage process:

<figure class="blog-figure">
  <img src="/img/blog/routing-pipeline.svg" alt="Pipeline: query in, routers chain, cluster group, healthy cluster selection, dialect translate, engine" />
  <figcaption>Group selection first, then cluster selection, then optional dialect translation.</figcaption>
</figure>

**Group selection.** A chain of routers inspects each query in order and returns a cluster group — a named pool of backends. The first match wins; if nothing matches, a fallback group takes the query. Routers can match on client protocol, HTTP headers, SQL patterns, client tags, the user's group, or a Python `route(query, ctx)` function.

**Cluster selection.** Within the group, a strategy picks one backend: round-robin, least-loaded, failover in priority order, engine affinity, or weighted. A cluster must be healthy, enabled, and under its concurrency limit. If none is, the query waits in line rather than being dropped or sent to an overloaded backend.

Operators express intent at two levels: which pool of engines fits a class of work, and how traffic spreads across that pool.

## Dialect translation as a first-class concern

Routing to the right engine solves only half the problem. Engines speak slightly different SQL, and expecting clients to know the dialect of whichever engine they were routed to defeats the point.

QueryFlux translates with sqlglot (30+ dialects). Translation runs after routing has picked a cluster and before the SQL reaches it: the source dialect comes from the client protocol, the target from the engine. When they match, translation is skipped. Where sqlglot doesn't cover a case, Python fixup scripts can adjust the parsed query, globally or per group.

A client writing Trino SQL can hit DuckDB or StarRocks without knowing it. The engine becomes an infrastructure decision, not an application one.

## Built to operate, not just to route

| Capability | What you get |
| --- | --- |
| **Capacity** | Per-group `maxRunningQueries`; with Postgres persistence, one limit across replicas |
| **Health** | Continuous checks; unhealthy clusters drop out and rejoin automatically |
| **Observability** | Prometheus metrics and a bundled Grafana dashboard |
| **Studio** | Live cluster health, searchable query history with routing traces, config via admin API |
| **Live config** | Routing rules and clusters reload without restarts when persistence is enabled |

## From "one copy, many reads" to "one query, many execution paths"

The lakehouse already embraced one copy of data with many readers. QueryFlux extends the idea to execution: **one query interface, many execution paths**.

Instead of asking which engine a team should use forever, or which dialect to write in, you can ask which engine fits this workload, whether you can fall back when one struggles, and how to keep application code stable while the engines underneath change.

## Why open source

If the data world moved to open table formats to escape lock-in, it makes no sense to solve compute interoperability with a proprietary black box in the middle. An open-source router is a neutral layer above the engines: a place for the community to add adapters and routing policies, and a system that stays composable instead of becoming another platform to be trapped in.

Open data deserves open compute coordination.

## The bigger opportunity: agents

Once queries can be normalized, understood, and routed across engines, you have a system that decides where work runs and how. That matters for analytics, and just as much for the AI agents and application backends that increasingly depend on live data.

Agents don't use data the way dashboards do. They explore, retry, and reformulate, generating SQL against systems they don't fully understand. If every engine stays a silo, every agent has to learn engine-specific dialects, limits, performance trade-offs, and connection logic. That is a poor foundation.

Sitting above the engines, QueryFlux gives agents one stable interface: an MCP endpoint or any SQL driver. It records which agent and conversation every query came from, and puts the same guardrails and access policies in front of agents as in front of everyone else. The agent says what it wants; QueryFlux decides where it runs.

Read next: **[Give agents SQL, not the keys to every engine](/blog/agents-sql-not-keys)**.

<div class="blog-cta">

[Get started](/docs/getting-started) · [Configuration](/docs/configuration) · [GitHub](https://github.com/lakeops-org/queryflux)

</div>

One copy of data. Many readers. One query. Many possible engines.
