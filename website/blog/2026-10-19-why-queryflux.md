---
slug: why-queryflux
title: "Why we built QueryFlux: the missing interoperability layer for query engines"
description: Open table formats made data interoperable. Query engines are still islands. Why QueryFlux exists, and what a neutral layer above the engines makes possible.
authors: [amit]
tags: [queryflux, lakehouse, routing, open-source]
image: img/queryflux-hero-banner.png
---

For the last few years, the data lake world has been quietly fixing one of data infrastructure's oldest problems: lock-in.

Apache Iceberg and the broader open table format movement changed the contract between storage and compute. Data stopped belonging to a single engine. Instead of copying the same dataset into five systems to support five workloads, teams keep one copy and let many engines read it: Spark for batch, Trino for interactive SQL, DuckDB for local analysis, StarRocks for serving. One dataset, many readers.

But even after all that progress, a gap remains. **Data became interoperable. Query engines did not.**

<!-- truncate -->

## We solved storage interoperability. Compute is next.

Most teams still treat query engines as silos, and they pay for it in ways that compound quietly.

Every engine speaks its own wire protocol, has its own SQL dialect quirks, and needs its own connection management. Multiply engines by clients (BI tools, notebooks, application code, CLIs) and you don't get a linear integration problem; you get a combinatorial one. Each pairing needs its own driver, dialect handling and retry logic. Routing, rate limiting and observability get reinvented in isolation, over and over.

Cost makes it worse. Cloud analytical systems charge either for compute time or for data scanned, and query shapes skew toward compute-bound or IO-bound work. Each class tends to run cheaper under a different pricing model. While building QueryFlux we ran the same analytical SQL across engines with different pricing models, and a poor match between query shape and billing model inflated cost by 2–5× against a better-matched engine. In a prototype that steered CPU-heavy work to compute-priced backends and scan-heavy work to byte-priced ones, total cost for one representative suite fell by up to about 56%, and some individual queries by up to about 90%. That was a prototype, not a built-in router (today you write that logic as routing rules or a Python router), but the gap it showed is structural.

Performance tells the same story from another angle. Trino is built for federation: a coordinator plans the query, schedules stages across workers and assembles results. For a query scanning millions of rows across dozens of nodes, that overhead is well spent. For a quick filter or a small aggregation, it isn't. DuckDB runs in-process, with no network round trips and no worker coordination, and the same small query can come back in tens of milliseconds instead of a second.

With QueryFlux, you express that as a routing rule:

```yaml
routers:
  - type: queryRegex
    rules:
      - regex: "FROM local\\..*"
        targetGroup: duckdb-local
  - type: protocolBased
    trinoHttp: trino-default
```

Or with more nuance, in Python:

```python
def route(query: str, ctx: dict) -> str | None:
    # Small, local queries go to DuckDB
    if "local." in query and "JOIN" not in query.upper():
        return "duckdb-local"
    return None
```

The client (a BI tool, a notebook, a CLI) sends queries as it always has. QueryFlux decides which engine runs them and translates the dialect. The application doesn't change.

Without one layer in front of the engines, capturing those savings is hard: there is no single place to observe queries, apply routing logic and dispatch to the right engine. Teams end up locked into one engine, or maintaining bespoke glue that can't reason about cost or capability in one place.

## What QueryFlux is

QueryFlux is an open-source SQL proxy and router, written in Rust: one front door, many engines, explicit routing, automatic dialect translation, and shared capacity limits and observability.

Clients connect over the protocol they already speak: Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake, Arrow Flight SQL, or MCP for AI agents. QueryFlux routes each query to a backend, translates the SQL when needed, and streams results back in the client's native format. The frontend never knows which engine it is talking to; the engine adapter never knows which protocol the client used.

QueryFlux ships native adapters for Trino, DuckDB, StarRocks, ClickHouse, Athena and MySQL, and integrates the Apache Arrow ADBC driver manager, so the same proxy reaches Snowflake, BigQuery, Databricks, Redshift, PostgreSQL, SQL Server and more. Adding an engine or a client protocol means implementing one well-defined Rust trait.

## How routing works

Routing is a two-stage process.

**Group selection.** A chain of routers looks at each query in order and returns a cluster group, a named pool of backends. The first match wins; if nothing matches, a fallback group takes the query. Routers match on the client protocol, HTTP headers, SQL patterns, client tags, the user's group, or a Python `route(query, ctx)` function that sees the SQL, protocol, user and session.

**Cluster selection.** Within the group, a strategy picks one backend: round-robin, least-loaded, failover in priority order, engine affinity, or weighted. A cluster must be healthy, enabled and under its concurrency limit. If none is, the query waits in line rather than being dropped or sent to an overloaded backend.

Operators express intent at two levels: which pool of engines fits a class of work, and how traffic spreads across that pool.

## Dialect translation as a first-class concern

Routing to the right engine solves only half the problem. Engines speak slightly different SQL, and expecting clients to know the dialect of whichever engine they were routed to defeats the point.

QueryFlux translates with sqlglot (30+ dialects). Translation runs after routing has picked a cluster and before the SQL reaches it: the source dialect comes from the client protocol, the target from the engine. When they match, translation is skipped. Where sqlglot doesn't cover a case, Python fixup scripts can adjust the parsed query, globally or per group.

A client writing Trino SQL can hit DuckDB or StarRocks without knowing it. The engine becomes an infrastructure decision, not an application one.

## Built to operate, not just to route

- **Capacity.** Each group has a `maxRunningQueries` limit. With Postgres persistence it holds as one limit across every replica and worker, and queries that have to wait are served first come, first served.
- **Health.** Health checks run continuously; unhealthy clusters drop out of routing and come back on their own.
- **Observability.** Prometheus metrics for queries, latency, translation, and running and queued counts, with a Grafana dashboard that ships with the project.
- **Studio.** A management UI with live cluster health, searchable query history with routing traces, per-engine stats, and configuration, all on the same admin API.
- **Config without restarts.** With Postgres persistence, routing rules and clusters reload live.

## From "one copy, many reads" to "one query, many execution paths"

The lakehouse already embraced one copy of data with many readers. QueryFlux extends the idea to execution: one query interface, many execution paths.

Instead of asking which engine a team should use forever, or which dialect to write in, you can ask which engine fits this workload, whether you can fall back when one struggles, and how to keep application code stable while the engines underneath change.

## Why open source

If the data world moved to open table formats to escape lock-in, it makes no sense to solve compute interoperability with a proprietary black box in the middle. An open-source router is a neutral layer above the engines: a place for the community to add adapters and routing policies, and a system that stays composable instead of becoming another platform to be trapped in.

Open data deserves open compute coordination.

## The bigger opportunity: agents

Once queries can be normalized, understood and routed across engines, you have a system that decides where work runs and how. That matters for analytics, and just as much for the AI agents and application backends that increasingly depend on live data.

Agents don't use data the way dashboards do. They explore, retry and reformulate, generating SQL against systems they don't fully understand. If every engine stays a silo, every agent has to learn engine-specific dialects, limits, performance trade-offs and connection logic. That is a poor foundation.

Sitting above the engines, QueryFlux gives agents one stable interface: an MCP endpoint or any SQL driver. It records which agent and conversation every query came from, and puts the same guardrails and access policies in front of agents as in front of everyone else. The agent says what it wants; QueryFlux decides where it runs. More on that in [Give agents SQL, not the keys to every engine](/blog/agents-sql-not-keys).

The first wave of the modern data lake was about open storage. The second is about open execution. QueryFlux doesn't ask the world to standardize on one more engine; it is a layer that helps engines cooperate.

The future isn't one engine to rule them all. It is engines that specialize, data that stays open, and routing that gets smarter.

One copy of data. Many readers. One query. Many possible engines.

[Get started](/docs/getting-started) · [GitHub](https://github.com/lakeops-org/queryflux)
