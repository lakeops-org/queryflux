---
title: Motivation & Goals
description: Why QueryFlux exists — fragmented engines, cost-aware routing, queueing at the proxy, and compute interoperability for lakehouses.
image: img/queryflux-hero-banner.png
---
# Motivation and goals

## Why QueryFlux exists

### The fragmented engine landscape

Modern data stacks are **fragmented by design**. Different engines exist because different problems demand different trade-offs:

- **Trino** for federated queries across heterogeneous sources
- **DuckDB** for lightweight embedded analytics or edge compute
- **StarRocks** and **ClickHouse** for high-throughput, low-latency serving layers
- **Athena** for serverless, pay-per-scan workloads on S3

Using the right engine for the right job is good engineering. But that fragmentation has a cost that compounds quietly. Every engine speaks its own wire protocol, has its own SQL dialect quirks, and needs its own connection management. When you multiply engines by clients — BI tools, notebooks, application code, CLI tools — you do not get a linear integration problem; you get a **combinatorial** one (**N×M**). Each pairing needs its own driver, dialect handling, and retry logic. Operational concerns like routing, rate limiting, and observability get reinvented in isolation, over and over.

---

### Open table formats solved storage — and created a new problem

The data industry spent years solving a different fragmentation problem: **every engine had its own storage format**. Hive tables for Spark, proprietary formats for Redshift, separate copies for each consumer. The cost was enormous — data duplication, ETL pipelines that existed purely to move data between systems, and freshness gaps as copies fell out of sync.

**Apache Iceberg, Delta Lake, and Apache Hudi solved this.** By separating the table format from the compute engine, they made it possible for multiple engines to read and write the same data in object storage — one copy, many readers. Point Trino, Spark, DuckDB, StarRocks, and Flink at the same Iceberg table and they all see consistent, up-to-date data. No ETL pipeline to keep them in sync. No data duplication. The storage interoperability problem, effectively solved.

This was a major architectural shift. It meant organizations could — and did — adopt multiple engines without guilt. Trino for federated SQL, Spark for heavy transformation, StarRocks for dashboard serving, DuckDB for embedded analytics: all legitimate, all reading the same data lake. The open table format layer removed the forcing function to consolidate onto one engine.

**But it created a new problem one layer up.**

With storage interoperability solved, the remaining fragmentation moved to **compute access**. The engines that can all read the same Iceberg table still speak different wire protocols, different SQL dialects, and have different operational characteristics. Clients — BI tools, notebooks, application code — still need to know which engine to connect to, configure the right driver, and speak that engine's dialect.

```
         ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
Clients  │BI tool   │  │Notebook  │  │App code  │  │CLI tool  │
         └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
              │ Trino HTTP  │ JDBC         │ psql         │ mysql
              ▼             ▼              ▼              ▼
         ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
Engines  │  Trino   │  │StarRocks │  │ DuckDB   │  │ Athena   │
         └──────────┘  └──────────┘  └──────────┘  └──────────┘
              │              │              │              │
              └──────────────┴──────────────┴──────────────┘
                         Apache Iceberg / Delta / Hudi
                         (same data, one copy in S3/GCS/ADLS)
```

The N×M client-to-engine wiring is still there. Dialect differences are still there. The decision of which engine handles which query — and all the performance, cost, and capacity considerations that decision implies — still happens at the edges, inconsistently, in application code or manual convention.

**Open table formats made multi-engine access possible. They did not make it manageable.**

QueryFlux is the missing layer above the table format: **compute interoperability**. It gives every client one endpoint regardless of what engine sits behind it, routes each query to the right engine based on explicit rules, translates dialect in flight, and manages capacity across the fleet. Iceberg unified the data; QueryFlux unifies the access.

---

### The emerging interoperability stack

The industry recognized the compute access problem and several projects are converging on it from different angles. Each solves a real piece of the puzzle — and understanding where they stop is the clearest way to understand what QueryFlux adds.

#### sqlglot — dialect translation

[sqlglot](https://github.com/tobymao/sqlglot) is a pure-Python SQL parser and transpiler supporting 31+ dialects. Given a SQL string written in Trino dialect, it produces the equivalent in DuckDB, StarRocks, Spark, or BigQuery SQL. It handles the hardest part of dialect bridging — syntax differences, function name mismatches, type coercions — without requiring a running engine.

QueryFlux uses sqlglot as its translation engine (via PyO3). But sqlglot on its own is a library, not a proxy: it has no concept of wire protocols, no routing, no capacity management. A BI tool cannot connect to sqlglot. It is a component in the interoperability stack, not the stack itself.

#### ibis — portable analytics at the Python API layer

[ibis](https://ibis-project.org) solves portability at a higher level of abstraction. Instead of writing SQL, you write Python expressions against an ibis table object, and ibis compiles them to the correct SQL for whatever backend you target — DuckDB, BigQuery, Trino, Snowflake, Spark, and more. Change the backend, the query compiles differently; your Python code does not change.

ibis is the right answer for Python-native analytics workflows: notebooks, data science pipelines, ad-hoc exploration. It is genuinely complementary to QueryFlux. An ibis-generated query submitted through QueryFlux is just SQL — QueryFlux routes it, translates it if needed, and manages capacity, while ibis handled the authoring-time portability. Where ibis stops: it is Python-only, and it operates at authoring time. It does not help JDBC applications, BI tools, CLI clients, or any non-Python consumer that already emits SQL. And it does not route, queue, or observe — it produces a query string and hands it off.

#### Substrait — engine-agnostic query plans

[Substrait](https://substrait.io) takes the most ambitious approach: rather than translating SQL text between dialects, it defines a **universal serialization format for query plans** (protobuf). A query is parsed once into a Substrait plan tree — a logical representation of projections, filters, joins, aggregations — and any engine that speaks Substrait can execute it natively, without dialect translation.

If every engine consumed Substrait natively, SQL dialect differences would become irrelevant at the plan level. Substrait is still early in adoption — support varies significantly across engines — and most clients in production today produce SQL text, not plan trees. But the trajectory is meaningful: Arrow, DuckDB, Velox, and others are building Substrait support, and it represents where the industry may land long-term.

QueryFlux is built to operate on SQL text today and could incorporate Substrait as an alternative translation path as engine support matures — parse SQL → Substrait plan → serialize to target engine's native format, bypassing the dialect translation step entirely for engines that support it.

#### How the layers fit together

These are not competing projects. They solve the interoperability problem at different levels of abstraction and for different audiences:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                            │
│  BI tools · JDBC apps · psql/mysql CLI · notebooks · scripts   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ibis (Python API → SQL, authoring-time portability)
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                       QueryFlux                                 │
│  Protocol translation · Routing · Capacity · Observability      │
│  sqlglot (dialect translation) · Substrait (future path)        │
└────────────────────────┬────────────────────────────────────────┘
                         │
        ┌────────────────┼──────────────────┐
        ▼                ▼                  ▼
     Trino           StarRocks           Athena  …
        └────────────────┴──────────────────┘
                Apache Iceberg / Delta / Hudi
                (same data, one copy in object storage)
```

- **Iceberg / Delta / Hudi** — storage interoperability: one data copy, many engines.
- **sqlglot** — dialect translation: SQL text in one dialect, SQL text out in another.
- **ibis** — authoring-time portability: Python expressions that compile to any engine's SQL.
- **Substrait** — plan-level portability: engine-agnostic logical query representation.
- **QueryFlux** — runtime infrastructure: protocol translation, routing, capacity management, and observability across the fleet.

Each layer is necessary; none of them replaces the others. A stack with Iceberg but no QueryFlux still has the N×M client-to-engine problem. A stack with ibis but no QueryFlux still has BI tools and JDBC apps connecting directly to individual engines. A stack with sqlglot but no QueryFlux still has no place to make routing decisions or enforce concurrency limits. QueryFlux is the runtime coordination layer that makes the rest of the stack coherent in production.

---

### The cluster allocation problem

Beyond the N×M integration complexity, there is a deeper structural issue in how organizations provision and use query infrastructure.

**Clusters are typically assigned by team or project.** The analytics team gets a Trino cluster. The product BI team gets a dedicated Snowflake warehouse. The ML platform team gets a StarRocks instance. Each cluster is sized at procurement time to handle that team's _peak_ load — a burst that may happen a few hours per day at most.

The result is predictable: **most clusters run significantly underloaded most of the time.**

- A Snowflake warehouse billed by the second sits idle at night and on weekends but remains provisioned because someone needs it first thing Monday.
- A dedicated Trino cluster sized for a Friday-evening report run idles at 10% utilization the rest of the week.
- A StarRocks cluster provisioned for dashboard traffic has headroom that could absorb ad-hoc queries — but no mechanism to steer them there.

**Provisioned capacity that cannot be shared is wasted capacity.** And wasted capacity in cloud infrastructure translates directly into avoidable cost, often without anyone on the team noticing because the cluster "works fine."

The problem compounds when you add **team boundaries**: even if the platform team can see that cluster A is idle and cluster B is saturated, there is no routing layer to redirect traffic from B to A without application-level changes or manual intervention.

---

### Cost: the wrong engine for the workload

Cloud analytical systems charge in fundamentally different ways:

- **Compute-time pricing** (Trino clusters, Snowflake warehouses, StarRocks): you pay for cluster uptime or CPU-seconds, regardless of data scanned.
- **Scan-based pricing** (Athena, BigQuery on-demand): you pay for bytes read, regardless of cluster size.

Query shapes are not uniform. **CPU-bound queries** (heavy joins, window functions, aggregations over pre-loaded hot data) run cheapest on compute-priced backends where you pay for capacity, not scan volume. **IO-bound queries** (selective filters over large cold datasets in object storage, exploration queries) often run cheapest on scan-priced backends where idle compute costs nothing.

Without a routing layer, the default is to send every query to the same engine regardless of its shape or the billing model it maps to. In our own benchmarking and cost modeling while building QueryFlux — running the same analytical SQL across engines with different pricing models — we repeatedly saw a **poor match** between query shape and billing model inflate cost by large factors (on the order of **2–5×** versus a better-matched engine in those runs). When we prototyped **workload-aware routing**, steering CPU-skewed work toward compute-priced backends and scan-heavy work toward byte-priced backends, **total workload cost** in one representative suite fell by up to about **56%**, and **individual queries** sometimes dropped by up to about **90%** compared with always using a single default. That gap is not a rounding error — it is a structural inefficiency in any stack that treats every query the same.

---

### Performance: the wrong engine for the access pattern

Cost is only half the story. **Performance** is the other half — and the two often pull in different directions.

#### Latency-sensitive vs. throughput-oriented workloads

Not all queries are created equal. Two queries may return the same result but have very different requirements:

| Workload | Example | Priority | Right engine |
|----------|---------|----------|-------------|
| Interactive dashboard | BI refresh, 200ms SLA | **Low latency** | StarRocks, ClickHouse — columnar store, pre-warmed cache |
| Ad-hoc exploration | Analyst browsing cold data | **Moderate latency, low cost** | Athena, Trino |
| Scheduled batch ETL | Nightly aggregation into a mart | **Throughput, not latency** | Trino, DuckDB |
| Embedded analytics | Per-user query in an app | **Low latency, isolated** | DuckDB (in-process) |

Without routing, all of these go to the same endpoint. A batch ETL job competing with a dashboard refresh on the same Trino cluster degrades both — the ETL saturates the cluster, the dashboard misses its SLA. A DuckDB instance that could answer a self-service query in 10 ms sits idle while that query waits in the Trino queue.

#### Routing for performance, not just cost

QueryFlux lets you express performance intent in routing rules without changing client code:

- **Protocol-based**: direct all PostgreSQL wire clients (typically interactive tooling) to a low-latency StarRocks group; Trino HTTP clients (typically scheduled jobs) to the Trino cluster.
- **Client tags**: Trino's `X-Trino-Client-Tags` header lets callers declare intent (`priority:interactive`, `workload:etl`) which routing rules can act on.
- **Regex on SQL**: detect common patterns (`LIMIT 1`, `SELECT *` exploration, `INSERT INTO`) and steer accordingly.
- **Python script**: arbitrary logic — time of day, user identity, query cost estimate, current cluster load — can all factor into routing.
- **Compound rules**: combine conditions with AND/OR, e.g. "user is a dashboard service account AND protocol is MySQL wire → low-latency pool."

The key insight is that **the routing layer is the right place for this logic.** It sees every query, has session context (user, protocol, headers), and can act before any backend commits resources. That is not something individual clients or schedulers can do without coordination.

---

### The queue problem

Every query engine has an internal concurrency limit. When that limit is hit, the engine queues, rejects, or degrades incoming requests. The behavior is engine-specific, poorly observable from the client side, and completely invisible across engines.

**Without a proxy:**

- A client submitting to a saturated Trino cluster either sees a slow queue it cannot introspect, or gets a capacity error it must handle with custom retry logic.
- There is no mechanism to redirect overflow to a healthier cluster or a different engine.
- Teams instrument per-engine queue depth separately, if at all.
- A client sending to the wrong cluster when a better one is available has no way to know.

**With QueryFlux:**

1. **Per-group concurrency limits** (`maxRunningQueries` on a cluster group) act as a controlled throttle _before_ requests reach the backend. QueryFlux enforces these limits uniformly across all clients and protocols.

2. **QueryFlux-side queuing**: when a group is at capacity, queries wait in QueryFlux's own queue rather than hammering the backend. The Trino HTTP async path suspends the client poll loop transparently; sync paths (DuckDB, StarRocks) retry with backoff. In both cases, the client sees standard protocol behavior — not an error.

3. **Spillover routing**: when one group is at capacity, compound or fallback rules can redirect overflow to a secondary group — e.g., spill ad-hoc queries from a saturated Trino cluster to a less-loaded DuckDB pool or a scan-priced Athena group.

4. **Centralized queue visibility**: `queryflux_queued_queries` (Gauge, per group) and `queryflux_running_queries` (Gauge, per cluster) are Prometheus metrics. You get a single pane of glass across all cluster groups instead of piecing together engine-specific UIs.

5. **Health-aware cluster selection**: unhealthy or disabled clusters are excluded from selection. Within a group, the `leastLoaded` strategy picks the member with the fewest in-flight queries; `failover` respects priority order. Capacity is never sent to a degraded backend.

The practical effect is that **QueryFlux absorbs burst pressure** at a controlled point, makes it observable, and can spill intelligently rather than letting it cascade to the engines.

---

### Bringing it together: one proxy, explicit intent

**QueryFlux** was created to cut through these compounding problems. Instead of solving the N×M integration problem at the edges, it introduces **one proxy** that clients talk to over a familiar protocol. Behind it, QueryFlux handles:

- **Protocol translation** — Trino HTTP, PostgreSQL wire, MySQL wire, Arrow Flight SQL: clients connect with what they already know.
- **Intelligent routing** — configurable rules (protocol, headers, regex, client tags, Python scripts, compound conditions) steer each query to the right cluster group.
- **SQL dialect normalization** — queries are automatically translated to the target engine's dialect via [sqlglot](https://github.com/tobymao/sqlglot) when needed.
- **Capacity management** — per-group concurrency limits, queueing when full, health-aware selection, and configurable load-balancing strategies.
- **Observability** — Prometheus metrics, Grafana dashboards, and an admin REST API give a unified view across all engines and groups.
- **Dynamic config** — routing rules and cluster groups can be updated at runtime via API (with Postgres persistence) without restarting the proxy.

That same layer is where **performance- and cost-aware routing** lives: matching queries to engines by their workload shape, their SLA requirements, their billing model fit, and the current load across the fleet — in one place, without touching client code.

The result is a **uniform client experience** — one URL, shared tooling, consistent behavior — without forcing consolidation onto a single engine, or leaving performance and money on the table by ignoring how differently queries and engines behave.

---

## Goals

QueryFlux aims to:

1. **Speak the client's protocol** — Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight SQL are all implemented; clients need no changes.
2. **Route each query to the right backend pool** — using configurable, traceable rules instead of hard-coding one engine per deployment.
3. **Translate SQL when dialects differ** — so clients can target any routed engine regardless of dialect.
4. **Operate like a serious proxy** — per-group concurrency limits, queueing, health-aware selection, retry, Prometheus metrics, and HA-ready state.
5. **Make routing intent explicit and observable** — routing decisions are traceable (`RoutingTrace`), metrics are exposed, and the admin API reflects live cluster state.

---

## Compared to Trino-only gateways

Some gateways optimize for **load-balanced Trino** behind one client protocol. QueryFlux targets **heterogeneous** deployments:

- **Multiple engine types** in one deployment (Trino, DuckDB, StarRocks, Athena, …).
- **Protocol choices at the edge** — not only Trino HTTP.
- **SQL dialect translation** when the routed engine's SQL differs from what the client naturally speaks.
- **Cross-engine capacity management** — overflow, spillover, and queue visibility across engine types, not just within one cluster.

---

## What success looks like for operators

- **Predictable routing**: rules are data (YAML or DB-backed config), ordered and traceable — no hidden logic.
- **Controlled blast radius**: groups cap concurrent queries; full groups queue at the proxy instead of overwhelming backends.
- **Observable behavior**: a single metrics surface for group/cluster load, queue depth, latency percentiles, and translation rates — across all engines.
- **Cost and performance leverage**: the routing layer is the right home for workload-aware dispatch; operators can encode intent once and apply it to all clients.
- **Incremental adoption**: start with a single group (e.g. one Trino pool) and add engines and routers as needs grow — no flag day required.

For the mechanics of routing configuration and cluster groups, see [routing-and-clusters.md](routing-and-clusters.md). For translation details, see [query-translation.md](query-translation.md).
