---
sidebar_position: 7
description: QueryFlux project roadmap — shipped features, in-progress work, and planned additions for routing, auth, and engine support.
---

# Roadmap

QueryFlux is under active development. This page tracks what is shipped, what is in progress, and where the project is headed.

---

## What is done

Everything below is implemented and available on the `main` branch.

| Area | Feature |
|------|---------|
| **Frontends** | Trino HTTP (port 8080) |
| | PostgreSQL wire protocol (port 5432) |
| | MySQL wire protocol (port 3306) |
| | Arrow Flight SQL (gRPC) |
| | Snowflake HTTP wire + SQL API v2 (configurable port) |
| | Admin REST API + OpenAPI / Swagger UI (port 9000) |
| **Backends** | Trino — async HTTP polling, transparent `nextUri` proxying |
| | DuckDB — embedded, in-process, Arrow result sets |
| | StarRocks — MySQL wire, sync Arrow path |
| | Athena — AWS SDK async, `StartQueryExecution` → `GetQueryResults` |
| | ADBC — generic Arrow Database Connectivity driver (Trino, DuckDB, and more) |
| **Routing** | `protocolBased`, `header`, `queryRegex`, `tags`, `pythonScript`, `compound` routers |
| | Router chain with ordered evaluation and `routingFallback` |
| | `route_with_trace` for per-request routing debug traces |
| **Cluster management** | Per-group concurrency limits (`maxRunningQueries`) |
| | Proxy-side queueing when groups are at capacity |
| | Load balancing strategies: `roundRobin`, `leastLoaded`, `failover`, `engineAffinity`, `weighted` |
| | Health-aware cluster selection and background health checks |
| **Translation** | Dialect-only translation via sqlglot (31+ dialects, PyO3) |
| | Graceful degradation when sqlglot is unavailable |
| **Persistence** | In-memory store (`DashMap`) — single-instance, zero config |
| | PostgreSQL store (JSONB) — production HA, shared state across replicas |
| | SQL migrations via sqlx |
| **Auth** | Authentication providers: none, static, OIDC, LDAP |
| | Authorization: allow-all, simple policy, OpenFGA |
| | Backend identity resolution (`BackendIdentityResolver`) |
| **Observability** | Prometheus metrics: queries, duration, translation, running, queued |
| | Grafana dashboard (auto-provisioned) |
| | QueryFlux Studio — Next.js UI: clusters, query history, engine registry |
| | Buffered + multi-store metrics pipeline |
| **Ops** | Dynamic config reload from Postgres (configurable interval + immediate on write) |
| | Per-example Docker Compose stacks (`minimal-trino`, `minimal-inmemory`, `quickstart`, `with-prometheus-grafana`, `full-stack`) |
| | Proxy overhead benchmarks (`queryflux-bench`): ~0.35 ms p50 added latency |

---

## Near-term (P2)

These are the next items actively being worked on or immediately queued.

### Schema-aware SQL translation

Today's translation is **dialect-only**: `sqlglot.transpile(sql, read=src, write=tgt)`. It handles syntax differences but cannot resolve semantic gaps that require knowing the target schema — e.g. resolving ambiguous column references, pushing down predicates, or rewriting unsupported functions against actual table definitions.

The plan: wire `SchemaContext` (populated from catalog discovery via `EngineAdapterTrait::list_tables` / `describe_table`) into the dispatch path so the translator can call `sqlglot.optimizer.optimize` with a `MappingSchema`. Dialect-only remains the fallback when the schema is unavailable or optimization fails.

### ClickHouse backend + HTTP frontend

ClickHouse is a natural fit for the QueryFlux engine set — high-throughput columnar OLAP, HTTP-native protocol, wide adoption alongside Trino and StarRocks in modern lake deployments. The plan:

- **Backend adapter**: ClickHouse HTTP protocol, sync Arrow path (similar to StarRocks).
- **Frontend**: ClickHouse HTTP interface so native ClickHouse clients can connect to QueryFlux without any driver change.
- **Dialect translation**: Trino → ClickHouse SQL via sqlglot.

### Routing telemetry in Studio

Today routing traces (`RoutingTrace`) are available in logs and will surface in the Studio **Queries** page — showing which router matched, which group was selected, and whether the fallback was used. This closes the gap between "routing is configured" and "routing is observable."

---

## Medium-term (P3)

### Cost- and performance-aware routing

The motivation doc describes the gap: without a routing layer that understands workload shape, queries are sent to the wrong engine for their cost profile or latency requirements. The plan is to expose first-class routing inputs that encode this:

- **Query complexity signals** available to the `pythonScript` router (estimated scan size, presence of joins, result LIMIT).
- **Cluster load as a routing input** — not just for cluster selection within a group, but for group selection itself. A `leastLoadedGroup` router type that routes to the group with the most available capacity.
- **Time-based routing** — route to cheaper scan-priced backends (Athena) during off-peak hours; reserve compute-priced clusters (StarRocks) for peak interactive traffic.
- **Cost annotations on groups** — tag groups with a cost tier (`interactive`, `batch`, `serverless`) so routing rules can reference intent rather than engine names.

### Snowflake backend

Snowflake is one of the most common analytical systems in enterprise data stacks. Adding a Snowflake adapter would let organizations route overflow or exploratory traffic to Snowflake from the same QueryFlux proxy that serves Trino and StarRocks — without clients changing connection strings.

Protocol: Snowflake's HTTP API or JDBC-compatible interface. Auth: key-pair or OAuth.

### BigQuery backend

BigQuery on-demand pricing (bytes scanned) maps cleanly to the scan-priced routing tier described in the cost-aware routing section. A BigQuery adapter enables the pattern: route selective, cold-data exploration queries to BigQuery; route pre-aggregated, hot-data dashboard queries to StarRocks.

### Redis persistence tier

The current persistence options are in-memory (single instance) and PostgreSQL (HA). Redis is the natural middle ground: low-latency shared state for multi-replica QueryFlux deployments where full Postgres isn't warranted. Planned scope: in-flight query state and live cluster state; routing config would still be Postgres-backed.

---

## Longer-term

### Federated query planning

When a query joins data that spans two engines — e.g. a Trino-managed Iceberg table and a StarRocks pre-aggregated mart — QueryFlux today routes the whole query to one engine. A federated planner would decompose the query, dispatch sub-queries to each appropriate engine in parallel, and merge results. This is a significant undertaking (Trino already does this for heterogeneous catalogs; QueryFlux would do it across engine types).

### Query result caching

A cache layer in the proxy that intercepts repeated identical queries (same SQL, same session parameters) and serves results from a short-lived store (Redis or in-process). Particularly useful for dashboard refresh patterns where dozens of users hit the same aggregation query within the same time window. Cache invalidation would be TTL-based initially, with Iceberg snapshot-aware invalidation on the roadmap.

### ML-driven routing

The `pythonScript` router already allows arbitrary routing logic. The longer-term vision is a feedback loop: QueryFlux records actual query duration and resource cost per engine per query shape (query history is already in Postgres). A lightweight model trained on that history could predict, for a new query, which engine will be fastest or cheapest — and the routing layer acts on that prediction. Initial form: a simple decision tree or lookup table; eventual form: a continuously updated model served alongside the proxy.

### Query budget enforcement

Per-user or per-team spending caps enforced at the proxy: track estimated or actual query cost in Postgres, reject or downgrade (route to a cheaper engine) when a team approaches its budget. Particularly relevant for scan-priced backends where a single bad query can incur significant cost.

---

## How priorities are set

The roadmap reflects:

1. **What unblocks the most deployments** — schema-aware translation and ClickHouse cover the most common next integration requests.
2. **What delivers the cost/performance story end-to-end** — cost-aware routing closes the loop between the motivation (wrong engine for the workload) and the solution (QueryFlux routes it correctly).
3. **What the open table format ecosystem needs** — Snowflake and BigQuery backends, federated planning, and cache complete the "compute interoperability" layer above Iceberg/Delta/Hudi.

Contributions are welcome — see [Contributing](/docs/contribute). If a feature here is blocking your use case, open an issue on [GitHub](https://github.com/lakeops-org/queryflux/issues) to help prioritize.
