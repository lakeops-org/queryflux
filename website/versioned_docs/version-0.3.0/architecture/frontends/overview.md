---
sidebar_label: Overview
title: Frontends Overview
description: QueryFlux client protocols — Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL, and Snowflake on a shared dispatch path.
image: img/queryflux-hero-banner.png
---
# Frontends

A **frontend** is the entry point for client traffic into QueryFlux. Each frontend speaks a specific wire or HTTP protocol, parses incoming SQL, builds a `SessionContext`, and hands the query to the shared dispatch layer. The client never knows which backend engine actually runs the query — the frontend translates results back into its native format before responding.

## Available frontends

| Frontend | Config key | Default port | Protocol | Dialect | Status |
|----------|------------|-------------|----------|---------|--------|
| [Trino HTTP](trino-http.md) | `trinoHttp` | 8080 | HTTP REST (JSON) | Trino | **Done** |
| [PostgreSQL wire](postgres-wire.md) | `postgresWire` | 5432 | PostgreSQL v3 wire | Postgres | **Done** |
| [MySQL wire](mysql-wire.md) | `mysqlWire` | 3306 | MySQL wire | MySQL | **Done** |
| [Arrow Flight SQL](flight-sql.md) | `flightSql` | 50051 | gRPC (Arrow Flight) | Generic | **Done** |
| [Snowflake](snowflake.md) | `snowflakeHttp` | 8443* | Snowflake HTTP wire + SQL API v2 | Snowflake | **Done** |
| ClickHouse HTTP | `clickhouseHttp` | 8123 | HTTP | ClickHouse | Planned |

\*Snowflake is optional: you must set `snowflakeHttp.port` in YAML (no implicit default). `8443` is typical; some repo examples use `8445`. Wire and SQL API v2 share this listener; routing uses `snowflakeHttp` vs `snowflakeSqlApi` — see [Snowflake](snowflake.md).

## Shared architecture

All frontends converge on the same internal pipeline. The differences are only in how SQL enters and how results leave.

```
Client  ──(native protocol)──►  Frontend Listener
                                      │
                                SessionContext + SQL
                                      │
                                      ▼
                                 Router Chain  ──► ClusterGroupName
                                      │
                                      ▼
                              ClusterGroupManager  ──► ClusterName
                                      │
                                      ▼
                              Translation Service  ──► translated SQL
                                      │
                                      ▼
                                Engine Adapter  ──► results
                                      │
                                      ▼
                                 ResultSink  ──► native protocol response
```

### Dispatch paths

The dispatch layer offers three execution paths:

| Path | Used by | Behavior |
|------|---------|----------|
| **`dispatch_query`** | Trino HTTP (async-capable groups) | Submit to engine, persist handle, return polling URL. Client follows `nextUri` to stream pages. Falls back to `execute_to_sink` when a sync adapter is selected from a mixed-engine group. |
| **`execute_to_sink` — native** | MySQL wire ↔ `MysqlWire` backend; Postgres wire ↔ `PostgresWire` backend | Wait for cluster capacity, call `execute_native` on the adapter, stream `NativeResultChunk`s directly to the sink. **Zero Arrow allocation.** |
| **`execute_to_sink` — Arrow** | All other frontend/backend combinations (including Snowflake HTTP wire and SQL API v2) | Wait for cluster capacity, call `execute_as_arrow`, stream `RecordBatch`es through a `ResultSink` that re-encodes to the native protocol response. |

#### Protocol matching (`ConnectionFormat`)

Each adapter declares the wire format it natively produces via `connection_format()`. Dispatch compares this against the incoming frontend protocol — when they match, the native (zero-serialization) path is taken; otherwise the Arrow path is used as a universal fallback.

```
Adapter declares:            Frontend needs:         Dispatch:
Arrow (ADBC/DuckDB)      ↔  FlightSql       → match  → Arrow passthrough (optimal)
MysqlWire (mysql_async)  ↔  MySqlWire       → match  → native NativeResultChunk path
PostgresWire (tokio-pg)  ↔  PostgresWire    → match  → native NativeResultChunk path
TrinoHttp                ↔  TrinoHttp       → match  → Raw bytes passthrough

MysqlWire                ↔  FlightSql       → no match → Arrow conversion
Arrow                    ↔  MySqlWire       → no match → Arrow conversion
```

The `ConnectionFormat` declaration is the only thing an adapter needs to change to opt into the native path — dispatch and frontends require no per-engine changes.

### SessionContext

Each frontend builds a `SessionContext` that travels with the query through routing and into dispatch. It is a flat struct — not protocol-specific:

| Field | Type | Description |
|-------|------|-------------|
| `user` | `Option<String>` | Resolved user identity, extracted at connection time. |
| `database` | `Option<String>` | Target database/catalog hint for routing and adapters. |
| `tags` | `QueryTags` | Query tags for routing and metrics. |
| `extra` | `HashMap<String, String>` | Protocol-specific key-value bag (headers, session params, etc.). |

Each frontend is responsible for extracting `user` and `database` from its protocol's native fields and placing remaining data into `extra`. Key conventions for `extra`: Trino/ClickHouse HTTP store lowercased header names; Postgres wire stores startup parameters; MySQL wire stores session variables; Snowflake HTTP / SQL API v2 store session and account-related fields from login or request metadata.

### Authentication

All frontends extract credentials from their protocol's native mechanism (HTTP headers, wire auth packets, gRPC metadata) and pass them to the shared `auth_provider`. The auth result determines whether the query proceeds and provides context for authorization checks downstream.

### Routing

The `FrontendProtocol` enum identifies which frontend originated a query. The `protocolBased` router uses this to map traffic from different protocols to different cluster groups:

```yaml
routers:
  - type: protocolBased
    trinoHttp: trino-default
    postgresWire: trino-default
    mysqlWire: starrocks-group
    flightSql: flight-analytics
    snowflakeHttp: trino-default
    snowflakeSqlApi: trino-default
```

### SQL dialect and translation

Each frontend has a **default source dialect** (`protocol.default_dialect()`). When the target engine's dialect differs, sqlglot translates the SQL automatically. When dialects match, translation is skipped entirely.

## Configuration

Each frontend is enabled under `queryflux.frontends` in `config.yaml`:

```yaml
queryflux:
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
    postgresWire:
      enabled: true
      port: 5432
    mysqlWire:
      enabled: true
      port: 3306
    flightSql:
      enabled: true
      port: 50051
    snowflakeHttp:
      enabled: true
      port: 8443
      sessionAffinityAcknowledged: false
```

Omitting `postgresWire`, `mysqlWire`, `flightSql`, or `snowflakeHttp` disables that listener. For any frontend block, `enabled: false` turns the listener off while keeping other settings. When `trinoHttp` is omitted from YAML, serde defaults apply (`enabled: true`, port `8080`).

See **[Snowflake](snowflake.md)** for session affinity, TTL fields, and SQL API v2 behavior.
