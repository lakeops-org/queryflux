---
title: Routing & Cluster Groups
description: Router chain, routingFallback, cluster groups, load-balancing strategies, concurrency limits, and proxy-side queueing.
image: img/queryflux-hero-banner.png
---
# Routing, cluster groups, and clusters

QueryFlux separates **where a query should go logically** (cluster **group**) from **which physical backend instance** serves it (**cluster** / adapter). This document explains that split, how routers work, and how the cluster manager balances load and enforces limits.

## Vocabulary

| Term | Meaning |
|------|--------|
| **Cluster** | A named backend instance in config (`clusters.<name>`): one engine (Trino, DuckDB, StarRocks, …), endpoint or DB path, auth, etc. At runtime it has an **engine adapter** and a **`ClusterState`** (running count, health, limits). |
| **Cluster group** | A named **pool** (`clusterGroups.<name>`) listing **member** cluster names, a **per-group** `maxRunningQueries`, optional `maxQueuedQueries`, optional **selection strategy**, and `enabled`. Routing returns a **group**; the cluster manager then picks a **member** cluster. |
| **Router** | A rule that inspects the SQL string, session context, and frontend protocol and optionally returns a target group name. |
| **Router chain** | All routers in config order, plus **`routingFallback`** when every router returns “no match.” |

## Two-stage placement

1. **Routing (group selection)**  
   `RouterChain` evaluates each `RouterTrait` implementation in order. The **first** router that returns `Some(ClusterGroupName)` wins. If all return `None`, **`routingFallback`** is used.  
   Implementation: `queryflux_routing::chain::RouterChain` (`route`, `route_with_trace`).

2. **Cluster selection (member selection)**  
   `ClusterGroupManager::acquire_cluster(group)` considers only clusters in that group that are **enabled**, **healthy**, and **under** `max_running_queries`. It then uses the group’s **strategy** to pick one member and increments that cluster’s running count.  
   If **no** member is eligible (e.g. all at capacity or unhealthy), `acquire_cluster` returns **`None`** → the query is **queued** (Trino HTTP async path) or **retried with backoff** (sync `execute_to_sink` path).  
   Implementation: `queryflux_cluster_manager::simple::SimpleClusterGroupManager`.

When the query finishes (success, failure, or cancel), **`release_cluster`** decrements the running count on that cluster.

## Router types (config → code)

Configured under `routers:` in YAML (`queryflux_core::config::RouterConfig`). Wired in `queryflux/src/main.rs`.

| `type` | Behavior |
|--------|----------|
| `protocolBased` | Maps the active frontend (`trinoHttp`, `postgresWire`, `mysqlWire`, `flightSql`, `clickhouseHttp`) to a group name. |
| `header` | Matches a header value to a group (useful for Trino HTTP and similar). |
| `queryRegex` | Ordered rules: first regex match on the SQL text wins. |
| `tags` | Routes based on query tags attached to the session. Each rule specifies one or more tag key/value conditions (AND logic); first matching rule wins. See [Tags router](#tags-router-tags) below. |
| `pythonScript` | Embedded or file-backed Python `route(query, ctx)` returning a group name or `None`. See [Python script router](#python-script-router-pythonscript) below. |
| `compound` | Multiple conditions combined with `all` (AND) or `any` (OR). Supported condition types: `protocol`, `header` (name + value), `user`, `clientTag`, `queryRegex`. |

All six router types are implemented. Unknown `type` values in config are skipped at startup with a warning.

## Tags router (`tags`)

Routes queries based on key/value tags that clients attach to their sessions. Rules are evaluated in order; the **first rule where all specified tags match** wins (AND logic within a rule).

```yaml
routers:
  - type: tags
    rules:
      - tags:
          team: eng
          env: prod
        targetGroup: prod-eng-cluster
      - tags:
          team: analytics
        targetGroup: analytics-cluster
      - tags:
          batch: null          # key-only match — any value (or no value) accepted
        targetGroup: batch-cluster
```

**Matching semantics per tag entry:**

| Config value | Matches when |
|---|---|
| `"some-value"` | Tag key is present **and** its value equals `"some-value"` |
| `null` | Tag key is present, regardless of its value (key-only match) |

**How clients attach tags** depends on the frontend protocol — see [Query tags](./query-tags) for full details on how to set tags from each frontend (Trino, MySQL wire, Postgres wire, ClickHouse HTTP).

## Cached routing config and DB reload (Postgres)

When **`persistence.type`** is **`postgres`**, routing rules and cluster/group definitions loaded from the database are held in memory inside **`LiveConfig`** (including the compiled **`RouterChain`**). Each request reads the current chain from that shared snapshot (`Arc<tokio::sync::RwLock<LiveConfig>>` in `queryflux-frontend`).

- **Periodic refresh:** `queryflux.configReloadIntervalSecs` in YAML (default **30** when omitted) controls how often a background task re-reads Postgres and replaces **`LiveConfig`** in one atomic swap. Implementation: `crates/queryflux/src/main.rs` (reload task) and `reload_live_config` → `load_routing_config`.
- **`0` disables polling only:** With **`configReloadIntervalSecs: 0`**, there is no timer-driven refresh; the in-memory config stays as loaded at startup until an **immediate refresh** runs (below).
- **Immediate refresh:** After Studio/admin API writes to routing, clusters, or groups, the proxy **notifies** the same task so a reload runs without waiting for the interval (`config_reload_notify` in `admin.rs`).
- **YAML-only mode:** With **`inMemory`** persistence there is no DB reload loop; routing comes from the process config at startup until restart.

## Python script router (`pythonScript`)

The script must define:

```python
def route(query: str, ctx: dict) -> str | None:
    ...
```

- **`query`**: SQL text (the same string the router chain sees).
- **`ctx`**: plain `dict` built by QueryFlux (string keys). **`protocol`** is always set; all other keys are protocol-agnostic.

| Key | Type | Meaning |
|-----|------|---------|
| `protocol` | `str` | One of `trinoHttp`, `postgresWire`, `mysqlWire`, `clickhouseHttp`, `flightSql`, `snowflakeHttp`, `snowflakeSqlApi` (camelCase). |
| `user` | `str \| None` | Resolved user identity. Extracted from `X-Trino-User`, Postgres startup message, MySQL handshake, etc. |
| `database` | `str \| None` | Target database/catalog hint. Extracted from `X-Trino-Catalog`, Postgres `database`, MySQL `USE`, etc. |
| `extra` | `dict[str, str]` | Protocol-specific key-value bag. For Trino/ClickHouse HTTP: lowercased header names (e.g. `x-trino-source`). For Postgres: startup parameters. For MySQL: session variables. Empty for other protocols. |
| `auth` | `dict \| None` | `{"user": str, "groups": [str, …], "roles": [str, …]}` when the request was authenticated. Raw JWT / bearer tokens are **not** passed into Python. |

Query tags are **not** injected into the Python context dict. Use the `tags` router type for tag-based routing instead.

**Example (route by user):**

```python
def route(query: str, ctx: dict) -> str | None:
    if ctx.get("user") == "batch":
        return "heavy-trino"
    return None
```

**Example (inspect a Trino HTTP header via `extra`):**

```python
def route(query: str, ctx: dict) -> str | None:
    if ctx.get("protocol") != "trinoHttp":
        return None
    source = (ctx.get("extra") or {}).get("x-trino-source")
    if source == "dbt":
        return "dbt-group"
    return None
```

## Routing trace

`route_with_trace` records each router’s decision (`matched`, optional `result`) and whether the **fallback** group was used. This supports debugging and future UI/metrics (see `RoutingTrace` in `queryflux_routing::chain`).

## Cluster group configuration (actual shape)

Clusters are defined **once** at the top level; groups **reference** them by name:

```yaml
clusters:
  trino-1:
    engine: trino
    endpoint: http://trino:8080
    enabled: true
  duckdb-1:
    engine: duckDb
    enabled: true

clusterGroups:
  trino-default:
    enabled: true
    maxRunningQueries: 100
    members: [trino-1]
    strategy:
      type: leastLoaded
  duckdb-local:
    enabled: true
    maxRunningQueries: 4
    members: [duckdb-1]
```

Notes:

- **`maxRunningQueries`** on the group applies to **each** member cluster’s `ClusterState` when those states are built (see `main.rs` pass 2). It is the cap used for **acquire** / capacity checks.
- **`members`** can list multiple clusters, including **mixed engines** (e.g. Trino and DuckDB in one group). For that, **`engineAffinity`** (or another strategy) helps express preference order across engine types (`queryflux_cluster_manager::strategy`).

## Selection strategies

Configured as `strategy: { type: ... }` on a group. Implemented in `strategy.rs`:

| Strategy | Behavior |
|----------|----------|
| `roundRobin` | Rotates among eligible members (default when strategy omitted). |
| `leastLoaded` | Picks the member with the smallest `running_queries`. |
| `failover` | First eligible member in **member list order** (priority ordering in YAML). |
| `engineAffinity` | Ordered engine preference; within each engine, least loaded. |
| `weighted` | Distributes by configured weights (deterministic pseudo-random from load). |

Eligible candidates are always **healthy**, **enabled**, and **not at capacity** before the strategy runs.

## Health and runtime updates

Each `ClusterState` tracks health (`is_healthy`), updated by background health checks in the main binary. Unhealthy clusters are excluded from acquisition.

The `ClusterGroupManager` trait also supports **`update_cluster`** (enable/disable, change `max_running_queries`) for admin-driven changes.

## Frontend dispatch: async vs sync

After a group is chosen, the Trino HTTP handler (`post_statement`) branches:

- If the group is considered **async-capable** (e.g. Trino-style polling), it uses **`dispatch_query`**: acquire cluster → translate → `submit_query` → persist executing state → rewrite `nextUri` to point back at QueryFlux.
- Otherwise it uses **`execute_to_sink`**: wait for capacity (backoff loop), translate, stream Arrow batches, and synthesize a Trino-compatible JSON response.

So **routing and cluster selection** are shared concepts; the **result delivery** shape depends on engine and frontend capabilities.

## Mental model

- **Routers** answer: *which pool (group) should handle this query?*
- **Cluster manager + strategy** answer: *which replica/instance in that pool?*
- **Translation** (separate doc) then aligns SQL with that instance’s engine.

See [system-map.md](system-map.md) for the full component diagram and [query-translation.md](query-translation.md) for dialect conversion details.
