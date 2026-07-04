---
sidebar_position: 1
sidebar_label: YAML reference
title: YAML Configuration Reference
description: Complete config.yaml reference — frontends, cluster groups, routing rules, persistence, translation, and admin API settings.
image: img/queryflux-hero-banner.png
---
# Configuration

Copy `config.example.yaml` in the repository root and adjust for your environment.

```yaml
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
    snowflakeHttp:
      enabled: true
      port: 8445
  persistence:
    type: inMemory  # or: postgres

clusters:
  trino-1:
    engine: trino
    endpoint: http://trino-host:8080
    enabled: true
    auth:
      type: basic
      username: user
      password: pass
  duckdb-1:
    engine: duckDb
    enabled: true
    databasePath: /tmp/queryflux.duckdb

clusterGroups:
  trino-default:
    maxRunningQueries: 100
    members: [trino-1]
  duckdb-local:
    maxRunningQueries: 4
    members: [duckdb-1]

routers:
  - type: protocolBased
    trinoHttp: trino-default
    snowflakeHttp: trino-default
    snowflakeSqlApi: trino-default

  - type: header
    headerName: x-target-engine
    headerValueToGroup:
      duckdb: duckdb-local

routingFallback: trino-default
```

## Admin API

```yaml
queryflux:
  adminApi:
    port: 9000            # Admin REST API + Studio proxy port (default: 9000)
    username: admin       # Bootstrap admin username — see note below (default: admin)
    password: admin       # Bootstrap admin password — see note below (default: admin)
```

`username` and `password` are the **bootstrap** credentials used on first boot. After you change the password through Studio's Security page, the new bcrypt hash is stored in Postgres and the YAML values are ignored.

Environment variables `QUERYFLUX_ADMIN_USER` and `QUERYFLUX_ADMIN_PASSWORD` override the YAML fields and follow the same bootstrap semantics.

See **[Studio & Admin Auth](./studio)** for the full credential priority rules and password-change instructions.

## Persistence and distributed mode

```yaml
queryflux:
  persistence:
    type: postgres
    postgres:
      url: postgres://queryflux:queryflux@localhost:5433/queryflux
  # Optional — defaults to true when postgres persistence is configured
  distributed: true
  configReloadIntervalSecs: 30
```

With **Postgres persistence** and **`distributed: true`** (the default when Postgres is configured), multiple QueryFlux replicas coordinate through Postgres:

| Concern | Mechanism |
|---|---|
| Config hot-reload | `configReloadIntervalSecs` + immediate reload on Admin API writes |
| Fleet-wide `maxRunningQueries` | Capacity leases in `cluster_capacity_leases` (`try_acquire` / `release`) |
| Engine running counts | Reconcile sweep publishes to `cluster_capacity_counters.running` |
| Queued query dispatch | Claim columns on `queued_queries` — one replica per query |

Set `distributed: false` to run Postgres-backed persistence without cross-replica coordination (single replica or explicit opt-out).

Helm / Kubernetes: see **[charts/queryflux/README.md](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/README.md#persistence-and-replicas)**. Full behavior: **[Cluster variants, health checks & reconciliation](./architecture/cluster-variants-and-health#distributed-mode-and-capacitystore)**.

## Query Cache

QueryFlux can cache deterministic query results to avoid repeated backend roundtrips. See the dedicated **[Caching](./architecture/caching)** page for full documentation.

Quick example:

```yaml
queryflux:
  cacheBackend:
    scheme: s3
    compression: lz4
    options:
      bucket: queryflux-cache
      endpoint: http://localhost:19000
      region: us-east-1
      access_key_id: minio-root-user
      secret_access_key: minio-root-password

clusterGroups:
  analytics:
    members: [trino-1]
    maxRunningQueries: 50
    cache:
      enabled: true
      ttlSecs: 300
      maxEntrySizeMb: 64
```

---

`config.example.yaml`, `config.local.yaml`, and the serde types in `queryflux-core` (`config.rs`) are the authoritative reference. For routing semantics and `clusterGroups`, see **[Routing and clusters](/docs/architecture/routing-and-clusters)**.

## Cluster variants (multi-warehouse)

A single cluster config can define **`variants`**: named overrides that expand into separate runtime clusters at load time. Use this when one credential set targets multiple Snowflake warehouses, Databricks SQL warehouses, BigQuery projects, or similar.

```yaml
clusters:
  my-snowflake:
    engine: adbc
    driver: snowflake
    uri: svc_user@myaccount/mydb/myschema
    auth:
      type: keyPair
      username: SVC_ACCOUNT
      privateKeyPem: "..."
    # Optional — leave empty to use built-in Snowflake introspection
    healthCheckQuery: "SHOW WAREHOUSES LIKE '{{sub_resource}}'"
    variants:
      - name: analytics
        overrides:
          warehouse: ANALYTICS_WH
      - name: etl
        overrides:
          warehouse: ETL_WH
          maxRunningQueries: 5

clusterGroups:
  snowflake-pool:
    maxRunningQueries: 20
    members:
      - my-snowflake::analytics
      - my-snowflake::etl
```

**Rules:**

- Runtime names are `{base}::{variant_name}` (for example `my-snowflake::analytics`).
- A cluster **with** variants does not create a runtime cluster for the base name alone.
- `healthCheckQuery` and `reconcileQuery` are set on the **base** config; `{{sub_resource}}` is substituted per variant.

Full details: **[Cluster variants, health checks & reconciliation](./architecture/cluster-variants-and-health)**.
