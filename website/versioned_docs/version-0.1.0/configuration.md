---
sidebar_position: 1
sidebar_label: YAML reference
description: Complete reference for config.yaml — frontends, cluster groups, routing rules, concurrency limits, and observability settings.
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

`config.example.yaml`, `config.local.yaml`, and the serde types in `queryflux-core` (`config.rs`) are the authoritative reference. For routing semantics and `clusterGroups`, see **[Routing and clusters](/docs/architecture/routing-and-clusters)**.
