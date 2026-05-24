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

---

`config.example.yaml`, `config.local.yaml`, and the serde types in `queryflux-core` (`config.rs`) are the authoritative reference. For routing semantics and `clusterGroups`, see **[Routing and clusters](/docs/architecture/routing-and-clusters)**.
