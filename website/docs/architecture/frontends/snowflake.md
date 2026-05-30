---
title: Snowflake Frontend
description: Snowflake HTTP wire and SQL API v2 on one port — SnowSQL, Python connector, and JDBC without a Snowflake account.
image: img/queryflux-hero-banner.png
---
# Snowflake

The Snowflake frontend lets Snowflake clients (SnowSQL CLI, Python `snowflake-connector-python`, JDBC) connect to QueryFlux as if it were a Snowflake account. QueryFlux terminates the Snowflake wire protocol entirely — no Snowflake account or warehouse is required. SQL is dispatched to whatever backend engine (Trino, StarRocks, DuckDB, etc.) is configured for the incoming session.

Both wire sub-protocols share a single port:

| Sub-protocol | Also known as | Used by |
|---|---|---|
| **Snowflake HTTP wire** (`snowflakeHttp`) | Form 1 / "Snowflake wire" | Python connector (default), SnowSQL CLI, most JDBC drivers |
| **Snowflake SQL REST API v2** (`snowflakeSqlApi`) | Form 2 / REST API v2 | Programmatic REST clients, curl, direct API users |

## Configuration

```yaml
queryflux:
  frontends:
    snowflakeHttp:
      enabled: true
      port: 8443
      sessionAffinityAcknowledged: false  # see below
      snowflakeSessionMaxAgeSecs: 86400   # optional, default 86400 (24 h)
      snowflakeSessionIdleTimeoutSecs: 14400  # optional, default 14400 (4 h)
```

Config key: `snowflakeHttp`. Protocol identifiers: `FrontendProtocol::SnowflakeHttp` (wire), `FrontendProtocol::SnowflakeSqlApi` (REST v2). Default dialect: `SqlDialect::Snowflake`.

The SQL API v2 routes are served on the same port as the wire protocol — there is no separate `snowflakeSqlApi` config block.

### Session affinity

The Snowflake HTTP wire protocol is **stateful**: a session token issued at login must be presented on every subsequent request. If you run multiple QueryFlux instances behind a load balancer, all requests from the same client must go to the same instance. Set `sessionAffinityAcknowledged: true` to confirm your load balancer provides this guarantee; otherwise QueryFlux will warn at startup.

## Endpoints

### Snowflake HTTP wire (`/session/…`, `/queries/…`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/session/v1/login-request` | Authenticate and create a session |
| `DELETE` | `/session` | Logout — invalidate session token |
| `GET` | `/session/heartbeat` | Keep-alive ping |
| `POST` | `/session/token-request` | Renew session token (returns remaining TTL) |
| `POST` | `/queries/v1/query-request` | Execute SQL |
| `GET` | `/queries/v1/query-monitoring-request` | Async query poll (stub — see limitations) |
| `DELETE` | `/queries/v1/{query_id}` | Cancel query (no-op — see limitations) |

### Snowflake SQL REST API v2 (`/api/v2/…`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v2/statements` | Submit SQL and execute synchronously |
| `GET` | `/api/v2/statements/{handle}` | Poll statement (stub — see limitations) |
| `DELETE` | `/api/v2/statements/{handle}` | Cancel statement (stub — see limitations) |

## Authentication

### HTTP wire

Credentials are extracted from the `/session/v1/login-request` body:

- `LOGIN_NAME` — username
- `PASSWORD` — password
- `SESSION_PARAMETERS.DATABASE` / `SESSION_PARAMETERS.SCHEMA` — optional database/schema hints (also accepted as `databaseName`/`schemaName` query parameters)

The credentials are authenticated against the configured `auth_provider`. A session token (UUID) is issued on success and must be passed in every subsequent request as `Authorization: Snowflake Token="<token>"`.

### SQL REST API v2

Stateless: credentials are provided on every request as `Authorization: Bearer <token>`. The token is validated against the configured `auth_provider`.

## Execution model

Both sub-protocols execute queries **synchronously** via `execute_to_sink`. All results are accumulated into memory and returned in a single response.

### HTTP wire response format

Results are returned in **Snowflake JSON format** (columnar values, type metadata). The `QUERY_RESULT_FORMAT` session parameter is set to `ARROW_FORCE` during login, which causes the Python connector to request Arrow-encoded results internally; QueryFlux converts its Arrow record batches to the Snowflake Arrow wire format.

### SQL REST API v2 response format

Results are returned in **`jsonv2` format** — a JSON array of rows where every value is stringified. Column metadata is embedded in `resultSetMetaData.rowType`.

## Session context

### HTTP wire

| Field | Source |
|-------|--------|
| `user` | `LOGIN_NAME` from login request body |
| `database` | `databaseName` query param → `SESSION_PARAMETERS.DATABASE` → `SESSION_PARAMETERS.SCHEMA` (first non-empty value) |
| `tags` | Not populated |
| `extra` | Empty |

Routing (group selection) happens at **login time**, not at query time. The cluster group is stored in the session and reused for all queries in that session. Changing the database after login (e.g. via `USE DATABASE`) does not re-route.

### SQL REST API v2

| Field | Source |
|-------|--------|
| `user` | Resolved from Bearer token by auth provider |
| `database` | Not extracted (always `None`) |
| `tags` | Not populated |
| `extra` | Empty |

## Client examples

```bash
# SnowSQL CLI
snowsql -a <account-placeholder> -u dev -h localhost -p 8443 --protocol https --insecure
```

```python
# Python snowflake-connector-python
import snowflake.connector

conn = snowflake.connector.connect(
    account="queryflux",
    user="dev",
    password="secret",
    host="localhost",
    port=8443,
    protocol="http",   # or https if TLS is configured upstream
    database="my_catalog",
    schema="my_schema",
)
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

```bash
# SQL API v2 (curl)
curl -X POST http://localhost:8443/api/v2/statements \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"statement": "SELECT 42 AS answer", "timeout": 60}'
```

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| Async query execution (`asyncExec: true`) | Not supported. `query-monitoring-request` returns an empty polling response so the connector stops polling. All queries run synchronously. |
| Query cancel (`DELETE /queries/v1/{id}`, `DELETE /api/v2/statements/{handle}`) | No-op — synchronous execution cannot be interrupted mid-flight via HTTP. Returns a success response. |
| SQL REST API v2 polling (`GET /api/v2/statements/{handle}`) | Stub — returns HTTP 404 with "already complete" message. |
| `database` in SQL REST API v2 | Not extracted. `SessionContext.database` is always `None`; protocolBased routing on database hint is not available for REST v2 clients. |
| Query tags | Not extracted for either sub-protocol. The `tags` router type cannot be used with Snowflake frontends. |
| Multiple statements per request | Not supported. Only the first statement in the request body is executed. |
| Transactions (`BEGIN` / `COMMIT` / `ROLLBACK`) | No transaction state is maintained. These statements are forwarded to the backend as-is. |
| `ALTER SESSION SET …` | Acknowledged but not stored in the session. Use `LOGIN_NAME` session parameters at connect time instead. |
| Session migration across instances | Sessions are in-memory per instance. Without sticky load balancing, requests routed to a different instance will fail with "session not found". |
| TLS | Not terminated by QueryFlux. Use an external TLS terminator (e.g. nginx, Envoy) in front of the Snowflake frontend. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `snowflakeHttp` / `snowflakeSqlApi`
