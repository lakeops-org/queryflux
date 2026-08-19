---
title: PostgreSQL Wire Frontend
description: PostgreSQL v3 wire protocol frontend — startup, simple query flow, session context, and psql compatibility.
image: img/queryflux-hero-banner.png
---
# PostgreSQL wire

The PostgreSQL wire frontend lets Postgres clients that work in **simple-query mode** (`psql`, many JDBC/Python drivers in autocommit/simple mode, etc.) connect to QueryFlux over the PostgreSQL v3 wire protocol. Queries are executed synchronously — the TCP connection stays open while the result streams back as standard Postgres wire messages.

## Configuration

```yaml
queryflux:
  frontends:
    postgresWire:
      enabled: true
      port: 5432
```

Config key: `postgresWire`. Protocol identifier: `FrontendProtocol::PostgresWire`. Default dialect: `SqlDialect::Postgres`.

## Protocol support

| Feature | Status |
|---------|--------|
| Startup (protocol version 3.0) | Supported |
| Simple query (`Q` message) | Supported |
| Extended query (Parse/Bind/Execute) | Not supported — returns error |
| SSL negotiation | Declined (`N` response) — plaintext only |
| `COPY` protocol | Not supported |

QueryFlux responds to the SSL request with `N` (no SSL), then processes the normal startup sequence. Clients that require TLS will need to connect without it or use an external TLS terminator.

## Startup and authentication

1. Client sends **StartupMessage** with `user`, `database`, and optional parameters.
2. QueryFlux extracts credentials from the startup `user` field and authenticates via the configured `auth_provider`.
3. On success, QueryFlux sends `AuthenticationOk`, followed by `ParameterStatus` messages (server version `16.0-queryflux`, encoding `UTF8`, etc.), `BackendKeyData`, and `ReadyForQuery`.

## Execution model

All queries execute **synchronously** via `execute_to_sink`. Results stream as standard Postgres wire messages:

1. `RowDescription` — column metadata (names, types, format codes).
2. `DataRow` messages — one per row, text-format values.
3. `CommandComplete` — summary (e.g. `SELECT 3`).
4. `ReadyForQuery` — ready for the next query.

Errors are returned as Postgres `ErrorResponse` messages with SQLSTATE codes.

## Session context

The Postgres wire frontend populates `SessionContext` as follows:

| Field | Source |
|-------|--------|
| `user` | Startup message `user` parameter |
| `database` | Startup message `database` parameter |
| `tags` | `query_tags` / `query_tag` startup parameters |
| `extra` | Empty (startup parameters beyond `user`/`database`/`query_tags` are not forwarded) |

The `database` and `user` fields are available to routers via `ctx["database"]` and `ctx["user"]` in the `pythonScript` router.

## SET handling

`SET` statements are acknowledged with `CommandComplete` + `ReadyForQuery` without forwarding to the backend. This keeps the connection in a valid state for clients that issue `SET` during startup.

## Client examples

```bash
# psql
psql -h localhost -p 5432 -U dev -d my_catalog -c "SELECT 1"

# Python (psycopg2)
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="dev", dbname="my_catalog")
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| Extended query protocol (Parse/Bind/Execute/Describe) | Not supported — returns a Postgres `ErrorResponse`. Clients must use simple-query mode. |
| SSL/TLS | Declined at the wire level (`N` response). Use an external TLS terminator or connect with TLS disabled. |
| `COPY` protocol | Not supported. |
| `COPY TO / FROM STDIN` | Not supported. |
| Session variables via `SET` | `SET` statements are acknowledged without effect. Variables are not stored or forwarded. |
| `extra` in `SessionContext` | Always empty. Startup parameters beyond `user`, `database`, and `query_tags` are not forwarded to routers or adapters. |
| Per-query routing based on startup params | Not available — only `user`, `database`, and tags can be used for routing. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `postgresWire`
