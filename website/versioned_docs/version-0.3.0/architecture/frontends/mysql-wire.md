---
title: MySQL Wire Frontend
description: MySQL wire protocol frontend — handshake, COM_QUERY, session variables, schema switching, and mysql client support.
image: img/queryflux-hero-banner.png
---
# MySQL wire

The MySQL wire frontend lets standard MySQL clients (`mysql` CLI, JDBC, Python `mysql-connector`, Go `go-sql-driver/mysql`, etc.) connect to QueryFlux over the MySQL wire protocol. This is the natural entry point when routing traffic to MySQL-protocol backends like StarRocks.

## Configuration

```yaml
queryflux:
  frontends:
    mysqlWire:
      enabled: true
      port: 3306
```

Config key: `mysqlWire`. Protocol identifier: `FrontendProtocol::MySqlWire`. Default dialect: `SqlDialect::MySql`.

## Protocol support

| Feature | Status |
|---------|--------|
| Server handshake (protocol 10) | Supported — advertises `8.0.0-queryflux` |
| `mysql_native_password` auth | Supported |
| `COM_QUERY` | Supported |
| `COM_PING` | Supported |
| `COM_INIT_DB` (schema switch) | Supported |
| `COM_QUIT` | Supported |
| `COM_FIELD_LIST` | Stub (returns empty EOF) |
| SSL/TLS handshake | Detected and rejected — use `--ssl-mode=DISABLED` |
| Prepared statements (`COM_STMT_*`) | Not supported |

Clients must connect without TLS (`--ssl-mode=DISABLED` for `mysql` CLI, `useSSL=false` for JDBC). If the client sends an SSL request, QueryFlux returns an error and closes the connection.

## Handshake and authentication

1. QueryFlux sends a server handshake packet (protocol version 10, server version `8.0.0-queryflux`, `mysql_native_password`).
2. Client responds with username and optional database.
3. QueryFlux authenticates via the configured `auth_provider`.
4. On success, an OK packet is returned and the connection is ready.

## Execution model

Queries execute **synchronously** via `execute_to_sink`. Results stream as MySQL text protocol result sets:

1. Column count packet.
2. Column definition packets.
3. Row data packets (text values).
4. EOF / OK packet.

### Native path vs. Arrow fallback

The dispatch layer chooses between two result-encoding paths based on the backend's `ConnectionFormat`:

| Backend | `ConnectionFormat` | Path |
|---------|-------------------|------|
| StarRocks, ClickHouse (`mysql_async` pool) | `MysqlWire` | **Native** — driver values encoded directly to `NativeResultChunk`; no Arrow allocation |
| DuckDB, ADBC engines (Snowflake, Databricks) | `Arrow` | **Arrow fallback** — `RecordBatch` stream re-encoded to MySQL text protocol |
| Trino (HTTP async) | `TrinoHttp` | **Arrow fallback** — internal submit+poll loop returns Arrow |

When backend and frontend formats match (`MysqlWire` ↔ `MySqlWire`), the entire Arrow columnar allocation is skipped. This also preserves type precision for `DECIMAL`, `DATETIME(6)`, and unsigned integers that would otherwise be approximated through the Arrow type system.

## Built-in query handling

The MySQL frontend intercepts several common queries that clients and drivers send during connection setup:

| Query pattern | Behavior |
|---------------|----------|
| `SET query_tags = '...'` / `SET SESSION query_tags = '...'` | Stores tags on the session for routing |
| `SET ...` (other) | Acknowledged without forwarding to backend |
| `USE <schema>` | Updates session schema |
| `SELECT @@version` | Returns `8.0.0-queryflux` |
| `SELECT DATABASE()` | Returns current session schema |
| `SHOW VARIABLES` / `SHOW STATUS` | Returns empty result set |

Leading `/* ... */` and `/*!...*/` conditional comments in SQL are stripped before processing.

## Session context

The MySQL wire frontend populates `SessionContext` as follows:

| Field | Source |
|-------|--------|
| `user` | Handshake response |
| `database` | `COM_INIT_DB`, `USE` command, or initial schema from handshake |
| `tags` | `SET query_tags` / `SET SESSION query_tags` |
| `extra` | Empty (generic `SET` variables are acknowledged but not stored) |

`database` is mutable per-connection — it updates whenever the client issues `USE db` or `COM_INIT_DB`. Available to routers via `ctx["database"]` and `ctx["user"]` in the `pythonScript` router.

## Client examples

```bash
# mysql CLI
mysql -h 127.0.0.1 -P 3306 -u dev --ssl-mode=DISABLED -e "SELECT 1"

# With database
mysql -h 127.0.0.1 -P 3306 -u dev -D my_catalog --ssl-mode=DISABLED
```

```python
# Python (mysql-connector)
import mysql.connector
conn = mysql.connector.connect(host="127.0.0.1", port=3306, user="dev", ssl_disabled=True)
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| Prepared statements (`COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE`) | Not supported — returns an error. Use plain `COM_QUERY` (text protocol). |
| SSL/TLS | Detected and rejected — the connection is closed. Use `--ssl-mode=DISABLED` or `useSSL=false`. |
| `COM_FIELD_LIST` | Stub — returns an empty EOF response (no column metadata). |
| `extra` in `SessionContext` | Always empty. Generic `SET` variables are acknowledged but not stored or forwarded to routers or adapters. |
| Binary result protocol | Not supported. All results use the MySQL text protocol. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `mysqlWire`
- [Query tags](/docs/architecture/query-tags) — setting tags via `SET query_tags`
