---
sidebar_position: 1
sidebar_label: Connect a client
title: Connect a SQL client
description: Copy-paste connection recipes for Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake HTTP, and Arrow Flight SQL — ports, auth, and client gotchas in one place.
image: img/queryflux-hero-banner.png
---

# Connect a SQL client

QueryFlux exposes several **frontends** (native wire or HTTP protocols). Point your existing client at the frontend that matches its driver, not at the backend engine directly. Routing, translation, guardrails, and query history all happen when traffic enters through QueryFlux.

This page is a **connection cookbook**. For protocol depth (message flows, dispatch paths, unsupported features), use the matching page under **[Frontends](/docs/architecture/frontends/overview)**.

:::tip Defaults are configurable
Ports below are the usual defaults from [`config.example.yaml`](https://github.com/lakeops-org/queryflux/blob/main/config.example.yaml) and frontend docs. Your deployment may differ — check `queryflux.frontends.*.port` in your YAML.
:::

---

## 1. Choose a frontend

| Client family | Frontend | Config key | Typical port | SQL dialect |
| --- | --- | --- | --- | --- |
| Trino CLI, Trino JDBC, Python `trino`, HTTP tools | Trino HTTP | `trinoHttp` | **8080** | Trino |
| `psql`, Postgres JDBC/Python in simple-query mode | PostgreSQL wire | `postgresWire` | **5432** | Postgres |
| `mysql` / `mariadb` CLI, MySQL JDBC/Python | MySQL wire | `mysqlWire` | **3306** | MySQL |
| SnowSQL, Snowflake JDBC/ODBC/Python, REST v2 | Snowflake HTTP + SQL API v2 | `snowflakeHttp` | **set in YAML** (often **8443** or **8445**) | Snowflake |
| Flight SQL JDBC, ADBC, DuckDB `ATTACH` | Arrow Flight SQL | `flightSql` | **50051** (gRPC) | Generic |

Enable each frontend in config before connecting. Snowflake has **no implicit port** — you must set `snowflakeHttp.port`. Wire and SQL API v2 share that listener; routing distinguishes `snowflakeHttp` vs `snowflakeSqlApi`.

---

## 2. Connection recipes

Replace `localhost`, usernames, and passwords with your deployment. Examples assume auth is off or uses simple static users unless noted.

### Trino HTTP (`trinoHttp`, port 8080)

**Trino CLI**

```bash
trino --server http://localhost:8080 --user dev --execute "SELECT 42"
```

**curl**

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT current_date"
```

**Python (`trino` package)**

```python
from trino.dbapi import connect

conn = connect(host="localhost", port=8080, user="dev")
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

**JDBC (Trino driver)** — host and port only; user via driver properties:

```text
jdbc:trino://localhost:8080
```

**What the client sends for auth:** `Authorization: Basic`, `Authorization: Bearer`, or `X-Trino-User` (see [Trino HTTP frontend](/docs/architecture/frontends/trino-http#authentication)).

Protocol reference: [Trino HTTP](/docs/architecture/frontends/trino-http).

---

### PostgreSQL wire (`postgresWire`, port 5432)

Use **simple-query mode**. Extended query (Parse/Bind/Execute) is not supported. QueryFlux declines SSL at the wire (`N` response) — connect in plaintext or terminate TLS in front of QueryFlux.

**psql**

```bash
psql -h localhost -p 5432 -U dev -d my_catalog -c "SELECT 1"
```

**Python (`psycopg2`)**

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="dev", dbname="my_catalog")
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

**JDBC (PostgreSQL driver)** — disable SSL if the driver insists on TLS:

```text
jdbc:postgresql://localhost:5432/my_catalog?user=dev&sslmode=disable
```

**What the client sends for auth:** username (and password when configured) in the startup message.

Protocol reference: [PostgreSQL wire](/docs/architecture/frontends/postgres-wire).

---

### MySQL wire (`mysqlWire`, port 3306)

QueryFlux advertises `mysql_native_password` and **rejects** client-initiated TLS. Use `--ssl-mode=DISABLED` (CLI) or `ssl_disabled=True` / `useSSL=false` (drivers).

**mysql CLI**

```bash
mysql -h 127.0.0.1 -P 3306 -u dev --ssl-mode=DISABLED -e "SELECT 1"
```

**With a default database**

```bash
mysql -h 127.0.0.1 -P 3306 -u dev -D my_catalog --ssl-mode=DISABLED
```

**Python (`mysql-connector-python`)**

```python
import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1", port=3306, user="dev", ssl_disabled=True
)
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

**JDBC (MySQL connector)**

```text
jdbc:mysql://127.0.0.1:3306/my_catalog?user=dev&useSSL=false
```

**What the client sends for auth:** username and password in the handshake.

Protocol reference: [MySQL wire](/docs/architecture/frontends/mysql-wire).

---

### Snowflake HTTP + SQL API v2 (`snowflakeHttp`)

Both Snowflake wire and REST API v2 listen on the **same port** you configure. QueryFlux does not terminate TLS — use `protocol=http` to QueryFlux directly, or HTTPS/`--insecure` only when a TLS terminator sits in front.

**SnowSQL**

```bash
snowsql -a <account-placeholder> -u dev -h localhost -p 8443 --protocol https --insecure
```

**Python (`snowflake-connector-python`)**

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account="queryflux",
    user="dev",
    password="secret",
    host="localhost",
    port=8443,
    protocol="http",
    database="my_catalog",
    schema="my_schema",
)
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

**SQL API v2 (curl)**

```bash
curl -X POST http://localhost:8443/api/v2/statements \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"statement": "SELECT 42 AS answer", "timeout": 60}'
```

**What the client sends for auth:** wire login uses `LOGIN_NAME` / `PASSWORD`; subsequent requests use `Authorization: Snowflake Token="<session-token>"`. REST v2 uses `Authorization: Bearer <token>` on every request.

**Multi-instance deployments:** Snowflake wire sessions are sticky to one QueryFlux process — configure load-balancer session affinity (`sessionAffinityAcknowledged`). See [Multi-replica operations](/docs/operations/multi-replica).

Protocol reference: [Snowflake frontend](/docs/architecture/frontends/snowflake).

---

### Arrow Flight SQL (`flightSql`, port 50051)

gRPC **plaintext** to QueryFlux unless you add an external TLS terminator. Catalog/schema RPCs are not implemented — run SQL via `GetFlightInfo` + `DoGet` only.

**Python (ADBC Flight SQL driver)**

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(uri="grpc://localhost:50051")
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

**DuckDB (Flight SQL extension)**

```sql
ATTACH 'grpc://localhost:50051' AS qf (TYPE flight_sql);
SELECT * FROM qf.my_catalog.my_schema.my_table LIMIT 10;
```

**What the client sends for auth:** gRPC metadata `authorization: Bearer …` or `Basic …`.

Protocol reference: [Arrow Flight SQL](/docs/architecture/frontends/flight-sql).

---

## 3. Authentication

Who may connect and which credential each frontend accepts are configured under **`auth`** (none, static, OIDC, LDAP). Backend identity (`queryAuth`) and data access (OPA) are separate — see **[Authentication & identity](/docs/authentication)**.

| Frontend | Typical client credential |
| --- | --- |
| Trino HTTP | Basic, Bearer, or `X-Trino-User` |
| PostgreSQL wire | Startup `user` / password |
| MySQL wire | Handshake user / password |
| Snowflake wire | Login body + session token header |
| Snowflake SQL API v2 | Bearer token per request |
| Flight SQL | gRPC `authorization` metadata |

When `auth.required: true`, unauthenticated requests are rejected on every enabled frontend.

---

## 4. Limitations that break clients

These are the most common “it works against the engine but fails against QueryFlux” causes:

| Topic | Affected frontends | What to do |
| --- | --- | --- |
| TLS terminated by QueryFlux | All | QueryFlux does not terminate TLS on frontends. Use plaintext to QueryFlux or put a reverse proxy in front. |
| Extended query / prepared statements | PostgreSQL wire, MySQL wire, Flight SQL, Trino HTTP | Use simple SQL text; see each frontend’s “Not supported” table. |
| SSL on MySQL wire | MySQL | `--ssl-mode=DISABLED` / `useSSL=false` / `ssl_disabled=True`. |
| SSL on PostgreSQL wire | PostgreSQL | Wire protocol declines SSL; use `sslmode=disable` or external TLS. |
| Snowflake session stickiness | Snowflake wire | Enable session affinity when running multiple replicas. |
| Flight SQL metadata RPCs | Flight SQL | No `GetTables` / catalogs — only statement execution RPCs. |

For full protocol behavior, open the linked frontend page under **[Frontends](/docs/architecture/frontends/overview)**.

---

## 5. Verify traffic went through QueryFlux

1. Run a distinctive query through the frontend (for example `SELECT 'via-queryflux' AS marker`).
2. Open **[QueryFlux Studio](/docs/studio)** → **Queries** and confirm the query appears with the expected frontend and user.
3. If nothing shows up, the client may be talking to a **backend port** instead of QueryFlux — see the warning in [Route your first query](/docs/quickstart/first-query#3-connect-a-client).

Structured logs and Prometheus metrics also record frontend and routing decisions; see **[Observability](/docs/architecture/observability)**.

---

## Related

- [Documentation overview](/docs/intro) — typical clients and ports at a glance
- [Configuration](/docs/configuration) — enable frontends and set ports
- [Getting started quickstarts](/docs/getting-started) — Docker Compose examples with QueryFlux in the path
