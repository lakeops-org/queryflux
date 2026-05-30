---
title: Arrow Flight SQL Frontend
description: Arrow Flight SQL gRPC frontend — implemented RPCs, metadata passthrough, and Flight SQL client examples.
image: img/queryflux-hero-banner.png
---
# Arrow Flight SQL

The Flight SQL frontend exposes a gRPC-based [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) service. Clients that speak Flight SQL (e.g. the JDBC Flight SQL driver, ADBC, DuckDB's `ATTACH` over Flight) can connect to QueryFlux and run SQL queries with results streamed as native Arrow record batches — zero serialization overhead for columnar consumers.

## Configuration

```yaml
queryflux:
  frontends:
    flightSql:
      enabled: true
      port: 50051
```

Config key: `flightSql`. Protocol identifier: `FrontendProtocol::FlightSql`. Default dialect: `SqlDialect::Generic`.

## Implemented RPCs

| RPC | Status | Description |
|-----|--------|-------------|
| `GetFlightInfo` (statement) | Implemented | Accepts SQL via `CommandStatementQuery`, returns a `FlightInfo` with a ticket |
| `DoGet` (statement) | Implemented | Decodes SQL from the ticket, executes the query, and streams Arrow `RecordBatch` results |
| All other Flight SQL RPCs | Unimplemented | Return gRPC `Unimplemented` status |

The two-step flow:

1. **`GetFlightInfo`** — client sends SQL in a `CommandStatementQuery`. QueryFlux returns a `FlightInfo` containing an endpoint with a ticket (the SQL is encoded in the ticket's `statement_handle`). The IPC schema in the response is empty at this stage.
2. **`DoGet`** — client presents the ticket. QueryFlux decodes the SQL, authenticates, routes, executes via `execute_to_sink`, and streams Arrow record batches through a `FlightDataEncoder`.

## Authentication

Credentials are read from gRPC metadata:

- **`authorization`** — `Bearer <token>` or `Basic <base64>`, depending on your configured auth provider.

## Execution model

Queries execute **synchronously** via `execute_to_sink`. Results are streamed as Arrow Flight data frames directly from the query's Arrow record batches — no intermediate serialization to JSON or text.

## Metadata and routing

ASCII metadata entries on the gRPC request (beyond `authorization`) are collected into a key/value map and passed through to the same routing and session path used for HTTP-style frontends, so routers and auth can inspect client-supplied fields consistently across protocols.

Query tags are **not** populated from Flight SQL metadata in the current implementation.

## Client examples

```python
# Python (ADBC Flight SQL driver)
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(uri="grpc://localhost:50051")
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

```bash
# DuckDB (if Flight SQL extension is available)
ATTACH 'grpc://localhost:50051' AS qf (TYPE flight_sql);
SELECT * FROM qf.my_catalog.my_schema.my_table LIMIT 10;
```

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| Catalog / schema RPCs (`GetCatalogs`, `GetSchemas`, `GetTables`, etc.) | Not implemented — return gRPC `Unimplemented`. |
| `DoPut` (bulk ingest) | Not implemented. |
| `DoExchange` | Not implemented. |
| `DoAction` / `ListActions` | Not implemented. |
| Prepared statements (`CreatePreparedStatement`, `ClosePreparedStatement`, `ExecutePreparedStatement`) | Not implemented. |
| Query tags | Not extracted. `SessionContext.tags` is always empty; the `tags` router type cannot be used with Flight SQL clients. |
| `database` hint | Not extracted. `SessionContext.database` is always `None`; only `user` and metadata entries in `extra` are available for routing. |
| TLS | Not terminated by QueryFlux. Use an external TLS terminator or gRPC plaintext. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `flightSql`
