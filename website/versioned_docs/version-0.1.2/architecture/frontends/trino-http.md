---
title: Trino HTTP Frontend
description: Trino-compatible HTTP frontend — async polling, nextUri rewriting, session headers, tags, and mixed-engine groups.
image: img/queryflux-hero-banner.png
---
# Trino HTTP

The Trino HTTP frontend lets any Trino-compatible client (Trino CLI, JDBC, Python `trino`, DBeaver, etc.) connect to QueryFlux as if it were a Trino coordinator. QueryFlux accepts the standard `/v1/statement` API, routes the query, and returns Trino-shaped JSON responses — including `nextUri` polling for async engines.

## Configuration

```yaml
queryflux:
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
```

Config key: `trinoHttp`. Protocol identifier: `FrontendProtocol::TrinoHttp`. Default dialect: `SqlDialect::Trino`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/statement` | Submit a new SQL query |
| `GET` | `/v1/statement/qf/queued/{id}/{seq}` | Poll a queued query (proxy-side backoff) |
| `GET` | `/v1/statement/qf/executing/{id}` | Poll an executing query |
| `GET` | `/v1/statement/{*path}` | Forward Trino-style poll paths |
| `DELETE` | `/v1/statement/qf/executing/{id}` | Cancel a running query |
| `DELETE` | `/v1/statement/{*path}` | Cancel via forwarded Trino path |

## Authentication

Credentials are extracted in this order:

1. `Authorization: Basic` — decoded username and password.
2. `Authorization: Bearer` — bearer token.
3. `X-Trino-User` header — username only (no password).

The extracted credentials are passed to the configured `auth_provider`. Failures return HTTP 401 (unauthorized) or 403 (forbidden by authorization policy).

## Execution model

Trino HTTP is the only frontend that supports **async polling**:

- **Async-capable group** (e.g. Trino backend): `dispatch_query` submits to the engine, persists the executing state, and returns a Trino JSON response. The `nextUri` in the response is **rewritten** to point back at QueryFlux (`externalAddress`), so subsequent polls flow through the proxy transparently.
- **At capacity**: when the group is full, QueryFlux returns a synthetic "queued" response with a `nextUri` pointing at `/v1/statement/qf/queued/{id}/{seq}`. The client polls this URL; QueryFlux retries cluster acquisition on each poll.
- **Sync engine in a Trino group** (e.g. DuckDB, StarRocks): falls back to `execute_to_sink` with a `TrinoHttpResultSink` — the query runs to completion and the result is returned as a single Trino JSON page (no polling).

## Session context

The Trino HTTP frontend populates `SessionContext` as follows:

| Field | Source |
|-------|--------|
| `user` | `X-Trino-User` header |
| `database` | `X-Trino-Catalog` header |
| `tags` | `X-Trino-Client-Tags` and `X-Trino-Session` headers (extracted at request time) |
| `extra` | All request headers, lowercased (e.g. `x-trino-user`, `x-trino-catalog`, `x-trino-schema`) |

Routers and the `pythonScript` router can inspect any header via `ctx["extra"]` (e.g. `ctx["extra"].get("x-trino-source")`). The common fields `ctx["user"]` and `ctx["database"]` are preferred for user/catalog routing.

## Query tags

Tags can be set via:

- `X-Trino-Client-Tags` header — comma-separated key/value pairs.
- `X-Trino-Session` header — `query_tags` or `query_tag` keys (percent-decoded).
- `SET SESSION query_tags = '...'` SQL statement — intercepted by the frontend (not forwarded to the backend). The response includes `X-Trino-Set-Session` so the client tracks the change.

Tags are used by the `tags` router for routing decisions. See [Query tags](/docs/architecture/query-tags).

## nextUri rewriting

When proxying Trino async queries, QueryFlux rewrites `nextUri` URLs in response JSON so the client always polls through the proxy rather than going directly to the Trino coordinator. The rewrite replaces the host/port with `externalAddress` while preserving the Trino path from `/v1/` onward. This is done via fast byte-level patching when possible, avoiding full JSON deserialization.

## Client examples

```bash
# Trino CLI
trino --server http://localhost:8080 --execute "SELECT 42"

# curl
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT current_date"
```

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| Prepared statements | Not supported. The Trino HTTP protocol does not use a separate prepare/execute flow; all queries arrive as raw SQL. |
| `X-Trino-Schema` as routing hint | The `X-Trino-Schema` header is stored in `extra` and forwarded to the backend but is not mapped to `SessionContext.database`. Use `X-Trino-Catalog` for the `ctx["database"]` routing field. |
| TLS termination | Not handled by QueryFlux. Use an external TLS terminator in front of the Trino HTTP listener. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — how `protocolBased` maps `trinoHttp` to a group
- [Query tags](/docs/architecture/query-tags) — tag-based routing from Trino sessions
