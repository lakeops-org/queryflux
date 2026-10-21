---
title: MCP Frontend
description: Model Context Protocol tool-call frontend — streamable HTTP, read-only by default, discovery and query tools for AI agents, and how it reuses QueryFlux's existing routing, auth, guardrails, and agent-context features.
image: img/queryflux-hero-banner.png
---
# MCP Frontend

The MCP frontend lets any [Model Context Protocol](https://modelcontextprotocol.io/) client — Claude, GPT-based agents, LangChain, or a custom framework — query QueryFlux without writing custom integration code. It is implemented as a submodule of `queryflux-frontend`, the same crate every other frontend (Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL, Snowflake) lives in, and it dispatches every tool call through the exact same `execute_to_sink` pipeline: routing, SQL translation, guardrails, and query-history persistence are identical to every other frontend. There is no separate MCP-specific execution path — what you already have configured applies here too. The one MCP-specific policy is the [read/write split](#read-only-by-default): `execute_query` only runs reads, and writes need a separate tool the operator has to turn on.

## Configuration

```yaml
queryflux:
  frontends:
    mcp:
      enabled: true
      port: 8811
      allowWrites: false   # default; true advertises execute_statement
```

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Start the MCP listener. |
| `port` | — | Required. The endpoint is served at `/mcp` on this port. |
| `maxConnections` | unlimited | Concurrent in-flight requests. |
| `allowWrites` | `false` | Advertise the `execute_statement` tool for writes and DDL. See [Read-only by default](#read-only-by-default). |

Config key: `mcp`. Protocol identifier: `FrontendProtocol::Mcp`. Default dialect: `SqlDialect::Generic`. There is no default port — set one explicitly, same as every other optional frontend.

## Transport

The MCP frontend speaks **streamable HTTP** only (`rmcp`'s `transport-streamable-http-server`), served at `http://<host>:<port>/mcp`. This matches every other QueryFlux frontend being a network service — stdio transport (common for embedding an MCP server as a local subprocess) doesn't fit QueryFlux's server deployment model and isn't offered.

## Authentication

Requests carry `Authorization: Bearer <token>`, checked against the same `AuthProvider` every other HTTP frontend uses (`none`, `static`, `oidc`, `ldap` — see [Authentication](../../authentication)). There is no MCP-specific auth mechanism — and specifically, QueryFlux does **not** implement the MCP spec's own OAuth 2.1 authorization flow (PKCE, `.well-known/oauth-authorization-server` discovery, dynamic client registration). A client expecting to auto-discover auth and interactively log a user in against QueryFlux directly won't find that here; every setup in [Connecting a client](#connecting-a-client) below relies on a token obtained some other way and pasted into config.

### Authenticating with an OIDC provider (Keycloak, etc.)

With `auth.provider: oidc`, QueryFlux only *validates* tokens (JWT signature + claims against your IdP's JWKS endpoint) — it never issues them. See [Authentication](../../authentication#frontend-authentication-auth) for the `oidc` config block, and **[`examples/with-keycloak-oidc`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-keycloak-oidc)** for a runnable Keycloak + QueryFlux stack (that example fetches a token via the password grant).

For a token that sits in a static MCP client config rather than a short-lived interactive session, a **client-credentials (service account)** token is usually the better fit — it's not tied to a human's login, and you control its lifetime independently:

```bash
TOKEN=$(curl -s https://keycloak.internal/realms/my-realm/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=queryflux-mcp \
  -d client_secret="$QUERYFLUX_MCP_CLIENT_SECRET" | jq -r .access_token)
```

(Requires a Keycloak client with "Service Accounts" enabled.) Two things to plan for either way:

- **The token will expire**, and nothing on QueryFlux's side refreshes it — validation enforces `exp` like any standard JWT check. A client config with a pasted-in token needs either a long-lived access-token setting on that Keycloak client, or periodic manual regeneration.
- **One token = one identity** for every call made through it. A shared service-account token means all MCP traffic authenticated with it shows up under that one identity in query history — use the [agent-context fields](../../agentic/agent-context) (`agent_id`, `conversation_id`, etc.) to distinguish individual agents/conversations if that matters to you, rather than provisioning per-user OIDC tokens for something as ephemeral as an MCP client config.

## Tools

| Tool | Annotations | Description |
|------|-------------|-------------|
| `execute_query(sql, engine_hint?, max_rows?, dialect?, ...agent context)` | `readOnlyHint` | Run a **read** statement (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`) and return rows as JSON, keyed by column name. Anything else is rejected before dispatch. Truncates at `max_rows` (default 1000) — a result-size bound, not a safety mechanism. |
| `execute_statement(sql, confirm, engine_hint?, dialect?, ...agent context)` | `destructiveHint` | Writes and DDL. Only exists when `allowWrites: true`. `confirm` must be `true`, or the tool returns an error telling the agent to get the user's approval first. |
| `list_schemas(engine_hint?, ...agent context)` | `readOnlyHint` | List schemas by querying `information_schema.schemata` — the SQL-standard view every supported engine implements. |
| `list_tables(catalog?, schema?, engine_hint?, ...agent context)` | `readOnlyHint` | List tables and views (`table_schema`, `table_name`, `table_type`) from `information_schema.tables`, capped at 1000 rows. `catalog` qualifies the view (`"<catalog>".information_schema.tables`, the Trino/Snowflake/DuckDB form); `schema` filters on `table_schema`. |
| `describe_table(schema?, table, sample_rows?, engine_hint?, ...agent context)` | `readOnlyHint` | Column metadata via `DESCRIBE`, plus a bounded sample (`SELECT * ... LIMIT sample_rows`) in the same call when `sample_rows > 0` — reduces agent round-trips. |
| `explain_query(sql, engine_hint?, dialect?, ...agent context)` | `readOnlyHint` | Query plan via `EXPLAIN`, without executing the query. |
| `explain_route(sql, ...agent context)` | `readOnlyHint` | Where the query would run (cluster group, engines, the routing rule that chose it) and whether guardrails would allow, rewrite or deny it — without running it. See [route explain](../routing-and-clusters#route-explain). |
| `get_query_status(query_id)` | `readOnlyHint` | Status of a query submitted via `execute_query` — `running`, `queued`, or `not_found_or_completed`. QueryFlux does not currently retain a lookup-by-id history of completed queries, so this only covers in-flight queries. |
| `cancel_query(query_id)` | `idempotentHint` (not read-only, not destructive) | Cancel a running or queued query. Only the agent/user that submitted it may cancel it (ownership-checked, same as every other owner-scoped operation in QueryFlux). |

The annotations are standard MCP [tool annotations](https://modelcontextprotocol.io/specification/2025-06-18/server/tools#tool-annotations). Clients use them to decide what to auto-approve: many run `readOnlyHint` tools without asking and prompt the user before a `destructiveHint` one. They are hints for the client, not enforcement; enforcement is the read-only check below and your guardrails.

The server's `instructions` (sent on connect) tell the agent to discover data with `list_schemas` → `list_tables` → `describe_table`, run reads with `execute_query`, add `LIMIT`s and selective filters, and fix and retry when a guardrail rejects a query, since the error says what to change.

Every tool that accepts `engine_hint` uses it as an exact cluster-group name when it matches a configured group; otherwise the query is routed normally through the configured `RouterChain` — the same routing behavior as every other frontend, just with an optional override.

Every tool except `get_query_status` / `cancel_query` also accepts the optional agent-context fields — `agent_id`, `conversation_id`, `step_index`, `tool_call_id`, `query_intent` — directly as tool parameters, in addition to the `X-Agent-Id` / `X-Conversation-Id` / etc. headers every other HTTP frontend already supports. Both are optional: when neither is supplied, `agent_id`/`conversation_id` default rather than being left unset (see [Agent context defaults on MCP](../../agentic/agent-context#agent-context-defaults-on-mcp)). See [Agentic context](../../agentic/agent-context#setting-context-via-mcp-tool-parameters) for why MCP gets both paths and which one wins when both are supplied.

### Read-only by default

`execute_query` classifies the SQL before dispatching it, with the same parser QueryFlux uses for guardrails (the `dialect` argument if given, otherwise generic SQL), and rejects anything that isn't a read:

```text
execute_query only runs read statements (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN). Writes are disabled on this endpoint; ask an operator to set frontends.mcp.allowWrites: true and use execute_statement.
```

The check is stricter than the `read_only` guard. It also rejects `SELECT ... INTO`, statements the parser only knows as a generic command (`CALL`, `VACUUM`, `UNLOAD`, …), data-modifying CTEs (`WITH d AS (DELETE … RETURNING *) SELECT …`), and an `EXPLAIN` of a write (`EXPLAIN ANALYZE DELETE` runs the delete on several engines). Every statement in a multi-statement string must be a read. SQL the parser can't handle falls back to a prefix check and is rejected if it mentions a write keyword anywhere.

With `allowWrites: false` (the default) `execute_statement` is not registered at all, so `tools/list` doesn't advertise it and agents never see a write tool. With `allowWrites: true` it is listed with `destructiveHint: true` and still requires `confirm: true` on each call. `execute_query` stays read-only either way.

This split is about what the agent *asks* for. It doesn't replace guardrails: both tools go through the normal dispatch path, so `read_only`, `row_limit`, scan limits, and access control still apply. To keep a group read-only for every client, use the `read_only` guard.

### The `dialect` parameter

Every other frontend implies a SQL dialect from its wire protocol — a PostgreSQL wire client is assumed to write Postgres-flavored SQL, for example. MCP has no such signal: an LLM agent can write SQL in any dialect, or (more commonly) in whatever dialect the target engine itself uses. Guessing wrong is worse than not translating at all — a wrong dialect assumption can cause sqlglot to parse the SQL under the wrong syntax rules and silently rewrite it incorrectly.

So `execute_query`, `execute_statement`, and `explain_query` don't guess at all: with no `dialect` given, translation is skipped entirely — sqlglot is never invoked, and the SQL is sent to the target engine exactly as written (this also means any configured translation fixup scripts don't run for that call, since those need a real dialect to parse under too). This is a deliberate no-op, not an assumption — QueryFlux doesn't claim to know what dialect the SQL is in unless you tell it. Set `dialect` when the SQL was written for a *different* engine and should actually be translated before being routed. Accepted values (case-insensitive):

`trino`, `athena`, `duckdb`, `starrocks`, `clickhouse`, `mysql`, `postgres` (or `postgresql`), `sqlite`, `snowflake`, `bigquery`, `databricks`, `tsql` (or `mssql`), `redshift`, `exasol`, `generic`.

An unrecognized value is rejected with an `invalid_params` error listing the accepted set, rather than being passed through to sqlglot unvalidated.

## Guardrails and access control

MCP queries flow through the same `GuardChain` as every other frontend — built-in guards (`read_only`, `row_limit`, `require_predicate`, …), `python_script` / `http_webhook` guards, and [access control](../../access-control/overview) (`opa_access`) all apply automatically to any group that serves MCP traffic, with no MCP-specific guard code involved. Apart from the [read/write split](#read-only-by-default), there is no default guard policy baked into the MCP frontend itself; configure restrictions the same way as for any other frontend — see [Guardrails](../guardrails).

Row filters and column masks are available via OPA access control (same pipeline as Trino HTTP and other frontends).

## Query history and session replay

Every MCP query is recorded the same way as every other frontend's queries. Unlike every other frontend, MCP always has agent context — even a tool call with no `agent_id`/`conversation_id` header or parameter still gets one, defaulted from the authenticated identity and the MCP session id (see [Agent context defaults on MCP](../../agentic/agent-context#agent-context-defaults-on-mcp)), so MCP traffic never goes missing from the **Agents** page the way an unlabeled query on another frontend would. MCP-originated queries show up in QueryFlux Studio's **Queries**, **Agents**, and **Conversations** pages exactly like agent traffic through Trino HTTP or PostgreSQL wire — no separate MCP-specific tooling needed to audit or replay what an agent did. See [Session replay and guardrails](../../agentic/session-replay) for the full persistence and replay model.

## Connecting a client

Point any streamable-HTTP MCP client at `http://<host>:<port>/mcp` with a bearer token. Every example below uses `http://localhost:8811/mcp` for local development — QueryFlux does not terminate TLS itself (same as every other frontend), so a bearer token sent to a **remote** host must go through HTTPS or a TLS-terminating reverse proxy in front of QueryFlux; otherwise the token travels in cleartext. Don't hardcode the token in a committed config file — every client below supports pulling it from an environment variable instead.

**[`examples/with-mcp`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-mcp)** is a runnable Docker Compose stack — QueryFlux with the MCP frontend enabled against the embedded DuckDB engine, no other services required — if you'd rather have something running before working through the client configs below.

### MCP Inspector (quick sanity check)

```bash
npx @modelcontextprotocol/inspector
# Transport: Streamable HTTP
# URL: http://localhost:8811/mcp
# Header: Authorization: Bearer <token>
```

### Cursor

Cursor supports streamable HTTP natively — no bridge needed. Add to `.cursor/mcp.json` (project-scoped) or Cursor's global MCP settings:

```json
{
  "mcpServers": {
    "queryflux": {
      "url": "http://localhost:8811/mcp",
      "headers": {
        "Authorization": "Bearer ${env:QUERYFLUX_MCP_TOKEN}"
      }
    }
  }
}
```

### Claude Code

```bash
claude mcp add --transport http queryflux http://localhost:8811/mcp \
  --header "Authorization: Bearer $QUERYFLUX_MCP_TOKEN"
```

Add `--scope user` to make it available across every project instead of just the current one. Run `/mcp` inside Claude Code afterward to confirm it shows as connected — a bad token shows as failed with the HTTP status QueryFlux returned (e.g. 401).

### Claude Desktop

Claude Desktop's `claude_desktop_config.json` only validates stdio server entries — pasting a streamable-HTTP URL directly into it is silently ignored. Two options:

- **Custom Connector (recommended)**: Settings → Connectors → Add custom connector, and enter the URL and bearer token there. No JSON file editing.
- **[`mcp-remote`](https://www.npmjs.com/package/mcp-remote) bridge**, if you need it in the config file itself:

```json
{
  "mcpServers": {
    "queryflux": {
      "command": "npx",
      "args": [
        "mcp-remote@latest",
        "http://localhost:8811/mcp",
        "--header",
        "Authorization:${AUTH_HEADER}"
      ],
      "env": {
        "AUTH_HEADER": "Bearer YOUR_TOKEN_HERE"
      }
    }
  }
}
```

### Other clients

Most other MCP-capable tools (Windsurf, VS Code's MCP support, etc.) follow the same `mcpServers: { "<name>": { "url": ..., "headers": {...} } }` convention Cursor uses above — check that client's own docs for where the file lives, but the `url`/`headers` shape is usually a direct copy-paste.

## Not supported / Known limitations

| Feature | Status |
|---------|--------|
| stdio transport | Not offered — streamable HTTP only. |
| MCP Resources (e.g. `schema:///{table}` subscriptions) | Not implemented. |
| Natural-language-to-SQL, semantic routing, query rewriting | Explicitly out of scope for this frontend — QueryFlux routes and executes the SQL it's given. |
| Result caching | Works as for every frontend: `execute_query` goes through the shared dispatch path, so repeated identical, deterministic SQL is served from the [result cache](../caching) when the group enables it. |
| Async submit/poll execution model | `execute_query` is synchronous — it blocks until the query completes. `get_query_status` / `cancel_query` are best-effort against the existing in-flight query registry (useful from a concurrent tool call), not a full async job API. |
| Column masks / row filters | Available via [access control](../../access-control/overview) (`opa_access`) — not MCP-specific. |
| TLS | Not terminated by QueryFlux. Use an external TLS terminator in front of QueryFlux for any deployment where the bearer token crosses an untrusted network. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Setting agent context](../../agentic/agent-context) — how MCP populates agent identity, via headers and tool parameters
- [Session replay and guardrails](../../agentic/session-replay) — what gets persisted and how to reconstruct an agent's session
- [Guardrails](../guardrails) — built-in guards
- [Access control](../../access-control/overview) — OPA row filters and column masks
