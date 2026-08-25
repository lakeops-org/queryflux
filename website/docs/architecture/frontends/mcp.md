---
title: MCP Frontend
description: Model Context Protocol tool-call frontend — streamable HTTP, six tools for AI agents, and how it reuses QueryFlux's existing routing, auth, guardrails, and agent-context features.
image: img/queryflux-hero-banner.png
---
# MCP Frontend

The MCP frontend lets any [Model Context Protocol](https://modelcontextprotocol.io/) client — Claude, GPT-based agents, LangChain, or a custom framework — query QueryFlux without writing custom integration code. It is implemented as a submodule of `queryflux-frontend`, the same crate every other frontend (Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL, Snowflake) lives in, and it dispatches every tool call through the exact same `execute_to_sink` pipeline: routing, SQL translation, guardrails, and query-history persistence are identical to every other frontend. There is no separate MCP-specific execution path and no MCP-specific safety policy layered on top — what you already have configured applies here too.

## Configuration

```yaml
queryflux:
  frontends:
    mcp:
      enabled: true
      port: 8811
```

Config key: `mcp`. Protocol identifier: `FrontendProtocol::Mcp`. Default dialect: `SqlDialect::Generic`. There is no default port — set one explicitly, same as every other optional frontend.

## Transport

The MCP frontend speaks **streamable HTTP** only (`rmcp`'s `transport-streamable-http-server`), served at `http://<host>:<port>/mcp`. This matches every other QueryFlux frontend being a network service — stdio transport (common for embedding an MCP server as a local subprocess) doesn't fit QueryFlux's server deployment model and isn't offered.

## Authentication

Requests carry `Authorization: Bearer <token>`, checked against the same `AuthProvider` every other HTTP frontend uses (`none`, `static`, `oidc`, `ldap` — see [Authentication](../../authentication)). There is no MCP-specific auth mechanism.

## Tools

| Tool | Description |
|------|-------------|
| `execute_query(sql, engine_hint?, max_rows?, dialect?, ...agent context)` | Execute SQL and return rows as JSON, keyed by column name. Truncates at `max_rows` (default 1000) — a result-size bound, not a safety mechanism. |
| `list_schemas(engine_hint?, ...agent context)` | List schemas by querying `information_schema.schemata` — the SQL-standard view every supported engine implements. |
| `describe_table(schema?, table, sample_rows?, engine_hint?, ...agent context)` | Column metadata via `DESCRIBE`, plus a bounded sample (`SELECT * ... LIMIT sample_rows`) in the same call when `sample_rows > 0` — reduces agent round-trips. |
| `explain_query(sql, engine_hint?, dialect?, ...agent context)` | Query plan via `EXPLAIN`, without executing the query. |
| `get_query_status(query_id)` | Status of a query submitted via `execute_query` — `running`, `queued`, or `not_found_or_completed`. QueryFlux does not currently retain a lookup-by-id history of completed queries, so this only covers in-flight queries. |
| `cancel_query(query_id)` | Cancel a running or queued query. Only the agent/user that submitted it may cancel it (ownership-checked, same as every other owner-scoped operation in QueryFlux). |

Every tool that accepts `engine_hint` uses it as an exact cluster-group name when it matches a configured group; otherwise the query is routed normally through the configured `RouterChain` — the same routing behavior as every other frontend, just with an optional override.

Every tool except `get_query_status` / `cancel_query` also accepts the optional agent-context fields — `agent_id`, `conversation_id`, `step_index`, `tool_call_id`, `query_intent` — directly as tool parameters, in addition to the `X-Agent-Id` / `X-Conversation-Id` / etc. headers every other HTTP frontend already supports. Both are optional: when neither is supplied, `agent_id`/`conversation_id` default rather than being left unset (see [Agent context defaults on MCP](../../agentic/agent-context#agent-context-defaults-on-mcp)). See [Agentic context](../../agentic/agent-context#setting-context-via-mcp-tool-parameters) for why MCP gets both paths and which one wins when both are supplied.

### The `dialect` parameter

Every other frontend implies a SQL dialect from its wire protocol — a PostgreSQL wire client is assumed to write Postgres-flavored SQL, for example. MCP has no such signal: an LLM agent can write SQL in any dialect, or (more commonly) in whatever dialect the target engine itself uses. Guessing wrong is worse than not translating at all — a wrong dialect assumption can cause sqlglot to parse the SQL under the wrong syntax rules and silently rewrite it incorrectly.

So `execute_query` and `explain_query` don't guess at all: with no `dialect` given, translation is skipped entirely — sqlglot is never invoked, and the SQL is sent to the target engine exactly as written (this also means any configured translation fixup scripts don't run for that call, since those need a real dialect to parse under too). This is a deliberate no-op, not an assumption — QueryFlux doesn't claim to know what dialect the SQL is in unless you tell it. Set `dialect` when the SQL was written for a *different* engine and should actually be translated before being routed. Accepted values (case-insensitive):

`trino`, `athena`, `duckdb`, `starrocks`, `clickhouse`, `mysql`, `postgres` (or `postgresql`), `sqlite`, `snowflake`, `bigquery`, `databricks`, `tsql` (or `mssql`), `redshift`, `exasol`, `generic`.

An unrecognized value is rejected with an `invalid_params` error listing the accepted set, rather than being passed through to sqlglot unvalidated.

## Guardrails

MCP queries flow through the exact same configurable `GuardChain` as every other frontend — `read_only`, `row_limit`, `require_predicate`, Python-script guards, and HTTP webhook guards all apply automatically to any group that serves MCP traffic, with no MCP-specific guard code involved. There is no default guard policy baked into the MCP frontend itself; if you want agent-facing queries restricted (e.g. read-only, row-capped), configure that the same way you would for any other frontend — see [Guardrails](../guardrails) for the full guard configuration reference.

Column/PII masking is not currently available for any frontend, MCP included — it's tracked as a separate, unimplemented design (row filtering and column masking via an external policy engine).

## Query history and session replay

Every MCP query is recorded the same way as every other frontend's queries. Unlike every other frontend, MCP always has agent context — even a tool call with no `agent_id`/`conversation_id` header or parameter still gets one, defaulted from the authenticated identity and the MCP session id (see [Agent context defaults on MCP](../../agentic/agent-context#agent-context-defaults-on-mcp)), so MCP traffic never goes missing from the **Agents** page the way an unlabeled query on another frontend would. MCP-originated queries show up in QueryFlux Studio's **Queries**, **Agents**, and **Conversations** pages exactly like agent traffic through Trino HTTP or PostgreSQL wire — no separate MCP-specific tooling needed to audit or replay what an agent did. See [Session replay and guardrails](../../agentic/session-replay) for the full persistence and replay model.

## Connecting a client

Point any streamable-HTTP MCP client at `http://<host>:<port>/mcp` with a bearer token. Every example below uses `http://localhost:8811/mcp` for local development — QueryFlux does not terminate TLS itself (same as every other frontend), so a bearer token sent to a **remote** host must go through HTTPS or a TLS-terminating reverse proxy in front of QueryFlux; otherwise the token travels in cleartext. Don't hardcode the token in a committed config file — every client below supports pulling it from an environment variable instead.

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
| Natural-language-to-SQL, semantic routing, result caching, query rewriting | Explicitly out of scope for this frontend — QueryFlux routes and executes the SQL it's given. |
| Async submit/poll execution model | `execute_query` is synchronous — it blocks until the query completes. `get_query_status` / `cancel_query` are best-effort against the existing in-flight query registry (useful from a concurrent tool call), not a full async job API. |
| Column/PII masking | Not implemented for any frontend yet. |
| TLS | Not terminated by QueryFlux. Use an external TLS terminator in front of QueryFlux for any deployment where the bearer token crosses an untrusted network. |

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Setting agent context](../../agentic/agent-context) — how MCP populates agent identity, via headers and tool parameters
- [Session replay and guardrails](../../agentic/session-replay) — what gets persisted and how to reconstruct an agent's session
- [Guardrails](../guardrails) — configuring `read_only`, `row_limit`, and other guards
