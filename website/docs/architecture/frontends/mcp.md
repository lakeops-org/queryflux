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

Requests carry `Authorization: Bearer <token>`, checked against the same `AuthProvider` every other HTTP frontend uses (`none`, `static`, `oidc`, `ldap` — see [Authentication](/docs/authentication)). There is no MCP-specific auth mechanism.

## Tools

| Tool | Description |
|------|-------------|
| `execute_query(sql, engine_hint?, max_rows?, ...agent context)` | Execute SQL and return rows as JSON, keyed by column name. Truncates at `max_rows` (default 1000) — a result-size bound, not a safety mechanism. |
| `list_schemas(engine_hint?, ...agent context)` | List schemas by querying `information_schema.schemata` — the SQL-standard view every supported engine implements. |
| `describe_table(schema?, table, sample_rows?, engine_hint?, ...agent context)` | Column metadata via `DESCRIBE`, plus a bounded sample (`SELECT * ... LIMIT sample_rows`) in the same call when `sample_rows > 0` — reduces agent round-trips. |
| `explain_query(sql, engine_hint?, ...agent context)` | Query plan via `EXPLAIN`, without executing the query. |
| `get_query_status(query_id)` | Status of a query submitted via `execute_query` — `running`, `queued`, or `not_found_or_completed`. QueryFlux does not currently retain a lookup-by-id history of completed queries, so this only covers in-flight queries. |
| `cancel_query(query_id)` | Cancel a running or queued query. Only the agent/user that submitted it may cancel it (ownership-checked, same as every other owner-scoped operation in QueryFlux). |

Every tool that accepts `engine_hint` uses it as an exact cluster-group name when it matches a configured group; otherwise the query is routed normally through the configured `RouterChain` — the same routing behavior as every other frontend, just with an optional override.

Every tool except `get_query_status` / `cancel_query` also accepts the optional agent-context fields — `agent_id`, `conversation_id`, `step_index`, `tool_call_id`, `query_intent` — directly as tool parameters, in addition to the `X-Agent-Id` / `X-Conversation-Id` / etc. headers every other HTTP frontend already supports. See [Agentic context](/docs/agentic/agent-context#setting-context-via-mcp-tool-parameters) for why MCP gets both paths and which one wins when both are supplied.

## Guardrails

MCP queries flow through the exact same configurable `GuardChain` as every other frontend — `read_only`, `row_limit`, `require_predicate`, Python-script guards, and HTTP webhook guards all apply automatically to any group that serves MCP traffic, with no MCP-specific guard code involved. There is no default guard policy baked into the MCP frontend itself; if you want agent-facing queries restricted (e.g. read-only, row-capped), configure that the same way you would for any other frontend — see [Guardrails](../guardrails) for the full guard configuration reference.

Column/PII masking is not currently available for any frontend, MCP included — it's tracked as a separate, unimplemented design (row filtering and column masking via an external policy engine).

## Query history and session replay

Every MCP query is recorded the same way as every other frontend's queries, with the agent-context fields populated when present. This means MCP-originated queries show up in QueryFlux Studio's **Queries**, **Agents**, and **Conversations** pages exactly like agent traffic through Trino HTTP or PostgreSQL wire — no separate MCP-specific tooling needed to audit or replay what an agent did. See [Session replay and guardrails](/docs/agentic/session-replay) for the full persistence and replay model.

## Connecting a client

Point any streamable-HTTP MCP client at `http://<host>:<port>/mcp` with a bearer token. The `http://localhost:8811/mcp` example below is for local development only — QueryFlux does not terminate TLS itself (same as every other frontend), so a bearer token sent to a **remote** host must go through HTTPS or a TLS-terminating reverse proxy in front of QueryFlux; otherwise the token travels in cleartext.

For example, with the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
npx @modelcontextprotocol/inspector
# Transport: Streamable HTTP
# URL: http://localhost:8811/mcp   (local dev — use https:// through a TLS terminator remotely)
# Header: Authorization: Bearer <token>
```

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
- [Setting agent context](/docs/agentic/agent-context) — how MCP populates agent identity, via headers and tool parameters
- [Session replay and guardrails](/docs/agentic/session-replay) — what gets persisted and how to reconstruct an agent's session
- [Guardrails](../guardrails) — configuring `read_only`, `row_limit`, and other guards
