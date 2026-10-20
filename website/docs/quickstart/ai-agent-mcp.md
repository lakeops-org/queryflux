---
sidebar_position: 5
sidebar_label: AI agent (MCP)
title: Query from an AI agent (MCP)
description: Point Claude Code, Cursor, or MCP Inspector at the QueryFlux MCP frontend and run SQL on embedded DuckDB through an AI agent.
image: img/queryflux-hero-banner.png
---
# Query from an AI agent (MCP)

In this quickstart you run QueryFlux with the **MCP (Model Context Protocol) frontend** enabled, backed by the embedded DuckDB engine, then connect an AI agent to it. No Trino, Postgres, or other external service is needed. At the end you have an agent running SQL through QueryFlux and its queries showing up in Studio.

## What you'll build

```mermaid
flowchart LR
  agent["MCP client<br/>(Claude Code, Cursor,<br/>MCP Inspector)"] -->|"streamable HTTP<br/>:8811/mcp"| qf["QueryFlux<br/>MCP frontend"]
  qf --> duck["Embedded DuckDB<br/>(in-process)"]
  studio["Studio<br/>:3000"] -->|"Admin API :9000"| qf
```

| Service | URL |
| --- | --- |
| MCP endpoint (streamable HTTP) | `http://localhost:8811/mcp` |
| Admin API | `http://localhost:9000` |
| Studio | `http://localhost:3000` |

## Prerequisites

- **Docker** and **Docker Compose**
- An MCP client. Any one of: [Claude Code](https://docs.claude.com/en/docs/claude-code), Cursor, Claude Desktop, or Node.js (`npx`) for the [MCP Inspector](https://github.com/modelcontextprotocol/inspector)
- Ports **8811**, **9000**, and **3000** free on your machine

## 1. Start the stack

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux/examples/with-mcp
docker compose up -d --wait
```

This starts two containers: `queryflux` (MCP frontend on 8811, Admin API on 9000) and `queryflux-studio` (on 3000). `--wait` returns once QueryFlux passes its `/health` check.

:::note No token needed
This example runs with no authentication, so the MCP endpoint accepts requests with any `Authorization` header, or none. For a real deployment, see [Secure QueryFlux with Keycloak (OIDC)](./oidc.md) — the MCP frontend validates bearer tokens with the same auth provider as every other frontend.
:::

## 2. Understand the config

The relevant parts of [`examples/with-mcp/config.yaml`](https://github.com/lakeops-org/queryflux/blob/main/examples/with-mcp/config.yaml):

```yaml title="examples/with-mcp/config.yaml"
queryflux:
  externalAddress: http://localhost:8811
  frontends:
    trinoHttp:
      enabled: false
      port: 8080
    mcp:
      enabled: true
      port: 8811
      allowWrites: true
  persistence:
    type: inMemory

clusters:
  duckdb-1:
    engine: duckDb
    enabled: true

clusterGroups:
  duckdb-local:
    enabled: true
    maxRunningQueries: 4
    members: [duckdb-1]

routers:
  - type: protocolBased
    mcp: duckdb-local

routingFallback: duckdb-local
```

- `frontends.mcp` turns on the MCP frontend. It has no default port, so `port: 8811` is set explicitly. The endpoint is always served at the `/mcp` path.
- `allowWrites: true` lets this demo create tables. The MCP endpoint is **read-only by default**: `execute_query` rejects anything that isn't a read, and the `execute_statement` write tool isn't advertised at all. Leave `allowWrites` off for agents that only need to read. See [Read-only by default](../architecture/frontends/mcp.md#read-only-by-default).
- `duckdb-1` is an **embedded** DuckDB engine: it runs inside the QueryFlux process, so there is no endpoint to configure.
- The `protocolBased` router sends all MCP traffic to the `duckdb-local` group. MCP tool calls go through the same routing, translation, guardrail, and query-history pipeline as every other frontend.

## 3. Connect an agent

Point any streamable-HTTP MCP client at `http://localhost:8811/mcp`. QueryFlux offers streamable HTTP only, not stdio.

**Claude Code**

```bash
claude mcp add --transport http queryflux http://localhost:8811/mcp
```

Add `--scope user` to make it available in every project. Run `/mcp` inside Claude Code to confirm it is connected.

**Cursor** — `.cursor/mcp.json` (project) or the global MCP settings:

```json title=".cursor/mcp.json"
{ "mcpServers": { "queryflux": { "url": "http://localhost:8811/mcp" } } }
```

**Claude Desktop** — `claude_desktop_config.json` only accepts stdio servers, so bridge with [`mcp-remote`](https://www.npmjs.com/package/mcp-remote):

```json title="claude_desktop_config.json"
{ "mcpServers": { "queryflux": { "command": "npx", "args": ["mcp-remote@latest", "http://localhost:8811/mcp"] } } }
```

(**Settings → Connectors → Add custom connector** works only for an endpoint Claude can reach over the internet, not `localhost`. See the [MCP frontend docs](../architecture/frontends/mcp.md#claude-desktop).)

**MCP Inspector** (no agent, just a check):

```bash
npx @modelcontextprotocol/inspector   # Transport: Streamable HTTP, URL: http://localhost:8811/mcp
```

Then paste this into your agent:

```text
Use the queryflux MCP server. Discover data with list_schemas, list_tables and describe_table before querying, run reads with execute_query, always add a LIMIT, and if a guardrail rejects a query, fix it the way the error says and retry.
```

:::tip With authentication enabled
When QueryFlux requires auth, every client needs an `Authorization: Bearer <token>` header — for example `--header "Authorization: Bearer $QUERYFLUX_MCP_TOKEN"` on `claude mcp add`, or a `"headers"` object in Cursor's `mcp.json`. Read the token from an environment variable rather than committing it. See [Connecting a client](../architecture/frontends/mcp.md#connecting-a-client) for every client's exact syntax.
:::

## 4. Run queries through the agent

QueryFlux exposes these MCP tools. The annotation column is what MCP clients see; most auto-approve read-only tools and ask before a destructive one.

| Tool | Annotation | What it does |
| --- | --- | --- |
| `execute_query` | read-only | Runs a read (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`) and returns rows as JSON keyed by column name. Truncates at `max_rows` (default 1000). Rejects writes. |
| `execute_statement` | destructive | Runs writes and DDL. Only listed when `allowWrites: true`, and each call needs `confirm: true`. |
| `list_schemas` | read-only | Lists schemas from `information_schema.schemata`. |
| `list_tables` | read-only | Lists tables and views from `information_schema.tables`, optionally for one `catalog` and/or `schema`. |
| `describe_table` | read-only | Returns column metadata via `DESCRIBE`, plus sample rows in the same call when `sample_rows > 0`. |
| `explain_query` | read-only | Returns the query plan via `EXPLAIN` without running the query. |
| `get_query_status` | read-only | Reports whether a query is `running`, `queued`, or `not_found_or_completed`. |
| `cancel_query` | idempotent | Cancels a running or queued query. Only the caller that submitted it can cancel it. |

Ask your agent something like:

> Create a table called `orders` with a few sample rows, describe its columns, then show me everything in it.

The agent calls `execute_statement` (CREATE + INSERT). The first call without `confirm: true` comes back with an error asking it to check with you, so expect it to show you the statement and ask. Then it calls `describe_table`, then `execute_query` (SELECT). Without `allowWrites`, the agent would get an error saying writes are disabled on this endpoint.

To drive the tools by hand from the Inspector instead, call `execute_statement` with `confirm: true` and this `sql`:

```sql
CREATE TABLE orders AS
SELECT * FROM (VALUES (1, 'widget', 9.99), (2, 'gadget', 19.99)) AS t(id, name, price);
```

Then call `execute_query` with:

```sql
SELECT * FROM orders;
```

The result is a JSON object with `columns`, `rows`, `row_count`, `truncated`, `elapsed_ms`, and `engine`, plus a `conversation_id`. Agents should pass that `conversation_id` back on their next call so QueryFlux groups the calls together.

:::note Data is not persisted
DuckDB here is in-memory and lives inside the QueryFlux process, and `persistence.type: inMemory` keeps query history in memory too. Everything is gone after `docker compose down`.
:::

### Optional tool parameters

- `engine_hint` — name of a cluster group to send the query to. If it doesn't match a configured group, normal routing applies.
- `dialect` — set this when the SQL was written for a different engine (for example `trino` or `postgres`) and should be translated before routing. By default, MCP does **no** translation and sends the SQL to the engine as written.
- `agent_id`, `conversation_id`, `step_index`, `tool_call_id`, `query_intent` — agent context fields (not on `get_query_status` / `cancel_query`). If the agent doesn't send `agent_id` / `conversation_id`, MCP fills them in from the authenticated user and the MCP session ID. See [Setting agent context](../agentic/agent-context.md#setting-context-via-mcp-tool-parameters).

## 5. Inspect agent activity in Studio

Open **http://localhost:3000**. MCP queries appear on the **Queries** page like any other frontend's traffic, and on the **Agents** and **Conversations** pages, grouped by `agent_id` and `conversation_id`. See [Session replay and guardrails](../agentic/session-replay.md) for what gets recorded.

## 6. (Optional) Add guardrails

Apart from the read/write split, nothing in this example limits what an agent can run: no row caps, no scan limits. You add guardrails the same way as for any other frontend, with a `guardrails:` block in `config.yaml` (for example `read_only` or `row_limit`). See [Guardrails](../architecture/guardrails.md), then restart the `queryflux` container to pick up the change:

```bash
docker compose up -d --force-recreate queryflux
```

## Clean up

```bash
docker compose down
```

## Next steps

- [Blog: Give agents SQL, not the keys to every engine](/blog/agents-sql-not-keys) — routing, guardrails, and impersonation for agents
- [MCP frontend](../architecture/frontends/mcp.md) — full tool reference, the `dialect` parameter, OIDC tokens for MCP clients, and known limitations
- [Agentic AI overview](../agentic/overview.md) — how QueryFlux tracks agent identity and conversations across frontends
- [Setting agent context](../agentic/agent-context.md) and [Session replay and guardrails](../agentic/session-replay.md)
- [Secure QueryFlux with Keycloak (OIDC)](./oidc.md) — require a bearer token on every request
- [Access control](../access-control/overview.md) — row filters and column masks that apply to MCP traffic too
- [Installation](../installation/overview.md) — run QueryFlux on Kubernetes, Docker, or as a binary
