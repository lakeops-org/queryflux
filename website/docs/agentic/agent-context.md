---
sidebar_label: Setting agent context
description: How to attach agent identity and conversation state to queries — via HTTP headers, SQL session params, or MCP tool parameters, depending on the frontend.
---

# Setting agent context

Agentic context can be set in three ways depending on which frontend protocol the agent uses:

- **HTTP headers** — for Trino HTTP, Snowflake HTTP, ClickHouse HTTP, and [MCP](../architecture/frontends/mcp) frontends.
- **SQL session params** — for MySQL wire and PostgreSQL wire frontends, where HTTP headers are not available.
- **MCP tool parameters** — the [MCP](../architecture/frontends/mcp) frontend additionally accepts these fields as explicit tool-call arguments, since not every MCP client lets you set custom headers per call.

All three approaches use the same underlying fields and produce identical records in query history — see [Session replay and guardrails](session-replay) for what gets persisted and how to reconstruct an agent's session afterward.

---

## Setting context via HTTP headers

HTTP frontends accept agentic context as request headers. Both `X-Agent-Id` and `X-Conversation-Id` must be present to activate agentic context — if either is missing the query is treated as a non-agentic request. **MCP is the one exception** — see [Agent context defaults on MCP](#agent-context-defaults-on-mcp) below.

| Header | Required | Description |
|--------|----------|-------------|
| `X-Agent-Id` | Yes | Stable identifier for the agent instance. |
| `X-Conversation-Id` | Yes | Groups all queries from one agent session together. |
| `X-Step-Index` | No | Integer position of this query within the conversation. |
| `X-Tool-Call-Id` | No | The tool-call ID from the agent framework that triggered this query. |
| `X-Query-Intent` | No | Hint about what the agent is trying to do. See [intent values](#query-intent). |

```http
POST /v1/statement HTTP/1.1
X-Trino-User: analyst
X-Agent-Id: my-agent-v2
X-Conversation-Id: conv-7f3a9b
X-Step-Index: 4
X-Tool-Call-Id: call_abc123
X-Query-Intent: aggregation

SELECT region, COUNT(*) FROM orders WHERE date > DATE '2026-01-01' GROUP BY 1
```

---

## Setting context via SQL session params

MySQL wire and PostgreSQL wire clients cannot set HTTP headers. Instead, pass agentic context as session parameters using the snake_case equivalents of the header names.

### MySQL wire

Issue `SET` statements before your query. QueryFlux intercepts them and updates the session — no round-trip to the backend occurs. Both `agent_id` and `conversation_id` must be set to activate agentic context.

```sql
SET agent_id = 'my-agent-v2';
SET conversation_id = 'conv-7f3a9b';
SET step_index = '4';
SET tool_call_id = 'call_abc123';
SET query_intent = 'aggregation';

SELECT region, COUNT(*) FROM orders WHERE date > DATE '2026-01-01' GROUP BY 1;
```

`SET SESSION` and `SET @@session.` prefixes are also accepted. Values persist for the lifetime of the connection and are re-applied to every subsequent query on that session.

### PostgreSQL wire

Pass the parameters in the connection string as startup parameters. Most clients support extra parameters via the `options` field or named parameters:

```text
postgresql://host:5432/db?agent_id=my-agent-v2&conversation_id=conv-7f3a9b&step_index=4&query_intent=aggregation
```

Or with psql:

```bash
psql "host=localhost port=5432 dbname=mydb agent_id=my-agent-v2 conversation_id=conv-7f3a9b"
```

Parameters are extracted once at connection time.

---

## Setting context via MCP tool parameters

The MCP frontend accepts `X-Agent-Id` / `X-Conversation-Id` / etc. as HTTP headers on the streamable-HTTP request, exactly like the other HTTP frontends. But many MCP clients (Claude Desktop and similar consumer hosts) only let you configure headers once for the whole server connection, not per tool call — which makes headers alone a poor fit for per-query agent identity.

To cover that case, every MCP tool (`execute_query`, `list_schemas`, `describe_table`, `explain_query`) also accepts the same fields as explicit, optional arguments:

```json
{
  "name": "execute_query",
  "arguments": {
    "sql": "SELECT region, COUNT(*) FROM orders WHERE date > DATE '2026-01-01' GROUP BY 1",
    "agent_id": "my-agent-v2",
    "conversation_id": "conv-7f3a9b",
    "step_index": 4,
    "tool_call_id": "call_abc123",
    "query_intent": "aggregation"
  }
}
```

When a value is supplied both ways — an `X-Agent-Id` header on the connection *and* an `agent_id` tool argument on the call — the **header wins**, matching the existing precedence between HTTP-header-style and SQL-session-param-style values used by every other frontend.

This mirrors how the wider MCP ecosystem is moving away from relying on transport-level session state for anything that needs to persist across tool calls, in favor of explicit, model-visible arguments — the tool parameters are the reliable path for any MCP client, headers are a free bonus for integrators who control their own HTTP client.

### Agent context defaults on MCP

Every other frontend requires **both** `agent_id` and `conversation_id` to activate agentic context — if a client supplies neither, the query is just a normal, non-agentic query. MCP does not follow that rule: since MCP traffic is agent traffic by definition, a tool call that supplies neither field still gets agent context, so it isn't silently invisible on the **Agents** page.

When `agent_id` / `conversation_id` aren't supplied via header or tool parameter, MCP fills them in:

| Field | Default |
|-------|---------|
| `agent_id` | The authenticated identity (`auth.user` — e.g. `"anonymous"` under `auth.provider: none`). |
| `conversation_id` | The transport's `Mcp-Session-Id`, so every tool call within one MCP session groups together. Falls back to a fresh UUID per call if no session id is available (e.g. a stateless client). |

Explicit headers and tool parameters still override both defaults — this only fills gaps, it never replaces a value you actually sent. Because `conversation_id` defaults to the session id, a client that never sets it explicitly still gets meaningful session-level grouping on the **Conversations** page for free, for as long as its MCP session lives; a client that wants grouping across multiple MCP sessions (or a stable identity independent of transport reconnects) should still set `conversation_id` explicitly.

---

## Query intent

`X-Query-Intent` (HTTP) or `query_intent` (SQL) classifies what the agent is trying to accomplish. When omitted, QueryFlux infers intent from the SQL using a lightweight heuristic.

| Value | Meaning |
|-------|---------|
| `schema_exploration` | Agent is discovering table structure — `SELECT *` without a `WHERE`. |
| `aggregation` | Agent is running an aggregate query (`COUNT`, `SUM`, `GROUP BY`). |
| `lookup` | Agent is fetching specific rows via a `WHERE` predicate. |
| `mutation` | Agent is attempting a write (`INSERT`, `UPDATE`, `DELETE`, DDL). |
| `unknown` | Intent could not be determined. |

Intent is stored on the query record and visible in Studio. It can also inform guardrail logic — a Python script guard can read `ctx["agent_context"]["query_intent"]` and apply stricter rules to `schema_exploration` queries on large tables.

Next: [Session replay and guardrails](session-replay) covers what gets persisted and how to reconstruct a full agent session from query history.
