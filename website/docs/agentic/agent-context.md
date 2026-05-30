---
description: Agentic context — attaching agent identity and conversation state to queries via HTTP headers or SQL session params, with full session replay and audit from QueryFlux Studio.
---

# Agentic context

When an AI agent queries through QueryFlux, it can identify itself and attach conversation state to every query. QueryFlux persists this context alongside every query record so you can replay an agent's full session, correlate queries across steps, and audit what the agent tried — including any guardrail decisions.

Agentic context can be set in two ways depending on which frontend protocol the agent uses:

- **HTTP headers** — for Trino HTTP, Snowflake HTTP, and ClickHouse HTTP frontends.
- **SQL session params** — for MySQL wire and PostgreSQL wire frontends, where HTTP headers are not available.

Both approaches use the same underlying fields and produce identical records in query history.

---

## Setting context via HTTP headers

HTTP frontends accept agentic context as request headers. Both `X-Agent-Id` and `X-Conversation-Id` must be present to activate agentic context — if either is missing the query is treated as a non-agentic request.

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

---

## What gets persisted

Each query record in Postgres stores:

| Column | Type | Description |
|--------|------|-------------|
| `agent_id` | `TEXT` | From `X-Agent-Id` or `SET agent_id`. |
| `conversation_id` | `TEXT` | From `X-Conversation-Id` or `SET conversation_id`. |
| `step_index` | `INTEGER` | From `X-Step-Index` or `SET step_index`. |
| `tool_call_id` | `TEXT` | From `X-Tool-Call-Id` or `SET tool_call_id`. |
| `query_intent` | `TEXT` | From `X-Query-Intent`, `SET query_intent`, or inferred. |
| `guard_actions` | `JSONB` | Ordered list of guard verdicts for this query. |
| `was_guard_blocked` | `BOOLEAN` | `true` if any guard denied the query. |

`agent_id`, `conversation_id`, and `was_guard_blocked` are indexed for efficient lookup.

---

## Replaying a session

With conversation ID and step index you can reconstruct exactly what an agent did:

```sql
-- Full session replay in order
SELECT
    step_index,
    query_intent,
    sql,
    status,
    was_guard_blocked,
    guard_actions
FROM query_records
WHERE conversation_id = 'conv-7f3a9b'
ORDER BY step_index;
```

```sql
-- All queries an agent was blocked on
SELECT created_at, sql, guard_actions
FROM query_records
WHERE agent_id = 'my-agent-v2'
  AND was_guard_blocked = TRUE
ORDER BY created_at DESC;
```

QueryFlux Studio shows agentic context inline on the **Queries** page — conversation ID, step index, intent, and the full guard action trail are visible per query without writing SQL.

---

## Using guardrails with agentic workloads

Guardrails are a general-purpose SQL safety layer, but they integrate with agentic context in two ways:

1. **Guard decisions are recorded per query** — every `allow`, `warn`, and `deny` is stored in `guard_actions`, so the agent session replay includes the full safety audit trail.
2. **Python script guards can inspect agent context** — the `ctx` dict passed to a script guard includes `agent_context` with all fields above, so you can write rules that behave differently for agents vs. human clients.

See [Guardrails](../architecture/guardrails) for the full guard configuration reference.
