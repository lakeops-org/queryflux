---
sidebar_label: Session replay & guardrails
description: What agentic context QueryFlux persists per query, how to replay an agent's full session from query history, and how guardrails integrate with agent traffic.
---

# Session replay and guardrails

Once agent context is attached to queries — see [Setting agent context](agent-context) — QueryFlux persists it alongside every query record, giving you a full audit trail you can query directly or browse in Studio.

---

## What gets persisted

Each query record in Postgres stores:

| Column | Type | Description |
|--------|------|-------------|
| `agent_id` | `TEXT` | From `X-Agent-Id` or `SET agent_id`. |
| `conversation_id` | `TEXT` | From `X-Conversation-Id` or `SET conversation_id`. |
| `step_index` | `INTEGER` | From `X-Step-Index` or `SET step_index`. |
| `tool_call_id` | `TEXT` | From `X-Tool-Call-Id` or `SET tool_call_id`. |
| `query_intent` | `TEXT` | From `X-Query-Intent`, `SET query_intent`, or inferred. See [query intent values](agent-context#query-intent). |
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

QueryFlux Studio shows agentic context inline on the **Queries** page — conversation ID, step index, intent, and the full guard action trail are visible per query without writing SQL. The **Agents** and **Conversations** pages group query history by `agent_id` and `conversation_id` directly, without hand-written SQL.

---

## Using guardrails with agentic workloads

Guardrails are a general-purpose SQL safety layer — they apply to every frontend identically, agent traffic included, with no agent-specific policy layer. They integrate with agentic context in two ways:

1. **Guard decisions are recorded per query** — every `allow`, `warn`, and `deny` is stored in `guard_actions`, so the agent session replay above includes the full safety audit trail.
2. **Python script guards can inspect agent context** — the `ctx` dict passed to a script guard includes `agent_context` with all the fields above, so you can write rules that behave differently for agents vs. human clients (e.g. stricter row limits for `schema_exploration` queries from an unrecognized `agent_id`).

See [Guardrails](../architecture/guardrails) for the full guard configuration reference, and [MCP Frontend](../architecture/frontends/mcp) for how this applies to MCP-originated queries specifically — MCP has no guard policy of its own, it flows through the same `GuardChain` as everything else.
