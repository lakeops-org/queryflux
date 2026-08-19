---
description: Guardrails — SQL-level safety controls for agentic and human queries, with per-group overrides, Python script guards, and full agentic audit trails.
---

# Guardrails

Guardrails are a configurable chain of safety checks that run on every query **before** it reaches a backend engine. They are designed primarily for agentic workloads — where an AI agent generates SQL dynamically — but apply equally to human clients.

Each guard inspects the translated SQL (after dialect translation, before engine dispatch) and returns one of three verdicts:

| Verdict | Effect |
|---------|--------|
| `allow` | Query proceeds. |
| `warn`  | Query proceeds; a warning is recorded in the audit log. |
| `deny`  | Query is blocked. A machine-readable error code is returned so agents can react programmatically. |

Every verdict is recorded in `guard_actions` on the query record, alongside a `was_guard_blocked` flag, making the full guard history queryable from Studio and the Admin API.

---

## How the chain works

Guards are evaluated in order. **The chain stops at the first deny** — subsequent guards are skipped.

Two layers of guards compose per query:

1. **Global guards** — defined once, run for every query regardless of cluster group.
2. **Per-group guards** — appended after the global chain for queries routed to that group.

This lets you apply baseline safety globally (e.g. read-only for all agents) while tightening or relaxing rules for specific groups (e.g. a stricter row limit for an analytics group).

---

## Built-in guards

### `read_only`

Blocks any statement that is not a `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, or `EXPLAIN`. Guards against agents issuing accidental `INSERT`, `UPDATE`, `DELETE`, or DDL.

```yaml
guardrails:
  global:
    - kind: built_in
      name: read_only
```

Error code on deny: `READ_ONLY_VIOLATION`

---

### `row_limit`

Requires the outermost query to have a `LIMIT` clause. Optionally enforces a maximum.

- **No LIMIT present** → `warn` (query still runs, but the warning is recorded).
- **LIMIT present but exceeds `max_rows`** → `deny`.

```yaml
guardrails:
  global:
    - kind: built_in
      name: row_limit
      max_rows: 10000
```

Error code on deny: `ROW_LIMIT_EXCEEDED`

The check is applied to the **outermost** query only — a subquery with `LIMIT 9999` inside a `SELECT … LIMIT 10` outer query correctly passes a `max_rows: 1000` guard.

---

### `require_predicate`

Rejects `SELECT` statements that have no `WHERE` clause. Prevents full table scans that can scan billions of rows and generate large cloud bills.

Use `applies_to` to restrict the check to specific table name patterns (glob syntax, `*` matches any sequence):

```yaml
guardrails:
  global:
    - kind: built_in
      name: require_predicate
      applies_to:
        - "fct_*"
        - "events.*"
```

With an empty `applies_to` list (or omitted), the guard applies to **all** tables.

Error code on deny: `MISSING_PREDICATE`

---

## Per-group overrides

Per-group guards are appended after the global chain. This is useful for giving different agent pools different safety profiles:

```yaml
guardrails:
  global:
    - kind: built_in
      name: read_only
  groups:
    agents:
      - kind: built_in
        name: row_limit
        max_rows: 5000
      - kind: built_in
        name: require_predicate
    analysts:
      - kind: built_in
        name: row_limit
        max_rows: 100000
```

Queries routed to the `agents` group run: `read_only` → `row_limit(5000)` → `require_predicate`.
Queries routed to the `analysts` group run: `read_only` → `row_limit(100000)`.

---

## Python script guards

> **Note:** Python script guards are not yet executed at runtime. The guard kind is accepted in configuration and stored, but the script body is skipped during dispatch. Use built-in guards or HTTP webhook guards for production safety rules.

For logic that can't be expressed as a built-in rule, Python script guards can be authored through the QueryFlux Studio **Guardrails** page. The script receives a `ctx` dict with `sql`, `translated_sql`, `engine_type`, `cluster_group`, `user`, and `agent_context` fields, and must return:

```python
# allow
return {"action": "allow"}

# warn
return {"action": "warn", "reason": "large join detected"}

# deny
return {"action": "deny", "reason": "cross-region query blocked", "code": "CROSS_REGION"}
```

---

## HTTP webhook guards

> **Note:** HTTP webhook guards are not yet executed at runtime. The guard kind is accepted in configuration, but the webhook is not called during dispatch.

Delegate guard decisions to an external service:

```yaml
guardrails:
  global:
    - kind: http_webhook
      url: "https://hooks.example.com/guard"
      timeout_ms: 5000
      fail_behavior: deny   # or: allow
```

QueryFlux POSTs the query context as JSON and expects the same `{action, reason?, code?}` response shape. `fail_behavior` controls what happens if the webhook is unreachable or times out — `deny` (default) is the safer choice for production.

---

## Agentic context

Every query record can carry agentic metadata when the caller is an AI agent:

| Field | Description |
|-------|-------------|
| `agent_id` | Stable identifier for the agent instance. |
| `conversation_id` | Groups all queries from one agent session. |
| `step_index` | Position of this query within the conversation. |
| `tool_call_id` | The specific tool-call that triggered the query. |
| `query_intent` | Free-text description of what the agent was trying to do. |

These fields are indexed in Postgres, so you can replay an agent's full session — every query it ran, in order, and every guard decision — directly from Studio or the Admin API.

```sql
-- All queries from a single agent conversation, in order
SELECT sql, query_intent, was_guard_blocked, guard_actions
FROM query_records
WHERE conversation_id = 'conv-abc123'
ORDER BY step_index;
```

---

## Configuring guardrails in Studio

The **Guardrails** page in QueryFlux Studio provides a live editor for the guard chain. Changes are applied without a proxy restart. Built-in guards can be toggled and parameterized; Python script guards can be written and tested in the browser.

---

## Observability

Guard decisions are recorded in `guard_actions` (JSONB array) on every `query_records` row. Each element has `guard`, `action`, `reason`, and `code` fields. The `was_guard_blocked` boolean column is indexed for fast filtering:

```sql
-- Recent blocked queries
SELECT created_at, sql, guard_actions
FROM query_records
WHERE was_guard_blocked = TRUE
ORDER BY created_at DESC
LIMIT 50;
```
