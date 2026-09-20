---
description: Guardrails — SQL-level safety controls for agentic and human queries, with per-group overrides, access control (`opa_access`), and full agentic audit trails.
---

# Guardrails

Guardrails are a configurable chain of safety checks that run on every query **before** it reaches a backend engine. They are designed primarily for agentic workloads — where an AI agent generates SQL dynamically — but apply equally to human clients.

Most built-in guards inspect the **translated** SQL (after dialect translation, before engine dispatch). Access control (`opa_access`) is the exception: it runs **before** translation on the client's source SQL so row filters and masks can ride the existing translation pass. Every guard returns one of:

| Verdict | Effect |
|---------|--------|
| `allow` | Query proceeds. |
| `warn`  | Query proceeds; a warning is recorded in the audit log. |
| `rewrite` | Query proceeds with modified SQL. Used by [access control](../access-control/overview) (`opa_access`) for row filters and column masks. |
| `deny`  | Query is blocked. A machine-readable error code is returned so agents can react programmatically. |

Every verdict is recorded in `guard_actions` on the query record, alongside a `was_guard_blocked` flag. Studio's **Queries** page shows the full trail — built-in guards and `opa_access` rewrites together — plus rewritten SQL separately from dialect-translated SQL.

---

## How the chain works

Guards are evaluated in order. **The chain stops at the first deny** — subsequent guards are skipped.

Two layers of guards compose per query:

1. **Global guards** — defined once, run for every query regardless of cluster group.
2. **Per-group guards** — appended after the global chain for queries routed to that group.

This lets you apply baseline safety globally (e.g. read-only for all agents) while tightening or relaxing rules for specific groups (e.g. a stricter row limit for an analytics group).

---

## Built-in guards

### `opa_access` (access control)

When `accessControl` is configured, QueryFlux registers **`opa_access`** as the rewriting guard in this same chain. It is not listed under `guardrails:` in YAML — it is enabled by the `accessControl:` block and asks OPA whether the verified identity may see each table, then **rewrites** or **denies**.

| Config | Guard name | When it runs | Studio |
| --- | --- | --- | --- |
| `accessControl:` | `opa_access` | Before dialect translation | Guard Actions (`rewrite` / `deny`) + **Rewritten SQL (access control)** panel |

Access-control **policy** (Rego, filters, masks, dry-run) is documented under **[Access control](../access-control/overview)** — provider docs: **[OPA](../access-control/opa)**. Observability is the same `guard_actions` array as every other guard.

When access control is on, the rewriting guard is `opa_access`.

**Scope** mirrors guardrails' global + `groups` pattern, but lives under `accessControl:` (edited on Studio **Access Control**, not **Guardrails** or **Clusters**): global `enabled` plus optional `groups.<clusterGroup>.enabled` to skip OPA for sandbox groups or opt in only where needed. Policy providers are configured as named `connections` (each with its own OPA endpoint, operations, cache and fail-open setting): a group uses its own `groups.<clusterGroup>.connection` if set, otherwise `defaultConnection`, and a group that resolves to no connection gets no access control.

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

For logic that can't be expressed as a built-in rule, a `python_script` guard runs a script you author against every query. The script must define a top-level `check(ctx)` function:

```yaml
guardrails:
  global:
    - kind: python_script
      script: |
        def check(ctx):
            if "cross_region" in ctx["query_tags"]:
                return {"action": "deny", "reason": "cross-region query blocked", "code": "CROSS_REGION"}
            return {"action": "allow"}
      timeout_ms: 500   # default 1000, capped at 30000
```

`ctx` is a dict built from the **source** SQL (before dialect translation): `sql`, `dialect`, `engine_type`, `cluster_group`, `user`, `groups`, `roles`, `attributes`, `agent_context`, `query_tags`. `check(ctx)` must return `None` (allow) or a dict `{"action": "allow" | "warn" | "deny", "reason"?, "code"?, "metadata"?}`.

The script runs off the async runtime in a blocking task; a script that exceeds `timeout_ms` is denied with `PYTHON_GUARD_TIMEOUT` (the task is aborted best-effort — native/FFI code already in flight can't be interrupted mid-call). A script error or a malformed return value denies with `PYTHON_GUARD_ERROR`. Studio-managed scripts are referenced by `script_id` rather than inlined; `script` (inline) takes precedence when both are set.

---

## HTTP webhook guards

Delegate a guard decision to an external service:

```yaml
guardrails:
  global:
    - kind: http_webhook
      url: "https://hooks.example.com/guard"
      timeout_ms: 5000   # default 1000, capped at 30000
      retry_count: 2     # retries on 5xx only; default 0
      fail_behavior: deny   # deny (default) | allow, when unreachable/erroring
      headers:
        Authorization: "Bearer ..."
```

QueryFlux `POST`s the same `ctx` payload described above as JSON and expects `{"action": "allow" | "warn" | "deny", "reason"?, "code"?, "metadata"?}` back. `fail_behavior` controls what happens after all attempts are exhausted (unreachable, timeout, non-2xx, or an unparseable body) — `deny` (default) is the safer choice for production. `retry_count` retries a timeout/connection error or a `5xx` response; a `4xx` response fails immediately without retrying.

Both guard kinds are enforced identically whether configured via YAML (`guardrails.global` / `guardrails.groups`) or persisted through Studio's **Guardrails** page.

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

The **Guardrails** page in QueryFlux Studio provides a live editor for the SQL-shape chain (built-ins such as `read_only` and `row_limit`). Changes are applied without a proxy restart.

The **Access Control** page connects QueryFlux to OPA (URL, decision path, credentials) and sets **scope by cluster group** (global default + per-group inherit / enabled / disabled). Grants still live in the OPA bundle. What Studio *does* combine on the **Queries** page is the audit trail:

- **Guard Actions** lists every guard, including `opa_access` with a **rewritten** badge and metadata (`tables`, `row_filtered`, `masked_columns`).
- **Rewritten SQL (access control)** shows the source-dialect SQL after filters/masks.
- **Translated SQL** shows dialect translation only (a query can be both rewritten and translated).

---

## Observability

Guard decisions are recorded in `guard_actions` (JSONB array) on every `query_records` row. Each element has `guard`, `action`, `reason`, `code`, and optional `metadata` (used by `opa_access` for which tables were filtered or masked). The `was_guard_blocked` boolean column is indexed for fast filtering. Access-control rewrites also set `was_rewritten` / `rewritten_sql` on the query record (distinct from `was_translated` / `translated_sql`).

```sql
-- Recent blocked queries
SELECT created_at, sql, guard_actions
FROM query_records
WHERE was_guard_blocked = TRUE
ORDER BY created_at DESC
LIMIT 50;
```
