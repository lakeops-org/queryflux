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
| `rewrite` | Query proceeds with modified SQL. Used by [access control](../access-control/overview) (`opa_access`) for row filters and column masks, and by `row_limit` with `inject_limit` to add or lower a `LIMIT`. |
| `deny`  | Query is blocked. The client gets the guard name, a machine-readable code and a hint on how to fix the query, so agents can correct it and retry. |

Every verdict is recorded in `guard_actions` on the query record, alongside a `was_guard_blocked` flag. Studio's **Queries** page shows the full trail — built-in guards and `opa_access` rewrites together — plus rewritten SQL separately from dialect-translated SQL.

---

## How the chain works

Guards are evaluated in order. **The chain stops at the first deny** — subsequent guards are skipped. When a guard rewrites the query, the guards after it check the rewritten SQL, and the rewritten SQL is what the engine runs.

A blocked query fails with a message of this form:

```text
blocked by guard partition_predicate_required [PARTITION_PREDICATE_REQUIRED]: query on events must filter on its partition column dt; add a WHERE condition on dt (e.g. dt = <value> or dt >= <start>)
```

The reasons are written for the caller: they say what to change. An agent that reads the error can fix its SQL and retry instead of giving up.

To check a query without running it, use [route explain](./routing-and-clusters#route-explain): it returns the guard verdict (allow, rewrite with the new SQL, or deny with this message) along with the routing decision.

Two layers of guards compose per query:

1. **Global guards** — defined once, run for every query regardless of cluster group.
2. **Per-group guards** — appended after the global chain for queries routed to that group.

This lets you apply baseline safety globally (e.g. read-only for all agents) while tightening or relaxing rules for specific groups (e.g. a stricter row limit for an analytics group).

---

## Table patterns

`require_predicate` (`applies_to`), `partition_predicate_required` and `time_range_limit` (`tables`) pick tables with patterns. A pattern matches a table name exactly, ignoring case, and `*` matches any run of characters:

| Pattern | Matches | Does not match |
|---|---|---|
| `events` | `events`, `analytics.events` | `events_archive`, `user_events` |
| `fct_*` | `fct_sales`, `dw.fct_sales` | `stage_fct_sales` |
| `dw.fct_*` | `dw.fct_sales` | `fct_sales`, `stage.fct_sales` |
| `*_events` | `page_events`, `raw.page_events` | `events_raw` |

A pattern without a `.` is compared with the table name alone, whatever the schema. A pattern with a `.` is compared with `schema.table`. The catalog part of a three-part name is ignored.

:::note Changed in 0.5.0
Before 0.5.0, a `require_predicate` pattern without `*` matched any table whose name *contained* it (`events` also matched `user_events`). Patterns now match exactly; add `*` where you relied on the old behavior (`*events*`).
:::

Guards run on the SQL sent to the engine, after dialect translation, so patterns see table names as the engine sees them.

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

The check looks inside reads too: `SELECT … INTO`, a CTE that deletes or updates (`WITH d AS (DELETE … RETURNING *) SELECT …`), `EXPLAIN ANALYZE` of a write (which runs the write on several engines), and procedure calls such as `CALL` are blocked. SQL that can't be parsed is blocked if it contains a write keyword.

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

Set `inject_limit: true` to fix the query instead of warning or denying. This suits agents: they get a bounded result rather than an error.

- **No LIMIT present** → `rewrite`, adding `LIMIT default_limit` (or `LIMIT max_rows` when `default_limit` is not set).
- **LIMIT above `max_rows`** → `rewrite`, lowering it to `max_rows`.

```yaml
guardrails:
  groups:
    agents:
      - kind: built_in
        name: row_limit
        max_rows: 10000
        inject_limit: true
        default_limit: 1000
```

The rewrite is recorded in `guard_actions` with the injected `limit` in its metadata. A query a guard rewrites is never served from the [result cache](./caching), whose key is the client's SQL. Batches of several statements are not rewritten; they get the warn/deny behavior.

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

### `partition_predicate_required`

Requires queries on chosen tables to filter on the table's partition column, so they read a few partitions instead of the whole table. `tables` maps a table pattern (same glob syntax as `applies_to`) to its partition column:

```yaml
guardrails:
  groups:
    agents:
      - kind: built_in
        name: partition_predicate_required
        tables:
          "events": dt
          "fct_*": event_date
```

Every `SELECT` that reads a matching table (in `FROM` or a `JOIN`, including inside CTEs and subqueries) must reference the column in its `WHERE`. Any condition on the column counts (`=`, `>=`, `BETWEEN`, `IN`).

The filter has to be on the `SELECT` that reads the table. A filter in an outer query does not count for a CTE or subquery that reads the table:

```sql
-- denied: the CTE reads events without a filter on dt
WITH e AS (SELECT * FROM events) SELECT * FROM e WHERE dt = '2026-10-01';
-- allowed
WITH e AS (SELECT * FROM events WHERE dt = '2026-10-01') SELECT * FROM e;
```

The engine might push the outer filter down, but the guard does not assume it. The deny message names the table and column, so an agent can move the filter.

Error code on deny: `PARTITION_PREDICATE_REQUIRED`

---

### `time_range_limit`

Bounds how far back queries on chosen tables may read. `tables` maps a table pattern to its time column, and `max_lookback` is the limit (`36h`, `90d`, `12w`, `1y`):

```yaml
guardrails:
  groups:
    agents:
      - kind: built_in
        name: time_range_limit
        tables:
          "events": ts
        max_lookback: 30d
```

A query on a matching table must give the column a lower bound (`ts >= …`, `ts > …`, or `ts BETWEEN … AND …`):

- **No lower bound** → `deny` with `TIME_RANGE_REQUIRED`.
- **A literal date older than now minus `max_lookback`** → `deny` with `TIME_RANGE_EXCEEDED`. The message gives the earliest date allowed.

#### Limits

This guard reads the shape of the `WHERE` clause; it does not evaluate it. Use it to steer agents toward bounded queries, not as a hard cost control. What it does not catch:

| Query | Result | Why |
|---|---|---|
| `WHERE ts >= current_date - INTERVAL '400' DAY` | allowed | Only literal dates are compared with `max_lookback`. A computed bound (functions, intervals, parameters, subqueries) counts as a lower bound whatever its value. |
| `WHERE ts >= DATE '2026-10-01' OR user_id = 7` | allowed | A lower bound anywhere in the `WHERE` counts, even inside an `OR` that makes it optional. |
| `WHERE ts >= 1262304000000` (epoch milliseconds for 2010) | allowed | Only date and timestamp literals (`'YYYY-MM-DD…'`, `DATE '…'`, `TIMESTAMP '…'`) are compared; numbers are not. |
| `WHERE CAST(ts AS DATE) >= DATE '2001-01-01'` | denied with `TIME_RANGE_REQUIRED` | The column must appear bare on one side of the comparison; wrapped in a function it is not recognized. |
| `WHERE e.ts >= …` where `ts` belongs to another joined table | checked as if it were the guarded table's column | Columns are matched by name; the table qualifier is not checked. |
| `WITH x AS (SELECT * FROM events) SELECT * FROM x WHERE ts >= …` | denied with `TIME_RANGE_REQUIRED` | The bound has to be on the `SELECT` that reads the table, as for `partition_predicate_required`. |
| SQL the parser can't read | allowed | See [SQL that can't be parsed](#sql-that-cant-be-parsed). |

Dates are compared in UTC, at day granularity: the earliest allowed literal is the UTC date of now minus `max_lookback`. With `max_lookback: 36h` at 10:00 UTC on October 21, that is October 19.

To bound the bytes a query reads for real, combine this guard with `partition_predicate_required` on the partition column and with `maxExecutionSecs` on the cluster group.

---

### `no_cross_join`

Blocks cartesian products: `CROSS JOIN` between tables or subqueries, a `JOIN` with no `ON` or `USING`, and `FROM a, b` with no `WHERE`. Row expansions are allowed (`CROSS JOIN UNNEST(...)`, `LATERAL`).

```yaml
guardrails:
  global:
    - kind: built_in
      name: no_cross_join
```

Error code on deny: `CROSS_JOIN`

---

## SQL that can't be parsed

Guards parse the SQL with the target engine's dialect. When parsing fails (an engine-specific construct the parser doesn't know), each guard falls back as follows:

| Guard | SQL that can't be parsed |
|---|---|
| `read_only` | Allowed only if it starts like a read (`SELECT`, `WITH`, `SHOW`, …) and contains no write keyword. |
| `row_limit` | Text check: warns without ` LIMIT `, denies a LIMIT above `max_rows`. No rewrite. |
| `require_predicate` | Text check: denied when it mentions a matching table and has no ` WHERE `. |
| `partition_predicate_required`, `time_range_limit`, `no_cross_join` | **Allowed.** |

The last three narrow how much a query reads; they are not access control, so they don't block what they can't read. If you need a hard stop, combine them with `read_only`, [access control](../access-control/overview) and the cluster group's `maxExecutionSecs` (see [Routing and clusters](./routing-and-clusters#cluster-group-configuration-actual-shape)).

## Configuring the new guard parameters

Every built-in guard parameter (`max_rows`, `inject_limit`, `default_limit`, `applies_to`, `tables`, `max_lookback`) can be set in YAML or through the admin API (`PUT /admin/config/guardrails`, same keys). Invalid parameters are rejected when the config is saved — for example `time_range_limit` without `max_lookback`, or `row_limit` with `default_limit` larger than `max_rows`. A guard stored with invalid parameters (an older row in Postgres) denies every query with a configuration error instead of being skipped.

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
      retry_count: 2     # retries timeouts, connection errors, and 5xx; default 0
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
