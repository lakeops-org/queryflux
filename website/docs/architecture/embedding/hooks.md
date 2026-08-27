---
sidebar_label: Query lifecycle hooks
title: Query Lifecycle Hooks
description: QueryHook observes or intercepts a query at every pipeline stage — routing, translation, guards, execution, errors, and cancellation.
image: img/queryflux-hero-banner.png
---

# Query lifecycle hooks

A `QueryHook` runs in-process on every query, alongside the guard chain — audit logs,
metering, SQL rewrite, or an extra deny. Methods default to no-ops; implement only the
stages you care about.

```rust
struct AuditHook;

#[async_trait]
impl QueryHook for AuditHook {
    async fn before_execute(&self, ctx: &HookContext<'_>) -> HookOutcome {
        tracing::info!(sql = %ctx.sql, group = ?ctx.group, "before_execute");
        HookOutcome::Continue
    }

    async fn after_execute(&self, ctx: &HookContext<'_>) {
        tracing::info!(sql = %ctx.sql, rows = ?ctx.rows, "after_execute");
    }

    async fn on_error(&self, ctx: &HookContext<'_>, err: &QueryFluxError) {
        tracing::warn!(sql = %ctx.sql, error = %err, "on_error");
    }
}
```

```rust
.hook(Arc::new(AuditHook))
```

Register several; they run in registration order. Hooks live on `AppState` for the
process lifetime — they are not rebuilt on a Studio config reload.

## Stages

| Method | When | Can rewrite SQL | Can deny |
|--------|------|-----------------|----------|
| `before_route` | Before the router chain | yes | yes |
| `after_route` | After a group is chosen (including auth-aware fallback) | no | yes |
| `before_translate` | Before sqlglot | yes | yes |
| `after_translate` | After translation (or skip) | no | yes |
| `before_guard` | Before the guard chain | no | yes |
| `after_guard` | After guards have allowed the query | no | yes |
| `before_execute` | About to submit to the adapter | no | yes |
| `after_execute` | Submit accepted (see [row counts](#row-counts-and-latency)) | no | no |
| `on_error` | Dispatch returned an error | no | — |
| `on_cancel` | Client dropped mid-stream on the **sync** path | no | — |

`before_route` / `after_route` only run if the frontend calls `AppState::route_query`
(all built-in frontends do). A custom frontend that skips it will miss those two stages
and the routing trace. See [Plugins — Frontend](plugins.md#frontend).

Hooks do not pick the cluster group. That is `RouterTrait`. A `before_*` hook that
needs to stop the query returns `HookOutcome::Deny { message }` — same
`QueryFluxError::Denied` as a guard deny, and the same history row.

## `HookContext`

| Field | Set from |
|-------|----------|
| `sql` | Incoming text; `before_route` / `before_translate` may replace it |
| `session`, `protocol`, `query_tags`, `auth` | Always |
| `group` | `after_route` onward |
| `cluster`, `engine_type` | After a member is acquired (`before_execute` onward). Cache hits use `EngineType::Cache` |
| `rows`, `execution_ms` | See below |

## Row counts and latency

`rows` and `execution_ms` are filled on `after_execute` / `on_error` for:

- sync dispatch (Postgres wire, MySQL wire, Flight SQL, custom `execute_to_sink`)
- query-result cache hits

On the Trino HTTP submit/poll path, `after_execute` runs when the query is **accepted**,
before the engine has a result, so both fields stay `None`. Polling the final result
does not call hooks today.

`on_cancel` runs when a sync client disconnects mid-stream. An explicit Trino
`DELETE` (or zombie eviction) still writes a `Cancelled` history row; it does not call
`on_cancel`.

An empty hook list is one `Arc` clone on the dispatch path.

## Example

[`examples/embed-queryflux`](https://github.com/lakeops-org/queryflux/blob/main/examples/embed-queryflux/src/main.rs)
logs `before_execute` / `after_execute` / `on_error`.
[`crates/queryflux/tests/embed.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/tests/embed.rs)
asserts `after_execute` row count on the real `execute_to_sink` path with no listener.
