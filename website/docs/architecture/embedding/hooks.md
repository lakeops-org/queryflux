---
sidebar_label: Query lifecycle hooks
title: Query Lifecycle Hooks
description: QueryHook observes or intercepts a query at every pipeline stage — routing, translation, guards, execution, errors, and cancellation.
---

# Query lifecycle hooks

`QueryHook` observes or intercepts a query at each pipeline stage. Every method has a
no-op default, so implement only what you need:

```rust
#[async_trait]
pub trait QueryHook: Send + Sync {
    async fn before_route(&self, ctx: &mut HookContext) -> HookOutcome { Continue }
    async fn after_route(&self, ctx: &HookContext) -> HookOutcome { Continue }
    async fn before_translate(&self, ctx: &mut HookContext) -> HookOutcome { Continue }
    async fn after_translate(&self, ctx: &HookContext) -> HookOutcome { Continue }
    async fn before_guard(&self, ctx: &HookContext) -> HookOutcome { Continue }
    async fn after_guard(&self, ctx: &HookContext) -> HookOutcome { Continue }
    async fn before_execute(&self, ctx: &HookContext) -> HookOutcome { Continue }
    async fn after_execute(&self, ctx: &HookContext) {}
    async fn on_error(&self, ctx: &HookContext, err: &QueryFluxError) {}
    async fn on_cancel(&self, ctx: &HookContext) {}
}
```

Register one (or several — they fire in registration order) with `.hook(Arc::new(...))`
on the builder. See [Plugins](plugins.md) for how this fits alongside engines, guards,
and routers.

## `HookContext`

A borrowed view of the SQL, session, protocol, tags, and auth user — plus `group` /
`cluster` / `engine_type`, which are `None` until routing (`after_route` onward) and
adapter selection have happened. `before_route` and `before_translate` take
`&mut HookContext` so they can rewrite `ctx.sql`; every other stage only observes.

`HookContext` also carries `rows: Option<u64>` and `execution_ms: Option<u64>`. Both are
`None` everywhere except where a result is actually known: `after_execute` and `on_error`
on the sync (MySQL/Postgres/Flight SQL) and cache-hit paths. The async (Trino
submit/poll) path's `after_execute` fires at submission-accepted, before the engine has
produced a result — polling for the final result happens in a separate code path this
hook system doesn't observe yet, so both stay `None` there. A usage-metering or
audit-logging hook gets real row counts and latency on the sync/cache paths today.

## Deny and error handling

Returning `HookOutcome::Deny { message }` from a `before_*` hook stops the query there,
mapped to the same `QueryFluxError::Denied` a guard deny produces, and — like a guard
deny — recorded as a `Failed`/`Denied` row in query history, not just returned to the
client. Hooks never pick the cluster group — that stays a `RouterTrait`'s job.

## Where hooks run

Hooks run alongside — not instead of — the guard chain, on every query path: the async
(Trino-style submit/poll) dispatch, the sync (MySQL/Postgres/Flight SQL) execute-to-sink
path, and the query-result-cache hit path (with `engine_type = EngineType::Cache`). An
empty hook list (the default) costs one `Arc` clone on the dispatch path — no extra
allocation, no behavior change from a build with no hooks registered.

`on_cancel` fires on the sync path when a client disconnects mid-stream. It does **not**
yet fire for Trino's async cancel (the client's explicit `DELETE` on a running query, or
zombie-query eviction) — those paths persist a `Cancelled` history row today but don't
call into `state.hooks`. If your hook needs to know about every cancellation regardless
of protocol, that gap is open for a follow-up; track it if it matters for your use case.

`AppState::route_query` is the one place `before_route` / `after_route` run — it's also
what unifies routing across every frontend (including MCP, which historically routed
without producing a `RoutingTrace`), and resolves the authorization-aware fallback group
before `after_route` fires, so `ctx.group` there always reflects the group a query
actually dispatches against, never a pre-fallback candidate. If you write a custom
frontend, call `state.route_query(sql, &session, &protocol, auth_ctx)` before dispatch
so your traffic gets the same hook coverage and trace as the built-in frontends.

## Example

[`examples/embed-queryflux/src/main.rs`](https://github.com/lakeops-org/queryflux/blob/main/examples/embed-queryflux/src/main.rs)
registers an `AuditHook` that logs `before_execute` / `after_execute` / `on_error`.
[`crates/queryflux/tests/embed.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/tests/embed.rs)
exercises a `RecordingHook` end to end — including asserting `after_execute`'s row
count — against the real dispatch path with no listening socket.
