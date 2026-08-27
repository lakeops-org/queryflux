---
sidebar_label: Embedding QueryFlux
title: Embedding QueryFlux
description: Construct QueryFlux as a library — register compiled-in engines, guards, routers, frontends, and query lifecycle hooks — instead of running the shipped binary.
---

# Embedding QueryFlux

QueryFlux is a workspace of library crates. The `queryflux` crate additionally exposes a
`QueryFlux::builder()` so you can construct an instance in your own binary, register
compiled-in plugins, and observe or intercept every query — instead of running the
shipped `queryflux` binary against a YAML file.

This is **Rust builder only**: plugins are compiled into your binary, not loaded at
runtime (no `dlopen`, no WASM). YAML/DB config does not need to name your custom
plugins — they're registered in code and survive config hot-reload.

The shipped binary is itself a thin consumer of this same builder:

```rust
QueryFlux::builder()
    .config_path(path)
    .with_builtin_plugins()
    .build().await?
    .serve().await
```

A full working example lives in [`examples/embed-queryflux/`](https://github.com/lakeops-org/queryflux/tree/main/examples/embed-queryflux) — one file registering
every plugin kind below, runnable with `cargo run -p embed-queryflux`.

## The builder

```rust
QueryFlux::builder()
    .config_path("config.yaml")
    .engine(Box::new(AcmeFactory))
    .guard(Box::new(CostCapGuard { max_usd: 1.0 }))
    .router_prepend(Box::new(GeoRouter::new()))
    .strategy("analytics", Arc::new(MyStrategy))
    .translation_script(include_str!("fixup.py"))
    .frontend(|state| Box::new(InternalApi::new(state)))
    .hook(Arc::new(AuditHook))
    .build()
    .await?
    .serve()
    .await
```

`build()` loads `config.yaml`, constructs the router/guard/adapter chain, and starts
background maintenance tasks (config hot-reload, health checks, capacity
coordination) — but does not start listening for client connections. It returns a
`QueryFlux` handle; call [`app_state()`](#dispatching-without-serving) to inspect or
dispatch against the live state directly, or call `serve()` to spawn every frontend
and block until shutdown.

`build()` also initializes a `tracing_subscriber` from `RUST_LOG` — but via
`try_init()`, not `init()`, so it's a no-op (not a panic) if your host process already
installed a global subscriber. If you want QueryFlux's logs and you set up your own
subscriber first, install it *before* calling `.build()`, or don't install one at all
and let QueryFlux's stand.

## Plugin reference

| Plugin | Trait | Builder method | Survives reload? |
|--------|-------|-----------------|-------------------|
| Backend engine | [`EngineAdapterFactory`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/lib.rs) | `.engine(factory)` | Yes — registry used on every adapter rebuild |
| Pre-built adapter | — | `.with_adapter(cluster, AdapterKind)` | Yes — config never round-trips through core for these |
| Guard | [`Guard`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-guardrails/src/built_in.rs) | `.guard(g)` / `.group_guard(name, g)` | Yes — re-appended after the YAML/DB chain |
| Router | [`RouterTrait`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-routing/src/lib.rs) | `.router_prepend(r)` / `.router_append(r)` | Yes — re-inserted around the YAML/DB routers |
| Cluster-selection strategy | [`ClusterSelectionStrategy`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-cluster-manager/src/strategy.rs) | `.strategy(group, s)` | Yes — overrides the YAML/DB strategy for that group |
| Translation script | `String` (Python, post-sqlglot) | `.translation_script(py)` | Yes — merged into every group's fixup chain |
| Frontend | [`FrontendListenerTrait`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/lib.rs) | `.frontend(\|state\| ...)` | N/A — spawned once at `serve()` |
| Query lifecycle hook | [`QueryHook`](#query-lifecycle-hooks) | `.hook(h)` | N/A — static on `AppState`, not part of hot-reloadable config |
| Auth provider | [`AuthProvider`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-auth/src/provider.rs) | `.auth_provider(p)` | N/A — overrides the YAML/DB auth build entirely |

Default placement: extra **routers prepend** (checked before the YAML/DB router
chain), extra **guards append** (YAML/DB policy runs first, then your restrictions).

Registered guards, routers, strategies, and translation scripts are re-applied on
**every** `LiveConfig` rebuild — both the initial load and any later admin-API-driven
hot reload — so they don't disappear when someone edits routing or guard config
through the admin API or Studio.

### Custom engines don't need core to know their config

`queryflux-core`'s `EngineConfig` enum (the YAML `clusters.*.engine:` value) stays
closed — custom engines aren't nameable in the plain YAML file. That's not a real
limitation, because custom engines never need to round-trip through that typed enum:

- **Compiled-in, code-constructed**: implement `EngineAdapterFactory::build_from_config_json`
  against whatever JSON shape you choose (core only ever sees an opaque
  `serde_json::Value`), or skip config entirely and hand a fully-built `AdapterKind` to
  `.with_adapter(cluster, adapter)`.
- **Admin-API-created clusters**: the DB stores an opaque `engine_key` string plus a
  JSON blob; `EngineAdapterFactory::build_from_config_json` on the matching registered
  factory parses it however it wants.

### Open enums

Two enums in `queryflux-core` have an escape-hatch variant for custom plugins:

- `EngineType::Custom(String)` — what a custom adapter's `engine_type()` returns.
  Dialect resolution should come from overriding `translation_target_dialect()` on
  your adapter (default falls back to `EngineType::dialect()`, which treats `Custom`
  as `SqlDialect::Generic`) rather than relying on the enum's own dialect table.
- `FrontendProtocol::Custom { name, dialect }` — what a custom frontend passes as its
  protocol so history and routing traces record something meaningful. The built-in
  `ProtocolBasedRouter` (YAML `type: protocolBased`) doesn't route on unrecognized
  protocol names — route custom traffic with a registered `RouterTrait` instead.

`EngineConfig` and `GuardKind` (the YAML-facing config enums) are **not** open — see
above for why custom engines don't need them, and custom guards are always
code-registered rather than YAML-named.

## Query lifecycle hooks

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

`HookContext` is a borrowed view of the SQL, session, protocol, tags, and auth user —
plus `group` / `cluster` / `engine_type`, which are `None` until routing (`after_route`
onward) and adapter selection have happened. `before_route` and `before_translate` take
`&mut HookContext` so they can rewrite `ctx.sql`; every other stage only observes.
Returning `HookOutcome::Deny { message }` from a `before_*` hook stops the query there,
mapped to the same `QueryFluxError::Denied` a guard deny produces, and — like a guard
deny — recorded as a `Failed`/`Denied` row in query history, not just returned to the
client. Hooks never pick the cluster group — that stays a `RouterTrait`'s job.

`HookContext` also carries `rows: Option<u64>` and `execution_ms: Option<u64>`. Both are
`None` everywhere except where a result is actually known: `after_execute` and `on_error`
on the sync (MySQL/Postgres/Flight SQL) and cache-hit paths. The async (Trino
submit/poll) path's `after_execute` fires at submission-accepted, before the engine has
produced a result — polling for the final result happens in a separate code path this
hook system doesn't observe yet, so both stay `None` there. A usage-metering or
audit-logging hook gets real row counts and latency on the sync/cache paths today.

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

## Dispatching without serving

`QueryFlux::app_state()` returns the same `Arc<AppState>` every frontend dispatches
through. This is useful for tests or for a custom frontend that wants to call
`queryflux_frontend::dispatch::execute_to_sink` / `dispatch_query` directly, without
going through `serve()`'s network listeners at all — see
[`crates/queryflux/tests/embed.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/tests/embed.rs)
for a complete example: a mock adapter registered via `.with_adapter()`, a guard that
denies `DROP`, and a hook that records `before_execute` / `on_error`, all exercised
through the real dispatch path with no listening socket.

## Out of scope

Runtime `.so` / WASM plugin loading, YAML-nameable custom engine/guard kinds, and
process-level hooks (`on_start` / `on_reload`) are not part of this API. See
[Extending QueryFlux](adding-support/overview.md) for adding a new **in-tree** backend
or frontend (the contributor path — a PR against this repo) as opposed to registering a
**compiled-in** plugin from your own binary (this page).
