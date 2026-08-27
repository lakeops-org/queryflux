---
sidebar_label: Overview
title: Embedding QueryFlux
description: Construct QueryFlux as a library with QueryFlux::builder() — register compiled-in plugins and dispatch queries directly, instead of running the shipped binary.
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
every plugin kind (see [Plugins](plugins.md) and [Query lifecycle hooks](hooks.md)),
runnable with `cargo run -p embed-queryflux`.

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

See [Plugins](plugins.md) for every kind of thing you can register on the builder, and
[Query lifecycle hooks](hooks.md) for `.hook(...)`.

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
[Extending QueryFlux](../adding-support/overview.md) for adding a new **in-tree** backend
or frontend (the contributor path — a PR against this repo) as opposed to registering a
**compiled-in** plugin from your own binary (this section).
