---
sidebar_label: Overview
title: Embedding QueryFlux
description: "Construct QueryFlux as a library with QueryFlux::builder() — register compiled-in plugins and dispatch queries directly, instead of running the shipped binary."
image: img/queryflux-hero-banner.png
---

# Embedding QueryFlux

The `queryflux` binary is a YAML-driven proxy. The same crate is also a library: you
construct a `QueryFlux` in your own Rust program, register extra engines, guards,
routers, frontends, or hooks in code, and either serve the usual listeners or dispatch
queries yourself.

That is the path for a product binary that ships private routing logic, a custom
backend, or an internal HTTP API — without forking this repo. Plugins are ordinary
Rust types compiled into your binary. They are not named in YAML, and a Studio config
edit will not drop them.

The shipped binary is the same builder with only the built-in engines registered:

```rust
QueryFlux::builder()
    .config_path(path)
    .with_builtin_plugins()
    .build()
    .await?
    .serve()
    .await
```

[`examples/embed-queryflux/`](https://github.com/lakeops-org/queryflux/tree/main/examples/embed-queryflux)
is a complete program: DuckDB from YAML, plus a DDL-blocking guard, a logging router, an
audit hook, and a tiny `POST /query` frontend. Run it with
`cargo run -p embed-queryflux`.

## Builder

```rust
QueryFlux::builder()
    .config_path("config.yaml")
    .with_builtin_plugins()
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

You still point at a `config.yaml` — clusters, groups, frontends, auth, persistence.
The builder methods *add* to that config; they do not replace it, except
`.auth_provider(...)`, which takes over authentication entirely.

`.with_builtin_plugins()` registers the engines the binary ships (Trino, DuckDB,
StarRocks, ClickHouse, Athena, ADBC). Skip it if you only want engines you register
yourself.

`build()` loads config, wires routers / guards / adapters, and starts background work
(config reload, health checks, capacity). It does **not** bind client ports. `serve()`
starts every enabled YAML frontend plus any `.frontend(...)` you registered, then
blocks until shutdown.

If your process already installed a `tracing` subscriber, `build()` leaves it alone
(`try_init`). Otherwise it installs one from `RUST_LOG`. Set yours up before `build()`
if you want control of log format.

Every registrable type is documented on [Plugins](plugins.md). Query lifecycle
callbacks are on [Hooks](hooks.md).

## Dispatch without `serve()`

`QueryFlux::app_state()` is the same `Arc<AppState>` the Trino / Postgres / MySQL
listeners use. Tests and in-process frontends call
`queryflux_frontend::dispatch::execute_to_sink` (or `dispatch_query`) against it
directly — no sockets.

[`crates/queryflux/tests/embed.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/tests/embed.rs)
does that: a mock adapter via `.with_adapter()`, a guard that denies `DROP`, a hook
that records `before_execute` / `on_error`.

## In-tree vs your binary

To add an engine or protocol that should ship for everyone, follow
[Extending QueryFlux](../adding-support/overview.md) and open a PR. Embedding is for
code that stays in *your* crate.
