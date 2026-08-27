---
sidebar_label: Plugins
title: Embedding Plugins
description: Every plugin kind QueryFlux::builder() accepts — engines, guards, routers, strategies, translation scripts, frontends, auth — and how custom engines get configured without core knowing their shape.
---

# Plugins

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
| Query lifecycle hook | [`QueryHook`](hooks.md) | `.hook(h)` | N/A — static on `AppState`, not part of hot-reloadable config |
| Auth provider | [`AuthProvider`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-auth/src/provider.rs) | `.auth_provider(p)` | N/A — overrides the YAML/DB auth build entirely |

Default placement: extra **routers prepend** (checked before the YAML/DB router
chain), extra **guards append** (YAML/DB policy runs first, then your restrictions).

Registered guards, routers, strategies, and translation scripts are re-applied on
**every** `LiveConfig` rebuild — both the initial load and any later admin-API-driven
hot reload — so they don't disappear when someone edits routing or guard config
through the admin API or Studio.

## Custom engines don't need core to know their config

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

## Open enums

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
