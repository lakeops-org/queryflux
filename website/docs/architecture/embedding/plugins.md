---
sidebar_label: Plugins
title: Embedding Plugins
description: "Every plugin kind QueryFlux::builder() accepts — engines, guards, routers, strategies, translation scripts, frontends, auth — with a worked example for each."
image: img/queryflux-hero-banner.png
---

# Plugins

Each plugin is a trait (or a string, for translation scripts). You implement it, pass
it to the builder, and QueryFlux runs it next to whatever came from YAML or the
database.

Most extras are re-applied whenever config reloads from Postgres / Studio, so an
operator changing routing rules does not drop your guard. Frontends and hooks are
process-lifetime: they start at `serve()` / `build()` and stay until shutdown.

| Plugin | Trait | Register with | Survives reload |
|--------|-------|---------------|-----------------|
| [Backend engine](#backend-engine) | `EngineAdapterFactory` | `.engine(...)` | yes |
| [Pre-built adapter](#pre-built-adapter) | `AdapterKind` | `.with_adapter(...)` | yes |
| [Guard](#guard) | `Guard` | `.guard(...)` / `.group_guard(...)` | yes |
| [Router](#router) | `RouterTrait` | `.router_prepend(...)` / `.router_append(...)` | yes |
| [Cluster-selection strategy](#cluster-selection-strategy) | `ClusterSelectionStrategy` | `.strategy(...)` | yes |
| [Translation script](#translation-script) | — | `.translation_script(...)` | yes |
| [Frontend](#frontend) | `FrontendListenerTrait` | `.frontend(...)` | n/a |
| [Query lifecycle hook](#query-lifecycle-hook) | [`QueryHook`](hooks.md) | `.hook(...)` | n/a |
| [Auth provider](#auth-provider) | `AuthProvider` | `.auth_provider(...)` | n/a |

## Backend engine

Use a factory when operators should create clusters of your engine from Studio or the
admin API. `engine_key` is the string stored on the cluster row; `build_from_config_json`
parses that row's JSON blob — QueryFlux never needs a typed schema for your fields.

```rust
struct AcmeFactory;

#[async_trait]
impl EngineAdapterFactory for AcmeFactory {
    fn engine_key(&self) -> &'static str {
        "acme"
    }

    fn descriptor(&self) -> EngineDescriptor {
        AcmeAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind> {
        let config = AcmeConfig::from_json(json, &cluster_name.0)?;
        Ok(AdapterKind::Sync(Arc::new(AcmeAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}
```

```rust
QueryFlux::builder()
    .config_path("config.yaml")
    .engine(Box::new(AcmeFactory))
    .build()
    .await?
```

`config.yaml`'s `clusters.*.engine:` only knows the built-in engines (Trino, DuckDB,
…). Custom clusters are created through the admin API (`engine_key` + JSON) or with
[`.with_adapter`](#pre-built-adapter).

Return `EngineType::Custom("acme".into())` from the adapter so history and metrics
label the engine. Override `translation_target_dialect()` if sqlglot should target
something other than generic SQL.

## Pre-built adapter

When you already have a live connection and do not want a factory or JSON config:

```rust
QueryFlux::builder()
    .config_path("config.yaml")
    .with_adapter("acme-1", AdapterKind::Sync(Arc::new(adapter)))
    .build()
    .await?
```

Put `"acme-1"` in some `clusterGroups.*.members` list (or the test YAML equivalent).
The adapter is not routable until a group claims it.

## Guard

Guards see translated SQL, engine, tags, and user, then allow, warn, or deny. Extra
guards run **after** the YAML/DB chain — global via `.guard(...)`, one group via
`.group_guard("analytics", ...)`.

```rust
struct NoDdlGuard;

#[async_trait]
impl Guard for NoDdlGuard {
    fn name(&self) -> &'static str {
        "no_ddl"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }

    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        let upper = ctx.translated_sql.to_uppercase();
        let is_ddl = ["DROP ", "CREATE ", "ALTER ", "TRUNCATE "]
            .iter()
            .any(|verb| upper.trim_start().starts_with(verb));
        if is_ddl {
            GuardResult::deny("DDL is disabled in this deployment", "NO_DDL")
        } else {
            GuardResult::allow()
        }
    }
}
```

```rust
.guard(Box::new(NoDdlGuard))
```

The same `NoDdlGuard` is in
[`examples/embed-queryflux`](https://github.com/lakeops-org/queryflux/blob/main/examples/embed-queryflux/src/main.rs).

## Router

A router returns `Route(group)`, `Deny { message }`, or `NoMatch` (try the next one).
`.router_prepend` runs **before** the YAML chain (first look). `.router_append` runs
after it, still before `routingFallback`.

```rust
struct LoggingRouter;

#[async_trait]
impl RouterTrait for LoggingRouter {
    fn type_name(&self) -> &'static str {
        "Logging"
    }

    async fn route(
        &self,
        sql: &str,
        _session: &SessionContext,
        protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        tracing::info!(?protocol, user = ?auth_ctx.map(|a| &a.user), sql, "incoming query");
        Ok(RoutingDecision::NoMatch)
    }
}
```

```rust
.router_prepend(Box::new(LoggingRouter))
```

Returning `NoMatch` is how you observe traffic without stealing it from
`protocolBased` / regex / Python routers in YAML.

## Cluster-selection strategy

Routing picks a **group**. The strategy picks **which member** of that group runs the
query. Built-ins: round-robin, least-loaded, failover, engine-affinity, weighted.
`.strategy("analytics", ...)` replaces that group's YAML strategy.

`pick` receives only healthy, under-capacity candidates and returns an index:

```rust
struct StickyFirstHealthy;

impl ClusterSelectionStrategy for StickyFirstHealthy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        Some(0)
    }
}
```

```rust
.strategy("analytics", Arc::new(StickyFirstHealthy))
```

Useful for primary/standby: if the primary is in the candidate list, it is always
index `0` when members are listed that way.

## Translation script

A Python `transform(sql, dialect)` that runs after sqlglot, same as Studio
`user_scripts`. Merged onto every group's fixup list:

```rust
.translation_script(include_str!("fixup.py"))
```

```python
def transform(sql: str, dialect: str) -> str:
    if dialect == "duckdb":
        return sql.replace("REGEXP_LIKE", "REGEXP_MATCHES")
    return sql
```

## Frontend

A frontend binds a port, accepts clients, and calls into dispatch. Built-ins are Trino
HTTP, Postgres wire, MySQL wire, Flight SQL, Snowflake, and MCP. Yours gets
`Arc<AppState>` via a factory — the listener cannot exist before `build()`.

```rust
.frontend(|state| Box::new(TinyHttpFrontend { state }))
```

Call `state.route_query(...)` before `execute_to_sink` so hooks, routing traces, and
authorization-aware fallback match the built-in listeners. Pass
`FrontendProtocol::Custom { name, dialect }` so history is not blank. YAML
`type: protocolBased` will not match that name — prepend your own `RouterTrait` if
custom traffic should land on a specific group.

Full `POST /query` listener:
[`examples/embed-queryflux/src/main.rs`](https://github.com/lakeops-org/queryflux/blob/main/examples/embed-queryflux/src/main.rs).

## Query lifecycle hook

`.hook(Arc::new(AuditHook))` — stages, SQL rewrite, deny, and what is filled in on
Trino vs sync paths: [Query lifecycle hooks](hooks.md).

## Auth provider

`.auth_provider(...)` **replaces** YAML/DB auth. Every frontend asks this type who the
client is. Use it for a token map, mTLS mapping, or an IdP QueryFlux does not ship —
not to add a second check on top of OIDC.

```rust
struct StaticTokenAuth {
    valid_tokens: HashMap<String, String>, // token -> username
}

#[async_trait]
impl AuthProvider for StaticTokenAuth {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext> {
        let token = creds.bearer_token.as_deref().ok_or_else(|| {
            QueryFluxError::Auth("missing bearer token".to_string())
        })?;
        let user = self.valid_tokens.get(token).ok_or_else(|| {
            QueryFluxError::Auth("unknown token".to_string())
        })?;
        Ok(AuthContext {
            user: user.clone(),
            raw_token: Some(token.to_string()),
            ..Default::default()
        })
    }
}
```

```rust
.auth_provider(Arc::new(StaticTokenAuth { valid_tokens }))
```
