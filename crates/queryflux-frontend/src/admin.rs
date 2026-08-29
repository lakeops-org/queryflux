use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, Request, State},
    http::{header::AUTHORIZATION, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Json, Router,
};
use queryflux_auth::{AdminCredentialsManager, AuthContext, AuthorizationChecker};
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::{
    config::{
        AuthConfig, AuthProviderConfig, AuthorizationConfig, AuthorizationProviderConfig,
        ClusterGroupConfig, FrontendConfig, FrontendsConfig, OpenFgaCredentials, RouterConfig,
    },
    engine_registry::EngineRegistry,
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, ExecutingQuery,
        FrontendProtocol, ProxyQueryId,
    },
    session::SessionContext,
    tags::{merge_tags, QueryTags},
};
use queryflux_guardrails::{GuardChain, GuardContext, GuardLayer};
use queryflux_metrics::prometheus_store::PrometheusMetrics;
use queryflux_persistence::{
    cluster_config::{
        ClusterConfigRecord, ClusterGroupConfigRecord, RenameConfigRequest, UpsertClusterConfig,
        UpsertClusterGroupConfig,
    },
    query_history::{
        AgentSummary, ConversationSummary, DashboardStats, EngineStatRow, GroupStatRow,
        QueryFilters, QuerySummary,
    },
    routing_json::{enrich_routers_for_api, resolve_routers_for_storage},
    script_library::{UpsertUserScript, UserScriptRecord, KIND_GUARD},
    AdminStore, GuardAction,
};
use queryflux_routing::{chain::RouterChain, chain::RoutingTrace, ChainRouteResult};
use queryflux_translation::TranslationService;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};
use utoipa::{OpenApi, ToSchema};

use std::future::Future;
use std::pin::Pin;

use crate::{
    routing_resolve::{check_group_authorized, resolve_routed_group},
    state::{AppState, LiveConfig},
    FrontendListenerTrait, ShutdownRx,
};

/// Callback type for testing a cluster config without persisting it.
/// Receives `(engine_key, config_json)` → returns `Ok(true)` if healthy, `Ok(false)` if
/// adapter built but health check failed, `Err(msg)` if adapter construction failed.
pub type TestClusterFn = Arc<
    dyn Fn(String, serde_json::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// OpenAPI spec
// ---------------------------------------------------------------------------

/// Live state snapshot of a single cluster returned by /admin/clusters.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClusterStateDto {
    pub group_name: String,
    pub cluster_name: String,
    pub engine_type: String,
    /// The HTTP endpoint of the cluster (e.g. `http://trino-1:8080`). Null for engines without a network endpoint (e.g. DuckDB).
    pub endpoint: Option<String>,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    /// Whether the most recent health check (every 30s) passed.
    pub is_healthy: bool,
    /// Whether this cluster is administratively enabled.
    pub enabled: bool,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "QueryFlux Admin API",
        version = "0.1.0",
        description = "Admin REST API for QueryFlux Studio — query history, cluster state, and dashboard stats."
    ),
    paths(
        health_handler,
        clusters_handler,
        update_cluster_handler,
        engine_registry_handler,
        list_queries_handler,
        list_running_queries_handler,
        cancel_query_handler,
        get_stats_handler,
        list_engines_handler,
        get_engine_stats_handler,
        get_group_stats_handler,
        frontends_status_handler,
        // Persisted cluster config CRUD
        list_cluster_configs_handler,
        get_cluster_config_handler,
        upsert_cluster_config_handler,
        rename_cluster_config_handler,
        delete_cluster_config_handler,
        test_cluster_config_handler,
        // Persisted cluster group config CRUD
        list_group_configs_handler,
        get_group_config_handler,
        upsert_group_config_handler,
        rename_group_config_handler,
        delete_group_config_handler,
        // User scripts
        list_user_scripts_handler,
        create_user_script_handler,
        get_user_script_handler,
        update_user_script_handler,
        delete_user_script_handler,
        // Security / routing / guardrails config
        get_security_config_handler,
        put_security_config_handler,
        get_routing_config_handler,
        put_routing_config_handler,
        get_guardrails_config_handler,
        put_guardrails_config_handler,
        // Agents & conversations
        list_agents_handler,
        list_conversations_handler,
        get_conversation_handler,
        // Routing preview
        route_explain_handler,
    ),
    components(schemas(
        ClusterStateDto,
        ClusterUpdateRequest,
        QuerySummary,
        RunningQueryDto,
        DashboardStats,
        EngineStatRow,
        GroupStatRow,
        UserScriptRecord,
        UpsertUserScript,
        ProtocolFrontendDto,
        FrontendsStatusDto,
        RouteExplainRequest,
        RouteExplainResponse,
        GroupCapacityDto,
        queryflux_persistence::cluster_config::ClusterConfigRecord,
        queryflux_persistence::cluster_config::UpsertClusterConfig,
        queryflux_persistence::cluster_config::ClusterGroupConfigRecord,
        queryflux_persistence::cluster_config::UpsertClusterGroupConfig,
        queryflux_persistence::cluster_config::RenameConfigRequest,
        queryflux_persistence::query_history::AgentSummary,
        queryflux_persistence::query_history::ConversationSummary,
    )),
    tags(
        (name = "admin", description = "Cluster and query management"),
        (name = "config", description = "Persisted cluster / group / script configuration"),
        (name = "metrics", description = "Prometheus metrics endpoint"),
    )
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// Security & Routing config DTOs (sanitized — no secrets)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SecurityConfigDto {
    pub auth_provider: String,
    pub auth_required: bool,
    pub oidc: Option<OidcConfigDto>,
    pub ldap: Option<LdapConfigDto>,
    /// Number of users defined when provider = "static". Passwords are never exposed.
    pub static_user_count: Option<usize>,
    /// Usernames + groups/roles so Studio can re-save without wiping the user list.
    /// Passwords are never included.
    #[serde(default)]
    pub static_user_summaries: Vec<StaticUserSummaryDto>,
    pub authorization_provider: String,
    pub openfga: Option<OpenFgaConfigDto>,
    /// Per-cluster-group simple allow-lists (used when authorization_provider = "none").
    pub group_authorization: HashMap<String, GroupAuthzDto>,
    /// IdP roles that may cancel any query.
    #[serde(default)]
    pub operator_roles: Vec<String>,
    /// IdP groups that may cancel any query.
    #[serde(default)]
    pub operator_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OidcConfigDto {
    pub issuer: String,
    pub jwks_uri: String,
    pub audience: Option<String>,
    pub groups_claim: String,
    pub roles_claim: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LdapConfigDto {
    pub url: String,
    pub bind_dn: String,
    pub user_search_base: String,
    pub user_search_filter: String,
    pub user_dn_template: Option<String>,
    pub group_search_base: Option<String>,
    pub group_name_attribute: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenFgaConfigDto {
    pub url: String,
    pub store_id: String,
    /// Credential method: "api_key" | "client_credentials" | null
    pub credentials_method: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupAuthzDto {
    pub allow_groups: Vec<String>,
    pub allow_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaticUserSummaryDto {
    pub username: String,
    pub groups: Vec<String>,
    pub roles: Vec<String>,
}

impl Default for SecurityConfigDto {
    fn default() -> Self {
        Self {
            auth_provider: "none".to_string(),
            auth_required: false,
            oidc: None,
            ldap: None,
            static_user_count: None,
            static_user_summaries: Vec::new(),
            authorization_provider: "none".to_string(),
            openfga: None,
            group_authorization: HashMap::new(),
            operator_roles: Vec::new(),
            operator_groups: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol frontends (read-only snapshot from startup YAML)
// ---------------------------------------------------------------------------

/// One entry protocol / client surface (Trino HTTP, MySQL wire, …).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProtocolFrontendDto {
    /// Stable id: `trino_http`, `mysql_wire`, `flight_sql`, …
    pub id: String,
    pub label: String,
    pub short_description: String,
    pub enabled: bool,
    /// Listening port when enabled and configured; `null` when the block is absent in config.
    pub port: Option<u16>,
}

/// Effective frontends from the running process config (not hot-reloaded).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FrontendsStatusDto {
    pub external_address: Option<String>,
    pub admin_api_port: u16,
    pub protocols: Vec<ProtocolFrontendDto>,
}

/// Build the snapshot returned by [`frontends_status_handler`] from the loaded `FrontendsConfig`.
pub fn build_frontends_status(
    frontends: &FrontendsConfig,
    admin_api_port: u16,
    external_address: Option<String>,
) -> FrontendsStatusDto {
    fn opt_fe(
        id: &str,
        label: &str,
        desc: &str,
        cfg: Option<&FrontendConfig>,
    ) -> ProtocolFrontendDto {
        match cfg {
            None => ProtocolFrontendDto {
                id: id.to_string(),
                label: label.to_string(),
                short_description: desc.to_string(),
                enabled: false,
                port: None,
            },
            Some(c) => ProtocolFrontendDto {
                id: id.to_string(),
                label: label.to_string(),
                short_description: desc.to_string(),
                enabled: c.enabled,
                port: Some(c.port),
            },
        }
    }

    let trino = &frontends.trino_http;
    let protocols = vec![
        ProtocolFrontendDto {
            id: "trino_http".to_string(),
            label: "Trino HTTP".to_string(),
            short_description: "Trino-compatible REST API (POST /v1/statement, poll nextUri)."
                .to_string(),
            enabled: trino.enabled,
            port: Some(trino.port),
        },
        opt_fe(
            "mysql_wire",
            "MySQL wire",
            "MySQL protocol (mysql CLI, JDBC mysql://, many drivers).",
            frontends.mysql_wire.as_ref(),
        ),
        opt_fe(
            "postgres_wire",
            "PostgreSQL wire",
            "PostgreSQL wire protocol (psql, JDBC postgresql://, etc.).",
            frontends.postgres_wire.as_ref(),
        ),
        opt_fe(
            "clickhouse_http",
            "ClickHouse HTTP",
            "ClickHouse HTTP interface (if implemented in this build).",
            frontends.clickhouse_http.as_ref(),
        ),
        opt_fe(
            "flight_sql",
            "Flight SQL",
            "Arrow Flight SQL / gRPC-style access (driver-dependent).",
            frontends.flight_sql.as_ref(),
        ),
        match &frontends.snowflake_http {
            None => ProtocolFrontendDto {
                id: "snowflake_http".to_string(),
                label: "Snowflake HTTP".to_string(),
                short_description:
                    "Snowflake wire protocol + SQL API v2 on one port (session and query endpoints)."
                        .to_string(),
                enabled: false,
                port: None,
            },
            Some(c) => ProtocolFrontendDto {
                id: "snowflake_http".to_string(),
                label: "Snowflake HTTP".to_string(),
                short_description:
                    "Snowflake wire protocol + SQL API v2 on one port (session and query endpoints)."
                        .to_string(),
                enabled: c.enabled,
                port: Some(c.port),
            },
        },
        opt_fe(
            "mcp",
            "MCP",
            "Model Context Protocol streamable-HTTP tool calls for AI agents.",
            frontends.mcp.as_ref(),
        ),
    ];

    FrontendsStatusDto {
        external_address,
        admin_api_port,
        protocols,
    }
}

impl SecurityConfigDto {
    pub fn from_config(
        auth: &AuthConfig,
        authz: &AuthorizationConfig,
        groups: &HashMap<String, ClusterGroupConfig>,
    ) -> Self {
        let auth_provider = match auth.provider {
            AuthProviderConfig::None => "none",
            AuthProviderConfig::Static => "static",
            AuthProviderConfig::Oidc => "oidc",
            AuthProviderConfig::Ldap => "ldap",
        }
        .to_string();

        let oidc = auth.oidc.as_ref().map(|o| OidcConfigDto {
            issuer: o.issuer.clone(),
            jwks_uri: o.jwks_uri.clone(),
            audience: o.audience.clone(),
            groups_claim: o.groups_claim.clone(),
            roles_claim: o.roles_claim.clone(),
        });

        let ldap = auth.ldap.as_ref().map(|l| LdapConfigDto {
            url: l.url.clone(),
            bind_dn: l.bind_dn.clone(),
            user_search_base: l.user_search_base.clone(),
            user_search_filter: l.user_search_filter.clone(),
            user_dn_template: l.user_dn_template.clone(),
            group_search_base: l.group_search_base.clone(),
            group_name_attribute: l.group_name_attribute.clone(),
        });

        let static_user_summaries: Vec<StaticUserSummaryDto> = auth
            .static_users
            .as_ref()
            .map(|s| {
                s.users
                    .iter()
                    .map(|(username, entry)| StaticUserSummaryDto {
                        username: username.clone(),
                        groups: entry.groups.clone(),
                        roles: entry.roles.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let static_user_count = if static_user_summaries.is_empty() {
            None
        } else {
            Some(static_user_summaries.len())
        };

        let authorization_provider = match authz.provider {
            AuthorizationProviderConfig::None => "none",
            AuthorizationProviderConfig::OpenFga => "openfga",
        }
        .to_string();

        let openfga = authz.openfga.as_ref().map(|o| {
            let credentials_method = o.credentials.as_ref().map(|c| match c {
                OpenFgaCredentials::ApiKey { .. } => "api_key".to_string(),
                OpenFgaCredentials::ClientCredentials { .. } => "client_credentials".to_string(),
            });
            OpenFgaConfigDto {
                url: o.url.clone(),
                store_id: o.store_id.clone(),
                credentials_method,
            }
        });

        let group_authorization = groups
            .iter()
            .filter(|(_, g)| {
                !g.authorization.allow_groups.is_empty() || !g.authorization.allow_users.is_empty()
            })
            .map(|(name, g)| {
                (
                    name.clone(),
                    GroupAuthzDto {
                        allow_groups: g.authorization.allow_groups.clone(),
                        allow_users: g.authorization.allow_users.clone(),
                    },
                )
            })
            .collect();

        Self {
            auth_provider,
            auth_required: auth.required,
            oidc,
            ldap,
            static_user_count,
            static_user_summaries,
            authorization_provider,
            openfga,
            group_authorization,
            operator_roles: auth.operator_roles.clone(),
            operator_groups: auth.operator_groups.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RoutingConfigDto {
    /// JSON key `routingFallback` — matches `ProxyConfig` / YAML camelCase.
    #[serde(rename = "routingFallback")]
    pub routing_fallback: String,
    /// Stable DB id of the fallback cluster group (when known).
    #[serde(
        rename = "routingFallbackGroupId",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_fallback_group_id: Option<i64>,
    pub routers: Vec<serde_json::Value>,
}

impl RoutingConfigDto {
    pub fn from_config(fallback: &str, routers: &[RouterConfig]) -> Self {
        Self {
            routing_fallback: fallback.to_string(),
            routing_fallback_group_id: None,
            routers: routers
                .iter()
                .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
                .collect(),
        }
    }
}

/// Request body for PUT /admin/config/security
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UpsertSecurityConfig {
    pub auth_provider: String,
    pub auth_required: bool,
    pub oidc: Option<serde_json::Value>,
    pub ldap: Option<serde_json::Value>,
    pub static_users: Option<serde_json::Value>,
    pub authorization_provider: String,
    pub openfga: Option<serde_json::Value>,
    #[serde(default)]
    pub operator_roles: Option<Vec<String>>,
    #[serde(default)]
    pub operator_groups: Option<Vec<String>>,
}

/// Request body for PUT /admin/config/routing
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UpsertRoutingConfig {
    /// Accept `routingFallback` (canonical) or legacy `routing_fallback` from older clients.
    #[serde(rename = "routingFallback", alias = "routing_fallback", default)]
    pub routing_fallback: String,
    #[serde(rename = "routingFallbackGroupId", default)]
    pub routing_fallback_group_id: Option<i64>,
    #[serde(default)]
    pub routers: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct AdminState {
    prometheus: Arc<PrometheusMetrics>,
    /// Hot-reloadable live config — used to get the current cluster manager.
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    /// Present when a full-featured persistence backend is configured (e.g. Postgres).
    /// None when running with in-memory persistence.
    admin_store: Option<Arc<dyn AdminStore>>,
    security_config: Arc<SecurityConfigDto>,
    routing_config: Arc<RoutingConfigDto>,
    engine_registry: Arc<EngineRegistry>,
    /// Wake the config reload task immediately after mutating persisted config.
    /// Uses `ConfigRevisionStore::bump_revision()` for distributed notification
    /// and falls back to `tokio::sync::Notify` for in-memory/local-only mode.
    config_reload_notify: Arc<tokio::sync::Notify>,
    /// Snapshot of protocol listeners from startup config (YAML); not hot-reloaded.
    frontends_status: FrontendsStatusDto,
    /// Admin API credential manager — validates Basic auth and handles password changes.
    admin_creds: Arc<AdminCredentialsManager>,
    /// Test a cluster config (build adapter + health_check) without persisting it.
    test_cluster_fn: TestClusterFn,
    /// Query result cache for invalidation endpoints.
    result_cache: Arc<dyn queryflux_cache::QueryResultCache>,
    /// Shared proxy state — in-flight queries, adapters, slot release.
    app: Arc<AppState>,
}

// ---------------------------------------------------------------------------
// AdminFrontend
// ---------------------------------------------------------------------------

pub struct AdminFrontend {
    prometheus: Arc<PrometheusMetrics>,
    live: Arc<tokio::sync::RwLock<LiveConfig>>,
    admin_store: Option<Arc<dyn AdminStore>>,
    port: u16,
    security_config: Arc<SecurityConfigDto>,
    routing_config: Arc<RoutingConfigDto>,
    engine_registry: Arc<EngineRegistry>,
    config_reload_notify: Arc<tokio::sync::Notify>,
    frontends_status: FrontendsStatusDto,
    admin_creds: Arc<AdminCredentialsManager>,
    test_cluster_fn: TestClusterFn,
    cors_allowed_origins: Vec<String>,
    result_cache: Arc<dyn queryflux_cache::QueryResultCache>,
    app: Arc<AppState>,
}

impl AdminFrontend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        prometheus: Arc<PrometheusMetrics>,
        live: Arc<tokio::sync::RwLock<LiveConfig>>,
        admin_store: Option<Arc<dyn AdminStore>>,
        port: u16,
        security_config: Arc<SecurityConfigDto>,
        routing_config: Arc<RoutingConfigDto>,
        engine_registry: Arc<EngineRegistry>,
        config_reload_notify: Arc<tokio::sync::Notify>,
        frontends_status: FrontendsStatusDto,
        admin_creds: Arc<AdminCredentialsManager>,
        test_cluster_fn: TestClusterFn,
        cors_allowed_origins: Vec<String>,
        result_cache: Arc<dyn queryflux_cache::QueryResultCache>,
        app: Arc<AppState>,
    ) -> Self {
        Self {
            prometheus,
            live,
            admin_store,
            port,
            security_config,
            routing_config,
            engine_registry,
            config_reload_notify,
            frontends_status,
            admin_creds,
            test_cluster_fn,
            cors_allowed_origins,
            result_cache,
            app,
        }
    }

    fn router(&self) -> Router {
        let state = Arc::new(AdminState {
            prometheus: self.prometheus.clone(),
            live: self.live.clone(),
            admin_store: self.admin_store.clone(),
            security_config: self.security_config.clone(),
            routing_config: self.routing_config.clone(),
            engine_registry: self.engine_registry.clone(),
            config_reload_notify: self.config_reload_notify.clone(),
            frontends_status: self.frontends_status.clone(),
            admin_creds: self.admin_creds.clone(),
            test_cluster_fn: self.test_cluster_fn.clone(),
            result_cache: self.result_cache.clone(),
            app: self.app.clone(),
        });

        let spec_json =
            serde_json::to_string(&ApiDoc::openapi()).unwrap_or_else(|_| "{}".to_string());

        // Public routes — no authentication required.
        let public = Router::new()
            .route("/health", get(health_handler))
            .route("/readyz", get(readiness_handler))
            // SECURITY: /metrics should be network-restricted in production (firewall or separate listener)
            .route("/metrics", get(metrics_handler))
            .route(
                "/openapi.json",
                get({
                    let spec = spec_json.clone();
                    move || {
                        let spec = spec.clone();
                        async move {
                            (StatusCode::OK, [("content-type", "application/json")], spec)
                        }
                    }
                }),
            )
            .route("/docs", get(swagger_ui_handler));

        // Protected routes — require valid Basic auth credentials.
        let protected = Router::new()
            .route("/admin/clusters", get(clusters_handler))
            .route("/admin/queries", get(list_queries_handler))
            .route("/admin/queries/running", get(list_running_queries_handler))
            .route("/admin/queries/{id}", delete(cancel_query_handler))
            .route("/admin/agents", get(list_agents_handler))
            .route("/admin/conversations", get(list_conversations_handler))
            .route("/admin/conversations/{id}", get(get_conversation_handler))
            .route("/admin/stats", get(get_stats_handler))
            .route("/admin/engines", get(list_engines_handler))
            .route("/admin/engine-stats", get(get_engine_stats_handler))
            .route("/admin/group-stats", get(get_group_stats_handler))
            .route("/admin/frontends", get(frontends_status_handler))
            .route(
                "/admin/clusters/{group}/{cluster}",
                patch(update_cluster_handler),
            )
            .route("/admin/engine-registry", get(engine_registry_handler))
            // Persisted cluster config CRUD (requires Postgres persistence)
            .route("/admin/config/clusters", get(list_cluster_configs_handler))
            .route(
                "/admin/config/clusters/test",
                post(test_cluster_config_handler),
            )
            .route(
                "/admin/config/clusters/{name}",
                get(get_cluster_config_handler)
                    .put(upsert_cluster_config_handler)
                    .patch(rename_cluster_config_handler)
                    .delete(delete_cluster_config_handler),
            )
            // Persisted cluster group config CRUD
            .route("/admin/config/groups", get(list_group_configs_handler))
            .route(
                "/admin/config/groups/{name}",
                get(get_group_config_handler)
                    .put(upsert_group_config_handler)
                    .patch(rename_group_config_handler)
                    .delete(delete_group_config_handler),
            )
            .route(
                "/admin/config/scripts",
                get(list_user_scripts_handler).post(create_user_script_handler),
            )
            .route(
                "/admin/config/scripts/{id}",
                get(get_user_script_handler)
                    .put(update_user_script_handler)
                    .delete(delete_user_script_handler),
            )
            // Security and routing config (read + write)
            .route(
                "/admin/config/security",
                get(get_security_config_handler).put(put_security_config_handler),
            )
            .route(
                "/admin/config/routing",
                get(get_routing_config_handler).put(put_routing_config_handler),
            )
            .route(
                "/admin/config/guardrails",
                get(get_guardrails_config_handler).put(put_guardrails_config_handler),
            )
            // Cache invalidation endpoints
            .route("/admin/cache", delete(invalidate_all_cache_handler))
            .route(
                "/admin/cache/{group}",
                delete(invalidate_group_cache_handler),
            )
            // Auth management endpoints
            .route("/admin/auth/status", get(auth_status_handler))
            .route("/admin/auth/change-password", post(change_password_handler))
            // Routing preview — dry-run, no query is executed and no capacity is consumed
            .route("/admin/route-explain", post(route_explain_handler))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                admin_auth_middleware,
            ));

        let cors = if self.cors_allowed_origins.is_empty() {
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::PATCH,
                    Method::PUT,
                    Method::POST,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any)
        } else {
            let origins: Vec<axum::http::HeaderValue> = self
                .cors_allowed_origins
                .iter()
                .filter_map(|o| o.parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([
                    Method::GET,
                    Method::PATCH,
                    Method::PUT,
                    Method::POST,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any)
        };

        Router::new()
            .merge(public)
            .merge(protected)
            .with_state(state)
            .layer(cors)
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for AdminFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!(
            "Admin server listening on {addr}  — Prometheus: {addr}/metrics  Swagger UI: {addr}/docs"
        );
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Engine(e.to_string()))?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .map_err(|e| queryflux_core::error::QueryFluxError::Engine(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn metrics_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let body = state.prometheus.gather_text();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Axum middleware that enforces HTTP Basic authentication on all protected routes.
///
/// Expects `Authorization: Basic <base64(username:password)>` on every request.
/// Returns `401 Unauthorized` with a `WWW-Authenticate` challenge on failure.
async fn admin_auth_middleware(
    State(state): State<Arc<AdminState>>,
    req: Request,
    next: Next,
) -> Response {
    let auth_header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some((username, password)) = parse_basic_auth(auth_header) {
        if state.admin_creds.verify(&username, &password).await {
            return next.run(req).await;
        }
        warn!(username, "Admin API: invalid credentials");
    } else {
        warn!("Admin API: missing or malformed Authorization header");
    }

    (
        StatusCode::UNAUTHORIZED,
        [(
            "WWW-Authenticate",
            r#"Basic realm="QueryFlux Admin", charset="UTF-8""#,
        )],
        "Unauthorized",
    )
        .into_response()
}

/// Parse `Authorization: Basic <base64(user:pass)>` → `(username, password)`.
fn parse_basic_auth(header: &str) -> Option<(String, String)> {
    let encoded = header.strip_prefix("Basic ")?;
    let decoded = base64_decode(encoded)?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Minimal base64 decoder (no extra deps — same approach as Trino HTTP frontend).
fn base64_decode(encoded: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    String::from_utf8(bytes).ok()
}

// ---------------------------------------------------------------------------
// Auth management handlers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct AuthStatusResponse {
    /// `true` once the operator has changed the password via the web UI.
    /// `false` means bootstrap (YAML/env) credentials are still in use.
    db_override: bool,
    /// `true` when settings are backed by Postgres (survive restart).
    durable_store: bool,
}

async fn auth_status_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let db_override = state.admin_creds.has_db_override().await;
    let durable_store = state.admin_creds.settings_are_durable();
    Json(AuthStatusResponse {
        db_override,
        durable_store,
    })
}

#[derive(Deserialize)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

async fn change_password_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<ChangePasswordRequest>,
) -> impl IntoResponse {
    match state
        .admin_creds
        .change_password(&body.current_password, &body.new_password)
        .await
    {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response(),
        Err(QueryFluxError::Auth(msg)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": msg})),
        )
            .into_response(),
        Err(QueryFluxError::Persistence(e)) => {
            tracing::error!(error = %e, "failed to persist admin password change");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "failed to persist password change"})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "unexpected error changing admin password");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "failed to change password"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Standard handlers
// ---------------------------------------------------------------------------

/// Liveness probe — unconditionally returns 200 to indicate the process is alive.
#[utoipa::path(
    get,
    path = "/health",
    tag = "admin",
    responses((status = 200, description = "Service is alive", body = str))
)]
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness probe — returns 200 only when the proxy has at least one configured
/// cluster group with a non-empty adapter set. Kubernetes should use this for the
/// `readinessProbe` so traffic is not routed to a replica that hasn't finished
/// loading its config (or whose last reload left it with zero adapters).
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "admin",
    responses(
        (status = 200, description = "Service is ready to accept traffic", body = str),
        (status = 503, description = "Service is not yet ready", body = str),
    )
)]
async fn readiness_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let live = state.live.read().await;
    if live.adapters.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no adapters loaded").into_response();
    }
    let cm = live.cluster_manager.clone();
    drop(live);

    match cm.all_cluster_states().await {
        Ok(states) if states.is_empty() => {
            (StatusCode::SERVICE_UNAVAILABLE, "no cluster groups").into_response()
        }
        Ok(_) => (StatusCode::OK, "ready").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "cluster manager error").into_response(),
    }
}

/// Protocol frontends enabled at process start (from YAML). Not hot-reloaded.
#[utoipa::path(
    get,
    path = "/admin/frontends",
    tag = "admin",
    responses(
        (status = 200, description = "Frontend protocol snapshot", body = FrontendsStatusDto),
    )
)]
async fn frontends_status_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    Json(state.frontends_status.clone())
}

/// Live state of all cluster groups.
#[utoipa::path(
    get,
    path = "/admin/clusters",
    tag = "admin",
    responses(
        (status = 200, description = "Cluster state snapshots", body = Vec<ClusterStateDto>),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn clusters_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let cluster_manager = state.live.read().await.cluster_manager.clone();
    match cluster_manager.all_cluster_states().await {
        Ok(snapshots) => {
            let dtos: Vec<ClusterStateDto> = snapshots
                .into_iter()
                .map(|s| ClusterStateDto {
                    group_name: s.group_name.0,
                    cluster_name: s.cluster_name.0,
                    engine_type: format!("{:?}", s.engine_type),
                    endpoint: s.endpoint,
                    running_queries: s.running_queries,
                    queued_queries: s.queued_queries,
                    max_running_queries: s.max_running_queries,
                    is_healthy: s.is_healthy,
                    enabled: s.enabled,
                })
                .collect();
            Json(dtos).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Paginated query history. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/queries",
    tag = "admin",
    params(QueryFilters),
    responses(
        (status = 200, description = "Query records (newest first)", body = Vec<QuerySummary>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_queries_handler(
    State(state): State<Arc<AdminState>>,
    Query(filters): Query<QueryFilters>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    match pg.list_queries(&filters).await {
        Ok(rows) => Json::<Vec<QuerySummary>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RunningQueryDto {
    pub id: String,
    pub backend_query_id: Option<String>,
    pub submitted_by: String,
    pub group: String,
    pub cluster: Option<String>,
    pub sql_preview: String,
    /// `executing` or `queued`
    pub state: String,
}

fn sql_preview(sql: &str) -> String {
    let t = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if t.chars().count() > 160 {
        format!("{}…", t.chars().take(159).collect::<String>())
    } else {
        t
    }
}

/// Build the admin "running queries" response from persistence.
async fn collect_running_queries(
    persistence: &dyn queryflux_persistence::Persistence,
) -> queryflux_core::error::Result<Vec<RunningQueryDto>> {
    let mut out = Vec::new();
    for q in persistence.list_all().await? {
        out.push(RunningQueryDto {
            id: q.id.0,
            backend_query_id: Some(q.backend_query_id.0),
            submitted_by: q.submitted_by,
            group: q.cluster_group.0,
            cluster: Some(q.cluster_name.0),
            sql_preview: sql_preview(&q.sql),
            state: "executing".to_string(),
        });
    }
    for q in persistence.list_queued().await? {
        out.push(RunningQueryDto {
            id: q.id.0,
            backend_query_id: None,
            submitted_by: q.submitted_by,
            group: q.cluster_group.0,
            cluster: None,
            sql_preview: sql_preview(&q.sql),
            state: "queued".to_string(),
        });
    }
    Ok(out)
}

/// Admin cancel path for a queued query (claim release handled by caller).
pub(crate) async fn delete_queued_if_exists(
    persistence: &dyn queryflux_persistence::Persistence,
    id: &str,
) -> queryflux_core::error::Result<Option<queryflux_core::query::QueuedQuery>> {
    persistence.take_queued(&ProxyQueryId(id.to_string())).await
}

pub(crate) async fn find_executing_query(
    persistence: &dyn queryflux_persistence::Persistence,
    id: &str,
) -> queryflux_core::error::Result<Option<ExecutingQuery>> {
    if let Some(q) = persistence.get(&BackendQueryId(id.to_string())).await? {
        return Ok(Some(q));
    }
    Ok(persistence
        .list_all()
        .await?
        .into_iter()
        .find(|q| q.id.0 == id))
}

/// In-flight executing + queued queries (not history).
#[utoipa::path(
    get,
    path = "/admin/queries/running",
    tag = "admin",
    responses(
        (status = 200, description = "In-flight queries", body = Vec<RunningQueryDto>),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_running_queries_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    match collect_running_queries(state.app.persistence.as_ref()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Cancel any in-flight or queued query. Admin Basic auth is the privilege.
#[utoipa::path(
    delete,
    path = "/admin/queries/{id}",
    tag = "admin",
    params(("id" = String, Path, description = "Proxy query id or backend query id")),
    responses(
        (status = 204, description = "Cancelled"),
        (status = 404, description = "Query not found", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn cancel_query_handler(
    State(state): State<Arc<AdminState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let persistence = &state.app.persistence;

    match find_executing_query(persistence.as_ref(), &id).await {
        Ok(Some(executing)) => return cancel_executing(&state, executing).await,
        Ok(None) => {}
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    let qid = ProxyQueryId(id.clone());
    match delete_queued_if_exists(persistence.as_ref(), &id).await {
        Ok(Some(queued)) => {
            state.app.record_queued_terminal(
                &queued,
                queryflux_core::query::QueryStatus::Cancelled,
                "admin cancelled",
            );
            // A worker may have claimed and started executing between the first
            // lookup and this delete. Cancel that path too.
            match find_executing_query(persistence.as_ref(), &id).await {
                Ok(Some(executing)) => return cancel_executing(&state, executing).await,
                Ok(None) => {}
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
            if let Some(qc) = &state.app.queue_coordinator {
                let _ = qc.release_claim(&qid.0).await;
            }
            info!(id = %qid, "Admin cancelled queued query");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => {
            // Claimed and moved to executing after the first lookup.
            match find_executing_query(persistence.as_ref(), &id).await {
                Ok(Some(executing)) => cancel_executing(&state, executing).await,
                Ok(None) => (StatusCode::NOT_FOUND, "query not found").into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn cancel_executing(
    state: &AdminState,
    executing: queryflux_core::query::ExecutingQuery,
) -> Response {
    match cancel_executing_query(
        &state.app,
        queryflux_core::query::FrontendProtocol::TrinoHttp,
        &executing,
        "admin cancelled",
    )
    .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
    }
}

/// Cancel an in-flight executing query on its backend and record the outcome.
///
/// Shared by the admin `DELETE /admin/queries/{id}` handler (above, no ownership check —
/// admin credentials are already full-privilege) and the MCP `cancel_query` tool (which
/// checks `require_query_owner` before calling this, since MCP callers authenticate as a
/// regular user, not an admin).
pub(crate) async fn cancel_executing_query(
    app: &Arc<AppState>,
    protocol: queryflux_core::query::FrontendProtocol,
    executing: &queryflux_core::query::ExecutingQuery,
    reason: &str,
) -> std::result::Result<(), String> {
    let Some(adapter) = app.adapter(&executing.cluster_name.0).await else {
        warn!(
            id = %executing.id,
            cluster = %executing.cluster_name,
            "Cancel: no adapter for cluster"
        );
        return Err("failed to cancel query on backend".to_string());
    };
    if let Err(e) = adapter
        .cancel_query(&executing.backend_query_id, executing.wire_auth.as_ref())
        .await
    {
        warn!(
            id = %executing.id,
            backend = %executing.backend_query_id,
            "Adapter cancel failed: {e}"
        );
        return Err("failed to cancel query on backend".to_string());
    }
    app.record_executing_cancelled(
        executing,
        protocol,
        adapter.engine_type(),
        adapter.translation_target_dialect(),
        reason,
    );
    app.release_query_slot(
        &executing.cluster_group,
        &executing.cluster_name,
        &executing.id.0,
    )
    .await;
    if let Err(e) = app.persistence.delete(&executing.backend_query_id).await {
        return Err(e.to_string());
    }
    info!(
        id = %executing.id,
        backend = %executing.backend_query_id,
        owner = %executing.submitted_by,
        "Cancelled executing query"
    );
    Ok(())
}

/// Distinct agents that have run queries, with aggregate stats.
#[utoipa::path(
    get,
    path = "/admin/agents",
    tag = "admin",
    params(
        ("limit" = Option<i64>, Query, description = "Page size (default 50)"),
        ("offset" = Option<i64>, Query, description = "Page offset (default 0)"),
    ),
    responses(
        (status = 200, description = "Agent summaries", body = Vec<queryflux_persistence::query_history::AgentSummary>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_agents_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    match pg.list_agents(limit, offset).await {
        Ok(rows) => Json::<Vec<AgentSummary>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Conversations grouped by conversation_id. Filter by agent_id via ?agent_id=.
#[utoipa::path(
    get,
    path = "/admin/conversations",
    tag = "admin",
    params(
        ("agent_id" = Option<String>, Query, description = "Filter by agent id"),
        ("limit" = Option<i64>, Query, description = "Page size (default 50)"),
        ("offset" = Option<i64>, Query, description = "Page offset (default 0)"),
    ),
    responses(
        (status = 200, description = "Conversation summaries", body = Vec<queryflux_persistence::query_history::ConversationSummary>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_conversations_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let agent_id = params.get("agent_id").map(|s| s.as_str());
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    match pg.list_conversations(agent_id, limit, offset).await {
        Ok(rows) => Json::<Vec<ConversationSummary>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// All query steps for a conversation, ordered by step_index.
#[utoipa::path(
    get,
    path = "/admin/conversations/{id}",
    tag = "admin",
    params(("id" = String, Path, description = "Conversation id")),
    responses(
        (status = 200, description = "Query steps for this conversation", body = Vec<QuerySummary>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_conversation_handler(
    State(state): State<Arc<AdminState>>,
    Path(conversation_id): Path<String>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    match pg.get_conversation(&conversation_id).await {
        Ok(rows) => Json::<Vec<QuerySummary>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Route explain — dry-run routing/guard/capacity preview
// ---------------------------------------------------------------------------

/// Request body for `POST /admin/route-explain`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RouteExplainRequest {
    pub sql: String,
    /// Wire protocol to route as — same values as `protocolBased` router config
    /// (`trinoHttp`, `postgresWire`, `mysqlWire`, `clickhouseHttp`, `flightSql`,
    /// `snowflakeHttp`, `snowflakeSqlApi`).
    #[schema(value_type = String)]
    pub protocol: FrontendProtocol,
    /// Simulated identity for authorization-aware routing and guard preview.
    /// **Not verified against any `AuthProvider`** — this endpoint answers "if a query came
    /// in as this user, what would happen," which is what makes it useful for testing
    /// `allowGroups`/`allowUsers` rules before rolling them out. Because it sits behind the
    /// same admin Basic-auth gate as every other `/admin/*` route, an admin credential
    /// holder can already probe outcomes for any simulated user — that is the intended use,
    /// not a bypass of real authentication.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub database: Option<String>,
    /// Query tags, e.g. `{"team": "eng", "batch": null}` — same shape session tags use.
    #[serde(default)]
    pub tags: QueryTags,
}

/// Response body for `POST /admin/route-explain`. Nothing here is persisted — this is a
/// pure computation over already-loaded config and live cluster state.
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteExplainResponse {
    /// Same shape Studio already renders for a historical query's `routing_trace`.
    #[schema(value_type = Object)]
    pub routing_trace: RoutingTrace,
    /// Set when a router or authorization check would deny the query before it ever reaches
    /// guardrails or dispatch. When set, `guard_actions` is empty and `capacity` is absent —
    /// nothing downstream of the deny was evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied: Option<String>,
    /// Guard verdicts from the pre-dispatch guard pass (global chain, then the resolved
    /// group's chain). Empty when no guardrails are configured for the resolved group.
    #[schema(value_type = Vec<Object>)]
    pub guard_actions: Vec<GuardAction>,
    /// True if any guard action above was a deny — the query would never reach dispatch.
    pub would_be_guard_blocked: bool,
    /// Best-effort, moment-in-time capacity snapshot of the resolved group's members.
    /// Unlike `routing_trace`/`guard_actions` (deterministic, config-driven — the same
    /// answer whether you ask now or later), this reflects live runtime state that can
    /// change before you act on it. It also doesn't check the group's `maxQueuedQueries`
    /// admission limit — `would_queue: true` means "would not dispatch immediately," not
    /// a guarantee the query would successfully queue rather than being rejected. Treat
    /// this field as advisory; for authoritative live state, poll `GET /admin/clusters`.
    /// Absent when `denied` is set (no group was resolved).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<GroupCapacityDto>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GroupCapacityDto {
    pub group_name: String,
    pub members: Vec<ClusterStateDto>,
    /// True when no member is currently enabled, healthy, and under `max_running_queries` —
    /// i.e. the query would not dispatch immediately, as of this snapshot. Best-effort: see
    /// the caveat on `RouteExplainResponse::capacity`.
    pub would_queue: bool,
}

fn empty_route_explain_response(mut trace: RoutingTrace, denied: String) -> RouteExplainResponse {
    // route_with_trace already sets trace.denied for a router-level Deny, but
    // resolve_routed_group and the unconditional authorization check below it never
    // touch the trace — set it here too so RoutingTraceView (which branches on
    // trace.denied, not on the top-level field) renders consistently with `denied`
    // regardless of which stage produced the denial.
    trace.denied = Some(denied.clone());
    RouteExplainResponse {
        routing_trace: trace,
        denied: Some(denied),
        guard_actions: Vec::new(),
        would_be_guard_blocked: false,
        capacity: None,
    }
}

/// Preview where a query would route, whether it would be authorized, and whether any
/// guardrail would block it — **without executing the query or consuming any capacity**.
/// The primary, deterministic answer this endpoint exists to give: routing/authz/guardrail
/// verdicts are config-driven, so the same request gives the same answer whether config
/// hasn't changed since. Also includes a best-effort capacity snapshot as a bonus signal
/// (see the caveat on `RouteExplainResponse::capacity`) — live runtime state, not something
/// this endpoint tries to make authoritative.
///
/// Mirrors the real dispatch order exactly (route → fallback-resolve → authorize → guard
/// preview → capacity), so the answer this endpoint gives matches what would actually
/// happen. Never calls `ClusterGroupManager::acquire_cluster` — capacity is read via the
/// same live snapshot `/admin/clusters` uses, so calling this endpoint has no side effects.
#[utoipa::path(
    post,
    path = "/admin/route-explain",
    tag = "admin",
    request_body = RouteExplainRequest,
    responses(
        (status = 200, description = "Routing/guard/capacity preview", body = RouteExplainResponse),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn route_explain_handler(
    State(state): State<Arc<AdminState>>,
    Json(req): Json<RouteExplainRequest>,
) -> impl IntoResponse {
    // Snapshot everything needed from LiveConfig in one lock acquisition — same discipline
    // dispatch_query uses — so no lock is held across the awaits in build_route_explain_response.
    let (
        router_chain,
        authorization,
        guard_chain,
        group_guard_chains,
        group_default_tags,
        group_translation_scripts,
        group_order,
        cluster_manager,
    ) = {
        let live = state.live.read().await;
        (
            live.router_chain.clone(),
            live.authorization.clone(),
            live.guard_chain.clone(),
            live.group_guard_chains.clone(),
            live.group_default_tags.clone(),
            live.group_translation_scripts.clone(),
            live.group_order.clone(),
            live.cluster_manager.clone(),
        )
    };

    match build_route_explain_response(
        router_chain.as_ref(),
        authorization.as_ref(),
        guard_chain.as_deref(),
        &group_guard_chains,
        &group_default_tags,
        &group_translation_scripts,
        &group_order,
        cluster_manager.as_ref(),
        state.app.translation.as_ref(),
        &req,
    )
    .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Core logic behind [`route_explain_handler`], factored out so it can be unit-tested with
/// lightweight fixtures instead of a full `AdminState`. Mirrors the real dispatch order
/// exactly (route → fallback-resolve → authorize → guard preview → capacity) and never calls
/// `ClusterGroupManager::acquire_cluster` — capacity is read via the same live snapshot
/// `/admin/clusters` uses, so calling it has no side effects.
#[allow(clippy::too_many_arguments)]
async fn build_route_explain_response(
    router_chain: &RouterChain,
    authorization: &dyn AuthorizationChecker,
    guard_chain: Option<&GuardChain>,
    group_guard_chains: &HashMap<String, Arc<GuardChain>>,
    group_default_tags: &HashMap<String, QueryTags>,
    group_translation_scripts: &HashMap<String, Vec<String>>,
    group_order: &[String],
    cluster_manager: &dyn ClusterGroupManager,
    translation: &TranslationService,
    req: &RouteExplainRequest,
) -> Result<RouteExplainResponse> {
    let auth_ctx = AuthContext {
        user: req.user.clone().unwrap_or_else(|| "anonymous".to_string()),
        groups: req.groups.clone(),
        ..Default::default()
    };
    let session = SessionContext {
        user: req.user.clone(),
        database: req.database.clone(),
        tags: req.tags.clone(),
        ..Default::default()
    };

    let (chain_result, mut trace) = router_chain
        .route_with_trace(&req.sql, &session, &req.protocol, Some(&auth_ctx))
        .await?;

    let group = match chain_result {
        ChainRouteResult::Denied { message } => {
            return Ok(empty_route_explain_response(trace, message));
        }
        ChainRouteResult::Routed(group) => group,
    };

    // Fallback-only authorization-aware reroute — mirrors what frontends do today.
    let group = match resolve_routed_group(group_order, authorization, group, &mut trace, &auth_ctx)
        .await
    {
        Ok(g) => g,
        Err(e) => return Ok(empty_route_explain_response(trace, e.to_string())),
    };

    // Unconditional per-group authorization check — dispatch enforces this for every
    // resolved group, not just ones reached via fallback. Same helper dispatch_query and
    // execute_to_sink use, so this can't quietly disagree with what they'd decide.
    if let Some(msg) = check_group_authorized(authorization, &auth_ctx, &group).await {
        return Ok(empty_route_explain_response(trace, msg));
    }

    // Live capacity — READ-ONLY. Never call acquire_cluster here: that mutates real state
    // (increments a running-query counter with nothing to ever release it).
    let all_states = cluster_manager.all_cluster_states().await?;
    let members: Vec<_> = all_states
        .into_iter()
        .filter(|s| s.group_name.0 == group.0)
        .collect();
    // Guard dialect parsing needs a concrete engine type; the first member stands in for the
    // group. Groups with heterogeneous engine types across members are not modeled here — a
    // guard that would pass on one member's dialect but not another's is a known limitation.
    let engine_type = members
        .first()
        .map(|m| m.engine_type.clone())
        .unwrap_or(EngineType::Undispatched);
    let would_queue = !members
        .iter()
        .any(|m| m.enabled && m.is_healthy && m.running_queries < m.max_running_queries);
    let capacity = GroupCapacityDto {
        group_name: group.0.clone(),
        members: members
            .into_iter()
            .map(|s| ClusterStateDto {
                group_name: s.group_name.0,
                cluster_name: s.cluster_name.0,
                engine_type: format!("{:?}", s.engine_type),
                endpoint: s.endpoint,
                running_queries: s.running_queries,
                queued_queries: s.queued_queries,
                max_running_queries: s.max_running_queries,
                is_healthy: s.is_healthy,
                enabled: s.enabled,
            })
            .collect(),
        would_queue,
    };

    // Guard preview — same chain-run pattern production uses pre-dispatch, but with the
    // resolved group's real engine type (more accurate than the placeholder engine type
    // production uses before cluster selection has happened). dispatch_query runs guards
    // *after* translation, against the final SQL (see "Guard chain: runs after translation"
    // in dispatch.rs) — translate here too so a client-dialect query routed to a
    // different-dialect engine is guard-checked against the SQL that would actually run,
    // not against SQL in the client's dialect parsed as if it were the engine's.
    let group_fixups = group_translation_scripts
        .get(&group.0)
        .cloned()
        .unwrap_or_default();
    let translated_sql = translation
        .maybe_translate(
            &req.sql,
            &req.protocol.default_dialect(),
            &engine_type.dialect(),
            &queryflux_translation::SchemaContext::default(),
            &group_fixups,
        )
        .await?;
    let effective_tags = merge_tags(
        &group_default_tags
            .get(&group.0)
            .cloned()
            .unwrap_or_default(),
        &session.tags,
    );
    let resolved_agent_ctx = session.resolved_agent_context();
    let guard_ctx = GuardContext {
        sql: &req.sql,
        translated_sql: &translated_sql,
        engine_type: &engine_type,
        cluster_group: &group,
        user: session.user(),
        agent_context: resolved_agent_ctx.as_ref(),
        query_tags: &effective_tags,
    };
    let group_guard_chain = group_guard_chains.get(&group.0).map(|c| c.as_ref());
    // Same shared function dispatch_query and execute_to_sink_inner run their guard
    // chains through — see queryflux_guardrails::run_guard_chains.
    let (guard_actions, would_be_guard_blocked) = queryflux_guardrails::run_guard_chains(
        [guard_chain, group_guard_chain],
        &guard_ctx,
        GuardLayer::Plan,
    )
    .await;

    Ok(RouteExplainResponse {
        routing_trace: trace,
        denied: None,
        guard_actions,
        would_be_guard_blocked,
        capacity: Some(capacity),
    })
}

/// Dashboard stats for the last hour. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/stats",
    tag = "admin",
    responses(
        (status = 200, description = "Aggregated last-hour stats", body = DashboardStats),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_stats_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    match pg.get_dashboard_stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Distinct engine types that have recorded queries. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/engines",
    tag = "admin",
    responses(
        (status = 200, description = "List of engine type strings", body = Vec<String>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_engines_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    match pg.list_engines().await {
        Ok(engines) => Json(engines).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Per-engine aggregated stats. Optional `?hours=N` window (default 24). Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/engine-stats",
    tag = "admin",
    params(
        ("hours" = Option<i64>, Query, description = "Look-back window in hours (default 24)")
    ),
    responses(
        (status = 200, description = "Per-engine aggregated stats", body = Vec<EngineStatRow>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_engine_stats_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<EngineStatsParams>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let hours = params.hours.unwrap_or(24).clamp(1, 168);
    match pg.get_engine_stats(hours).await {
        Ok(rows) => Json::<Vec<EngineStatRow>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Per-cluster-group aggregated stats. Optional `?hours=N` window (default 24). Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/group-stats",
    tag = "admin",
    params(
        ("hours" = Option<i64>, Query, description = "Look-back window in hours (default 24)")
    ),
    responses(
        (status = 200, description = "Per-group aggregated stats", body = Vec<GroupStatRow>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_group_stats_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<EngineStatsParams>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let hours = params.hours.unwrap_or(24).clamp(1, 168);
    match pg.get_group_stats(hours).await {
        Ok(rows) => Json::<Vec<GroupStatRow>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, serde::Deserialize)]
struct EngineStatsParams {
    hours: Option<i64>,
}

/// Request body for `PATCH /admin/clusters/:group/:cluster`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ClusterUpdateRequest {
    /// Set the administrative enabled state. `null` / absent = no change.
    pub enabled: Option<bool>,
    /// Update the maximum concurrent query limit. `null` / absent = no change.
    pub max_running_queries: Option<u64>,
}

/// Update mutable runtime config for a cluster (enable/disable, concurrency limit).
#[utoipa::path(
    patch,
    path = "/admin/clusters/{group}/{cluster}",
    tag = "admin",
    params(
        ("group" = String, Path, description = "Cluster group name"),
        ("cluster" = String, Path, description = "Cluster name"),
    ),
    request_body = ClusterUpdateRequest,
    responses(
        (status = 200, description = "Updated cluster state snapshot", body = ClusterStateDto),
        (status = 404, description = "Cluster not found", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn update_cluster_handler(
    State(state): State<Arc<AdminState>>,
    Path((group, cluster)): Path<(String, String)>,
    Json(body): Json<ClusterUpdateRequest>,
) -> impl IntoResponse {
    let group = ClusterGroupName(group);
    let cluster_name = ClusterName(cluster);

    let cluster_manager = state.live.read().await.cluster_manager.clone();
    match cluster_manager
        .update_cluster(
            &group,
            &cluster_name,
            body.enabled,
            body.max_running_queries,
        )
        .await
    {
        Ok(false) => (StatusCode::NOT_FOUND, "Cluster not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Ok(true) => match cluster_manager.cluster_state(&group, &cluster_name).await {
            Ok(Some(s)) => Json(ClusterStateDto {
                group_name: s.group_name.0,
                cluster_name: s.cluster_name.0,
                engine_type: format!("{:?}", s.engine_type),
                endpoint: s.endpoint,
                running_queries: s.running_queries,
                queued_queries: s.queued_queries,
                max_running_queries: s.max_running_queries,
                is_healthy: s.is_healthy,
                enabled: s.enabled,
            })
            .into_response(),
            Ok(None) => (StatusCode::NOT_FOUND, "Cluster not found after update").into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
    }
}

// ---------------------------------------------------------------------------
// Persisted cluster config CRUD
// ---------------------------------------------------------------------------

/// Redact sensitive fields from a cluster config JSON value in-place.
fn redact_cluster_config_secrets(config: &mut serde_json::Value) {
    if let Some(obj) = config.as_object_mut() {
        for key in [
            "authPassword",
            "password",
            "authToken",
            "awsSecretAccessKey",
            "secretAccessKey",
            "token",
        ] {
            if obj.contains_key(key) {
                obj.insert(
                    key.to_string(),
                    serde_json::Value::String("***REDACTED***".to_string()),
                );
            }
        }
        for (_, val) in obj.iter_mut() {
            if val.is_object() {
                redact_cluster_config_secrets(val);
            } else if let Some(arr) = val.as_array_mut() {
                for item in arr.iter_mut() {
                    redact_cluster_config_secrets(item);
                }
            }
        }
    }
}

/// Clone a `ClusterConfigRecord` with secrets redacted from the `config` field.
fn redacted_cluster_config(record: ClusterConfigRecord) -> ClusterConfigRecord {
    let mut record = record;
    redact_cluster_config_secrets(&mut record.config);
    record
}

macro_rules! require_store {
    ($state:expr) => {
        match &$state.admin_store {
            Some(store) => store,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Persistent backend not configured",
                )
                    .into_response()
            }
        }
    };
}

/// Notify all QueryFlux replicas that config has changed.
///
/// When a persistence backend with `ConfigRevisionStore` is available (e.g.
/// Postgres), bumps the shared revision counter which triggers `LISTEN/NOTIFY`
/// to all replicas. Always also wakes the local reload task via `Notify` as a
/// fast-path for the instance that made the change.
fn notify_live_config_reload(state: &AdminState) {
    if let Some(store) = &state.admin_store {
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = store.bump_revision().await {
                tracing::warn!("Failed to bump config revision: {e}");
            }
        });
    }
    state.config_reload_notify.notify_one();
}

fn rename_persistence_error_status(e: &queryflux_core::error::QueryFluxError) -> StatusCode {
    let msg = e.to_string();
    if msg.contains("not found") {
        StatusCode::NOT_FOUND
    } else if msg.contains("already in use") {
        StatusCode::CONFLICT
    } else if msg.contains("must not be empty") {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// List all persisted cluster configurations.
#[utoipa::path(
    get,
    path = "/admin/config/clusters",
    tag = "config",
    responses(
        (status = 200, description = "All cluster config records", body = Vec<queryflux_persistence::cluster_config::ClusterConfigRecord>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_cluster_configs_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.list_cluster_configs().await {
        Ok(rows) => {
            let redacted: Vec<ClusterConfigRecord> =
                rows.into_iter().map(redacted_cluster_config).collect();
            Json(redacted).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Get a single cluster configuration by name.
#[utoipa::path(
    get,
    path = "/admin/config/clusters/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Cluster name")),
    responses(
        (status = 200, description = "Cluster config record", body = queryflux_persistence::cluster_config::ClusterConfigRecord),
        (status = 404, description = "Not found", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.get_cluster_config(&name).await {
        Ok(Some(r)) => Json(redacted_cluster_config(r)).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Cluster config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Mirrors the startup validation in `main.rs` for clusters loaded from YAML — the
/// engine × `queryAuth` matrix must reject the same combinations here, at the moment an
/// operator saves them through Studio, not only at the next process restart.
fn validate_query_auth_for_upsert(body: &UpsertClusterConfig) -> std::result::Result<(), String> {
    let engine = queryflux_core::engine_registry::parse_engine_key(&body.engine_key)
        .map_err(|e| e.to_string())?;
    let query_auth =
        queryflux_core::engine_registry::parse_query_auth_from_config_json(&body.config)
            .map_err(|e| e.to_string())?;
    if let Some(mode) = &query_auth {
        // ADBC's `driver` key lives in the raw config JSON, not on `EngineConfig` itself —
        // `query_auth_supported` needs it to tell an OAuth-capable driver (e.g. snowflake)
        // apart from one that isn't.
        let driver = body.config.get("driver").and_then(|v| v.as_str());
        queryflux_core::config::query_auth_supported(Some(&engine), driver, mode)?;
    }
    Ok(())
}

/// Create or fully replace a cluster configuration.
#[utoipa::path(
    put,
    path = "/admin/config/clusters/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Cluster name")),
    request_body = queryflux_persistence::cluster_config::UpsertClusterConfig,
    responses(
        (status = 200, description = "Updated cluster config record", body = queryflux_persistence::cluster_config::ClusterConfigRecord),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn upsert_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<UpsertClusterConfig>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    if let Err(e) = validate_query_auth_for_upsert(&body) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    match pg.upsert_cluster_config(&name, &body).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            Json(redacted_cluster_config(r)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Rename a cluster configuration.
#[utoipa::path(
    patch,
    path = "/admin/config/clusters/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Current cluster name")),
    request_body = queryflux_persistence::cluster_config::RenameConfigRequest,
    responses(
        (status = 200, description = "Renamed cluster config record", body = queryflux_persistence::cluster_config::ClusterConfigRecord),
        (status = 409, description = "Name already in use", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn rename_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<RenameConfigRequest>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.rename_cluster_config(&name, &body.new_name).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            Json(redacted_cluster_config(r)).into_response()
        }
        Err(e) => (rename_persistence_error_status(&e), e.to_string()).into_response(),
    }
}

/// Delete a cluster configuration.
#[utoipa::path(
    delete,
    path = "/admin/config/clusters/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Cluster name")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn delete_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.delete_cluster_config(&name).await {
        Ok(true) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, "Cluster config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct TestClusterConfigRequest {
    engine_key: String,
    config: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
struct TestClusterConfigResponse {
    ok: bool,
    message: String,
}

/// Test a cluster connection without persisting it.
#[utoipa::path(
    post,
    path = "/admin/config/clusters/test",
    tag = "config",
    request_body = TestClusterConfigRequest,
    responses(
        (status = 200, description = "Connection test result", body = TestClusterConfigResponse),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn test_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<TestClusterConfigRequest>,
) -> impl IntoResponse {
    match (state.test_cluster_fn)(body.engine_key, body.config).await {
        Ok(true) => Json(TestClusterConfigResponse {
            ok: true,
            message: "Connection successful".to_string(),
        })
        .into_response(),
        Ok(false) => Json(TestClusterConfigResponse {
            ok: false,
            message: "Adapter built but health check failed — check credentials and connectivity"
                .to_string(),
        })
        .into_response(),
        Err(e) => Json(TestClusterConfigResponse {
            ok: false,
            message: e.to_string(),
        })
        .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Persisted cluster group config CRUD
// ---------------------------------------------------------------------------

/// List all persisted cluster group configurations.
#[utoipa::path(
    get,
    path = "/admin/config/groups",
    tag = "config",
    responses(
        (status = 200, description = "All cluster group config records", body = Vec<queryflux_persistence::cluster_config::ClusterGroupConfigRecord>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_group_configs_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.list_group_configs().await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Get a single cluster group configuration by name.
#[utoipa::path(
    get,
    path = "/admin/config/groups/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Group name")),
    responses(
        (status = 200, description = "Cluster group config record", body = queryflux_persistence::cluster_config::ClusterGroupConfigRecord),
        (status = 404, description = "Not found", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.get_group_config(&name).await {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Group config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Create or fully replace a cluster group configuration.
#[utoipa::path(
    put,
    path = "/admin/config/groups/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Group name")),
    request_body = queryflux_persistence::cluster_config::UpsertClusterGroupConfig,
    responses(
        (status = 200, description = "Updated cluster group config record", body = queryflux_persistence::cluster_config::ClusterGroupConfigRecord),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn upsert_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<UpsertClusterGroupConfig>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.upsert_group_config(&name, &body).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            Json(r).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Rename a cluster group configuration.
#[utoipa::path(
    patch,
    path = "/admin/config/groups/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Current group name")),
    request_body = queryflux_persistence::cluster_config::RenameConfigRequest,
    responses(
        (status = 200, description = "Renamed cluster group config record", body = queryflux_persistence::cluster_config::ClusterGroupConfigRecord),
        (status = 409, description = "Name already in use", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn rename_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<RenameConfigRequest>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.rename_group_config(&name, &body.new_name).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            Json(r).into_response()
        }
        Err(e) => (rename_persistence_error_status(&e), e.to_string()).into_response(),
    }
}

/// Delete a cluster group configuration.
#[utoipa::path(
    delete,
    path = "/admin/config/groups/{name}",
    tag = "config",
    params(("name" = String, Path, description = "Group name")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found", body = str),
        (status = 409, description = "Still referenced by routing rules", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn delete_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.delete_group_config(&name).await {
        Ok(true) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, "Group config not found").into_response(),
        Err(e) => {
            let msg = e.to_string();
            let code = if msg.contains("still referenced by routing") {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, msg).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// User script library (translation fixups + routing — reusable snippets)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct UserScriptListQuery {
    kind: Option<String>,
}

/// List user scripts. Optional `?kind=translation_fixup` or `?kind=guard` filter.
#[utoipa::path(
    get,
    path = "/admin/config/scripts",
    tag = "config",
    params(
        ("kind" = Option<String>, Query, description = "Filter by kind: `translation_fixup` or `guard`")
    ),
    responses(
        (status = 200, description = "Script records", body = Vec<UserScriptRecord>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_user_scripts_handler(
    State(state): State<Arc<AdminState>>,
    Query(q): Query<UserScriptListQuery>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    let kind = q.kind.as_deref().filter(|s| !s.is_empty());
    match pg.list_user_scripts(kind).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Create a new user script.
#[utoipa::path(
    post,
    path = "/admin/config/scripts",
    tag = "config",
    request_body = UpsertUserScript,
    responses(
        (status = 201, description = "Created script record", body = UserScriptRecord),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn create_user_script_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<UpsertUserScript>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.create_user_script(&body).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            (StatusCode::CREATED, Json(r)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Get a user script by id.
#[utoipa::path(
    get,
    path = "/admin/config/scripts/{id}",
    tag = "config",
    params(("id" = i64, Path, description = "Script id")),
    responses(
        (status = 200, description = "Script record", body = UserScriptRecord),
        (status = 404, description = "Not found", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_user_script_handler(
    State(state): State<Arc<AdminState>>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.get_user_script(id).await {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Script not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Replace a user script by id.
#[utoipa::path(
    put,
    path = "/admin/config/scripts/{id}",
    tag = "config",
    params(("id" = i64, Path, description = "Script id")),
    request_body = UpsertUserScript,
    responses(
        (status = 200, description = "Updated script record", body = UserScriptRecord),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn update_user_script_handler(
    State(state): State<Arc<AdminState>>,
    Path(id): Path<i64>,
    Json(body): Json<UpsertUserScript>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.update_user_script(id, &body).await {
        Ok(r) => {
            notify_live_config_reload(&state);
            Json(r).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Delete a user script by id.
#[utoipa::path(
    delete,
    path = "/admin/config/scripts/{id}",
    tag = "config",
    params(("id" = i64, Path, description = "Script id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn delete_user_script_handler(
    State(state): State<Arc<AdminState>>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let pg = require_store!(state);
    match pg.delete_user_script(id).await {
        Ok(true) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, "Script not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Security and routing config handlers
// ---------------------------------------------------------------------------

/// Get the current security configuration.
#[utoipa::path(
    get,
    path = "/admin/config/security",
    tag = "config",
    responses(
        (status = 200, description = "Security config JSON", body = serde_json::Value),
    )
)]
async fn get_security_config_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    if let Some(store) = &state.admin_store {
        if let Ok(Some(v)) = store.get_proxy_setting("security_config").await {
            if !queryflux_core::security_setting::is_blank_security_setting(&v) {
                return Json(security_dto_from_stored(&v, &state.security_config)).into_response();
            }
        }
    }
    Json(state.security_config.as_ref()).into_response()
}

/// Sanitized view of a stored security blob. Never returns raw passwords.
fn security_dto_from_stored(
    v: &serde_json::Value,
    fallback: &SecurityConfigDto,
) -> SecurityConfigDto {
    let (auth, authz) = queryflux_core::security_setting::parse_security_setting(v);
    let mut dto = fallback.clone();
    if let Some(auth) = auth.as_ref() {
        let tmp =
            SecurityConfigDto::from_config(auth, &AuthorizationConfig::default(), &HashMap::new());
        dto.auth_provider = tmp.auth_provider;
        dto.auth_required = tmp.auth_required;
        dto.oidc = tmp.oidc;
        dto.ldap = tmp.ldap;
        dto.static_user_count = tmp.static_user_count;
        dto.static_user_summaries = tmp.static_user_summaries;
    }
    if let Some(authz) = authz.as_ref() {
        let tmp = SecurityConfigDto::from_config(&AuthConfig::default(), authz, &HashMap::new());
        dto.authorization_provider = tmp.authorization_provider;
        dto.openfga = tmp.openfga;
    }
    dto
}

fn group_id_maps(
    groups: &[ClusterGroupConfigRecord],
) -> (HashMap<String, i64>, HashMap<i64, String>) {
    let mut name_to_id = HashMap::with_capacity(groups.len());
    let mut id_to_name = HashMap::with_capacity(groups.len());
    for g in groups {
        name_to_id.insert(g.name.clone(), g.id);
        id_to_name.insert(g.id, g.name.clone());
    }
    (name_to_id, id_to_name)
}

/// Get the current routing configuration.
#[utoipa::path(
    get,
    path = "/admin/config/routing",
    tag = "config",
    responses(
        (status = 200, description = "Routing config JSON", body = serde_json::Value),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_routing_config_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    if let Some(store) = &state.admin_store {
        match store.load_routing_config().await {
            Ok(Some(loaded)) => {
                let enriched = match store.list_group_configs().await {
                    Ok(groups) => {
                        let (name_to_id, _) = group_id_maps(&groups);
                        enrich_routers_for_api(&loaded.routers, &name_to_id)
                    }
                    Err(_) => loaded.routers.clone(),
                };
                return Json(RoutingConfigDto {
                    routing_fallback: loaded.routing_fallback,
                    routing_fallback_group_id: loaded.routing_fallback_group_id,
                    routers: enriched,
                })
                .into_response();
            }
            Ok(None) => {}
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
        // Legacy monolithic blob (only if migration has not run yet).
        if let Ok(Some(v)) = store.get_proxy_setting("routing_config").await {
            return Json(v).into_response();
        }
    }
    Json(state.routing_config.as_ref()).into_response()
}

/// Replace the security configuration.
#[utoipa::path(
    put,
    path = "/admin/config/security",
    tag = "config",
    request_body = UpsertSecurityConfig,
    responses(
        (status = 204, description = "Saved"),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn put_security_config_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<UpsertSecurityConfig>,
) -> impl IntoResponse {
    let Some(store) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let incoming = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
    let existing = store
        .get_proxy_setting("security_config")
        .await
        .ok()
        .flatten();
    let value =
        queryflux_core::security_setting::merge_security_setting(existing.as_ref(), incoming);

    let (auth, _) = queryflux_core::security_setting::parse_security_setting(&value);
    match auth {
        None if body.auth_provider != "none" => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "could not parse auth.provider = {} (check static users / OIDC / LDAP fields)",
                    body.auth_provider
                ),
            )
                .into_response();
        }
        Some(cfg)
            if matches!(cfg.provider, AuthProviderConfig::Static)
                && cfg
                    .static_users
                    .as_ref()
                    .map(|s| s.users.is_empty())
                    .unwrap_or(true) =>
        {
            return (
                StatusCode::BAD_REQUEST,
                "auth.provider = static requires at least one user",
            )
                .into_response();
        }
        Some(cfg) if matches!(cfg.provider, AuthProviderConfig::Oidc) && cfg.oidc.is_none() => {
            return (
                StatusCode::BAD_REQUEST,
                "auth.provider = oidc requires oidc issuer and jwks_uri",
            )
                .into_response();
        }
        Some(cfg) if matches!(cfg.provider, AuthProviderConfig::Ldap) && cfg.ldap.is_none() => {
            return (
                StatusCode::BAD_REQUEST,
                "auth.provider = ldap requires ldap url and user_search_base",
            )
                .into_response();
        }
        _ => {}
    }

    match store.set_proxy_setting("security_config", value).await {
        Ok(()) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Reject invalid query-regex patterns before persisting routing config.
fn validate_query_regex_patterns(routers: &[serde_json::Value]) -> std::result::Result<(), String> {
    use queryflux_routing::implementations::query_regex::QueryRegexRouter;

    for router in routers {
        if router.get("type").and_then(|t| t.as_str()) != Some("queryRegex") {
            continue;
        }
        let Some(rules) = router.get("rules").and_then(|r| r.as_array()) else {
            continue;
        };
        for rule in rules {
            if let Some(regex) = rule.get("regex").and_then(|r| r.as_str()) {
                if !regex.is_empty() {
                    QueryRegexRouter::validate_pattern(regex)?;
                }
            }
        }
    }
    Ok(())
}

/// Replace the routing configuration.
#[utoipa::path(
    put,
    path = "/admin/config/routing",
    tag = "config",
    request_body = UpsertRoutingConfig,
    responses(
        (status = 204, description = "Saved"),
        (status = 400, description = "Invalid routing config", body = str),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn put_routing_config_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<UpsertRoutingConfig>,
) -> impl IntoResponse {
    let Some(store) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Postgres persistence not configured",
        )
            .into_response();
    };
    let groups = match store.list_group_configs().await {
        Ok(g) => g,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };
    let (name_to_id, id_to_name) = group_id_maps(&groups);

    let fallback_name = if let Some(id) = body.routing_fallback_group_id {
        match id_to_name.get(&id) {
            Some(n) => n.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("routingFallbackGroupId {id} is not a known cluster group"),
                )
                    .into_response();
            }
        }
    } else {
        body.routing_fallback.clone()
    };

    if !fallback_name.is_empty() && !name_to_id.contains_key(&fallback_name) {
        return (
            StatusCode::BAD_REQUEST,
            format!("routingFallback '{fallback_name}' is not a known cluster group"),
        )
            .into_response();
    }

    if let Err(msg) = validate_query_regex_patterns(&body.routers) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }

    let fallback_gid = body.routing_fallback_group_id.or_else(|| {
        if fallback_name.is_empty() {
            None
        } else {
            name_to_id.get(&fallback_name).copied()
        }
    });

    let resolved = match resolve_routers_for_storage(&body.routers, &id_to_name) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    match store
        .replace_routing_config(&fallback_name, fallback_gid, &resolved)
        .await
    {
        Ok(()) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            let msg = e.to_string();
            let code = if msg.contains("unknown cluster group") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, msg).into_response()
        }
    }
}

/// Static engine registry — metadata and config schema for every supported engine.
#[utoipa::path(
    get,
    path = "/admin/engine-registry",
    tag = "admin",
    responses(
        (status = 200, description = "List of engine descriptors", body = str),
    )
)]
async fn engine_registry_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    Json(state.engine_registry.all().to_vec())
}

/// Swagger UI — interactive API explorer (loads spec from /openapi.json via CDN).
async fn swagger_ui_handler() -> impl IntoResponse {
    const HTML: &str = r##"<!DOCTYPE html>
<html>
<head>
  <title>QueryFlux Admin API</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui", presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset], layout: "BaseLayout" });
</script>
</body>
</html>"##;
    (StatusCode::OK, [("content-type", "text/html")], HTML)
}

/// Get the current guardrails configuration.
#[utoipa::path(
    get,
    path = "/admin/config/guardrails",
    tag = "config",
    responses(
        (status = 200, description = "Guardrails config JSON (`{ global: [...], groups: {...} }`)", body = serde_json::Value),
    )
)]
async fn get_guardrails_config_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    if let Some(store) = &state.admin_store {
        if let Ok(Some(v)) = store.get_proxy_setting("guardrails_config").await {
            return Json(v).into_response();
        }
    }
    Json(serde_json::json!({ "global": [], "groups": {} })).into_response()
}

/// Validates that the body matches the guardrails wire format used by Studio and the DB:
/// `{ global: GuardSpecDto[], groups: Record<string, GuardSpecDto[]> }`.
/// This is intentionally separate from `queryflux_guardrails::GuardChainConfig`, which
/// uses a nested `{ plan: [...] }` structure that differs from the Studio/DB flat format.
#[derive(Deserialize)]
#[allow(dead_code)]
struct GuardrailsConfigDto {
    #[serde(default)]
    global: Vec<GuardSpecDto>,
    #[serde(default)]
    groups: HashMap<String, Vec<GuardSpecDto>>,
}

impl GuardrailsConfigDto {
    fn validate(&self) -> std::result::Result<(), String> {
        for (idx, spec) in self.global.iter().enumerate() {
            spec.validate().map_err(|e| format!("global[{idx}]: {e}"))?;
        }
        for (group, specs) in &self.groups {
            for (idx, spec) in specs.iter().enumerate() {
                spec.validate()
                    .map_err(|e| format!("groups.{group}[{idx}]: {e}"))?;
            }
        }
        Ok(())
    }

    fn referenced_script_ids(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self
            .global
            .iter()
            .chain(self.groups.values().flatten())
            .filter_map(GuardSpecDto::script_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WebhookFailBehavior {
    Deny,
    Allow,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[allow(dead_code)]
enum GuardSpecDto {
    BuiltIn {
        name: Option<String>,
        #[serde(default)]
        max_rows: Option<u64>,
        #[serde(default)]
        applies_to: Option<Vec<String>>,
    },
    PythonScript {
        script_id: Option<i64>,
        #[serde(default)]
        script: Option<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    HttpWebhook {
        url: Option<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
        #[serde(default)]
        retry_count: Option<u32>,
        #[serde(default)]
        fail_behavior: Option<WebhookFailBehavior>,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
    },
}

impl GuardSpecDto {
    fn validate(&self) -> std::result::Result<(), String> {
        match self {
            GuardSpecDto::BuiltIn { name, .. } => match name.as_deref() {
                Some("read_only" | "row_limit" | "require_predicate") => Ok(()),
                Some(other) => Err(format!("unsupported built_in guard name \"{other}\"")),
                None => Err("built_in guard is missing required field \"name\"".to_string()),
            },
            GuardSpecDto::PythonScript {
                script_id, script, ..
            } => {
                let has_script = script.as_ref().is_some_and(|s| !s.trim().is_empty());
                let has_id = script_id.is_some();
                if has_script && has_id {
                    return Err(
                        "python_script guard must set either \"script\" or \"script_id\", not both"
                            .to_string(),
                    );
                }
                if !has_script && !has_id {
                    return Err(
                        "python_script guard requires either \"script\" or \"script_id\""
                            .to_string(),
                    );
                }
                Ok(())
            }
            GuardSpecDto::HttpWebhook { url, .. } => {
                let raw = url.as_deref().unwrap_or_default().trim();
                if raw.is_empty() {
                    return Err("http_webhook guard is missing required field \"url\"".to_string());
                }
                match url::Url::parse(raw) {
                    Ok(parsed) => match parsed.scheme() {
                        "http" | "https" => Ok(()),
                        other => Err(format!(
                            "http_webhook url must use http or https scheme, got \"{other}\""
                        )),
                    },
                    Err(e) => Err(format!("http_webhook url is not a valid URL: {e}")),
                }
            }
        }
    }

    fn script_id(&self) -> Option<i64> {
        match self {
            GuardSpecDto::PythonScript { script_id, .. } => *script_id,
            _ => None,
        }
    }
}

/// Replace the guardrails configuration.
#[utoipa::path(
    put,
    path = "/admin/config/guardrails",
    tag = "config",
    responses(
        (status = 204, description = "Saved"),
        (status = 400, description = "Invalid guardrails format", body = str),
        (status = 503, description = "Persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn put_guardrails_config_handler(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let Some(store) = &state.admin_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Persistence not configured",
        )
            .into_response();
    };
    let dto = match serde_json::from_value::<GuardrailsConfigDto>(body.clone()) {
        Ok(dto) => dto,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid guardrails config: {e}"),
            )
                .into_response();
        }
    };
    if let Err(e) = dto.validate() {
        return (
            StatusCode::BAD_REQUEST,
            format!("invalid guardrails config: {e}"),
        )
            .into_response();
    }
    for script_id in dto.referenced_script_ids() {
        match store.get_user_script(script_id).await {
            Ok(Some(script)) if script.kind == KIND_GUARD => {}
            Ok(Some(script)) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!(
                        "invalid guardrails config: python_script guard references script id {script_id} with kind \"{}\", expected \"guard\"",
                        script.kind
                    ),
                )
                    .into_response();
            }
            Ok(None) => {
                tracing::warn!(
                    script_id,
                    "python_script guard references missing script id; \
                     saving config but guard will DENY all queries at runtime via MisconfiguredGuard"
                );
            }
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }
    match store.set_proxy_setting("guardrails_config", body).await {
        Ok(()) => {
            notify_live_config_reload(&state);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Cache invalidation handlers
// ---------------------------------------------------------------------------

async fn invalidate_all_cache_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    match state.result_cache.invalidate_all().await {
        Ok(count) => Json(serde_json::json!({ "deleted": count })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn invalidate_group_cache_handler(
    State(state): State<Arc<AdminState>>,
    Path(group): Path<String>,
) -> impl IntoResponse {
    match state.result_cache.invalidate_group(&group).await {
        Ok(count) => Json(serde_json::json!({ "deleted": count })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_route_explain_response, collect_running_queries, delete_queued_if_exists,
        sql_preview, GuardrailsConfigDto, RouteExplainRequest, SecurityConfigDto,
    };
    use chrono::Utc;
    use queryflux_auth::{AllowAllAuthorization, AuthorizationChecker, SimpleAuthorizationPolicy};
    use queryflux_cluster_manager::{
        cluster_state::ClusterState,
        simple::SimpleClusterGroupManager,
        strategy::{ClusterSelectionStrategy, RoundRobinStrategy},
        ClusterGroupManager,
    };
    use queryflux_core::config::{
        AuthConfig, AuthProviderConfig, AuthorizationConfig, ClusterGroupAuthorizationConfig,
        QueryRegexRule, RegexRouteAction, StaticUserEntry, StaticUsersConfig,
    };
    use queryflux_core::query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, ExecutingQuery,
        FrontendProtocol, ProxyQueryId, QueuedQuery,
    };
    use queryflux_core::session::SessionContext;
    use queryflux_guardrails::{built_in::ReadOnlyGuard, GuardChain};
    use queryflux_persistence::{in_memory::InMemoryPersistence, Persistence};
    use queryflux_routing::{
        chain::RouterChain, implementations::query_regex::QueryRegexRouter, RouterTrait,
    };
    use queryflux_translation::TranslationService;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn security_config_dto_omits_static_user_passwords() {
        let mut users = HashMap::new();
        users.insert(
            "alice".into(),
            StaticUserEntry {
                password: "s3cret".into(),
                groups: vec!["analytics".into()],
                roles: vec!["reader".into()],
            },
        );
        let auth = AuthConfig {
            provider: AuthProviderConfig::Static,
            required: true,
            static_users: Some(StaticUsersConfig { users }),
            ..AuthConfig::default()
        };
        let dto =
            SecurityConfigDto::from_config(&auth, &AuthorizationConfig::default(), &HashMap::new());
        let serialized = serde_json::to_value(&dto).unwrap().to_string();
        assert!(!serialized.contains("s3cret"));
        assert!(!serialized.contains("\"password\""));
        assert_eq!(dto.static_user_summaries.len(), 1);
        assert_eq!(dto.static_user_summaries[0].username, "alice");
        assert_eq!(dto.static_user_summaries[0].groups, vec!["analytics"]);
    }

    #[test]
    fn sql_preview_truncates_to_160_chars() {
        let long = "word ".repeat(50);
        let preview = sql_preview(&long);
        assert!(preview.chars().count() <= 160);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn sql_preview_collapses_whitespace() {
        assert_eq!(sql_preview("SELECT   1\nFROM  t"), "SELECT 1 FROM t");
    }

    #[tokio::test]
    async fn collect_running_queries_includes_executing_and_queued() {
        let store = Arc::new(InMemoryPersistence::new());
        let now = Utc::now();
        store
            .upsert(ExecutingQuery {
                id: ProxyQueryId("exec-1".into()),
                sql: "SELECT 1".into(),
                translated_sql: None,
                cluster_group: ClusterGroupName("analytics".into()),
                cluster_name: ClusterName("trino".into()),
                cluster_group_config_id: None,
                cluster_config_id: None,
                backend_query_id: BackendQueryId("backend-1".into()),
                poll_base_url: None,
                creation_time: now,
                last_accessed: now,
                query_tags: Default::default(),
                agent_context: None,
                submitted_guard_actions: vec![],
                was_guard_blocked: false,
                submitted_by: "alice".into(),
                wire_auth: None,
            })
            .await
            .unwrap();
        store
            .upsert_queued(QueuedQuery {
                id: ProxyQueryId("queued-1".into()),
                sql: "SELECT 2".into(),
                session: SessionContext::default(),
                frontend_protocol: FrontendProtocol::TrinoHttp,
                cluster_group: ClusterGroupName("analytics".into()),
                creation_time: now,
                last_accessed: now,
                sequence: 0,
                submitted_by: "bob".into(),
            })
            .await
            .unwrap();

        let rows = collect_running_queries(store.as_ref()).await.unwrap();
        assert_eq!(rows.len(), 2);
        let exec = rows.iter().find(|r| r.id == "exec-1").unwrap();
        assert_eq!(exec.state, "executing");
        assert_eq!(exec.submitted_by, "alice");
        assert_eq!(exec.backend_query_id.as_deref(), Some("backend-1"));
        let queued = rows.iter().find(|r| r.id == "queued-1").unwrap();
        assert_eq!(queued.state, "queued");
        assert_eq!(queued.submitted_by, "bob");
        assert!(queued.backend_query_id.is_none());
    }

    #[tokio::test]
    async fn delete_queued_if_exists_removes_row() {
        let store = Arc::new(InMemoryPersistence::new());
        store
            .upsert_queued(QueuedQuery {
                id: ProxyQueryId("q-cancel".into()),
                sql: "SELECT 1".into(),
                session: SessionContext::default(),
                frontend_protocol: FrontendProtocol::TrinoHttp,
                cluster_group: ClusterGroupName("g".into()),
                creation_time: Utc::now(),
                last_accessed: Utc::now(),
                sequence: 0,
                submitted_by: "alice".into(),
            })
            .await
            .unwrap();

        assert!(delete_queued_if_exists(store.as_ref(), "q-cancel")
            .await
            .unwrap()
            .is_some());
        assert!(store
            .get_queued(&ProxyQueryId("q-cancel".into()))
            .await
            .unwrap()
            .is_none());
        assert!(delete_queued_if_exists(store.as_ref(), "missing")
            .await
            .unwrap()
            .is_none());
    }

    #[test]
    fn guardrails_dto_allows_supported_built_in_guards() {
        let dto: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [
                { "kind": "built_in", "name": "read_only" },
                { "kind": "built_in", "name": "row_limit", "max_rows": 1000 }
            ],
            "groups": {
                "analytics": [
                    { "kind": "built_in", "name": "require_predicate", "applies_to": ["fct_*"] }
                ]
            }
        }))
        .expect("valid dto");

        dto.validate().expect("supported built-ins should validate");
    }

    #[test]
    fn guardrails_dto_allows_external_guard_kinds() {
        let python: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "python_script", "script_id": 42, "timeout_ms": 250 }]
        }))
        .expect("shape should parse");
        python
            .validate()
            .expect("python script guard should validate");

        let webhook: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "http_webhook", "url": "https://policy.example/guard" }]
        }))
        .expect("shape should parse");
        webhook
            .validate()
            .expect("http webhook guard should validate");
    }

    #[test]
    fn guardrails_dto_requires_external_guard_fields() {
        let python: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "python_script" }]
        }))
        .expect("shape should parse");
        assert!(python.validate().unwrap_err().contains("script_id"));

        let webhook: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "http_webhook" }]
        }))
        .expect("shape should parse");
        assert!(webhook.validate().unwrap_err().contains("url"));
    }

    #[test]
    fn guardrails_dto_rejects_blank_inline_script() {
        let dto: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "python_script", "script": "  " }]
        }))
        .expect("shape should parse");
        assert!(dto.validate().unwrap_err().contains("script"));
    }

    #[test]
    fn guardrails_dto_rejects_both_script_and_id() {
        let dto: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "python_script", "script_id": 1, "script": "def check(ctx): pass" }]
        }))
        .expect("shape should parse");
        assert!(dto.validate().unwrap_err().contains("not both"));
    }

    #[test]
    fn guardrails_dto_rejects_invalid_fail_behavior() {
        let result = serde_json::from_value::<GuardrailsConfigDto>(json!({
            "global": [{ "kind": "http_webhook", "url": "https://x.co/g", "fail_behavior": "typo" }]
        }));
        assert!(
            result.is_err(),
            "typo in fail_behavior should be rejected by serde"
        );
    }

    #[test]
    fn guardrails_dto_rejects_non_http_url_scheme() {
        let file_url: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "http_webhook", "url": "file:///etc/passwd" }]
        }))
        .expect("shape should parse");
        assert!(file_url.validate().unwrap_err().contains("http or https"));

        let ftp_url: GuardrailsConfigDto = serde_json::from_value(json!({
            "global": [{ "kind": "http_webhook", "url": "ftp://evil.example" }]
        }))
        .expect("shape should parse");
        assert!(ftp_url.validate().unwrap_err().contains("http or https"));
    }

    // -----------------------------------------------------------------
    // route_explain_handler / build_route_explain_response
    // -----------------------------------------------------------------

    /// (cluster_name, max_running, running, enabled, healthy)
    fn cluster_manager_with_members(
        group: &str,
        members: Vec<(&str, u64, u64, bool, bool)>,
    ) -> Arc<dyn ClusterGroupManager> {
        let group_name = ClusterGroupName(group.to_string());
        let states: Vec<Arc<ClusterState>> = members
            .into_iter()
            .map(|(name, max_running, running, enabled, healthy)| {
                let state = Arc::new(ClusterState::new(
                    ClusterName(name.to_string()),
                    group_name.clone(),
                    None,
                    None,
                    EngineType::Trino,
                    Some(format!("http://{name}.test:8080")),
                    max_running,
                    enabled,
                ));
                state.set_running_queries(running);
                state.set_healthy(healthy);
                state
            })
            .collect();
        let mut groups = HashMap::new();
        groups.insert(
            group_name,
            (
                states,
                Arc::new(RoundRobinStrategy::new()) as Arc<dyn ClusterSelectionStrategy>,
            ),
        );
        Arc::new(SimpleClusterGroupManager::new(groups))
    }

    fn explain_request(sql: &str) -> RouteExplainRequest {
        RouteExplainRequest {
            sql: sql.to_string(),
            protocol: FrontendProtocol::TrinoHttp,
            user: None,
            groups: vec![],
            database: None,
            tags: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn route_explain_denied_by_router_skips_capacity_and_guards() {
        let router = QueryRegexRouter::from_rules(vec![QueryRegexRule {
            regex: r"(?i)^\s*DROP".into(),
            target_group: None,
            action: RegexRouteAction::Deny,
            error: Some("no drops".into()),
        }]);
        let routers: Vec<Box<dyn RouterTrait>> = vec![Box::new(router)];
        let chain = RouterChain::new(routers, ClusterGroupName("default".into()));
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(AllowAllAuthorization::default());
        let cluster_manager =
            cluster_manager_with_members("default", vec![("c1", 5, 0, true, true)]);

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &["default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &explain_request("DROP TABLE t"),
        )
        .await
        .unwrap();

        assert_eq!(resp.denied.as_deref(), Some("no drops"));
        assert!(resp.capacity.is_none());
        assert!(resp.guard_actions.is_empty());
    }

    #[tokio::test]
    async fn route_explain_fallback_reroutes_to_authorized_group() {
        // No routers match -> fallback fires -> resolve_routed_group picks the first
        // group in group_order the simulated user is authorized for.
        let chain = RouterChain::new(vec![], ClusterGroupName("default".into()));
        let policies = HashMap::from([
            (
                "analytics".to_string(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-a".into()],
                    allow_users: vec![],
                },
            ),
            (
                "default".to_string(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-b".into()],
                    allow_users: vec![],
                },
            ),
        ]);
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(SimpleAuthorizationPolicy::new(policies));
        let cluster_manager =
            cluster_manager_with_members("analytics", vec![("c1", 5, 0, true, true)]);

        let mut req = explain_request("SELECT 1");
        req.groups = vec!["team-a".to_string()];

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &["analytics".to_string(), "default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &req,
        )
        .await
        .unwrap();

        assert!(resp.denied.is_none());
        assert_eq!(resp.routing_trace.final_group, "analytics");
        assert!(resp.routing_trace.used_fallback);
        assert_eq!(resp.capacity.unwrap().group_name, "analytics");
    }

    #[tokio::test]
    async fn route_explain_read_only_guard_blocks_write() {
        let chain = RouterChain::new(vec![], ClusterGroupName("default".into()));
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(AllowAllAuthorization::default());
        let cluster_manager =
            cluster_manager_with_members("default", vec![("c1", 5, 0, true, true)]);
        let group_guard_chains: HashMap<String, Arc<GuardChain>> = HashMap::from([(
            "default".to_string(),
            Arc::new(GuardChain::new(vec![Box::new(ReadOnlyGuard)])),
        )]);

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &group_guard_chains,
            &HashMap::new(),
            &HashMap::new(),
            &["default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &explain_request("INSERT INTO t VALUES (1)"),
        )
        .await
        .unwrap();

        assert!(resp.denied.is_none());
        assert!(resp.would_be_guard_blocked);
        assert_eq!(resp.guard_actions.len(), 1);
        assert_eq!(resp.guard_actions[0].action, "deny");
        assert_eq!(
            resp.guard_actions[0].code.as_deref(),
            Some("READ_ONLY_VIOLATION")
        );
    }

    #[tokio::test]
    async fn route_explain_would_queue_true_when_group_is_full() {
        let chain = RouterChain::new(vec![], ClusterGroupName("default".into()));
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(AllowAllAuthorization::default());
        // Single member, at capacity (running == max).
        let cluster_manager =
            cluster_manager_with_members("default", vec![("c1", 2, 2, true, true)]);

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &["default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &explain_request("SELECT 1"),
        )
        .await
        .unwrap();

        assert!(resp.capacity.unwrap().would_queue);
    }

    #[tokio::test]
    async fn route_explain_would_queue_false_with_one_healthy_member_under_capacity() {
        let chain = RouterChain::new(vec![], ClusterGroupName("default".into()));
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(AllowAllAuthorization::default());
        // One disabled member (would never be picked) plus one healthy, under-capacity member.
        let cluster_manager = cluster_manager_with_members(
            "default",
            vec![("c1", 5, 5, false, true), ("c2", 5, 1, true, true)],
        );

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &["default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &explain_request("SELECT 1"),
        )
        .await
        .unwrap();

        let capacity = resp.capacity.unwrap();
        assert!(!capacity.would_queue);
        assert_eq!(capacity.members.len(), 2);
    }

    #[tokio::test]
    async fn route_explain_auth_denial_sets_trace_denied_too() {
        // Regression test: an authorization-stage denial (not a router-stage deny) must
        // set routing_trace.denied, not just the top-level `denied` field — otherwise
        // Studio's RoutingTraceView (which branches on trace.denied) renders a normal
        // "Final group" success footer alongside a separate "would be denied" banner.
        let chain = RouterChain::new(vec![], ClusterGroupName("default".into()));
        let policies = HashMap::from([(
            "default".to_string(),
            ClusterGroupAuthorizationConfig {
                allow_groups: vec!["nobody-has-this".into()],
                allow_users: vec![],
            },
        )]);
        let authorization: Arc<dyn AuthorizationChecker> =
            Arc::new(SimpleAuthorizationPolicy::new(policies));
        let cluster_manager =
            cluster_manager_with_members("default", vec![("c1", 5, 0, true, true)]);

        let resp = build_route_explain_response(
            &chain,
            authorization.as_ref(),
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &["default".to_string()],
            cluster_manager.as_ref(),
            &TranslationService::disabled(),
            &explain_request("SELECT 1"),
        )
        .await
        .unwrap();

        assert!(resp.denied.is_some());
        assert_eq!(resp.routing_trace.denied, resp.denied);
        assert!(resp.capacity.is_none());
    }
}
