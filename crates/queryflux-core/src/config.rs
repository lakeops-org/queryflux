use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::tags::{deserialize_config_tags, QueryTags};

// ---------------------------------------------------------------------------
// Cluster selection strategies
// ---------------------------------------------------------------------------

/// How the cluster manager picks a cluster within a group.
/// Default when omitted: `RoundRobin`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum StrategyConfig {
    /// Rotate through eligible clusters in order.
    #[serde(rename = "roundRobin")]
    RoundRobin,
    /// Pick the cluster with the most remaining capacity.
    #[serde(rename = "leastLoaded")]
    LeastLoaded,
    /// Try clusters in member order; use later ones only when earlier ones are full/unhealthy.
    #[serde(rename = "failover")]
    Failover,
    /// For mixed-engine groups: prefer engines in the given order, fall back when full.
    #[serde(rename = "engineAffinity")]
    EngineAffinity {
        /// Engine types in preference order (e.g. ["trino", "starRocks", "duckDb"]).
        preference: Vec<EngineConfig>,
    },
    /// Route traffic proportionally by weight.
    #[serde(rename = "weighted")]
    Weighted {
        /// cluster_name → relative weight (e.g. { "trino-1": 3, "trino-2": 1 }).
        weights: HashMap<String, u32>,
    },
}

/// Root configuration for a QueryFlux deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfig {
    pub queryflux: QueryFluxConfig,
    /// Top-level cluster definitions. Each key is the cluster name.
    /// A cluster can be a member of multiple groups.
    #[serde(default)]
    pub clusters: HashMap<String, ClusterConfig>,
    /// Logical groups of clusters (routing targets). Omitted in YAML when using
    /// persistence + Studio to define groups in the database.
    #[serde(default)]
    pub cluster_groups: HashMap<String, ClusterGroupConfig>,
    /// Routing rules evaluated in order. Omitted when defined via Studio / Postgres.
    #[serde(default)]
    pub routers: Vec<RouterConfig>,
    /// Default group when no router matches. Empty string is valid at parse time;
    /// configure routing in Studio before serving traffic.
    #[serde(default)]
    pub routing_fallback: String,
    #[serde(default)]
    pub translation: TranslationConfig,
    #[serde(default)]
    pub catalog_provider: CatalogProviderConfig,
    /// Frontend authentication. Defaults to `NoneAuthProvider` (network-trust only).
    #[serde(default)]
    pub auth: AuthConfig,
    /// Gateway-level authorization. Defaults to allow-all.
    #[serde(default)]
    pub authorization: AuthorizationConfig,
    /// Admin REST API (port 9000) configuration.
    #[serde(default)]
    pub admin_api: AdminApiConfig,
    /// Guardrails evaluated on every query after routing and translation.
    /// Omit to disable all guardrails.
    #[serde(default)]
    pub guardrails: Option<GuardrailsConfig>,
}

// ---------------------------------------------------------------------------
// Guardrails configuration
// ---------------------------------------------------------------------------

/// Top-level guardrails section in the YAML config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuardrailsConfig {
    /// Guards that run for every query regardless of cluster group.
    #[serde(default)]
    pub global: Vec<GuardSpecConfig>,
    /// Per-group additional guards appended after the global chain.
    #[serde(default)]
    pub groups: HashMap<String, Vec<GuardSpecConfig>>,
}

impl GuardrailsConfig {
    /// Validate guardrails at config load time so unsupported kinds do not silently no-op.
    pub fn validate(&self) -> std::result::Result<(), String> {
        for (idx, spec) in self.global.iter().enumerate() {
            spec.validate()
                .map_err(|e| format!("guardrails.global[{idx}]: {e}"))?;
        }
        for (group, specs) in &self.groups {
            for (idx, spec) in specs.iter().enumerate() {
                spec.validate()
                    .map_err(|e| format!("guardrails.groups.{group}[{idx}]: {e}"))?;
            }
        }
        Ok(())
    }
}

/// One guard entry in the config.
///
/// Supports both the current flat format (`kind: http_webhook`, `url: ...`) and
/// the legacy nested format (`kind: { http_webhook: { url: ..., timeout_ms: ... } }`)
/// that shipped before the Python/webhook guardrails refactor.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GuardSpecConfig {
    pub kind: GuardKindConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `python_script`: numeric id of a guard script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_id: Option<i64>,
    /// `python_script`: inline script body. Prefer `script_id` for Studio-managed configs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    /// `http_webhook`: endpoint URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `http_webhook` / `python_script`: timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// `http_webhook`: number of retries after the first failed attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
    /// `http_webhook`: `deny` (default) or `allow` when the webhook cannot be reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fail_behavior: Option<GuardFailBehaviorConfig>,
    /// `http_webhook`: extra request headers sent with every call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    /// `row_limit`: maximum rows allowed (default: none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// `require_predicate`: table patterns this guard applies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applies_to: Option<Vec<String>>,
}

impl<'de> Deserialize<'de> for GuardSpecConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Intermediate struct that accepts both the new unit-variant `kind` and
        // the legacy struct-variant `kind: { http_webhook: { url, timeout_ms } }`.
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        struct Raw {
            kind: serde_json::Value,
            #[serde(default)]
            name: Option<String>,
            #[serde(default)]
            script_id: Option<i64>,
            #[serde(default)]
            script: Option<String>,
            #[serde(default)]
            url: Option<String>,
            #[serde(default)]
            timeout_ms: Option<u64>,
            #[serde(default)]
            retry_count: Option<u32>,
            #[serde(default)]
            fail_behavior: Option<GuardFailBehaviorConfig>,
            #[serde(default)]
            headers: Option<HashMap<String, String>>,
            #[serde(default)]
            max_rows: Option<u64>,
            #[serde(default)]
            applies_to: Option<Vec<String>>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // Resolve `kind` — it can be a plain string or a legacy nested map.
        let (kind, legacy_url, legacy_timeout) = match &raw.kind {
            serde_json::Value::String(s) => {
                let k: GuardKindConfig =
                    serde_json::from_value(serde_json::Value::String(s.clone()))
                        .map_err(serde::de::Error::custom)?;
                (k, None, None)
            }
            serde_json::Value::Object(map) => {
                if map.contains_key("http_webhook") {
                    let inner = &map["http_webhook"];
                    let url = inner
                        .get("url")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let timeout = inner.get("timeout_ms").and_then(|v| v.as_u64());
                    (GuardKindConfig::HttpWebhook, url, timeout)
                } else if map.contains_key("built_in") {
                    (GuardKindConfig::BuiltIn, None, None)
                } else if map.contains_key("python_script") {
                    (GuardKindConfig::PythonScript, None, None)
                } else {
                    return Err(serde::de::Error::custom(format!(
                        "unrecognized guard kind: {map:?}"
                    )));
                }
            }
            other => {
                return Err(serde::de::Error::custom(format!(
                    "expected string or map for guard kind, got {other}"
                )));
            }
        };

        Ok(GuardSpecConfig {
            kind,
            name: raw.name,
            script_id: raw.script_id,
            script: raw.script,
            url: raw.url.or(legacy_url),
            timeout_ms: raw.timeout_ms.or(legacy_timeout),
            retry_count: raw.retry_count,
            fail_behavior: raw.fail_behavior,
            headers: raw.headers,
            max_rows: raw.max_rows,
            applies_to: raw.applies_to,
        })
    }
}

impl GuardSpecConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        match &self.kind {
            GuardKindConfig::BuiltIn => match self.name.as_deref() {
                Some("read_only" | "row_limit" | "require_predicate") => Ok(()),
                Some(other) => Err(format!("unsupported built_in guard name \"{other}\"")),
                None => Err("built_in guard is missing required field \"name\"".to_string()),
            },
            GuardKindConfig::PythonScript => {
                let has_script = self.script.as_ref().is_some_and(|s| !s.trim().is_empty());
                let has_id = self.script_id.is_some();
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
            GuardKindConfig::HttpWebhook => {
                if self.url.as_deref().unwrap_or_default().trim().is_empty() {
                    return Err("http_webhook guard is missing required field \"url\"".to_string());
                }
                Ok(())
            }
        }
    }
}

/// Which kind of guard this is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardKindConfig {
    BuiltIn,
    PythonScript,
    HttpWebhook,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GuardFailBehaviorConfig {
    #[default]
    Deny,
    Allow,
}

// ---------------------------------------------------------------------------
// Auth / AuthZ configuration
// ---------------------------------------------------------------------------

/// Frontend authentication configuration.
///
/// Controls how QueryFlux verifies the identity of incoming clients.
/// Default (`provider: none`) derives identity from session metadata with no
/// cryptographic verification — suitable for trusted networks only.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    #[serde(default)]
    pub provider: AuthProviderConfig,
    /// When true, reject requests that carry no username.
    /// With `provider: none` this is network-trust only (no JWT verification).
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub oidc: Option<OidcConfig>,
    #[serde(default)]
    pub ldap: Option<LdapConfig>,
    #[serde(default)]
    pub static_users: Option<StaticUsersConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: AuthProviderConfig::None,
            required: false,
            oidc: None,
            ldap: None,
            static_users: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum AuthProviderConfig {
    /// No verification — identity from session username (network-trust only).
    #[default]
    #[serde(rename = "none")]
    None,
    /// User/password map in config (dev/simple deployments).
    #[serde(rename = "static")]
    Static,
    /// JWT validation via JWKS endpoint (Keycloak, Auth0, etc.).
    #[serde(rename = "oidc")]
    Oidc,
    /// LDAP bind + group membership lookup.
    #[serde(rename = "ldap")]
    Ldap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OidcConfig {
    pub issuer: String,
    pub jwks_uri: String,
    pub audience: Option<String>,
    /// JWT claim name for group memberships (e.g. `"groups"`).
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
    /// JWT claim name for roles (e.g. `"realm_access.roles"` for Keycloak).
    #[serde(default)]
    pub roles_claim: Option<String>,
}

fn default_groups_claim() -> String {
    "groups".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LdapConfig {
    /// LDAP server URL, e.g. `ldap://ldap.internal:389` or `ldaps://ldap.internal:636`.
    pub url: String,
    /// Service account DN used to search for users, e.g. `cn=svc,ou=serviceaccounts,dc=example,dc=com`.
    /// Leave empty to attempt an anonymous bind before searching.
    #[serde(default)]
    pub bind_dn: String,
    /// Password for the service account `bindDn`. Optional for anonymous-bind servers.
    #[serde(default)]
    pub bind_password: Option<String>,
    /// Base DN under which users are searched, e.g. `ou=users,dc=example,dc=com`.
    pub user_search_base: String,
    /// LDAP filter for locating a user by username. `{}` is replaced with the escaped username.
    /// Default: `(uid={})`. For Active Directory use `(sAMAccountName={})`.
    #[serde(default = "default_user_search_filter")]
    pub user_search_filter: String,
    /// Instead of searching, bind directly as this DN template.
    /// `{}` is replaced with the username. E.g. `cn={},ou=users,dc=example,dc=com`.
    /// When set, `bindDn` / `bindPassword` / `userSearchBase` are not used for auth.
    #[serde(default)]
    pub user_dn_template: Option<String>,
    /// Base DN for group membership search. When set, groups are resolved by searching
    /// here with `(member={user_dn})`. When absent, groups are read from the `memberOf`
    /// attribute on the user entry (works for AD and most LDAP setups).
    #[serde(default)]
    pub group_search_base: Option<String>,
    /// LDAP attribute used as the group name. Default: `cn`.
    #[serde(default = "default_group_name_attribute")]
    pub group_name_attribute: String,
}

fn default_user_search_filter() -> String {
    "(uid={})".to_string()
}

fn default_group_name_attribute() -> String {
    "cn".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticUsersConfig {
    /// username → { password, groups }
    pub users: HashMap<String, StaticUserEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticUserEntry {
    pub password: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Gateway-level authorization configuration.
///
/// Controls which authenticated users/groups may access which cluster groups.
/// Default (`provider: none`) uses `allowGroups`/`allowUsers` lists on each
/// cluster group. If those are also absent, all authenticated users are allowed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationConfig {
    #[serde(default)]
    pub provider: AuthorizationProviderConfig,
    #[serde(default)]
    pub openfga: Option<OpenFgaConfig>,
}

impl Default for AuthorizationConfig {
    fn default() -> Self {
        Self {
            provider: AuthorizationProviderConfig::None,
            openfga: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum AuthorizationProviderConfig {
    /// Use `allowGroups`/`allowUsers` per cluster group, or allow-all if unset.
    #[default]
    #[serde(rename = "none")]
    None,
    /// OpenFGA Zanzibar-style fine-grained authorization.
    #[serde(rename = "openfga")]
    OpenFga,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenFgaConfig {
    pub url: String,
    pub store_id: String,
    #[serde(default)]
    pub credentials: Option<OpenFgaCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "method")]
pub enum OpenFgaCredentials {
    #[serde(rename = "api_key")]
    ApiKey { api_key: String },
    #[serde(rename = "client_credentials")]
    ClientCredentials {
        client_id: String,
        client_secret: String,
        token_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    /// Tag keys that should NOT be emitted as Prometheus labels.
    ///
    /// By default all query tags are emitted as `queryflux_query_tags_total` labels.
    /// Use this list to suppress high-cardinality tag keys (e.g. request IDs, timestamps).
    #[serde(default)]
    pub tags_deny_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFluxConfig {
    pub external_address: Option<String>,
    #[serde(default)]
    pub frontends: FrontendsConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub admin_api: AdminApiConfig,
    /// How often (in seconds) to poll the DB for routing rules and cluster/group config changes.
    /// Only takes effect when Postgres persistence is configured.
    /// **Omitted → 30 seconds.** **`0` → disable polling** (no periodic refresh); config still reloads
    /// when the admin API notifies after Studio or other writes.
    #[serde(default)]
    pub config_reload_interval_secs: Option<u64>,
    /// Number of days to retain query history records. When set, a background task
    /// runs hourly and deletes `query_records` rows older than this many days.
    /// Only takes effect when Postgres persistence is configured.
    /// Omit or set to `null` to keep history indefinitely.
    #[serde(default)]
    pub query_history_retention_days: Option<u64>,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Enable distributed (multi-instance) mode. When `true`, QueryFlux enforces global
    /// capacity limits and config propagation via the persistence backend.
    /// Requires `persistence.type = postgres`. Auto-detected as `true` when persistence
    /// is Postgres; set explicitly to `false` to opt out while still using Postgres persistence.
    #[serde(default)]
    pub distributed: Option<bool>,
    /// Maximum seconds to wait for in-flight queries to drain during graceful shutdown.
    /// After this timeout, the process exits regardless of remaining work.
    /// Defaults to 30 seconds.
    #[serde(default)]
    pub shutdown_drain_timeout_secs: Option<u64>,
    /// OpenTelemetry OTLP exporter endpoint (e.g. `http://localhost:4317`).
    /// When set, QueryFlux exports distributed traces via gRPC OTLP.
    /// Requires the binary to be built with `--features otlp`.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Storage backend for query result caching. Startup-only (not hot-reloadable).
    /// Per-group cache settings live on each `ClusterGroupConfig.cache`.
    #[serde(default)]
    pub cache_backend: Option<CacheBackendConfig>,
}

impl QueryFluxConfig {
    /// Seconds to wait for in-flight queries to drain on shutdown. Defaults to 30.
    pub fn shutdown_drain_timeout_secs(&self) -> u64 {
        self.shutdown_drain_timeout_secs.unwrap_or(30)
    }

    /// Interval for **periodic** background reload of routing rules and cluster/group config from Postgres.
    ///
    /// - [`None`](Option::None) (field omitted in YAML) → **Some(30)**.
    /// - **Some(0)** → **None** (disable periodic polling; reload only via admin notify).
    /// - **Some(n)** with n > 0 → poll every n seconds.
    pub fn periodic_config_reload_interval_secs(&self) -> Option<u64> {
        match self.config_reload_interval_secs {
            None => Some(30),
            Some(0) => None,
            Some(n) => Some(n),
        }
    }

    /// Resolve whether distributed mode is active and validate the configuration.
    ///
    /// `backend_supports_coordination` is whether a `DistributedBackendStore`
    /// is wired at startup and its `supports_distributed_coordination()` returns
    /// `true` (Postgres today). Backends that only implement `BackendStore` pass
    /// `false`. Distributed mode defaults to on whenever coordination is
    /// available; set `distributed: false` to opt out explicitly.
    ///
    /// Returns `Err` when distributed mode is requested but the persistence
    /// backend cannot coordinate across replicas.
    pub fn resolve_distributed(&self, backend_supports_coordination: bool) -> Result<bool, String> {
        let distributed = self.distributed.unwrap_or(backend_supports_coordination);
        if distributed && !backend_supports_coordination {
            return Err(
                "Distributed mode requires a persistence backend that supports \
                 multi-replica coordination (e.g. persistence.type = postgres). \
                 InMemory persistence cannot coordinate across replicas."
                    .to_string(),
            );
        }
        Ok(distributed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FrontendsConfig {
    #[serde(default)]
    pub trino_http: FrontendConfig,
    #[serde(default)]
    pub postgres_wire: Option<FrontendConfig>,
    #[serde(default)]
    pub mysql_wire: Option<FrontendConfig>,
    #[serde(default)]
    pub clickhouse_http: Option<FrontendConfig>,
    #[serde(default)]
    pub flight_sql: Option<FrontendConfig>,
    #[serde(default)]
    pub snowflake_http: Option<SnowflakeHttpFrontendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub port: u16,
    /// Maximum number of concurrent connections (TCP-based frontends) or concurrent in-flight
    /// requests (HTTP-based frontends like Trino HTTP) this frontend will accept. For HTTP
    /// frontends the limit is enforced via `tower::ConcurrencyLimitLayer`, so keep-alive clients
    /// that are idle between requests do not count against it. `None` or `0` = unlimited.
    #[serde(default)]
    pub max_connections: Option<usize>,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            max_connections: None,
        }
    }
}

/// Config for the Snowflake frontend (HTTP wire v1 + SQL API v2 on the same port).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnowflakeHttpFrontendConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub port: u16,
    /// Maximum number of concurrent in-flight HTTP requests this frontend will accept.
    /// Enforced via `tower::ConcurrencyLimitLayer` — idle keep-alive clients do not count.
    /// `None` or `0` = unlimited.
    #[serde(default)]
    pub max_connections: Option<usize>,
    /// Must be set to `true` when running multiple QueryFlux replicas to acknowledge that
    /// the load balancer is configured for sticky session affinity. A warning is logged at
    /// startup when this is `false`, because HTTP wire v1 sessions are process-local and
    /// clients will break if requests are routed to a different replica.
    #[serde(default)]
    pub session_affinity_acknowledged: bool,
    /// Maximum session lifetime in seconds. 0 = no limit. Default: 86400 (24 h).
    #[serde(default = "default_session_max_age_secs")]
    pub session_max_age_secs: u64,
    /// Session idle timeout in seconds. 0 = no limit. Default: 14400 (4 h).
    #[serde(default = "default_session_idle_timeout_secs")]
    pub session_idle_timeout_secs: u64,
}

fn default_session_max_age_secs() -> u64 {
    86400
}

fn default_session_idle_timeout_secs() -> u64 {
    14400
}

fn default_true() -> bool {
    true
}

/// Postgres persistence: set a single `url`, **or** `host`, `user`, `database`, and optionally
/// `password` / `port` (default 5432). The URL form wins when both are present.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPersistenceConfig {
    /// Full `postgres://` connection string. When non-empty, `host` / `user` / … are ignored.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    /// Max connections in the sqlx pool (`poolSize` in YAML). Omit for the
    /// sqlx default (10). The pool serves the dispatch hot path (capacity
    /// acquire/release per query) plus persistence, admin, LISTEN/NOTIFY,
    /// and sweeps — raise this before adding replicas under high QPS.
    #[serde(default)]
    pub pool_size: Option<u32>,
    /// Max seconds to wait for a connection from the pool before returning an error.
    /// Defaults to 30 seconds.
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
    /// Postgres `statement_timeout` set on each connection (seconds). Prevents
    /// runaway queries from holding pool connections indefinitely. Defaults to 60s.
    #[serde(default)]
    pub statement_timeout_secs: Option<u64>,
}

impl PostgresPersistenceConfig {
    /// Connection URL for sqlx / `PgPool` (includes password encoding for special characters).
    pub fn connection_url(&self) -> Result<String, String> {
        if let Some(ref u) = self.url {
            let u = u.trim();
            if !u.is_empty() {
                return Ok(u.to_string());
            }
        }

        let host = self
            .host
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "postgres persistence: set `url`, or set `host`, `user`, and `database`".to_string()
            })?;
        let user = self
            .user
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "postgres persistence: set `url`, or set `host`, `user`, and `database`".to_string()
            })?;
        let database = self
            .database
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "postgres persistence: set `url`, or set `host`, `user`, and `database`".to_string()
            })?;

        let port = self.port.unwrap_or(5432);
        let password = self.password.as_deref().unwrap_or("");

        let mut url = url::Url::parse("postgres://localhost/").map_err(|e| e.to_string())?;
        url.set_host(Some(host)).map_err(|e| e.to_string())?;
        url.set_username(user)
            .map_err(|_| "invalid user for postgres URL (unsupported characters)".to_string())?;
        if password.is_empty() {
            let _ = url.set_password(None);
        } else {
            url.set_password(Some(password)).map_err(|_| {
                "invalid password for postgres URL (unsupported characters)".to_string()
            })?;
        }
        url.set_port(Some(port))
            .map_err(|_| "invalid port for postgres URL".to_string())?;
        url.set_path(&format!("/{}", database.trim_start_matches('/')));

        Ok(url.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PersistenceConfig {
    #[default]
    #[serde(rename = "inMemory")]
    InMemory,
    Redis {
        url: String,
    },
    Postgres {
        #[serde(flatten)]
        conn: PostgresPersistenceConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminApiConfig {
    #[serde(default = "default_admin_port")]
    pub port: u16,
    /// Bootstrap admin username. Overridden by `QUERYFLUX_ADMIN_USER` env var.
    #[serde(default = "default_admin_username")]
    pub username: String,
    /// Bootstrap admin password (plain text). Overridden by `QUERYFLUX_ADMIN_PASSWORD` env var.
    /// Ignored once the password has been changed via the web UI (DB hash takes precedence).
    #[serde(default = "default_admin_password")]
    pub password: String,
    /// Allowed CORS origins for the admin API. When set, only these origins may
    /// make cross-origin requests. When empty or omitted, all origins are allowed
    /// (a startup warning is logged because `allow_origin(Any)` on the admin API
    /// is a security risk in production).
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
}

fn default_admin_port() -> u16 {
    9000
}

fn default_admin_username() -> String {
    "admin".to_string()
}

fn default_admin_password() -> String {
    "admin".to_string()
}

impl Default for AdminApiConfig {
    fn default() -> Self {
        Self {
            port: 9000,
            username: default_admin_username(),
            password: default_admin_password(),
            cors_allowed_origins: Vec::new(),
        }
    }
}

// --- Cluster groups ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Cluster names that belong to this group (references top-level `clusters` map).
    pub members: Vec<String>,
    /// Selection strategy for picking a cluster within the group.
    /// Defaults to `RoundRobin` when omitted.
    #[serde(default)]
    pub strategy: Option<StrategyConfig>,
    pub max_running_queries: u64,
    #[serde(default)]
    pub max_queued_queries: Option<u64>,
    /// Simple authorization policy (used when `authorization.provider: none`).
    /// If both lists are empty/absent, all authenticated users are allowed.
    #[serde(default)]
    pub authorization: ClusterGroupAuthorizationConfig,
    /// Default tags applied to every query routed to this group.
    /// Merged with session-level tags at dispatch time; session tags win on the same key.
    /// Example: `{ team: analytics, cost_center: "701" }`
    #[serde(default, deserialize_with = "deserialize_config_tags")]
    pub default_tags: QueryTags,
    /// Query result cache configuration for this group. Omit to disable caching.
    #[serde(default)]
    pub cache: Option<GroupCacheConfig>,
}

/// Simple allow-list authorization for a cluster group.
/// Used when `authorization.provider: none` (no OpenFGA).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupAuthorizationConfig {
    /// Group names (from `AuthContext.groups`) that may access this cluster group.
    #[serde(default)]
    pub allow_groups: Vec<String>,
    /// Individual usernames (from `AuthContext.user`) that may access this cluster group.
    #[serde(default)]
    pub allow_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineConfig {
    Trino,
    DuckDb,
    /// DuckDB running as a remote HTTP server (community `httpserver` extension).
    DuckDbHttp,
    StarRocks,
    ClickHouse,
    /// Amazon Athena — serverless SQL over S3 via the AWS SDK.
    Athena,
    /// Generic ADBC adapter — runtime-loaded shared library driver.
    Adbc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    pub engine: Option<EngineConfig>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Max concurrent queries for this cluster. When `None`, the cluster group's
    /// `maxRunningQueries` is used at runtime.
    #[serde(default)]
    pub max_running_queries: Option<u64>,
    /// StarRocks MySQL connection pool size for QueryFlux (`poolSize` in JSON/YAML). Other engines ignore this.
    #[serde(default)]
    pub pool_size: Option<usize>,
    /// HTTP(S) endpoint for Trino / ClickHouse / StarRocks FE.
    pub endpoint: Option<String>,
    /// Local file path for DuckDB.
    pub database_path: Option<String>,
    /// AWS region for cloud backends (e.g. `us-east-1`). Required for Athena.
    pub region: Option<String>,
    /// S3 URI where Athena writes query results (e.g. `s3://my-bucket/athena-results/`).
    /// Required when engine is `athena`.
    pub s3_output_location: Option<String>,
    /// Athena workgroup to submit queries to. Defaults to `"primary"` when omitted.
    #[serde(default)]
    pub workgroup: Option<String>,
    /// Default Athena catalog. Defaults to `"AwsDataCatalog"` when omitted.
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Type 1 credentials — service account used for health checks and (by default) query execution.
    #[serde(default)]
    pub auth: Option<ClusterAuth>,
    /// Type 2 credentials — how to authenticate per-user queries to this cluster.
    /// Default when omitted: `serviceAccount` (use Type 1 for all queries).
    #[serde(default)]
    pub query_auth: Option<QueryAuthConfig>,
}

/// Authentication credentials for a backend cluster (Type 1 — service account).
///
/// - `basic`: HTTP Basic auth (Trino, ClickHouse) or MySQL username+password (StarRocks).
///   Password may be empty for backends that allow it (e.g. Trino with no auth).
/// - `bearer`: HTTP Bearer token (Trino with JWT / OAuth2).
/// - `keyPair`: RSA key-pair (Snowflake, Databricks — future adapters).
/// - `accessKey`: AWS static access key (Athena and other AWS backends).
/// - `roleArn`: AWS IAM role assumption via STS AssumeRole (Athena).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClusterAuth {
    #[serde(rename_all = "camelCase")]
    Basic { username: String, password: String },
    #[serde(rename_all = "camelCase")]
    Bearer { token: String },
    /// RSA key-pair authentication. Used for Snowflake service accounts.
    /// `privateKeyPem` should be a PKCS#8 or PKCS#1 PEM string.
    /// In production load via `secretRef`; inline only for dev/test.
    #[serde(rename_all = "camelCase")]
    KeyPair {
        username: String,
        private_key_pem: String,
        #[serde(default)]
        private_key_passphrase: Option<String>,
    },
    /// AWS static access key credentials. Used for Athena and other AWS backends.
    /// When omitted, the default AWS credential chain is used (env vars, instance profile, etc.).
    /// `session_token` is optional and required only for temporary/STS-vended credentials.
    #[serde(rename_all = "camelCase")]
    AccessKey {
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        session_token: Option<String>,
    },
    /// AWS IAM role assumption via STS `AssumeRole`.
    /// The proxy's own credentials (from the credential chain) are used to assume `role_arn`.
    /// `external_id` is optional and required only when the role trust policy mandates it.
    #[serde(rename_all = "camelCase")]
    RoleArn {
        role_arn: String,
        #[serde(default)]
        external_id: Option<String>,
    },
}

/// How QueryFlux authenticates a specific user's query to this backend cluster (Type 2).
///
/// Resolved per-request from `AuthContext` + this config. Default when omitted: `serviceAccount`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum QueryAuthConfig {
    /// Use cluster service credentials (Type 1) for every query.
    /// User identity is known to QueryFlux (audit/metrics) but backend sees only the service account.
    #[serde(rename = "serviceAccount")]
    ServiceAccount,
    /// Service account authenticates to the backend; user identity injected via `X-Trino-User`.
    /// **Trino only** — startup validation rejects this for other engines.
    #[serde(rename = "impersonate")]
    Impersonate,
    /// Exchange the user's OIDC JWT for a backend-scoped OAuth token.
    /// Requires `OidcAuthProvider` on the frontend (so `raw_token` is populated).
    /// Falls back to `serviceAccount` when `raw_token` is absent.
    #[serde(rename = "tokenExchange")]
    TokenExchange(TokenExchangeConfig),
}

/// Configuration for the OAuth 2.0 token exchange flow (RFC 8693).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenExchangeConfig {
    /// OAuth token endpoint, e.g. `https://keycloak.internal/realms/my-realm/protocol/openid-connect/token`.
    pub token_endpoint: String,
    /// Client ID registered in the IdP for QueryFlux.
    pub client_id: String,
    /// Client secret for the above client.
    pub client_secret: String,
    /// Target audience for the exchanged token (Keycloak: target client ID; Snowflake: omit).
    #[serde(default)]
    pub target_audience: Option<String>,
    /// OAuth scope for the exchanged token (e.g. `session:role:ANALYST` for Snowflake).
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
}

// --- Routers ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum RouterConfig {
    #[serde(rename_all = "camelCase")]
    ProtocolBased {
        #[serde(default)]
        trino_http: Option<String>,
        #[serde(default)]
        postgres_wire: Option<String>,
        #[serde(default)]
        mysql_wire: Option<String>,
        #[serde(default)]
        clickhouse_http: Option<String>,
        #[serde(default)]
        flight_sql: Option<String>,
        #[serde(default)]
        snowflake_http: Option<String>,
        #[serde(default)]
        snowflake_sql_api: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Header {
        header_name: String,
        header_value_to_group: HashMap<String, String>,
    },
    #[serde(rename_all = "camelCase")]
    UserGroup {
        user_to_group: HashMap<String, String>,
    },
    QueryRegex {
        rules: Vec<QueryRegexRule>,
    },
    /// Route by query tags. Rules are evaluated in order; first rule where all specified
    /// tags match wins. Tag matching: key must be present; if config value is `Some(v)`,
    /// session tag value must equal `v`; if config value is `None`, any value (or no value)
    /// matches.
    #[serde(rename_all = "camelCase")]
    Tags {
        rules: Vec<TagRoutingRule>,
    },
    #[serde(rename_all = "camelCase")]
    PythonScript {
        script: String,
        script_file: Option<String>,
    },
    /// Match when sub-conditions combine per `combine` (`all` = AND, `any` = OR).
    #[serde(rename_all = "camelCase")]
    Compound {
        combine: CompoundCombineMode,
        conditions: Vec<CompoundCondition>,
        target_group: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRegexRule {
    pub regex: String,
    pub target_group: String,
}

/// A single rule in a [`RouterConfig::Tags`] router.
///
/// All entries in `tags` must match the query's effective tags (AND logic).
/// `None` as a config value means "key must be present, any value accepted".
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagRoutingRule {
    /// Tags that must all match. Value `null`/absent means key-only match.
    pub tags: HashMap<String, Option<String>>,
    /// Cluster group to route to when this rule matches.
    pub target_group: String,
}

/// How sub-conditions are combined in a [`RouterConfig::Compound`] rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum CompoundCombineMode {
    /// Every condition must match.
    #[default]
    All,
    /// At least one condition must match.
    Any,
}

/// One predicate inside a compound routing rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum CompoundCondition {
    /// Matches when the client used this frontend protocol (`trinoHttp`, `mysqlWire`, …).
    #[serde(rename_all = "camelCase")]
    Protocol { protocol: String },
    /// Exact header match (Trino HTTP and ClickHouse HTTP only; case-insensitive header name).
    #[serde(rename_all = "camelCase")]
    Header {
        header_name: String,
        header_value: String,
    },
    /// Authenticated / session username equals this value (see `SessionContext::user`).
    #[serde(rename_all = "camelCase")]
    User { username: String },
    /// Trino `X-Trino-Client-Tags` contains this tag.
    #[serde(rename_all = "camelCase")]
    ClientTag { tag: String },
    /// SQL text matches this regex (same semantics as `QueryRegex` router).
    #[serde(rename_all = "camelCase")]
    QueryRegex { regex: String },
}

// --- Translation ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TranslationConfig {
    /// If true, fail the query when sqlglot cannot translate a construct.
    /// If false (default), pass through best-effort.
    #[serde(default)]
    pub error_on_unsupported: bool,
    /// Python scripts run after every sqlglot translation.
    /// Each script must define `def transform(ast, src: str, dst: str) -> None:`.
    /// Top-level imports and helper functions are supported.
    /// Scripts mutate `ast` in-place; `src`/`dst` carry the dialect names.
    #[serde(default)]
    pub python_scripts: Vec<String>,
}

// --- Catalog provider ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum CatalogProviderConfig {
    #[default]
    Null,
    Static {
        schemas: Vec<StaticTableSchema>,
    },
    Trino {
        /// Name of the cluster group to use for metadata queries.
        cluster_group: String,
    },
    HiveMetastore {
        uri: String,
    },
    Glue {
        region: Option<String>,
    },
    Caching {
        ttl_seconds: u64,
        max_entries: usize,
        #[serde(flatten)]
        delegate: Box<CatalogProviderConfig>,
    },
    Fallback {
        primary: Box<CatalogProviderConfig>,
        secondary: Box<CatalogProviderConfig>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticTableSchema {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<StaticColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticColumnDef {
    pub name: String,
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Cache configuration
// ---------------------------------------------------------------------------

/// Top-level cache backend config (startup-only, not hot-reloadable).
/// Defines the storage backend and compression for Arrow IPC files.
///
/// Uses OpenDAL's generic `scheme` + `options` map, so any OpenDAL-supported
/// service (fs, s3, gcs, azblob, etc.) can be used without code changes.
///
/// Example YAML:
/// ```yaml
/// cacheBackend:
///   scheme: s3
///   compression: lz4
///   options:
///     bucket: my-cache
///     endpoint: http://localhost:19000
///     region: us-east-1
///     access_key_id: minio-root-user
///     secret_access_key: minio-root-password
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheBackendConfig {
    /// OpenDAL service scheme: `fs`, `s3`, `gcs`, `azblob`, etc.
    #[serde(default = "default_cache_scheme")]
    pub scheme: String,
    #[serde(default)]
    pub compression: CacheCompression,
    /// Flat key-value options passed directly to `Operator::via_iter(scheme, options)`.
    /// Keys are service-specific (see OpenDAL docs for each service).
    #[serde(default)]
    pub options: HashMap<String, String>,
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheCompression {
    None,
    #[default]
    Lz4,
    Zstd,
}

fn default_cache_scheme() -> String {
    "fs".to_string()
}

fn default_cleanup_interval() -> u64 {
    300
}

/// Per-group cache configuration (hot-reloadable via admin API / YAML).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GroupCacheConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cache_ttl")]
    pub ttl_secs: u64,
    #[serde(default)]
    pub max_entry_size_mb: Option<u64>,
}

fn default_cache_ttl() -> u64 {
    300
}

#[cfg(test)]
mod tests {
    use super::{
        GuardKindConfig, GuardSpecConfig, PersistenceConfig, PostgresPersistenceConfig,
        ProxyConfig, RouterConfig,
    };

    #[test]
    fn router_config_deserializes_admin_style_json_routers_array() {
        let j = r#"[
            {"type":"header","headerName":"X-Env","headerValueToGroup":{"prod":"g-analytics"}},
            {"type":"queryRegex","rules":[{"regex":"(?i)fact_","targetGroup":"g-facts"}]},
            {"type":"pythonScript","script":"def route(q,c):\n return None\n","scriptFile":null}
        ]"#;
        let routers: Vec<RouterConfig> = serde_json::from_str(j).expect("routers JSON");
        assert_eq!(routers.len(), 3);
        match &routers[0] {
            RouterConfig::Header {
                header_name,
                header_value_to_group,
            } => {
                assert_eq!(header_name, "X-Env");
                assert_eq!(
                    header_value_to_group.get("prod").map(String::as_str),
                    Some("g-analytics")
                );
            }
            _ => panic!("expected header"),
        }
        match &routers[1] {
            RouterConfig::QueryRegex { rules } => {
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].regex, "(?i)fact_");
                assert_eq!(rules[0].target_group, "g-facts");
            }
            _ => panic!("expected queryRegex"),
        }
        match &routers[2] {
            RouterConfig::PythonScript {
                script,
                script_file,
            } => {
                assert!(script.contains("def route"));
                assert!(script_file.is_none());
            }
            _ => panic!("expected pythonScript"),
        }
    }

    #[test]
    fn router_config_compound_deserializes() {
        let j = r#"{
            "type": "compound",
            "combine": "all",
            "conditions": [
                {"type": "protocol", "protocol": "trinoHttp"},
                {"type": "header", "headerName": "X-Tenant", "headerValue": "acme"}
            ],
            "targetGroup": "g1"
        }"#;
        let r: RouterConfig = serde_json::from_str(j).unwrap();
        match r {
            RouterConfig::Compound {
                combine,
                conditions,
                target_group,
            } => {
                assert_eq!(target_group, "g1");
                assert_eq!(conditions.len(), 2);
                assert!(matches!(combine, super::CompoundCombineMode::All));
            }
            _ => panic!("expected compound"),
        }
    }

    #[test]
    fn periodic_config_reload_interval_secs_default_zero_and_explicit() {
        let cfg_default: ProxyConfig = serde_yaml::from_str("queryflux: {}").unwrap();
        assert_eq!(
            cfg_default.queryflux.periodic_config_reload_interval_secs(),
            Some(30)
        );

        let cfg_120: ProxyConfig =
            serde_yaml::from_str("queryflux:\n  configReloadIntervalSecs: 120\n").unwrap();
        assert_eq!(
            cfg_120.queryflux.periodic_config_reload_interval_secs(),
            Some(120)
        );

        let cfg_zero: ProxyConfig =
            serde_yaml::from_str("queryflux:\n  configReloadIntervalSecs: 0\n").unwrap();
        assert_eq!(
            cfg_zero.queryflux.periodic_config_reload_interval_secs(),
            None
        );
    }

    /// Studio-first / Postgres: YAML may omit clusters, clusterGroups, routers, routingFallback.
    /// When maps are non-empty, QueryFlux upserts those entries into Postgres on each startup;
    /// omitted maps leave DB-managed resources unchanged.
    #[test]
    fn proxy_config_minimal_yaml_omits_groups_and_routing() {
        let yaml = r#"
queryflux: {}
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).expect("minimal-trino YAML should parse");
        assert!(cfg.clusters.is_empty());
        assert!(cfg.cluster_groups.is_empty());
        assert!(cfg.routers.is_empty());
        assert!(cfg.routing_fallback.is_empty());
    }

    #[test]
    fn guardrails_validation_accepts_external_guard_kinds() {
        let python = GuardSpecConfig {
            kind: GuardKindConfig::PythonScript,
            name: None,
            script_id: Some(42),
            script: None,
            url: None,
            timeout_ms: Some(250),
            retry_count: None,
            fail_behavior: None,
            headers: None,
            max_rows: None,
            applies_to: None,
        };
        python.validate().expect("python_script should validate");

        let webhook = GuardSpecConfig {
            kind: GuardKindConfig::HttpWebhook,
            name: None,
            script_id: None,
            script: None,
            url: Some("https://policy.example/guard".to_string()),
            timeout_ms: Some(500),
            retry_count: Some(1),
            fail_behavior: Some(super::GuardFailBehaviorConfig::Deny),
            headers: None,
            max_rows: None,
            applies_to: None,
        };
        webhook.validate().expect("http_webhook should validate");
    }

    #[test]
    fn guardrails_yaml_python_script_parses_and_validates() {
        let yaml = r#"
queryflux: {}
guardrails:
  global:
    - kind: python_script
      script_id: 42
      timeout_ms: 250
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).expect("YAML shape should parse");
        cfg.guardrails
            .expect("guardrails")
            .validate()
            .expect("python_script is supported");
    }

    #[test]
    fn guardrails_validation_rejects_blank_inline_script() {
        let blank = GuardSpecConfig {
            kind: GuardKindConfig::PythonScript,
            name: None,
            script_id: None,
            script: Some("  ".to_string()),
            url: None,
            timeout_ms: None,
            retry_count: None,
            fail_behavior: None,
            headers: None,
            max_rows: None,
            applies_to: None,
        };
        assert!(blank.validate().unwrap_err().contains("script"));
    }

    #[test]
    fn guardrails_validation_rejects_both_script_and_id() {
        let both = GuardSpecConfig {
            kind: GuardKindConfig::PythonScript,
            name: None,
            script_id: Some(1),
            script: Some("def check(ctx): return {'action':'allow'}".to_string()),
            url: None,
            timeout_ms: None,
            retry_count: None,
            fail_behavior: None,
            headers: None,
            max_rows: None,
            applies_to: None,
        };
        assert!(both.validate().unwrap_err().contains("not both"));
    }

    #[test]
    fn guardrails_yaml_legacy_http_webhook_parses_and_validates() {
        let yaml = r#"
queryflux: {}
guardrails:
  global:
    - kind:
        http_webhook:
          url: "https://policy.internal/guard"
          timeout_ms: 500
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).expect("legacy YAML shape should parse");
        let guardrails = cfg.guardrails.expect("guardrails section");
        guardrails
            .validate()
            .expect("legacy http_webhook should validate");
        let spec = &guardrails.global[0];
        assert!(matches!(spec.kind, GuardKindConfig::HttpWebhook));
        assert_eq!(spec.url.as_deref(), Some("https://policy.internal/guard"));
        assert_eq!(spec.timeout_ms, Some(500));
    }

    #[test]
    fn guardrails_validation_requires_external_guard_fields() {
        let yaml = r#"
queryflux: {}
guardrails:
  global:
    - kind: python_script
    - kind: http_webhook
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).expect("YAML shape should parse");
        let err = cfg
            .guardrails
            .expect("guardrails")
            .validate()
            .expect_err("missing external fields should fail");
        assert!(err.contains("script"));
    }

    #[test]
    fn postgres_persistence_prefers_url() {
        let c = PostgresPersistenceConfig {
            url: Some("postgres://u:p@h:5432/db".into()),
            host: Some("ignored".into()),
            ..Default::default()
        };
        assert_eq!(c.connection_url().unwrap(), "postgres://u:p@h:5432/db");
    }

    #[test]
    fn postgres_persistence_from_parts_builds_url() {
        let c = PostgresPersistenceConfig {
            host: Some("localhost".into()),
            port: Some(5433),
            user: Some("queryflux".into()),
            password: Some("secret@x".into()),
            database: Some("queryflux".into()),
            url: None,
            ..Default::default()
        };
        let u = c.connection_url().unwrap();
        assert!(u.starts_with("postgres://"));
        assert!(u.contains("5433"));
        assert!(u.contains("localhost"));
    }

    #[test]
    fn persistence_postgres_yaml_url_only() {
        let yaml = r#"
queryflux:
  persistence:
    type: postgres
    url: postgres://a:b@localhost:5432/db
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        match &cfg.queryflux.persistence {
            PersistenceConfig::Postgres { conn } => {
                assert_eq!(
                    conn.connection_url().unwrap(),
                    "postgres://a:b@localhost:5432/db"
                );
            }
            _ => panic!("expected postgres"),
        }
    }

    #[test]
    fn persistence_postgres_yaml_discrete_fields() {
        let yaml = r#"
queryflux:
  persistence:
    type: postgres
    host: localhost
    port: 5433
    user: queryflux
    password: queryflux
    database: queryflux
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        match &cfg.queryflux.persistence {
            PersistenceConfig::Postgres { conn } => {
                let u = conn.connection_url().unwrap();
                assert!(u.contains("localhost") && u.contains("5433"));
            }
            _ => panic!("expected postgres"),
        }
    }

    // -----------------------------------------------------------------------
    // Distributed mode config
    // -----------------------------------------------------------------------

    #[test]
    fn distributed_defaults_to_none() {
        let cfg: ProxyConfig = serde_yaml::from_str("queryflux: {}").unwrap();
        assert!(cfg.queryflux.distributed.is_none());
    }

    #[test]
    fn distributed_explicit_true() {
        let yaml = "queryflux:\n  distributed: true\n";
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.queryflux.distributed, Some(true));
    }

    #[test]
    fn distributed_explicit_false() {
        let yaml = "queryflux:\n  distributed: false\n";
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.queryflux.distributed, Some(false));
    }

    #[test]
    fn distributed_true_with_in_memory_persistence_is_invalid_at_runtime() {
        let yaml = r#"
queryflux:
  distributed: true
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.queryflux.distributed, Some(true));
        assert!(matches!(
            cfg.queryflux.persistence,
            PersistenceConfig::InMemory
        ));
    }

    #[test]
    fn distributed_true_with_postgres_is_valid_at_runtime() {
        let yaml = r#"
queryflux:
  distributed: true
  persistence:
    type: postgres
    url: postgres://a:b@localhost:5432/db
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.queryflux.distributed, Some(true));
        assert!(matches!(
            cfg.queryflux.persistence,
            PersistenceConfig::Postgres { .. }
        ));
    }

    // -----------------------------------------------------------------------
    // resolve_distributed validation
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_distributed_auto_detects_postgres() {
        let cfg: ProxyConfig = serde_yaml::from_str("queryflux: {}").unwrap();
        assert!(cfg.queryflux.resolve_distributed(true).unwrap());
        assert!(!cfg.queryflux.resolve_distributed(false).unwrap());
    }

    #[test]
    fn resolve_distributed_explicit_true_with_postgres_ok() {
        let yaml = "queryflux:\n  distributed: true\n";
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.queryflux.resolve_distributed(true).unwrap());
    }

    #[test]
    fn resolve_distributed_explicit_true_without_postgres_fails() {
        let yaml = "queryflux:\n  distributed: true\n";
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.queryflux.resolve_distributed(false).unwrap_err();
        assert!(err.contains("InMemory persistence cannot coordinate"));
    }

    #[test]
    fn resolve_distributed_explicit_false_with_postgres_ok() {
        let yaml = "queryflux:\n  distributed: false\n";
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.queryflux.resolve_distributed(true).unwrap());
    }
}
