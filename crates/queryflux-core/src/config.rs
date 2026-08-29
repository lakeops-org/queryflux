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
    /// Run operator-supplied Python for groups the built-in strategies can't express.
    /// Script must define `def select_cluster(candidates: list[dict]) -> str | None`,
    /// returning a member cluster name or `None` to fall back to the first candidate.
    #[serde(rename = "pythonScript")]
    #[serde(rename_all = "camelCase")]
    PythonScript {
        #[serde(default)]
        script: Option<String>,
        #[serde(default)]
        script_file: Option<String>,
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

impl ProxyConfig {
    /// Fail-closed checks run before auth providers are constructed at startup.
    pub fn validate_startup_security(&self) -> std::result::Result<(), String> {
        self.auth.validate_for_required()?;
        self.authorization.validate()?;
        if let Some(guardrails) = &self.guardrails {
            guardrails.validate()?;
        }
        Ok(())
    }
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
                let raw = self.url.as_deref().unwrap_or_default().trim();
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
    /// IdP roles that may cancel any in-flight or queued query (not poll).
    #[serde(default)]
    pub operator_roles: Vec<String>,
    /// IdP groups that may cancel any in-flight or queued query (not poll).
    #[serde(default)]
    pub operator_groups: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: AuthProviderConfig::None,
            required: false,
            oidc: None,
            ldap: None,
            static_users: None,
            operator_roles: Vec::new(),
            operator_groups: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Fail closed when `required` is true but the selected provider is incomplete.
    ///
    /// When `required` is false (dev/trusted networks), incomplete provider blocks
    /// are not rejected here — `build_auth_provider` may still fail later.
    pub fn validate_for_required(&self) -> std::result::Result<(), String> {
        if !self.required {
            return Ok(());
        }
        match self.provider {
            AuthProviderConfig::None => Err("auth.required is true but auth.provider is 'none' — \
                 set provider to oidc, ldap, or static"
                .into()),
            AuthProviderConfig::Static => {
                let Some(users) = &self.static_users else {
                    return Err("auth.required is true and auth.provider is 'static' but \
                         auth.staticUsers is missing"
                        .into());
                };
                if users.users.is_empty() {
                    return Err("auth.required is true and auth.provider is 'static' but \
                         auth.staticUsers.users is empty"
                        .into());
                }
                Ok(())
            }
            AuthProviderConfig::Oidc => {
                let Some(oidc) = &self.oidc else {
                    return Err("auth.required is true and auth.provider is 'oidc' but \
                         auth.oidc is missing"
                        .into());
                };
                if oidc.issuer.trim().is_empty() {
                    return Err(
                        "auth.oidc.issuer must not be empty when auth.required is true".into(),
                    );
                }
                if oidc.jwks_uri.trim().is_empty() {
                    return Err(
                        "auth.oidc.jwksUri must not be empty when auth.required is true".into(),
                    );
                }
                match &oidc.audience {
                    Some(aud) if !aud.trim().is_empty() => Ok(()),
                    _ => Err("auth.oidc.audience is required when auth.required is true \
                         (fail closed — no JWT accepted without an audience)"
                        .into()),
                }
            }
            AuthProviderConfig::Ldap => {
                let Some(ldap) = &self.ldap else {
                    return Err("auth.required is true and auth.provider is 'ldap' but \
                         auth.ldap is missing"
                        .into());
                };
                if ldap.url.trim().is_empty() {
                    return Err("auth.ldap.url must not be empty when auth.required is true".into());
                }
                let user_dn_template = ldap
                    .user_dn_template
                    .as_deref()
                    .filter(|template| !template.trim().is_empty());
                if let Some(template) = user_dn_template {
                    if !template.contains("{}") {
                        return Err("auth.ldap.userDnTemplate must contain '{}' when \
                             auth.required is true"
                            .into());
                    }
                } else if ldap.user_search_base.trim().is_empty() {
                    return Err("auth.ldap must set userDnTemplate or userSearchBase when \
                         auth.required is true"
                        .into());
                }
                if user_dn_template.is_none() && ldap.user_search_filter.trim().is_empty() {
                    return Err("auth.ldap.userSearchFilter must not be empty when \
                         auth.required is true"
                        .into());
                }
                Ok(())
            }
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

impl AuthorizationConfig {
    /// Refuse OpenFGA when the provider is selected but URL/store are incomplete.
    pub fn validate(&self) -> std::result::Result<(), String> {
        match self.provider {
            AuthorizationProviderConfig::None => Ok(()),
            AuthorizationProviderConfig::OpenFga => {
                let Some(fga) = &self.openfga else {
                    return Err(
                        "authorization.provider is 'openfga' but authorization.openfga is missing"
                            .into(),
                    );
                };
                if fga.url.trim().is_empty() {
                    return Err("authorization.openfga.url must not be empty".into());
                }
                if fga.store_id.trim().is_empty() {
                    return Err("authorization.openfga.storeId must not be empty".into());
                }
                Ok(())
            }
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
    /// Number of days to retain query history. When set, a background task runs
    /// hourly and deletes `query_records` (`created_at`), `query_digest_stats`
    /// (`last_seen`), and `cluster_snapshots` (`recorded_at`) older than this many days.
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
    /// MCP (Model Context Protocol) streamable-HTTP frontend for AI agent tool calls.
    #[serde(default)]
    pub mcp: Option<FrontendConfig>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Legacy total connection budget (`poolSize` in YAML), **inclusive of** the
    /// dedicated `PgListener` connection. When the per-workload pool sizes below
    /// are omitted, one slot is reserved for LISTEN and the remainder is split
    /// 60% query / 20% coordination / 20% admin (minimum 1 each). Must be at least
    /// 4. Ignored when any workload-specific pool size is set.
    #[serde(default)]
    pub pool_size: Option<u32>,
    /// Hot-path pool for executing/queued query state (`queryPoolSize`).
    #[serde(default)]
    pub query_pool_size: Option<u32>,
    /// Pool for distributed capacity, queue claims, and sweep locks (`coordinationPoolSize`).
    #[serde(default)]
    pub coordination_pool_size: Option<u32>,
    /// Pool for admin API, metrics, cache metadata, and config reload reads (`adminPoolSize`).
    #[serde(default)]
    pub admin_pool_size: Option<u32>,
    /// Max seconds to wait for a connection from the pool before returning an error.
    /// Defaults to 30 seconds.
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
    /// Postgres `statement_timeout` set on each connection (seconds). Prevents
    /// runaway queries from holding pool connections indefinitely. Defaults to 60s.
    #[serde(default)]
    pub statement_timeout_secs: Option<u64>,
    /// When true (default), QueryFlux runs Refinery migrations on server start.
    /// Set false when migrations are applied separately via `queryflux migrate`
    /// (or a Kubernetes Job) so multi-replica rollouts do not race on schema DDL.
    #[serde(default = "default_true")]
    pub auto_migrate: bool,
}

impl Default for PostgresPersistenceConfig {
    fn default() -> Self {
        Self {
            url: None,
            host: None,
            port: None,
            user: None,
            password: None,
            database: None,
            pool_size: None,
            query_pool_size: None,
            coordination_pool_size: None,
            admin_pool_size: None,
            acquire_timeout_secs: None,
            statement_timeout_secs: None,
            auto_migrate: true,
        }
    }
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

    /// Dedicated `PgListener` connections held outside the three sqlx pools.
    pub const LISTENER_CONNECTIONS: u32 = 1;
    /// Minimum legacy `poolSize`: 3 isolated pools (1 each) + 1 LISTEN connection.
    pub const MIN_LEGACY_POOL_SIZE: u32 = 4;

    /// Resolve isolated pool sizes for query, coordination, and admin workloads.
    ///
    /// Legacy `poolSize` is inclusive of [`Self::LISTENER_CONNECTIONS`]. Workload-specific
    /// sizes (`queryPoolSize` / …) are pool-only; the listener is extra in that mode.
    pub fn resolve_pool_sizes(&self) -> Result<(u32, u32, u32), String> {
        const DEFAULT_TOTAL: u32 = 10;

        if self.query_pool_size.is_some()
            || self.coordination_pool_size.is_some()
            || self.admin_pool_size.is_some()
        {
            return Ok((
                self.query_pool_size.unwrap_or(6).max(1),
                self.coordination_pool_size.unwrap_or(2).max(1),
                self.admin_pool_size.unwrap_or(2).max(1),
            ));
        }

        let total = self.pool_size.unwrap_or(DEFAULT_TOTAL);
        if total < Self::MIN_LEGACY_POOL_SIZE {
            return Err(format!(
                "postgres persistence: poolSize must be at least {} \
                 (3 isolated pools + {} LISTEN connection); got {total}",
                Self::MIN_LEGACY_POOL_SIZE,
                Self::LISTENER_CONNECTIONS
            ));
        }

        let pool_budget = total - Self::LISTENER_CONNECTIONS;
        let coordination = (pool_budget * 20 / 100).max(1);
        let admin = (pool_budget * 20 / 100).max(1);
        let query = pool_budget
            .saturating_sub(coordination)
            .saturating_sub(admin)
            .max(1);
        debug_assert!(
            query + coordination + admin + Self::LISTENER_CONNECTIONS <= total,
            "legacy poolSize split must not exceed configured budget"
        );
        Ok((query, coordination, admin))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PersistenceConfig {
    #[default]
    #[serde(rename = "inMemory")]
    InMemory,
    /// Reserved for a future Redis backend. QueryFlux rejects this at startup
    /// (no silent fallback to in-memory).
    Redis { url: String },
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

/// Default proxy-side wait for cluster capacity (`capacityWaitTimeoutSecs`).
pub const DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS: u64 = 300;

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
    /// Max seconds a query may wait for capacity (sync spin or async queue).
    /// Defaults to [`DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS`] when omitted.
    #[serde(default)]
    pub capacity_wait_timeout_secs: Option<u64>,
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

impl ClusterGroupConfig {
    /// Resolved capacity wait timeout; omitted / zero → [`DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS`].
    pub fn capacity_wait_timeout_secs_or_default(&self) -> u64 {
        match self.capacity_wait_timeout_secs {
            Some(n) if n > 0 => n,
            _ => DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS,
        }
    }
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Max seconds QueryFlux will poll Athena for query completion
    /// (`maxWaitSecs` in JSON/YAML). Defaults to 600 (10 minutes) when omitted.
    /// On timeout the proxy calls `StopQueryExecution`.
    #[serde(default)]
    pub max_wait_secs: Option<u64>,
    /// Max bytes of a single query result QueryFlux buffers in memory
    /// (`maxResultBufferBytes` in JSON/YAML). ClickHouse only; defaults to
    /// 1 GiB when omitted. Other engines ignore this.
    #[serde(default)]
    pub max_result_buffer_bytes: Option<u64>,
    /// ADBC driver name (e.g. `"snowflake"`, `"flightsql"`) — only meaningful when
    /// `engine` is [`EngineConfig::Adbc`]. ADBC clusters are admin-API-only (never YAML),
    /// so this is populated from the persisted JSONB `driver` key, not deserialized from
    /// this struct's own YAML/JSON representation. Used by [`query_auth_supported`] since
    /// `EngineConfig::Adbc` alone doesn't say which underlying driver — and therefore
    /// which OAuth-capable drivers — a cluster uses.
    #[serde(default)]
    pub driver: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Type 1 credentials — service account used for health checks and (by default) query execution.
    #[serde(default)]
    pub auth: Option<ClusterAuth>,
    /// Type 2 credentials — how to authenticate per-user queries to this cluster.
    /// Default when omitted: `serviceAccount` (use Type 1 for all queries).
    #[serde(default)]
    pub query_auth: Option<QueryAuthConfig>,
    /// Sub-resource variants that expand this config into multiple runtime clusters.
    /// Each variant produces an independent cluster named `{base}::{variant.name}`.
    #[serde(default)]
    pub variants: Vec<ClusterVariant>,
    /// Custom SQL query for health checks. When set, replaces built-in introspection.
    /// When absent, driver-specific defaults apply (e.g. Snowflake `SHOW WAREHOUSES`).
    /// Supports `{{sub_resource}}` placeholder for variant clusters.
    #[serde(default)]
    pub health_check_query: Option<String>,
    /// Custom SQL query for running-query reconciliation. Must return a single integer
    /// (or a result set with a `running` column for Snowflake `SHOW WAREHOUSES`).
    /// When absent, driver-specific defaults apply. Databricks uses REST (no SQL default).
    /// Supports `{{sub_resource}}` placeholder for variant clusters.
    #[serde(default)]
    pub reconcile_query: Option<String>,
}

/// A variant that overrides specific fields of a base cluster config,
/// expanding into an independent runtime cluster named `{base}::{name}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVariant {
    pub name: String,
    #[serde(default)]
    pub overrides: serde_json::Value,
    #[serde(default)]
    pub max_running_queries: Option<u64>,
}

/// Result of expanding a single variant from a base cluster config.
#[derive(Debug, Clone)]
pub struct ExpandedCluster {
    pub expanded_name: String,
    pub merged_config: serde_json::Value,
    pub max_running_queries: Option<u64>,
    pub health_check_query: Option<String>,
    pub reconcile_query: Option<String>,
}

/// Maps user-friendly variant override keys to ADBC driver-specific `dbKwargs` keys.
pub fn variant_field_to_db_kwarg(driver: &str, key: &str) -> Option<&'static str> {
    match (driver, key) {
        ("snowflake", "warehouse") => Some("adbc.snowflake.sql.warehouse"),
        ("snowflake", "role") => Some("adbc.snowflake.sql.role"),
        ("snowflake", "database") => Some("adbc.snowflake.sql.db"),
        ("snowflake", "schema") => Some("adbc.snowflake.sql.schema"),
        ("databricks", "httpPath") => Some("http_path"),
        ("bigquery", "project") => Some("project_id"),
        ("redshift", "workgroup") => Some("workgroup"),
        _ => None,
    }
}

/// Resolves the sub-resource name from variant overrides for `{{sub_resource}}` substitution.
/// `driver_or_engine` is the ADBC driver name for ADBC clusters, or the
/// `engine_key` itself for non-ADBC engines that have their own sub-resource
/// concept (Athena's `workgroup` — one account/region shared across named
/// workgroups, the same shape as an ADBC SaaS driver's warehouse/project).
fn resolve_sub_resource(driver_or_engine: &str, overrides: &serde_json::Value) -> Option<String> {
    let primary_keys: &[&str] = match driver_or_engine {
        "snowflake" => &["warehouse"],
        "databricks" => &["httpPath"],
        "bigquery" => &["project"],
        "redshift" => &["workgroup"],
        "athena" => &["workgroup"],
        _ => &[],
    };
    for key in primary_keys {
        if let Some(val) = overrides.get(key).and_then(|v| v.as_str()) {
            return Some(val.to_string());
        }
    }
    None
}

fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

fn json_db_kwarg(config: &serde_json::Value, key: &str) -> Option<String> {
    config
        .get("dbKwargs")
        .and_then(|v| v.get(key))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Resolve the primary sub-resource (warehouse, project, httpPath) from a merged ADBC config.
pub fn resolve_adbc_sub_resource(driver: &str, config: &serde_json::Value) -> Option<String> {
    match driver {
        "snowflake" => config
            .get("warehouse")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| json_db_kwarg(config, "adbc.snowflake.sql.warehouse"))
            .or_else(|| json_db_kwarg(config, "warehouse")),
        "bigquery" => json_db_kwarg(config, "project_id").or_else(|| {
            config
                .get("project")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        }),
        "databricks" => json_db_kwarg(config, "http_path").or_else(|| {
            config
                .get("httpPath")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        }),
        _ => None,
    }
}

fn build_bigquery_reconcile_sql(project_id: &str, region: Option<&str>) -> String {
    if let Some(region) = region {
        format!(
            "SELECT COUNT(*) FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
             WHERE state = 'RUNNING' AND project_id = '{}'",
            escape_sql_literal(project_id)
        )
    } else {
        format!(
            "SELECT COUNT(*) FROM `{project}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
             WHERE state = 'RUNNING'",
            project = project_id
        )
    }
}

/// Built-in reconcile SQL when the operator does not set `reconcileQuery`.
/// Returns `None` for engines that use non-SQL introspection (e.g. Databricks REST).
pub fn default_reconcile_query(engine_key: &str, config: &serde_json::Value) -> Option<String> {
    match engine_key {
        "trino" => {
            Some("SELECT count(*) - 1 FROM system.runtime.queries WHERE state = 'RUNNING'".into())
        }
        "starRocks" => Some(
            "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND = 'Query'".into(),
        ),
        "adbc" => {
            let driver = config.get("driver")?.as_str()?;
            match driver {
                "snowflake" => {
                    let wh = resolve_adbc_sub_resource(driver, config)?;
                    Some(format!(
                        "SHOW WAREHOUSES LIKE '{}'",
                        escape_sql_literal(&wh)
                    ))
                }
                "bigquery" => {
                    let project = resolve_adbc_sub_resource(driver, config)?;
                    let region = json_db_kwarg(config, "location")
                        .or_else(|| json_db_kwarg(config, "region"));
                    Some(build_bigquery_reconcile_sql(&project, region.as_deref()))
                }
                "redshift" => {
                    Some("SELECT COUNT(*) FROM stv_recents WHERE status = 'Running'".into())
                }
                "trino" => Some(
                    "SELECT count(*) - 1 FROM system.runtime.queries WHERE state = 'RUNNING'"
                        .into(),
                ),
                "flightsql" => Some(
                    "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND = 'Query'"
                        .into(),
                ),
                "clickhouse" => Some("SELECT count() FROM system.processes".into()),
                // Databricks uses REST introspection — no SQL default.
                "databricks" => None,
                _ => None,
            }
        }
        _ => None,
    }
}

/// Built-in health-check SQL when the operator does not set `healthCheckQuery`.
pub fn default_health_check_query(engine_key: &str, config: &serde_json::Value) -> Option<String> {
    if engine_key != "adbc" {
        return None;
    }
    let driver = config.get("driver")?.as_str()?;
    match driver {
        "snowflake" => {
            let wh = resolve_adbc_sub_resource(driver, config)?;
            Some(format!(
                "SHOW WAREHOUSES LIKE '{}'",
                escape_sql_literal(&wh)
            ))
        }
        _ => None,
    }
}

/// Fill unset probe queries on a cluster config from engine/driver defaults.
pub fn apply_default_probe_queries(
    cfg: &mut ClusterConfig,
    engine_key: &str,
    config: &serde_json::Value,
) {
    if cfg
        .health_check_query
        .as_ref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        cfg.health_check_query = default_health_check_query(engine_key, config);
    }
    if cfg
        .reconcile_query
        .as_ref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        cfg.reconcile_query = default_reconcile_query(engine_key, config);
    }
}

fn substitute_sub_resource(query: &str, sub_resource: Option<&str>) -> String {
    match sub_resource {
        Some(sr) => query.replace("{{sub_resource}}", sr),
        None => query.to_string(),
    }
}

fn effective_probe_query(
    user: Option<&str>,
    default: Option<String>,
    sub_resource: Option<&str>,
) -> Option<String> {
    if let Some(q) = user.filter(|s| !s.trim().is_empty()) {
        return Some(substitute_sub_resource(q, sub_resource));
    }
    default
}

/// Deep-merge `patch` into `base`. Object values are recursively merged;
/// all other types in `patch` overwrite the corresponding key in `base`.
fn deep_merge(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (Some(base_obj), Some(patch_obj)) = (base.as_object_mut(), patch.as_object()) {
        for (k, v) in patch_obj {
            if v.is_object() {
                let entry = base_obj
                    .entry(k.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                deep_merge(entry, v);
            } else {
                base_obj.insert(k.clone(), v.clone());
            }
        }
    } else {
        *base = patch.clone();
    }
}

/// Validates and expands cluster variants into independent runtime clusters.
///
/// For ADBC clusters, user-friendly override keys (e.g., `warehouse`) are
/// additionally injected into `dbKwargs` using driver-specific mappings.
pub fn expand_cluster_variants(
    base_name: &str,
    base_config: &serde_json::Value,
    engine_key: &str,
    variants: &[ClusterVariant],
    health_check_query: Option<&str>,
    reconcile_query: Option<&str>,
) -> Result<Vec<ExpandedCluster>, String> {
    let mut seen_names = std::collections::HashSet::new();
    let mut result = Vec::with_capacity(variants.len());

    let driver = base_config
        .get("driver")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let is_adbc = engine_key == "adbc";

    for variant in variants {
        if variant.name.is_empty() {
            return Err(format!(
                "cluster '{base_name}': variant name cannot be empty"
            ));
        }
        if variant.name.contains("::") {
            return Err(format!(
                "cluster '{base_name}': variant name '{}' must not contain '::'",
                variant.name
            ));
        }
        if !seen_names.insert(&variant.name) {
            return Err(format!(
                "cluster '{base_name}': duplicate variant name '{}'",
                variant.name
            ));
        }

        let expanded_name = format!("{base_name}::{}", variant.name);

        let mut merged = base_config.clone();
        if variant.overrides.is_object() {
            deep_merge(&mut merged, &variant.overrides);

            if is_adbc {
                let db_kwargs = merged
                    .as_object_mut()
                    .unwrap()
                    .entry("dbKwargs")
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                if let Some(kwargs_obj) = db_kwargs.as_object_mut() {
                    for (key, val) in variant.overrides.as_object().unwrap() {
                        if let Some(db_key) = variant_field_to_db_kwarg(driver, key) {
                            kwargs_obj.insert(db_key.to_string(), val.clone());
                        }
                    }
                }
            }
        }

        // Non-ADBC engines (e.g. Athena) have no `driver` field; fall back to
        // `engine_key` so they can still have a sub-resource concept.
        let driver_or_engine = if driver.is_empty() {
            engine_key
        } else {
            driver
        };
        let sub_resource = resolve_sub_resource(driver_or_engine, &variant.overrides);
        let default_hcq = default_health_check_query(engine_key, &merged);
        let default_rq = default_reconcile_query(engine_key, &merged);
        let hcq = effective_probe_query(health_check_query, default_hcq, sub_resource.as_deref());
        let rq = effective_probe_query(reconcile_query, default_rq, sub_resource.as_deref());

        result.push(ExpandedCluster {
            expanded_name,
            merged_config: merged,
            max_running_queries: variant.max_running_queries,
            health_check_query: hcq,
            reconcile_query: rq,
        });
    }

    Ok(result)
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

impl ClusterAuth {
    /// True when this auth type sets the HTTP `Authorization` header itself (Basic/Bearer).
    /// `AccessKey`/`KeyPair`/`RoleArn` do not set that header, so a client's own
    /// `Authorization` may still need to be forwarded under `serviceAccount` for
    /// backward compatibility with today's implicit passthrough behavior.
    pub fn sets_http_authorization(&self) -> bool {
        matches!(self, ClusterAuth::Basic { .. } | ClusterAuth::Bearer { .. })
    }
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
    /// Forward the client's own credential (Bearer/Basic) to the backend unchanged — the
    /// backend authenticates the user's own token, not a QueryFlux-vouched identity.
    /// **Trino only in this release** — startup validation rejects this for other engines
    /// until their adapters are wired (see `query_auth_supported`).
    #[serde(rename = "passthrough")]
    Passthrough,
    /// Service account authenticates to the backend; user identity injected via an
    /// engine-specific mechanism — Trino's `X-Trino-User` header, or ClickHouse's
    /// `EXECUTE AS {user} {sql}` (self-hosted 25.11+ only, requires `GRANT IMPERSONATE`).
    /// **Trino and ClickHouse** — startup validation rejects this for other engines.
    #[serde(rename = "impersonate")]
    Impersonate,
    /// Exchange the user's OIDC JWT (RFC 8693) for a backend-scoped OAuth token.
    /// Requires `OidcAuthProvider` on the frontend (so `raw_token` is populated).
    /// **Fails closed**: if `raw_token` is absent or the exchange fails, the query is
    /// rejected with an auth error — it never silently falls back to `serviceAccount`,
    /// since that would submit the query under the wrong principal.
    /// **Trino, and ADBC/Snowflake** in this release — startup validation rejects this for
    /// other engines/drivers until their adapters are wired (see `query_auth_supported`).
    #[serde(rename = "tokenExchange")]
    TokenExchange(TokenExchangeConfig),
}

/// ADBC drivers whose adapter code knows how to put an OAuth token into connection
/// options via a per-identity sub-pool (see `AdbcAdapter`). Every other ADBC driver is
/// `serviceAccount`-only — `EngineConfig::Adbc` alone can't tell drivers apart, so this
/// list is what actually gates `tokenExchange` for ADBC clusters.
const ADBC_TOKEN_EXCHANGE_DRIVERS: &[&str] = &["snowflake"];

/// ADBC drivers whose adapter code knows how to turn a `ClusterAuth::KeyPair` (Type 1,
/// baseline cluster auth) into driver-specific JWT connection options — see
/// `AdbcAdapter::new`/`jwt_auth_options`. Every other ADBC driver is `basic`-only.
/// `EngineConfig::Adbc` alone can't tell drivers apart, so `engine_registry`'s startup
/// validation checks this list before accepting `authType: keyPair` on an ADBC cluster —
/// the same shape as [`ADBC_TOKEN_EXCHANGE_DRIVERS`], just for Type 1 auth instead of Type 2.
pub const ADBC_KEYPAIR_AUTH_DRIVERS: &[&str] = &["snowflake"];

/// ADBC drivers whose adapter code knows how to route a query to a per-caller-credential
/// sub-pool for `passthrough` (see `AdbcAdapter::execute_as_arrow`'s `PoolScopeKey`
/// passthrough dimension). Every other ADBC driver is `serviceAccount`-only — same shape as
/// [`ADBC_TOKEN_EXCHANGE_DRIVERS`], just for `passthrough` instead of `tokenExchange`.
const ADBC_PASSTHROUGH_AUTH_DRIVERS: &[&str] = &["snowflake"];

/// Centralizes the engine × `queryAuth` support matrix so YAML load, Studio PUT, and
/// startup validation can't drift apart. Returns `Err` with an operator-facing message
/// when `mode` is not implemented for `engine` (and, for ADBC, `driver`) yet.
///
/// `serviceAccount` is always allowed. Trino supports every mode. ADBC supports
/// `tokenExchange` only for drivers in [`ADBC_TOKEN_EXCHANGE_DRIVERS`] — `driver` is
/// ignored for every other engine. Everything else (ClickHouse, StarRocks, and all other
/// ADBC drivers) is `serviceAccount`-only in this release; accepting the config silently
/// would produce a cluster that claims per-user identity but never applies it.
pub fn query_auth_supported(
    engine: Option<&EngineConfig>,
    driver: Option<&str>,
    mode: &QueryAuthConfig,
) -> Result<(), String> {
    if matches!(mode, QueryAuthConfig::ServiceAccount) {
        return Ok(());
    }
    let mode_name = match mode {
        QueryAuthConfig::Passthrough => "passthrough",
        QueryAuthConfig::Impersonate => "impersonate",
        QueryAuthConfig::TokenExchange(_) => "tokenExchange",
        QueryAuthConfig::ServiceAccount => unreachable!("handled above"),
    };
    match engine {
        Some(EngineConfig::Trino) => Ok(()),
        Some(EngineConfig::ClickHouse) if matches!(mode, QueryAuthConfig::Impersonate) => Ok(()),
        Some(EngineConfig::ClickHouse) => Err(format!(
            "queryAuth.type = {mode_name} is not supported for ClickHouse in this release \
             (only impersonate, self-hosted ClickHouse 25.11+ with \
             access_control_improvements.allow_impersonate_user = 1 and GRANT IMPERSONATE — \
             not supported on ClickHouse Cloud)"
        )),
        // `passthrough` here means LDAP-password `COM_CHANGE_USER`, not a bearer token —
        // see `mysql_native::apply_passthrough_identity`. Only valid when the cluster's own
        // TLS + `enable_cleartext_plugin` requirements are met; the adapter constructor
        // (`StarRocksAdapter::new`) enforces that, not this function (it doesn't have the
        // connection URL). `impersonate`/`tokenExchange` have no StarRocks mechanism.
        Some(EngineConfig::StarRocks) if matches!(mode, QueryAuthConfig::Passthrough) => Ok(()),
        Some(EngineConfig::StarRocks) => Err(format!(
            "queryAuth.type = {mode_name} is not supported for StarRocks in this release \
             (only passthrough, which requires TLS on the cluster endpoint — see \
             auth-authz-design.md \"StarRocks\")"
        )),
        Some(EngineConfig::Adbc)
            if matches!(mode, QueryAuthConfig::TokenExchange(_))
                && driver.is_some_and(|d| ADBC_TOKEN_EXCHANGE_DRIVERS.contains(&d)) =>
        {
            Ok(())
        }
        // Passthrough here means a real per-caller Snowflake username/password, captured at
        // Snowflake wire v1 login and routed to a dedicated sub-pool — see
        // `AdbcAdapter::execute_as_arrow`. SQL API v2 (stateless, Bearer-only) has no password
        // to capture, so a passthrough-configured cluster reached that way fails closed at
        // dispatch time with a clear error instead of silently falling back to the service
        // account — no separate check needed here for that case.
        Some(EngineConfig::Adbc)
            if matches!(mode, QueryAuthConfig::Passthrough)
                && driver.is_some_and(|d| ADBC_PASSTHROUGH_AUTH_DRIVERS.contains(&d)) =>
        {
            Ok(())
        }
        Some(EngineConfig::Adbc) => {
            let driver_name = driver.unwrap_or("<unknown>");
            Err(format!(
                "queryAuth.type = {mode_name} is not supported for ADBC driver '{driver_name}' \
                 in this release (only tokenExchange for {ADBC_TOKEN_EXCHANGE_DRIVERS:?}, or \
                 passthrough for {ADBC_PASSTHROUGH_AUTH_DRIVERS:?})"
            ))
        }
        other => {
            let engine_name = other
                .map(|e| format!("{e:?}"))
                .unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "queryAuth.type = {mode_name} is only supported for Trino, ClickHouse \
                 (impersonate only), StarRocks (passthrough only), and ADBC/snowflake \
                 (tokenExchange or passthrough) in this release, got {engine_name}"
            ))
        }
    }
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
        #[serde(default)]
        mcp: Option<String>,
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
    /// Cluster group to route to when [`Self::action`] is [`RegexRouteAction::Route`].
    #[serde(default)]
    pub target_group: Option<String>,
    /// `route` (default) or `deny`.
    #[serde(default)]
    pub action: RegexRouteAction,
    /// Client-visible message when [`Self::action`] is [`RegexRouteAction::Deny`].
    #[serde(default)]
    pub error: Option<String>,
}

/// Action taken when a [`QueryRegexRule`] matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum RegexRouteAction {
    #[default]
    Route,
    Deny,
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
    /// Max time to spend on catalog lookup for schema-aware translation before
    /// falling back to dialect-only translation. Default 1500ms.
    #[serde(default = "default_schema_resolution_timeout_ms")]
    pub schema_resolution_timeout_ms: u64,
}

fn default_schema_resolution_timeout_ms() -> u64 {
    1500
}

// --- Catalog provider ---

// NOTE: `rename_all = "camelCase"` on this container does NOT reliably reach
// multi-word field names *inside* struct-like variants of an internally-tagged
// (`tag = "type"`) enum with this serde/serde_yaml version combination — confirmed
// empirically (deserialization silently reports the snake_case name as "missing"
// even when the container attribute claims camelCase). Every multi-word variant
// field below has an explicit `#[serde(rename = "...")]` because of this; don't
// rely on the container-level rename_all for new fields added here.
/// TTL + capacity-bounded cache for one catalog provider's lookups. Every
/// provider that makes a real network call per lookup carries this as its own
/// `cache` field (rather than a separate wrapper type to remember to nest) —
/// table/column metadata rarely changes, so a much longer TTL than a query
/// result cache is normally safe. `None` (the default) means every lookup
/// goes straight to the backing service.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCacheConfig {
    #[serde(default = "default_catalog_cache_ttl_seconds")]
    pub ttl_seconds: u64,
    #[serde(default = "default_catalog_cache_max_entries")]
    pub max_entries: usize,
}

fn default_catalog_cache_ttl_seconds() -> u64 {
    300
}

fn default_catalog_cache_max_entries() -> usize {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum CatalogProviderConfig {
    #[default]
    Null,
    /// Delegates catalog discovery to a cluster group's own adapter (Trino, DuckDB,
    /// StarRocks, Athena, or ClickHouse — whichever engine the group runs). Opt-in
    /// convenience/fallback, not a recommended default: its accuracy and availability
    /// are tied to that cluster group's health. Not yet implemented — see
    /// `plans/` catalog integration plan for phasing.
    EngineDelegate {
        /// Name of the cluster group to use for metadata queries.
        #[serde(rename = "clusterGroup")]
        cluster_group: String,
        #[serde(default)]
        cache: Option<CatalogCacheConfig>,
    },
    HiveMetastore {
        uri: String,
        #[serde(default)]
        cache: Option<CatalogCacheConfig>,
    },
    Glue {
        region: Option<String>,
        /// AWS credentials — reuses the same `ClusterAuth` shape as engine cluster
        /// config (`accessKey`/`roleArn`; `basic`/`bearer`/`keyPair` don't apply
        /// here). Omitted: the default AWS credential chain (env vars, ECS task
        /// role, EC2 instance profile, etc.).
        #[serde(default)]
        auth: Option<ClusterAuth>,
        #[serde(default)]
        cache: Option<CatalogCacheConfig>,
    },
    /// Tries `primary` first, falls through to `secondary` on error (or a missing
    /// single table, for `get_table_schema` specifically). Each side configures
    /// its own `cache` independently — this is about composing two *sources*,
    /// not about caching.
    Fallback {
        primary: Box<CatalogProviderConfig>,
        secondary: Box<CatalogProviderConfig>,
    },
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
        query_auth_supported, AuthConfig, AuthProviderConfig, AuthorizationConfig,
        AuthorizationProviderConfig, ClusterAuth, EngineConfig, GuardKindConfig, GuardSpecConfig,
        LdapConfig, OidcConfig, OpenFgaConfig, PersistenceConfig, PostgresPersistenceConfig,
        ProxyConfig, QueryAuthConfig, RouterConfig, StrategyConfig, TokenExchangeConfig,
    };

    fn token_exchange_mode() -> QueryAuthConfig {
        QueryAuthConfig::TokenExchange(TokenExchangeConfig {
            token_endpoint: "https://idp.example/token".to_string(),
            client_id: "queryflux".to_string(),
            client_secret: "secret".to_string(),
            target_audience: None,
            scope: None,
        })
    }

    #[test]
    fn service_account_is_allowed_for_every_engine_including_unknown() {
        assert!(query_auth_supported(
            Some(&EngineConfig::ClickHouse),
            None,
            &QueryAuthConfig::ServiceAccount
        )
        .is_ok());
        assert!(query_auth_supported(None, None, &QueryAuthConfig::ServiceAccount).is_ok());
    }

    #[test]
    fn passthrough_impersonate_token_exchange_allowed_for_trino() {
        for mode in [
            QueryAuthConfig::Passthrough,
            QueryAuthConfig::Impersonate,
            token_exchange_mode(),
        ] {
            assert!(
                query_auth_supported(Some(&EngineConfig::Trino), None, &mode).is_ok(),
                "Trino should support {mode:?}"
            );
            assert!(
                query_auth_supported(None, None, &mode).is_err(),
                "an unknown engine should not support {mode:?}"
            );
        }
    }

    #[test]
    fn duckdb_and_athena_are_service_account_only() {
        // No per-user identity concept (DuckDB embedded, Athena IAM) — neither gets any
        // mode beyond serviceAccount in this release.
        for engine in [
            EngineConfig::DuckDb,
            EngineConfig::DuckDbHttp,
            EngineConfig::Athena,
        ] {
            for mode in [
                QueryAuthConfig::Passthrough,
                QueryAuthConfig::Impersonate,
                token_exchange_mode(),
            ] {
                assert!(
                    query_auth_supported(Some(&engine), None, &mode).is_err(),
                    "{engine:?} should not support {mode:?} in this release"
                );
            }
            assert!(
                query_auth_supported(Some(&engine), None, &QueryAuthConfig::ServiceAccount).is_ok()
            );
        }
    }

    #[test]
    fn starrocks_allows_only_passthrough() {
        assert!(query_auth_supported(
            Some(&EngineConfig::StarRocks),
            None,
            &QueryAuthConfig::Passthrough
        )
        .is_ok());
        assert!(query_auth_supported(
            Some(&EngineConfig::StarRocks),
            None,
            &QueryAuthConfig::Impersonate
        )
        .is_err());
        assert!(
            query_auth_supported(Some(&EngineConfig::StarRocks), None, &token_exchange_mode())
                .is_err()
        );
        assert!(query_auth_supported(
            Some(&EngineConfig::StarRocks),
            None,
            &QueryAuthConfig::ServiceAccount
        )
        .is_ok());
    }

    #[test]
    fn clickhouse_allows_only_impersonate() {
        assert!(query_auth_supported(
            Some(&EngineConfig::ClickHouse),
            None,
            &QueryAuthConfig::Impersonate
        )
        .is_ok());
        assert!(query_auth_supported(
            Some(&EngineConfig::ClickHouse),
            None,
            &QueryAuthConfig::Passthrough
        )
        .is_err());
        assert!(query_auth_supported(
            Some(&EngineConfig::ClickHouse),
            None,
            &token_exchange_mode()
        )
        .is_err());
    }

    #[test]
    fn adbc_token_exchange_allowed_only_for_snowflake_driver() {
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            Some("snowflake"),
            &token_exchange_mode()
        )
        .is_ok());
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            Some("postgresql"),
            &token_exchange_mode()
        )
        .is_err());
        assert!(
            query_auth_supported(Some(&EngineConfig::Adbc), None, &token_exchange_mode()).is_err()
        );
    }

    #[test]
    fn adbc_impersonate_rejected_even_for_snowflake_driver() {
        // No ADBC driver has an `impersonate` mechanism in this release.
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            Some("snowflake"),
            &QueryAuthConfig::Impersonate
        )
        .is_err());
    }

    #[test]
    fn adbc_passthrough_allowed_only_for_snowflake_driver() {
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            Some("snowflake"),
            &QueryAuthConfig::Passthrough
        )
        .is_ok());
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            Some("postgresql"),
            &QueryAuthConfig::Passthrough
        )
        .is_err());
        assert!(query_auth_supported(
            Some(&EngineConfig::Adbc),
            None,
            &QueryAuthConfig::Passthrough
        )
        .is_err());
    }

    #[test]
    fn cluster_auth_sets_http_authorization_matches_basic_and_bearer_only() {
        assert!(ClusterAuth::Basic {
            username: "u".into(),
            password: "p".into()
        }
        .sets_http_authorization());
        assert!(ClusterAuth::Bearer { token: "t".into() }.sets_http_authorization());
        assert!(!ClusterAuth::AccessKey {
            access_key_id: "a".into(),
            secret_access_key: "s".into(),
            session_token: None
        }
        .sets_http_authorization());
    }

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
                assert_eq!(rules[0].target_group.as_deref(), Some("g-facts"));
                assert!(matches!(rules[0].action, super::RegexRouteAction::Route));
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
    fn strategy_config_python_script_deserializes_and_roundtrips() {
        let j = r#"{"type":"pythonScript","script":"def select_cluster(c):\n return None\n"}"#;
        let strategy: StrategyConfig = serde_json::from_str(j).expect("strategy JSON");
        match &strategy {
            StrategyConfig::PythonScript {
                script,
                script_file,
            } => {
                assert_eq!(
                    script.as_deref(),
                    Some("def select_cluster(c):\n return None\n")
                );
                assert!(script_file.is_none());
            }
            _ => panic!("expected pythonScript"),
        }
        let out = serde_json::to_value(&strategy).unwrap();
        assert_eq!(out["type"], "pythonScript");
    }

    #[test]
    fn strategy_config_python_script_accepts_script_file() {
        let j = r#"{"type":"pythonScript","scriptFile":"/etc/queryflux/select.py"}"#;
        let strategy: StrategyConfig = serde_json::from_str(j).expect("strategy JSON");
        match &strategy {
            StrategyConfig::PythonScript {
                script,
                script_file,
            } => {
                assert!(script.is_none());
                assert_eq!(script_file.as_deref(), Some("/etc/queryflux/select.py"));
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
    fn guardrails_validation_rejects_non_http_url_scheme() {
        let webhook = GuardSpecConfig {
            kind: GuardKindConfig::HttpWebhook,
            name: None,
            script_id: None,
            script: None,
            url: Some("file:///etc/passwd".to_string()),
            timeout_ms: None,
            retry_count: None,
            fail_behavior: None,
            headers: None,
            max_rows: None,
            applies_to: None,
        };
        let err = webhook.validate().unwrap_err();
        assert!(err.contains("http or https"), "{err}");
    }

    #[test]
    fn startup_rejects_invalid_guardrails() {
        let yaml = r#"
queryflux: {}
guardrails:
  global:
    - kind: http_webhook
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate_startup_security().unwrap_err();
        assert!(err.contains("url"), "{err}");
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

    fn assert_legacy_budget_inclusive_of_listener(cfg: &PostgresPersistenceConfig) {
        let total = cfg.pool_size.unwrap_or(10);
        let (query, coordination, admin) = cfg.resolve_pool_sizes().unwrap();
        assert!(
            query + coordination + admin + PostgresPersistenceConfig::LISTENER_CONNECTIONS <= total,
            "pools ({query}+{coordination}+{admin}) + listener must not exceed poolSize {total}"
        );
    }

    #[test]
    fn postgres_pool_sizes_split_legacy_total() {
        let c = PostgresPersistenceConfig {
            pool_size: Some(20),
            ..Default::default()
        };
        assert_eq!(c.resolve_pool_sizes().unwrap(), (13, 3, 3));
        assert_legacy_budget_inclusive_of_listener(&c);
    }

    #[test]
    fn postgres_pool_sizes_default_total_includes_listener() {
        let c = PostgresPersistenceConfig::default();
        assert_eq!(c.resolve_pool_sizes().unwrap(), (7, 1, 1));
        let with_ten = PostgresPersistenceConfig {
            pool_size: Some(10),
            ..Default::default()
        };
        assert_eq!(with_ten.resolve_pool_sizes().unwrap(), (7, 1, 1));
        assert_legacy_budget_inclusive_of_listener(&with_ten);
    }

    #[test]
    fn postgres_pool_sizes_minimum_legacy_total() {
        let c = PostgresPersistenceConfig {
            pool_size: Some(4),
            ..Default::default()
        };
        assert_eq!(c.resolve_pool_sizes().unwrap(), (1, 1, 1));
        assert_legacy_budget_inclusive_of_listener(&c);
    }

    #[test]
    fn postgres_pool_sizes_rejects_legacy_total_below_four() {
        for n in [0, 1, 2, 3] {
            let c = PostgresPersistenceConfig {
                pool_size: Some(n),
                ..Default::default()
            };
            let err = c.resolve_pool_sizes().unwrap_err();
            assert!(err.contains("at least 4"), "poolSize {n}: {err}");
        }
    }

    #[test]
    fn postgres_pool_sizes_explicit_workloads() {
        let c = PostgresPersistenceConfig {
            query_pool_size: Some(12),
            coordination_pool_size: Some(3),
            admin_pool_size: Some(5),
            pool_size: Some(99),
            ..Default::default()
        };
        assert_eq!(c.resolve_pool_sizes().unwrap(), (12, 3, 5));
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
                assert!(conn.auto_migrate, "autoMigrate defaults to true");
            }
            _ => panic!("expected postgres"),
        }
    }

    #[test]
    fn persistence_postgres_auto_migrate_can_be_disabled() {
        let yaml = r#"
queryflux:
  persistence:
    type: postgres
    url: postgres://a:b@localhost:5432/db
    autoMigrate: false
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        match &cfg.queryflux.persistence {
            PersistenceConfig::Postgres { conn } => {
                assert!(!conn.auto_migrate);
            }
            _ => panic!("expected postgres"),
        }
    }

    #[test]
    fn postgres_persistence_default_auto_migrate_true() {
        assert!(PostgresPersistenceConfig::default().auto_migrate);
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

    // -----------------------------------------------------------------------
    // Cluster variant expansion
    // -----------------------------------------------------------------------

    #[test]
    fn expand_variants_basic_snowflake() {
        let base = serde_json::json!({
            "driver": "snowflake",
            "uri": "svc@myaccount/mydb",
            "dbKwargs": {}
        });
        let variants = vec![
            super::ClusterVariant {
                name: "analytics".into(),
                overrides: serde_json::json!({"warehouse": "ANALYTICS_WH"}),
                max_running_queries: None,
            },
            super::ClusterVariant {
                name: "etl".into(),
                overrides: serde_json::json!({"warehouse": "ETL_WH"}),
                max_running_queries: Some(5),
            },
        ];
        let result = super::expand_cluster_variants("sf", &base, "adbc", &variants, None, None)
            .expect("expansion should succeed");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].expanded_name, "sf::analytics");
        assert_eq!(result[1].expanded_name, "sf::etl");
        assert_eq!(result[1].max_running_queries, Some(5));
        // Verify dbKwargs injection.
        let db_kwargs = result[0].merged_config["dbKwargs"].as_object().unwrap();
        assert_eq!(
            db_kwargs["adbc.snowflake.sql.warehouse"].as_str(),
            Some("ANALYTICS_WH")
        );
    }

    #[test]
    fn expand_variants_databricks_http_path() {
        let base = serde_json::json!({
            "driver": "databricks",
            "uri": "https://adb-123.net",
            "dbKwargs": {"token": "abc"}
        });
        let variants = vec![super::ClusterVariant {
            name: "wh1".into(),
            overrides: serde_json::json!({"httpPath": "/sql/1.0/warehouses/xyz"}),
            max_running_queries: None,
        }];
        let result =
            super::expand_cluster_variants("db", &base, "adbc", &variants, None, None).unwrap();
        assert_eq!(result[0].expanded_name, "db::wh1");
        let kwargs = result[0].merged_config["dbKwargs"].as_object().unwrap();
        assert_eq!(
            kwargs["http_path"].as_str(),
            Some("/sql/1.0/warehouses/xyz")
        );
    }

    /// A non-object `base_config` (e.g. a corrupted JSONB row) must not panic
    /// the dbKwargs-injection unwrap: `deep_merge`'s non-object branch always
    /// replaces `merged` with `variant.overrides` wholesale, and the caller
    /// only reaches that unwrap when `variant.overrides.is_object()` already
    /// held — so `merged` is guaranteed to be an object either way.
    #[test]
    fn expand_variants_non_object_base_config_does_not_panic() {
        let base = serde_json::Value::String("not an object".into());
        let variants = vec![super::ClusterVariant {
            name: "wh1".into(),
            overrides: serde_json::json!({"warehouse": "WH1"}),
            max_running_queries: None,
        }];
        let result =
            super::expand_cluster_variants("sf", &base, "adbc", &variants, None, None).unwrap();
        assert_eq!(result[0].expanded_name, "sf::wh1");
        assert!(result[0].merged_config.is_object());
    }

    #[test]
    fn expand_variants_deep_merge() {
        let base = serde_json::json!({
            "driver": "snowflake",
            "dbKwargs": {"a": "1", "b": "2"},
            "nested": {"x": 1, "y": 2}
        });
        let variants = vec![super::ClusterVariant {
            name: "v1".into(),
            overrides: serde_json::json!({
                "dbKwargs": {"b": "replaced", "c": "new"},
                "nested": {"y": 99, "z": 3}
            }),
            max_running_queries: None,
        }];
        let result =
            super::expand_cluster_variants("base", &base, "adbc", &variants, None, None).unwrap();
        let cfg = &result[0].merged_config;
        let kwargs = cfg["dbKwargs"].as_object().unwrap();
        assert_eq!(kwargs["a"].as_str(), Some("1")); // preserved
        assert_eq!(kwargs["b"].as_str(), Some("replaced")); // overridden
        assert_eq!(kwargs["c"].as_str(), Some("new")); // added
        let nested = cfg["nested"].as_object().unwrap();
        assert_eq!(nested["x"].as_i64(), Some(1)); // preserved
        assert_eq!(nested["y"].as_i64(), Some(99)); // overridden
        assert_eq!(nested["z"].as_i64(), Some(3)); // added
    }

    #[test]
    fn expand_variants_rejects_empty_name() {
        let base = serde_json::json!({"driver": "snowflake"});
        let variants = vec![super::ClusterVariant {
            name: "".into(),
            overrides: serde_json::Value::Null,
            max_running_queries: None,
        }];
        let err = super::expand_cluster_variants("c", &base, "adbc", &variants, None, None)
            .expect_err("should fail");
        assert!(err.contains("cannot be empty"));
    }

    #[test]
    fn expand_variants_rejects_double_colon_in_name() {
        let base = serde_json::json!({"driver": "snowflake"});
        let variants = vec![super::ClusterVariant {
            name: "a::b".into(),
            overrides: serde_json::Value::Null,
            max_running_queries: None,
        }];
        let err = super::expand_cluster_variants("c", &base, "adbc", &variants, None, None)
            .expect_err("should fail");
        assert!(err.contains("must not contain '::'"));
    }

    #[test]
    fn expand_variants_rejects_duplicates() {
        let base = serde_json::json!({"driver": "snowflake"});
        let variants = vec![
            super::ClusterVariant {
                name: "dup".into(),
                overrides: serde_json::Value::Null,
                max_running_queries: None,
            },
            super::ClusterVariant {
                name: "dup".into(),
                overrides: serde_json::Value::Null,
                max_running_queries: None,
            },
        ];
        let err = super::expand_cluster_variants("c", &base, "adbc", &variants, None, None)
            .expect_err("should fail");
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn expand_variants_sub_resource_substitution() {
        let base = serde_json::json!({
            "driver": "snowflake",
            "dbKwargs": {}
        });
        let variants = vec![super::ClusterVariant {
            name: "wh1".into(),
            overrides: serde_json::json!({"warehouse": "MY_WH"}),
            max_running_queries: None,
        }];
        let hcq = "SHOW WAREHOUSES LIKE '{{sub_resource}}'";
        let rq = "SELECT count FROM wh WHERE name = '{{sub_resource}}'";
        let result =
            super::expand_cluster_variants("c", &base, "adbc", &variants, Some(hcq), Some(rq))
                .unwrap();
        assert_eq!(
            result[0].health_check_query.as_deref(),
            Some("SHOW WAREHOUSES LIKE 'MY_WH'")
        );
        assert_eq!(
            result[0].reconcile_query.as_deref(),
            Some("SELECT count FROM wh WHERE name = 'MY_WH'")
        );
    }

    #[test]
    fn variant_field_to_db_kwarg_mappings() {
        assert_eq!(
            super::variant_field_to_db_kwarg("snowflake", "warehouse"),
            Some("adbc.snowflake.sql.warehouse")
        );
        assert_eq!(
            super::variant_field_to_db_kwarg("snowflake", "role"),
            Some("adbc.snowflake.sql.role")
        );
        assert_eq!(
            super::variant_field_to_db_kwarg("databricks", "httpPath"),
            Some("http_path")
        );
        assert_eq!(
            super::variant_field_to_db_kwarg("bigquery", "project"),
            Some("project_id")
        );
        assert_eq!(
            super::variant_field_to_db_kwarg("redshift", "workgroup"),
            Some("workgroup")
        );
        assert_eq!(super::variant_field_to_db_kwarg("unknown", "foo"), None);
    }

    #[test]
    fn cluster_variant_serde_roundtrip() {
        let v = super::ClusterVariant {
            name: "analytics".into(),
            overrides: serde_json::json!({"warehouse": "WH1"}),
            max_running_queries: Some(10),
        };
        let json = serde_json::to_string(&v).unwrap();
        let v2: super::ClusterVariant = serde_json::from_str(&json).unwrap();
        assert_eq!(v2.name, "analytics");
        assert_eq!(v2.max_running_queries, Some(10));
        assert_eq!(v2.overrides["warehouse"].as_str(), Some("WH1"));
    }

    #[test]
    fn non_adbc_engine_no_dbkwargs_injection() {
        let base = serde_json::json!({"endpoint": "http://localhost:8080"});
        let variants = vec![super::ClusterVariant {
            name: "v1".into(),
            overrides: serde_json::json!({"workgroup": "primary"}),
            max_running_queries: None,
        }];
        let result =
            super::expand_cluster_variants("athena", &base, "athena", &variants, None, None)
                .unwrap();
        // Generic overrides are merged but dbKwargs injection is NOT applied.
        assert_eq!(
            result[0].merged_config["workgroup"].as_str(),
            Some("primary")
        );
        assert!(result[0].merged_config.get("dbKwargs").is_none());
    }

    #[test]
    fn athena_variants_substitute_sub_resource_from_workgroup() {
        // Athena has no `driver` field, so resolve_sub_resource must fall back
        // to engine_key — otherwise {{sub_resource}} in a custom probe query
        // would never get substituted for Athena workgroup variants.
        let base = serde_json::json!({"endpoint": "http://localhost:8080"});
        let variants = vec![super::ClusterVariant {
            name: "etl".into(),
            overrides: serde_json::json!({"workgroup": "etl-workgroup"}),
            max_running_queries: None,
        }];
        let hcq = "-- checking {{sub_resource}}";
        let result =
            super::expand_cluster_variants("athena", &base, "athena", &variants, Some(hcq), None)
                .unwrap();
        assert_eq!(
            result[0].health_check_query.as_deref(),
            Some("-- checking etl-workgroup")
        );
    }

    #[test]
    fn expand_variants_null_health_and_reconcile_queries() {
        let base = serde_json::json!({"driver": "snowflake", "uri": "acct/db"});
        let variants = vec![super::ClusterVariant {
            name: "wh1".into(),
            overrides: serde_json::json!({"warehouse": "WH1"}),
            max_running_queries: None,
        }];
        let result =
            super::expand_cluster_variants("sf", &base, "adbc", &variants, None, None).unwrap();
        assert_eq!(
            result[0].health_check_query.as_deref(),
            Some("SHOW WAREHOUSES LIKE 'WH1'")
        );
        assert_eq!(
            result[0].reconcile_query.as_deref(),
            Some("SHOW WAREHOUSES LIKE 'WH1'")
        );
    }

    #[test]
    fn default_reconcile_query_snowflake_and_bigquery() {
        let sf = serde_json::json!({
            "driver": "snowflake",
            "warehouse": "ANALYTICS_WH"
        });
        assert_eq!(
            super::default_reconcile_query("adbc", &sf).as_deref(),
            Some("SHOW WAREHOUSES LIKE 'ANALYTICS_WH'")
        );

        let bq = serde_json::json!({
            "driver": "bigquery",
            "project": "my-proj",
            "dbKwargs": { "location": "region-us" }
        });
        let sql = super::default_reconcile_query("adbc", &bq).unwrap();
        assert!(sql.contains("`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"));
        assert!(sql.contains("project_id = 'my-proj'"));
    }

    #[test]
    fn default_reconcile_query_databricks_is_none() {
        let cfg =
            serde_json::json!({ "driver": "databricks", "httpPath": "/sql/1.0/warehouses/x" });
        assert!(super::default_reconcile_query("adbc", &cfg).is_none());
    }

    #[test]
    fn default_health_check_query_does_not_backslash_escape_wildcards() {
        // Snowflake's `SHOW ... LIKE` clause has no ESCAPE support (confirmed
        // against Snowflake's docs — unlike the standard LIKE predicate used
        // in SELECT/WHERE). A backslash here would be taken literally, so
        // `SHOW WAREHOUSES LIKE 'ETL\_WH'` would match nothing at all rather
        // than the intended "ETL_WH" warehouse. `_`/`%` in a warehouse name
        // remain LIKE wildcards (can over-match, e.g. "ETL_WH" also matching
        // "ETLXWH") — a known, pre-existing limitation of SHOW ... LIKE, not
        // something we can escape our way out of.
        let cfg = serde_json::json!({ "driver": "snowflake", "warehouse": "ETL_WH" });
        assert_eq!(
            super::default_health_check_query("adbc", &cfg).as_deref(),
            Some("SHOW WAREHOUSES LIKE 'ETL_WH'")
        );
    }

    #[test]
    fn apply_default_probe_queries_fills_reconcile_only_when_unset() {
        let mut cfg = super::ClusterConfig {
            engine: Some(super::EngineConfig::Adbc),
            enabled: true,
            max_running_queries: None,
            pool_size: None,
            endpoint: None,
            database_path: None,
            region: None,
            s3_output_location: None,
            workgroup: None,
            catalog: None,
            max_wait_secs: None,
            max_result_buffer_bytes: None,
            driver: None,
            tls: None,
            auth: None,
            query_auth: None,
            variants: vec![],
            health_check_query: Some("CUSTOM HC".into()),
            reconcile_query: None,
        };
        let json = serde_json::json!({
            "driver": "redshift"
        });
        super::apply_default_probe_queries(&mut cfg, "adbc", &json);
        assert_eq!(cfg.health_check_query.as_deref(), Some("CUSTOM HC"));
        assert_eq!(
            cfg.reconcile_query.as_deref(),
            Some("SELECT COUNT(*) FROM stv_recents WHERE status = 'Running'")
        );
    }

    #[test]
    fn expand_variants_bigquery_project_substitution() {
        let base = serde_json::json!({"driver": "bigquery"});
        let variants = vec![
            super::ClusterVariant {
                name: "p1".into(),
                overrides: serde_json::json!({"project": "proj-a"}),
                max_running_queries: None,
            },
            super::ClusterVariant {
                name: "p2".into(),
                overrides: serde_json::json!({"project": "proj-b"}),
                max_running_queries: Some(3),
            },
        ];
        let hcq = "SELECT 1 -- {{sub_resource}}";
        let result =
            super::expand_cluster_variants("bq", &base, "adbc", &variants, Some(hcq), None)
                .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].health_check_query.as_deref(),
            Some("SELECT 1 -- proj-a")
        );
        assert_eq!(
            result[1].health_check_query.as_deref(),
            Some("SELECT 1 -- proj-b")
        );
        assert_eq!(result[1].max_running_queries, Some(3));
        assert_eq!(
            result[0].merged_config["dbKwargs"]["project_id"].as_str(),
            Some("proj-a")
        );
    }

    #[test]
    fn expand_variants_without_sub_resource_leaves_placeholder() {
        let base = serde_json::json!({"driver": "redshift"});
        let variants = vec![super::ClusterVariant {
            name: "main".into(),
            overrides: serde_json::json!({}),
            max_running_queries: None,
        }];
        let hcq = "SELECT 1 WHERE x = '{{sub_resource}}'";
        let result =
            super::expand_cluster_variants("rs", &base, "adbc", &variants, Some(hcq), Some(hcq))
                .unwrap();
        assert_eq!(
            result[0].health_check_query.as_deref(),
            Some("SELECT 1 WHERE x = '{{sub_resource}}'")
        );
    }

    #[test]
    fn auth_required_none_provider_fails_validation() {
        let auth = AuthConfig {
            required: true,
            provider: AuthProviderConfig::None,
            ..Default::default()
        };
        assert!(auth.validate_for_required().is_err());
    }

    #[test]
    fn auth_required_oidc_needs_audience() {
        let auth = AuthConfig {
            required: true,
            provider: AuthProviderConfig::Oidc,
            oidc: Some(OidcConfig {
                issuer: "https://idp.example".into(),
                jwks_uri: "https://idp.example/jwks".into(),
                audience: None,
                groups_claim: "groups".into(),
                roles_claim: None,
            }),
            ..Default::default()
        };
        let err = auth.validate_for_required().unwrap_err();
        assert!(err.contains("audience"), "{err}");
    }

    #[test]
    fn auth_required_oidc_ok_with_audience() {
        let auth = AuthConfig {
            required: true,
            provider: AuthProviderConfig::Oidc,
            oidc: Some(OidcConfig {
                issuer: "https://idp.example".into(),
                jwks_uri: "https://idp.example/jwks".into(),
                audience: Some("queryflux".into()),
                groups_claim: "groups".into(),
                roles_claim: None,
            }),
            ..Default::default()
        };
        assert!(auth.validate_for_required().is_ok());
    }

    #[test]
    fn auth_not_required_skips_provider_checks() {
        let auth = AuthConfig {
            required: false,
            provider: AuthProviderConfig::Oidc,
            oidc: None,
            ..Default::default()
        };
        assert!(auth.validate_for_required().is_ok());
    }

    #[test]
    fn auth_required_ldap_user_dn_template_needs_placeholder() {
        let auth = AuthConfig {
            required: true,
            provider: AuthProviderConfig::Ldap,
            ldap: Some(LdapConfig {
                url: "ldap://ldap.internal:389".into(),
                bind_dn: String::new(),
                bind_password: None,
                user_search_base: String::new(),
                user_search_filter: "(uid={})".into(),
                user_dn_template: Some("cn=admin,ou=users,dc=example,dc=com".into()),
                group_search_base: None,
                group_name_attribute: "cn".into(),
            }),
            ..Default::default()
        };
        let err = auth.validate_for_required().unwrap_err();
        assert!(err.contains("userDnTemplate"), "{err}");
    }

    #[test]
    fn auth_required_ldap_search_mode_needs_filter() {
        let auth = AuthConfig {
            required: true,
            provider: AuthProviderConfig::Ldap,
            ldap: Some(LdapConfig {
                url: "ldap://ldap.internal:389".into(),
                bind_dn: String::new(),
                bind_password: None,
                user_search_base: "ou=users,dc=example,dc=com".into(),
                user_search_filter: String::new(),
                user_dn_template: None,
                group_search_base: None,
                group_name_attribute: "cn".into(),
            }),
            ..Default::default()
        };
        let err = auth.validate_for_required().unwrap_err();
        assert!(err.contains("userSearchFilter"), "{err}");
    }

    #[test]
    fn startup_rejects_required_auth_none_provider() {
        let yaml = r#"
queryflux: {}
auth:
  required: true
  provider: none
"#;
        let cfg: ProxyConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate_startup_security().unwrap_err();
        assert!(err.contains("auth.required"), "{err}");
    }

    #[test]
    fn openfga_provider_requires_url_and_store() {
        let authz = AuthorizationConfig {
            provider: AuthorizationProviderConfig::OpenFga,
            openfga: Some(OpenFgaConfig {
                url: "".into(),
                store_id: "store".into(),
                credentials: None,
            }),
        };
        assert!(authz.validate().is_err());
    }
}

/// Regression coverage for `CatalogProviderConfig` YAML round-tripping.
///
/// This enum's variants went untested against real YAML for a long time (it was
/// declared but never wired to a live provider), and that let two real bugs slip
/// in unnoticed: `#[serde(flatten)]` on a nested internally-tagged enum requiring
/// two `type` keys in one mapping, and `rename_all = "camelCase"` on the
/// container not reaching multi-word field names inside this enum's variants
/// (confirmed empirically against this serde/serde_yaml version combination —
/// every such field now has an explicit `#[serde(rename = "...")]` instead).
/// These tests exist so both classes of bug can't silently reappear.
#[cfg(test)]
mod catalog_provider_config_tests {
    use super::{CatalogProviderConfig, ClusterAuth};

    #[test]
    fn null_is_the_default_and_parses_from_yaml() {
        assert!(matches!(
            CatalogProviderConfig::default(),
            CatalogProviderConfig::Null
        ));
        let cfg: CatalogProviderConfig = serde_yaml::from_str("type: null\n").unwrap();
        assert!(matches!(cfg, CatalogProviderConfig::Null));
    }

    #[test]
    fn engine_delegate_cluster_group_uses_camel_case_key() {
        let cfg: CatalogProviderConfig =
            serde_yaml::from_str("type: engineDelegate\nclusterGroup: trino-default\n").unwrap();
        match cfg {
            CatalogProviderConfig::EngineDelegate {
                cluster_group,
                cache,
            } => {
                assert_eq!(cluster_group, "trino-default");
                assert!(cache.is_none());
            }
            other => panic!("expected EngineDelegate, got {other:?}"),
        }
    }

    #[test]
    fn fallback_primary_and_secondary_nest_independently() {
        // "null" must be quoted here — a bare YAML `null` parses as YAML's null
        // type, not the string the `type` tag needs to match the Null variant.
        let yaml = r#"
type: fallback
primary:
  type: glue
  region: us-east-1
secondary:
  type: "null"
"#;
        let cfg: CatalogProviderConfig = serde_yaml::from_str(yaml).unwrap();
        match cfg {
            CatalogProviderConfig::Fallback { primary, secondary } => {
                assert!(matches!(*primary, CatalogProviderConfig::Glue { .. }));
                assert!(matches!(*secondary, CatalogProviderConfig::Null));
            }
            other => panic!("expected Fallback, got {other:?}"),
        }
    }

    #[test]
    fn glue_and_hive_metastore_parse_with_no_cache_by_default() {
        let glue: CatalogProviderConfig =
            serde_yaml::from_str("type: glue\nregion: us-east-1\n").unwrap();
        assert!(matches!(
            glue,
            CatalogProviderConfig::Glue {
                region: Some(ref r),
                auth: None,
                cache: None,
            } if r == "us-east-1"
        ));

        let hms: CatalogProviderConfig =
            serde_yaml::from_str("type: hiveMetastore\nuri: thrift://localhost:9083\n").unwrap();
        assert!(matches!(
            hms,
            CatalogProviderConfig::HiveMetastore { cache: None, .. }
        ));
    }

    #[test]
    fn glue_role_arn_auth_and_cache_parse_together() {
        let yaml = r#"
type: glue
region: us-east-1
auth:
  type: roleArn
  roleArn: arn:aws:iam::123456789012:role/queryflux-glue-readonly
  externalId: queryflux-ext-id
cache:
  ttlSeconds: 600
  maxEntries: 5000
"#;
        let glue: CatalogProviderConfig = serde_yaml::from_str(yaml).unwrap();
        match glue {
            CatalogProviderConfig::Glue {
                region: Some(region),
                auth:
                    Some(ClusterAuth::RoleArn {
                        role_arn,
                        external_id,
                    }),
                cache: Some(cache),
            } => {
                assert_eq!(region, "us-east-1");
                assert_eq!(
                    role_arn,
                    "arn:aws:iam::123456789012:role/queryflux-glue-readonly"
                );
                assert_eq!(external_id.as_deref(), Some("queryflux-ext-id"));
                assert_eq!(cache.ttl_seconds, 600);
                assert_eq!(cache.max_entries, 5000);
            }
            other => panic!("expected Glue with RoleArn auth + cache, got {other:?}"),
        }
    }

    #[test]
    fn cache_ttl_and_max_entries_default_when_omitted() {
        let yaml = "type: glue\nregion: us-east-1\ncache: {}\n";
        let glue: CatalogProviderConfig = serde_yaml::from_str(yaml).unwrap();
        match glue {
            CatalogProviderConfig::Glue {
                cache: Some(cache), ..
            } => {
                assert_eq!(cache.ttl_seconds, 300);
                assert_eq!(cache.max_entries, 10_000);
            }
            other => panic!("expected Glue with defaulted cache, got {other:?}"),
        }
    }
}
