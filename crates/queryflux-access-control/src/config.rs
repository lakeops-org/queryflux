use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// What to do when the query's referenced table columns can't be resolved from the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum OnMissingSchema {
    /// Still call the provider with "all columns" and let policy decide (default).
    #[default]
    Evaluate,
    /// Deny the query outright.
    Deny,
}

/// The policy provider selection. Tagged enum, same shape as `auth.provider`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProviderConfig {
    Opa(OpaProviderConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpaProviderConfig {
    /// Base URL of the OPA server, e.g. `http://localhost:8181`.
    pub url: String,
    /// Decision document path, e.g. `/v1/data/queryflux/access`.
    #[serde(default = "default_decision_path")]
    pub decision_path: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Static bearer token for the OPA server, if it requires one.
    #[serde(default)]
    pub bearer_token: Option<String>,
    /// OAuth2 client-credentials for the OPA server, if it requires a fetched token.
    #[serde(default)]
    pub client_credentials: Option<ClientCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: String,
    pub token_endpoint: String,
}

fn default_decision_path() -> String {
    "/v1/data/queryflux/access".to_string()
}
fn default_timeout_ms() -> u64 {
    1_000
}
fn default_operations() -> Vec<String> {
    vec!["table.select".to_string()]
}
fn default_cache_ttl_ms() -> u64 {
    5_000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GroupOverride {
    #[serde(default)]
    pub fail_open: Option<bool>,
}

/// Top-level `access_control:` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessControlConfig {
    pub provider: ProviderConfig,
    /// Namespaced operations to evaluate. Others skip the stage entirely.
    #[serde(default = "default_operations")]
    pub operations: Vec<String>,
    #[serde(default)]
    pub on_missing_schema: OnMissingSchema,
    /// Global fail-open default (provider error → allow instead of deny).
    #[serde(default)]
    pub fail_open: bool,
    /// Decision cache TTL. `0` disables the cache.
    #[serde(default = "default_cache_ttl_ms")]
    pub cache_ttl_ms: u64,
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,
    /// `SessionContext.extra` keys forwarded to the provider as `context.sessionParams`.
    #[serde(default)]
    pub session_param_keys: Vec<String>,
    /// Per-cluster-group overrides.
    #[serde(default)]
    pub groups: HashMap<String, GroupOverride>,
}

fn default_cache_capacity() -> usize {
    10_000
}

impl AccessControlConfig {
    pub fn validate(&self) -> Result<(), String> {
        let ProviderConfig::Opa(opa) = &self.provider;
        let parsed = url::Url::parse(opa.url.trim())
            .map_err(|e| format!("access_control.provider.opa.url is not a valid URL: {e}"))?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Err(format!(
                    "access_control.provider.opa.url must be http or https, got {other}"
                ))
            }
        }
        if opa.decision_path.trim().is_empty() {
            return Err("access_control.provider.opa.decisionPath must not be empty".into());
        }
        if !opa.decision_path.starts_with('/') {
            return Err("access_control.provider.opa.decisionPath must start with '/'".into());
        }
        for op in &self.operations {
            if !op.contains('.') {
                return Err(format!(
                    "access_control.operations entry {op:?} must be namespaced (e.g. table.select)"
                ));
            }
        }
        Ok(())
    }
}
