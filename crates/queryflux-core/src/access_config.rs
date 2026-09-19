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

/// The policy provider selection. A simple discriminator + one config field per provider —
/// same pattern as `auth.provider` (`AuthProviderConfig`) + `auth.oidc`. OPA is the only
/// provider in v1; a second provider adds a variant here and a sibling config field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ProviderKind {
    #[default]
    Opa,
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
/// Operations the guard classifies and can evaluate.
pub const SUPPORTED_OPERATIONS: &[&str] = &[
    "table.select",
    "table.insert",
    "table.update",
    "table.delete",
];

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

/// Top-level `accessControl:` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessControlConfig {
    #[serde(default)]
    pub provider: ProviderKind,
    /// Required when `provider: opa` (the only provider today, and the default).
    #[serde(default)]
    pub opa: Option<OpaProviderConfig>,
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
    /// The active provider's OPA config. `Err` if `provider: opa` (the default) but no
    /// `opa:` block was given.
    pub fn opa_config(&self) -> Result<&OpaProviderConfig, String> {
        match self.provider {
            ProviderKind::Opa => self.opa.as_ref().ok_or_else(|| {
                "access_control.provider is \"opa\" but no opa: block was given".to_string()
            }),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let opa = self.opa_config()?;
        let parsed = url::Url::parse(opa.url.trim())
            .map_err(|e| format!("access_control.opa.url is not a valid URL: {e}"))?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Err(format!(
                    "access_control.opa.url must be http or https, got {other}"
                ))
            }
        }
        if opa.decision_path.trim().is_empty() {
            return Err("access_control.opa.decisionPath must not be empty".into());
        }
        if !opa.decision_path.starts_with('/') {
            return Err("access_control.opa.decisionPath must start with '/'".into());
        }
        // The client secret is POSTed to the token endpoint, so it must not travel in clear text.
        // A loopback host never leaves the machine, which keeps local demos workable.
        if let Some(credentials) = &opa.client_credentials {
            let token_endpoint = url::Url::parse(credentials.token_endpoint.trim()).map_err(|e| {
                format!("access_control.opa.clientCredentials.tokenEndpoint is not a valid URL: {e}")
            })?;
            let loopback = match token_endpoint.host() {
                Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                None => false,
            };
            let secure = token_endpoint.scheme() == "https"
                || (token_endpoint.scheme() == "http" && loopback);
            if !secure {
                return Err(
                    "access_control.opa.clientCredentials.tokenEndpoint must use https \
                     (plain http is only allowed for a loopback host)"
                        .into(),
                );
            }
        }
        // `operations` is an allowlist: an entry that names no real operation (say
        // `table.selet`) would silently skip that protection, so unknown names are rejected.
        for op in &self.operations {
            if !SUPPORTED_OPERATIONS.contains(&op.as_str()) {
                return Err(format!(
                    "access_control.operations entry {op:?} is not supported (supported: {})",
                    SUPPORTED_OPERATIONS.join(", ")
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(token_endpoint: Option<&str>, operations: &[&str]) -> AccessControlConfig {
        let mut opa = serde_json::json!({ "url": "https://opa.example.com" });
        if let Some(t) = token_endpoint {
            opa["clientCredentials"] = serde_json::json!({
                "clientId": "id", "clientSecret": "secret", "tokenEndpoint": t
            });
        }
        serde_json::from_value(serde_json::json!({ "opa": opa, "operations": operations }))
            .expect("valid access-control config")
    }

    /// The client secret is POSTed to the token endpoint, so it must travel over TLS. A
    /// loopback host never leaves the machine and stays allowed for local setups.
    #[test]
    fn token_endpoint_must_be_https_unless_loopback() {
        let ok = |t: &str| config_with(Some(t), &["table.select"]).validate();
        for allowed in [
            "https://idp.example.com/oauth/token",
            "http://localhost/token",
            "http://LOCALHOST:8080/token",
            "http://127.0.0.1:8080/token",
            "http://[::1]:8080/token",
        ] {
            assert!(ok(allowed).is_ok(), "{allowed} should be accepted");
        }
        for rejected in [
            "http://idp.example.com/oauth/token",
            "http://10.0.0.5/token",
            "http://localhost.evil.example/token",
            "ftp://idp.example.com/token",
            "not a url",
        ] {
            let err = ok(rejected).unwrap_err();
            assert!(err.contains("tokenEndpoint"), "{rejected}: {err}");
        }
        // No client credentials → nothing to check.
        assert!(config_with(None, &["table.select"]).validate().is_ok());
    }

    /// `operations` is an allowlist: a typo would silently skip the protection it meant to
    /// apply, so unknown names — including the classifier's internal `statement.other` — fail.
    #[test]
    fn operations_must_be_supported_names() {
        for ok in [
            &["table.select"][..],
            &["table.select", "table.delete"],
            &[],
        ] {
            assert!(config_with(None, ok).validate().is_ok(), "{ok:?}");
        }
        for bad in ["table.selet", "statement.other", "select", "table.merge"] {
            let err = config_with(None, &[bad]).validate().unwrap_err();
            assert!(
                err.contains(bad) && err.contains("table.select"),
                "{bad}: {err}"
            );
        }
    }
}
