use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// The name every cluster group resolves to when it (and no `groups.<name>` override) names
/// a connection explicitly. Always required in `connections`.
pub const DEFAULT_CONNECTION: &str = "default";

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
fn default_operations() -> Vec<String> {
    vec!["table.select".to_string()]
}
fn default_cache_ttl_ms() -> u64 {
    5_000
}
fn default_cache_capacity() -> usize {
    10_000
}
fn default_enabled() -> bool {
    true
}

impl Default for OpaProviderConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8181".to_string(),
            decision_path: default_decision_path(),
            timeout_ms: default_timeout_ms(),
            bearer_token: None,
            client_credentials: None,
        }
    }
}

/// One named policy-provider connection. Every cluster group resolves to exactly one
/// connection — its own `groups.<name>.connection`, or `"default"` when unset — so a
/// segmented deployment (a sandboxed VPC, a different compliance boundary, a per-team OPA
/// during a provider migration) doesn't have to share one HTTP endpoint, cache, or
/// fail-open policy across every group. Most deployments need only `"default"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessConnectionConfig {
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
    /// Fail-open default for groups on this connection (provider timeout/error → allow
    /// instead of deny). A group using this connection can still override via
    /// `groups.<name>.failOpen`.
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
}

impl Default for AccessConnectionConfig {
    fn default() -> Self {
        Self {
            provider: ProviderKind::Opa,
            opa: Some(OpaProviderConfig::default()),
            operations: default_operations(),
            on_missing_schema: OnMissingSchema::Evaluate,
            fail_open: false,
            cache_ttl_ms: default_cache_ttl_ms(),
            cache_capacity: default_cache_capacity(),
            session_param_keys: Vec::new(),
        }
    }
}

impl AccessConnectionConfig {
    /// The active provider's OPA config. `Err` if `provider: opa` (the default) but no
    /// `opa:` block was given.
    pub fn opa_config(&self) -> Result<&OpaProviderConfig, String> {
        match self.provider {
            ProviderKind::Opa => self
                .opa
                .as_ref()
                .ok_or_else(|| "provider is \"opa\" but no opa: block was given".to_string()),
        }
    }

    pub fn validate(&self, name: &str) -> Result<(), String> {
        let opa = self
            .opa_config()
            .map_err(|e| format!("accessControl.connections.{name}: {e}"))?;
        let parsed = url::Url::parse(opa.url.trim()).map_err(|e| {
            format!("accessControl.connections.{name}.opa.url is not a valid URL: {e}")
        })?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Err(format!(
                    "accessControl.connections.{name}.opa.url must be http or https, got {other}"
                ))
            }
        }
        if opa.decision_path.trim().is_empty() {
            return Err(format!(
                "accessControl.connections.{name}.opa.decisionPath must not be empty"
            ));
        }
        if !opa.decision_path.starts_with('/') {
            return Err(format!(
                "accessControl.connections.{name}.opa.decisionPath must start with '/'"
            ));
        }
        for op in &self.operations {
            if !op.contains('.') {
                return Err(format!(
                    "accessControl.connections.{name}.operations entry {op:?} must be namespaced (e.g. table.select)"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GroupOverride {
    /// When set, overrides the global [`AccessControlConfig::enabled`] default for this group.
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub fail_open: Option<bool>,
    /// Named entry in `accessControl.connections` this group uses. Omit to use
    /// `"default"`. Must reference a key present in `connections`.
    #[serde(default)]
    pub connection: Option<String>,
}

/// Top-level `accessControl:` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessControlConfig {
    /// Default for cluster groups without an explicit `groups.<name>.enabled` override.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Named policy-provider connections. Must include a `"default"` entry — every group
    /// without an explicit `groups.<name>.connection` override resolves to it. Most
    /// deployments define only `"default"`; add more only when a group genuinely needs a
    /// different endpoint (network segmentation, blast-radius isolation, migration).
    #[serde(default = "default_connections")]
    pub connections: HashMap<String, AccessConnectionConfig>,
    /// Per-cluster-group overrides: on/off, fail-open, and which named connection to use.
    #[serde(default)]
    pub groups: HashMap<String, GroupOverride>,
}

fn default_connections() -> HashMap<String, AccessConnectionConfig> {
    HashMap::from([(
        DEFAULT_CONNECTION.to_string(),
        AccessConnectionConfig::default(),
    )])
}

impl Default for AccessControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            connections: default_connections(),
            groups: HashMap::new(),
        }
    }
}

impl AccessControlConfig {
    /// Parse a Studio / Admin API blob.
    ///
    /// JSON `null`, or a bare `{ "enabled": false }` with no `connections`, turns access
    /// control off entirely. Any other object is deserialized as [`AccessControlConfig`]
    /// and validated. `enabled: false` **with** a `connections` block is kept loaded (not
    /// disabled) so per-group `groups.<name>.enabled: true` can still opt in.
    pub fn from_admin_value(v: &serde_json::Value) -> Result<Option<Self>, String> {
        if v.is_null() {
            return Ok(None);
        }
        let has_connections = v
            .get("connections")
            .and_then(|c| c.as_object())
            .is_some_and(|o| !o.is_empty());
        if v.get("enabled").and_then(|e| e.as_bool()) == Some(false) && !has_connections {
            return Ok(None);
        }
        let mut obj = v.clone();
        if let Some(conns) = obj
            .get_mut("connections")
            .and_then(|c| c.as_object_mut())
        {
            for conn in conns.values_mut() {
                let Some(opa) = conn.get_mut("opa").and_then(|o| o.as_object_mut()) else {
                    continue;
                };
                opa.remove("bearerTokenSet");
                if let Some(cc) = opa
                    .get_mut("clientCredentials")
                    .and_then(|c| c.as_object_mut())
                {
                    cc.remove("clientSecretSet");
                }
            }
        }
        let cfg: Self = serde_json::from_value(obj)
            .map_err(|e| format!("invalid accessControl config: {e}"))?;
        cfg.validate()?;
        Ok(Some(cfg))
    }

    /// Whether access control runs for queries routed to `group`.
    pub fn enabled_for_group(&self, group: &str) -> bool {
        self.groups
            .get(group)
            .and_then(|g| g.enabled)
            .unwrap_or(self.enabled)
    }

    /// The named connection `group` resolves to (its own override, else `"default"`).
    pub fn connection_name_for_group<'a>(&'a self, group: &str) -> &'a str {
        self.groups
            .get(group)
            .and_then(|g| g.connection.as_deref())
            .unwrap_or(DEFAULT_CONNECTION)
    }

    /// The resolved connection config for `group`. `None` only if config validation was
    /// skipped — `validate()` guarantees every group's resolved name exists.
    pub fn connection_for_group(&self, group: &str) -> Option<&AccessConnectionConfig> {
        self.connections.get(self.connection_name_for_group(group))
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.connections.contains_key(DEFAULT_CONNECTION) {
            return Err(
                "accessControl.connections must include a \"default\" entry".to_string(),
            );
        }
        for (name, conn) in &self.connections {
            conn.validate(name)?;
        }
        for (group, ov) in &self.groups {
            if let Some(conn_name) = &ov.connection {
                if !self.connections.contains_key(conn_name) {
                    return Err(format!(
                        "accessControl.groups.{group}.connection {conn_name:?} is not defined under accessControl.connections"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn from_admin_value_disabled() {
        assert!(
            AccessControlConfig::from_admin_value(&json!({ "enabled": false }))
                .unwrap()
                .is_none()
        );
        assert!(AccessControlConfig::from_admin_value(&json!(null))
            .unwrap()
            .is_none());
    }

    #[test]
    fn from_admin_value_enabled_opa() {
        let v = json!({
            "enabled": true,
            "connections": {
                "default": {
                    "provider": "opa",
                    "opa": { "url": "http://opa:8181", "decisionPath": "/v1/data/queryflux/access" },
                    "sessionParamKeys": ["customer"]
                }
            }
        });
        let cfg = AccessControlConfig::from_admin_value(&v).unwrap().unwrap();
        let default = &cfg.connections[DEFAULT_CONNECTION];
        assert_eq!(default.opa.as_ref().unwrap().url, "http://opa:8181");
        assert_eq!(default.session_param_keys, vec!["customer"]);
    }

    #[test]
    fn from_admin_value_rejects_bad_url() {
        let v = json!({
            "connections": { "default": { "opa": { "url": "not-a-url" } } }
        });
        assert!(AccessControlConfig::from_admin_value(&v).is_err());
    }

    #[test]
    fn from_admin_value_missing_default_connection_rejected() {
        let v = json!({
            "connections": {
                "eu": { "opa": { "url": "http://opa:8181" } }
            }
        });
        assert!(AccessControlConfig::from_admin_value(&v).is_err());
    }

    #[test]
    fn from_admin_value_global_off_with_connections_keeps_config() {
        let v = json!({
            "enabled": false,
            "connections": {
                "default": { "opa": { "url": "http://opa:8181", "decisionPath": "/v1/data/queryflux/access" } }
            },
            "groups": { "analytics": { "enabled": true } }
        });
        let cfg = AccessControlConfig::from_admin_value(&v).unwrap().unwrap();
        assert!(!cfg.enabled);
        assert!(cfg.enabled_for_group("analytics"));
        assert!(!cfg.enabled_for_group("sandbox"));
    }

    #[test]
    fn enabled_for_group_inherit_and_override() {
        let mut cfg = AccessControlConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(cfg.enabled_for_group("any"));
        cfg.enabled = false;
        assert!(!cfg.enabled_for_group("any"));
        cfg.groups.insert(
            "analytics".into(),
            GroupOverride {
                enabled: Some(true),
                fail_open: None,
                connection: None,
            },
        );
        cfg.groups.insert(
            "sandbox".into(),
            GroupOverride {
                enabled: Some(false),
                fail_open: None,
                connection: None,
            },
        );
        assert!(cfg.enabled_for_group("analytics"));
        assert!(!cfg.enabled_for_group("sandbox"));
        assert!(!cfg.enabled_for_group("other"));
    }

    #[test]
    fn connection_resolution_defaults_and_overrides() {
        let mut cfg = AccessControlConfig::default();
        cfg.connections.insert(
            "eu".to_string(),
            AccessConnectionConfig {
                opa: Some(OpaProviderConfig {
                    url: "https://eu-opa.internal".to_string(),
                    ..OpaProviderConfig::default()
                }),
                ..AccessConnectionConfig::default()
            },
        );
        cfg.groups.insert(
            "eu-group".to_string(),
            GroupOverride {
                enabled: None,
                fail_open: None,
                connection: Some("eu".to_string()),
            },
        );
        assert_eq!(cfg.connection_name_for_group("eu-group"), "eu");
        assert_eq!(cfg.connection_name_for_group("trino-prod"), DEFAULT_CONNECTION);
        assert_eq!(
            cfg.connection_for_group("eu-group").unwrap().opa.as_ref().unwrap().url,
            "https://eu-opa.internal"
        );
        cfg.validate().expect("valid config with two connections");
    }

    #[test]
    fn validate_rejects_group_referencing_unknown_connection() {
        let mut cfg = AccessControlConfig::default();
        cfg.groups.insert(
            "eu-group".to_string(),
            GroupOverride {
                enabled: None,
                fail_open: None,
                connection: Some("does-not-exist".to_string()),
            },
        );
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("does-not-exist"), "unexpected error: {err}");
    }
}
