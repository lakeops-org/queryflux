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
/// same pattern as `auth.provider` (`AuthProviderConfig`) + `auth.oidc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ProviderKind {
    #[default]
    Opa,
    Cerbos,
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

/// Cerbos PDP connection. Row filters / column masks arrive as policy `outputs` — see
/// `queryflux_access_control::providers::cerbos::wire` for the exact `{"kind": ...}`
/// contract policy authors must emit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CerbosProviderConfig {
    /// Base URL of the Cerbos PDP's HTTP API, e.g. `http://localhost:3592`.
    pub url: String,
    /// `POST` path for the check-resources call.
    #[serde(default = "default_check_resources_path")]
    pub check_resources_path: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Static bearer/API token, for Cerbos Hub/Cloud or a PDP fronted by one. Self-hosted
    /// Cerbos typically needs none (secured by network policy instead).
    #[serde(default)]
    pub bearer_token: Option<String>,
}

impl Default for CerbosProviderConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:3592".to_string(),
            check_resources_path: default_check_resources_path(),
            timeout_ms: default_timeout_ms(),
            bearer_token: None,
        }
    }
}

fn default_check_resources_path() -> String {
    "/api/check/resources".to_string()
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

/// One named policy-provider connection. A cluster group resolves to a connection via its
/// own `groups.<name>.connection`, or the top-level `defaultConnection` when unset — there
/// is no reserved connection name. A segmented deployment (a sandboxed VPC, a different
/// compliance boundary, a per-team OPA during a provider migration) adds another named entry
/// instead of sharing one HTTP endpoint, cache, or fail-open policy across every group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessConnectionConfig {
    #[serde(default)]
    pub provider: ProviderKind,
    /// Required when `provider: opa` (the default).
    #[serde(default)]
    pub opa: Option<OpaProviderConfig>,
    /// Required when `provider: cerbos`.
    #[serde(default)]
    pub cerbos: Option<CerbosProviderConfig>,
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
            cerbos: None,
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
    /// The active provider's OPA config. `Err` if `provider: opa` but no `opa:` block was
    /// given, or if a different provider is configured.
    pub fn opa_config(&self) -> Result<&OpaProviderConfig, String> {
        match self.provider {
            ProviderKind::Opa => self
                .opa
                .as_ref()
                .ok_or_else(|| "provider is \"opa\" but no opa: block was given".to_string()),
            ProviderKind::Cerbos => Err("provider is \"cerbos\", not \"opa\"".to_string()),
        }
    }

    /// The active provider's Cerbos config. `Err` if `provider: cerbos` but no `cerbos:`
    /// block was given, or if a different provider is configured.
    pub fn cerbos_config(&self) -> Result<&CerbosProviderConfig, String> {
        match self.provider {
            ProviderKind::Cerbos => self
                .cerbos
                .as_ref()
                .ok_or_else(|| "provider is \"cerbos\" but no cerbos: block was given".to_string()),
            ProviderKind::Opa => Err("provider is \"opa\", not \"cerbos\"".to_string()),
        }
    }

    pub fn validate(&self, name: &str) -> Result<(), String> {
        match self.provider {
            ProviderKind::Opa => {
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
            }
            ProviderKind::Cerbos => {
                let cerbos = self
                    .cerbos_config()
                    .map_err(|e| format!("accessControl.connections.{name}: {e}"))?;
                let parsed = url::Url::parse(cerbos.url.trim()).map_err(|e| {
                    format!("accessControl.connections.{name}.cerbos.url is not a valid URL: {e}")
                })?;
                match parsed.scheme() {
                    "http" | "https" => {}
                    other => {
                        return Err(format!(
                            "accessControl.connections.{name}.cerbos.url must be http or https, got {other}"
                        ))
                    }
                }
                if cerbos.check_resources_path.trim().is_empty() {
                    return Err(format!(
                        "accessControl.connections.{name}.cerbos.checkResourcesPath must not be empty"
                    ));
                }
                if !cerbos.check_resources_path.starts_with('/') {
                    return Err(format!(
                        "accessControl.connections.{name}.cerbos.checkResourcesPath must start with '/'"
                    ));
                }
            }
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
    /// Named entry in `accessControl.connections` this group uses. Omit to inherit
    /// [`AccessControlConfig::default_connection`]. Must reference a key present in
    /// `connections` when set.
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
    /// Named connection groups resolve to when they have no explicit
    /// `groups.<name>.connection` override. `None` means such groups get **no** access
    /// control at all, regardless of `enabled` — there is nothing to route them to. Must
    /// reference a key present in `connections` when set.
    #[serde(default)]
    pub default_connection: Option<String>,
    /// Named policy-provider connections. No name is reserved or required — a group only
    /// gets access control when it resolves to one, via its own `groups.<name>.connection`
    /// or `defaultConnection`.
    #[serde(default)]
    pub connections: HashMap<String, AccessConnectionConfig>,
    /// Per-cluster-group overrides: on/off, fail-open, and which named connection to use.
    #[serde(default)]
    pub groups: HashMap<String, GroupOverride>,
}

impl Default for AccessControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_connection: None,
            connections: HashMap::new(),
            groups: HashMap::new(),
        }
    }
}

impl AccessControlConfig {
    /// Parse a Studio / Admin API blob.
    ///
    /// JSON `null`, or any body with no (or an empty) `connections` map, turns access
    /// control off entirely — with nothing under `connections`, no group could ever resolve
    /// a connection anyway. Any other object is deserialized as [`AccessControlConfig`] and
    /// validated.
    pub fn from_admin_value(v: &serde_json::Value) -> Result<Option<Self>, String> {
        if v.is_null() {
            return Ok(None);
        }
        let has_connections = v
            .get("connections")
            .and_then(|c| c.as_object())
            .is_some_and(|o| !o.is_empty());
        if !has_connections {
            return Ok(None);
        }
        let mut obj = v.clone();
        if let Some(conns) = obj.get_mut("connections").and_then(|c| c.as_object_mut()) {
            for conn in conns.values_mut() {
                if let Some(opa) = conn.get_mut("opa").and_then(|o| o.as_object_mut()) {
                    opa.remove("bearerTokenSet");
                    if let Some(cc) = opa
                        .get_mut("clientCredentials")
                        .and_then(|c| c.as_object_mut())
                    {
                        cc.remove("clientSecretSet");
                    }
                }
                if let Some(cerbos) = conn.get_mut("cerbos").and_then(|c| c.as_object_mut()) {
                    cerbos.remove("bearerTokenSet");
                }
            }
        }
        let cfg: Self = serde_json::from_value(obj)
            .map_err(|e| format!("invalid accessControl config: {e}"))?;
        cfg.validate()?;
        Ok(Some(cfg))
    }

    /// Whether access control is administratively enabled for `group`. Does **not** by
    /// itself mean access control runs — `group` must also resolve a connection; see
    /// [`Self::connection_name_for_group`].
    pub fn enabled_for_group(&self, group: &str) -> bool {
        self.groups
            .get(group)
            .and_then(|g| g.enabled)
            .unwrap_or(self.enabled)
    }

    /// The named connection `group` resolves to: its own override, else
    /// [`Self::default_connection`]. `None` means access control does not apply to this
    /// group at all, regardless of [`Self::enabled_for_group`].
    pub fn connection_name_for_group<'a>(&'a self, group: &str) -> Option<&'a str> {
        self.groups
            .get(group)
            .and_then(|g| g.connection.as_deref())
            .or(self.default_connection.as_deref())
    }

    /// The resolved connection config for `group`, if any.
    pub fn connection_for_group(&self, group: &str) -> Option<&AccessConnectionConfig> {
        self.connection_name_for_group(group)
            .and_then(|name| self.connections.get(name))
    }

    pub fn validate(&self) -> Result<(), String> {
        for (name, conn) in &self.connections {
            conn.validate(name)?;
        }
        if let Some(name) = &self.default_connection {
            if !self.connections.contains_key(name) {
                return Err(format!(
                    "accessControl.defaultConnection {name:?} is not defined under accessControl.connections"
                ));
            }
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
        assert!(
            AccessControlConfig::from_admin_value(&json!({ "connections": {} }))
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
            "defaultConnection": "prod",
            "connections": {
                "prod": {
                    "provider": "opa",
                    "opa": { "url": "http://opa:8181", "decisionPath": "/v1/data/queryflux/access" },
                    "sessionParamKeys": ["customer"]
                }
            }
        });
        let cfg = AccessControlConfig::from_admin_value(&v).unwrap().unwrap();
        let prod = &cfg.connections["prod"];
        assert_eq!(prod.opa.as_ref().unwrap().url, "http://opa:8181");
        assert_eq!(prod.session_param_keys, vec!["customer"]);
        assert_eq!(cfg.connection_name_for_group("anything"), Some("prod"));
    }

    #[test]
    fn from_admin_value_enabled_cerbos() {
        let v = json!({
            "enabled": true,
            "defaultConnection": "prod",
            "connections": {
                "prod": {
                    "provider": "cerbos",
                    "cerbos": { "url": "http://cerbos:3592" }
                }
            }
        });
        let cfg = AccessControlConfig::from_admin_value(&v).unwrap().unwrap();
        let prod = &cfg.connections["prod"];
        assert_eq!(prod.provider, ProviderKind::Cerbos);
        assert_eq!(prod.cerbos.as_ref().unwrap().url, "http://cerbos:3592");
        assert_eq!(
            prod.cerbos.as_ref().unwrap().check_resources_path,
            "/api/check/resources"
        );
        // A cerbos connection must not be validated against the (absent) opa block.
        assert!(prod.opa.is_none());
    }

    #[test]
    fn cerbos_connection_without_cerbos_block_fails_validation() {
        let v = json!({
            "enabled": true,
            "defaultConnection": "prod",
            "connections": {
                "prod": { "provider": "cerbos" }
            }
        });
        let err = AccessControlConfig::from_admin_value(&v).unwrap_err();
        assert!(err.contains("no cerbos: block"), "got: {err}");
    }

    #[test]
    fn from_admin_value_rejects_bad_url() {
        let v = json!({
            "connections": { "prod": { "opa": { "url": "not-a-url" } } }
        });
        assert!(AccessControlConfig::from_admin_value(&v).is_err());
    }

    #[test]
    fn from_admin_value_rejects_unknown_default_connection() {
        let v = json!({
            "defaultConnection": "does-not-exist",
            "connections": { "prod": { "opa": { "url": "http://opa:8181" } } }
        });
        assert!(AccessControlConfig::from_admin_value(&v).is_err());
    }

    #[test]
    fn from_admin_value_no_default_connection_keeps_config() {
        // A connection can exist purely for groups that explicitly opt into it, with no
        // fleet-wide default at all.
        let v = json!({
            "connections": { "eu": { "opa": { "url": "http://opa:8181" } } },
            "groups": { "eu-group": { "connection": "eu" } }
        });
        let cfg = AccessControlConfig::from_admin_value(&v).unwrap().unwrap();
        assert_eq!(cfg.default_connection, None);
        assert_eq!(cfg.connection_name_for_group("eu-group"), Some("eu"));
        assert_eq!(cfg.connection_name_for_group("other-group"), None);
    }

    #[test]
    fn from_admin_value_global_off_with_connections_keeps_config() {
        let v = json!({
            "enabled": false,
            "defaultConnection": "prod",
            "connections": {
                "prod": { "opa": { "url": "http://opa:8181", "decisionPath": "/v1/data/queryflux/access" } }
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
    fn connection_resolution_default_and_overrides() {
        let mut cfg = AccessControlConfig {
            default_connection: Some("prod".to_string()),
            ..Default::default()
        };
        cfg.connections
            .insert("prod".to_string(), AccessConnectionConfig::default());
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
        assert_eq!(cfg.connection_name_for_group("eu-group"), Some("eu"));
        assert_eq!(cfg.connection_name_for_group("trino-prod"), Some("prod"));
        assert_eq!(
            cfg.connection_for_group("eu-group")
                .unwrap()
                .opa
                .as_ref()
                .unwrap()
                .url,
            "https://eu-opa.internal"
        );
        cfg.validate().expect("valid config with two connections");
    }

    #[test]
    fn connection_resolution_none_when_no_default_and_no_override() {
        let mut cfg = AccessControlConfig::default();
        cfg.connections
            .insert("eu".to_string(), AccessConnectionConfig::default());
        // No `defaultConnection` set — a group with no explicit override gets nothing.
        assert_eq!(cfg.connection_name_for_group("trino-prod"), None);
        assert!(cfg.connection_for_group("trino-prod").is_none());
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

    #[test]
    fn validate_rejects_unknown_default_connection() {
        let cfg = AccessControlConfig {
            default_connection: Some("does-not-exist".to_string()),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("does-not-exist"), "unexpected error: {err}");
    }
}
