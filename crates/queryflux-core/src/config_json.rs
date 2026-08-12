//! JSON parsing helpers for cluster configuration JSONB blobs.
//!
//! These functions parse the flat-key format used by persistence:
//! `authType`, `authUsername`, `authPassword`, `authToken` for Type 1 auth,
//! and `queryAuth` (nested) for Type 2.
//!
//! Shared by all engine adapters and by `main.rs` for the `BackendIdentityResolver`.

use crate::config::{ClusterAuth, ClusterConfig, EngineConfig, QueryAuthConfig};

/// Extract a `ClusterAuth` from the flat DB JSON format used by persistence.
///
/// The JSON blob stores auth as flat keys: `authType`, `authUsername`,
/// `authPassword`, `authToken`. This is the canonical format produced by
/// `UpsertClusterConfig::from_core()` and stored in the `config` JSONB column.
///
/// - Missing / empty `authType` → `Ok(None)`.
/// - Known `authType` with missing required fields → `Err` (so callers fail fast instead of
///   building adapters with empty credentials).
/// - `basic`: `authUsername` is required; `authPassword` may be empty (e.g. Trino with no password).
pub fn parse_auth_from_config_json(
    json: &serde_json::Value,
) -> Result<Option<ClusterAuth>, String> {
    let s =
        |key: &str| -> Option<String> { json.get(key).and_then(|v| v.as_str()).map(String::from) };
    let require = |key: &str| -> Result<String, String> {
        s(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("missing or empty '{key}' for this authType"))
    };
    match s("authType").as_deref() {
        None | Some("") => Ok(None),
        Some("basic") => Ok(Some(ClusterAuth::Basic {
            username: require("authUsername")?,
            password: s("authPassword").unwrap_or_default(),
        })),
        Some("bearer") => Ok(Some(ClusterAuth::Bearer {
            token: require("authToken")?,
        })),
        Some("keyPair") => Ok(Some(ClusterAuth::KeyPair {
            username: require("authUsername")?,
            private_key_pem: require("authPassword")?,
            private_key_passphrase: s("authToken"),
        })),
        Some("accessKey") => Ok(Some(ClusterAuth::AccessKey {
            access_key_id: require("authUsername")?,
            secret_access_key: require("authPassword")?,
            session_token: s("authToken"),
        })),
        Some("roleArn") => Ok(Some(ClusterAuth::RoleArn {
            role_arn: require("authUsername")?,
            external_id: s("authToken"),
        })),
        Some(other) => Err(format!("unsupported authType: '{other}'")),
    }
}

/// Extract per-query auth (`queryAuth` / Type 2) from the cluster `config` JSONB blob.
///
/// Same JSON shape as YAML `queryAuth` on [`ClusterConfig`] (written on upsert from YAML
/// and preserved in Postgres `cluster_configs.config`).
///
/// Returns [`Ok(None)`] when the field is omitted or null. A present but malformed payload
/// yields [`Err`].
pub fn parse_query_auth_from_config_json(
    json: &serde_json::Value,
) -> Result<Option<QueryAuthConfig>, serde_json::Error> {
    match json.get("queryAuth") {
        None => Ok(None),
        Some(v) if v.is_null() => Ok(None),
        Some(v) => Ok(Some(serde_json::from_value::<QueryAuthConfig>(v.clone())?)),
    }
}

/// Extract an optional string field from a config JSON blob.
pub fn json_str(json: &serde_json::Value, key: &str) -> Option<String> {
    json.get(key).and_then(|v| v.as_str()).map(String::from)
}

/// Extract a boolean field from a config JSON blob (defaults to `false`).
pub fn json_bool(json: &serde_json::Value, key: &str) -> bool {
    json.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// Skip TLS certificate verification for HTTP clients (Trino, DuckDB HTTP, …).
///
/// Reads nested `tls.insecureSkipVerify` first (matches Studio / [`crate::config::TlsConfig`] JSON).
/// If that key is absent under `tls`, falls back to legacy top-level `tlsInsecureSkipVerify`.
pub fn json_tls_insecure_skip_verify(json: &serde_json::Value) -> bool {
    if let Some(tls) = json.get("tls") {
        if let Some(v) = tls.get("insecureSkipVerify") {
            return v.as_bool().unwrap_or(false);
        }
    }
    json_bool(json, "tlsInsecureSkipVerify")
}

/// Positive integer `poolSize` from JSON (Studio / persisted config), or `None` if missing/invalid.
pub fn json_pool_size(config: &serde_json::Value) -> Option<usize> {
    config.get("poolSize").and_then(json_positive_usize)
}

fn json_positive_usize(v: &serde_json::Value) -> Option<usize> {
    if let Some(u) = v.as_u64() {
        return (u >= 1).then(|| usize::try_from(u).ok()).flatten();
    }
    if let Some(i) = v.as_i64() {
        return (i >= 1).then(|| usize::try_from(i).ok()).flatten();
    }
    let f = v.as_f64()?;
    if f.fract() != 0.0 || f < 1.0 || f > usize::MAX as f64 {
        return None;
    }
    usize::try_from(f as u64).ok()
}

/// Build a [`ClusterConfig`] from a persisted `cluster_configs.config` JSON blob plus parsed auth.
///
/// Produces a minimal `ClusterConfig` containing only the fields consumed by:
/// - Pass 2 group resolution in `main.rs` (`engine`, `enabled`, `max_running_queries`, `endpoint`)
/// - [`BackendIdentityResolver`] (`auth`, `query_auth`)
/// - StarRocks: `poolSize` → [`ClusterConfig::pool_size`] for YAML-compat / `from_cluster_config` builds
///
/// Other engine-specific fields (`databasePath`, `region`, `s3OutputLocation`, `workgroup`, `catalog`,
/// `maxWaitSecs`, `tls`) are **not** extracted here — each adapter reads those directly from JSON via its own
/// [`EngineConfigParseable::from_json`] implementation.
pub fn cluster_config_from_persisted_json(
    engine: EngineConfig,
    enabled: bool,
    max_running_queries: Option<u64>,
    config: &serde_json::Value,
    auth: Option<ClusterAuth>,
    query_auth: Option<QueryAuthConfig>,
) -> ClusterConfig {
    ClusterConfig {
        engine: Some(engine),
        enabled,
        max_running_queries,
        pool_size: json_pool_size(config),
        endpoint: json_str(config, "endpoint"),
        database_path: None,
        region: None,
        s3_output_location: None,
        workgroup: None,
        catalog: None,
        max_wait_secs: None,
        tls: None,
        max_result_buffer_bytes: None,
        auth,
        query_auth,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod auth_parse_tests {
    use super::*;
    use crate::config::ClusterAuth;

    #[test]
    fn parse_auth_basic_allows_empty_password() {
        let json = serde_json::json!({
            "authType": "basic",
            "authUsername": "admin",
            "authPassword": "",
        });
        let auth = parse_auth_from_config_json(&json).unwrap().unwrap();
        match auth {
            ClusterAuth::Basic { username, password } => {
                assert_eq!(username, "admin");
                assert!(password.is_empty());
            }
            _ => panic!("expected Basic auth"),
        }
    }

    #[test]
    fn parse_auth_basic_omitted_password_is_empty() {
        let json = serde_json::json!({
            "authType": "basic",
            "authUsername": "u",
        });
        let auth = parse_auth_from_config_json(&json).unwrap().unwrap();
        match auth {
            ClusterAuth::Basic { password, .. } => assert!(password.is_empty()),
            _ => panic!("expected Basic auth"),
        }
    }
}

#[cfg(test)]
mod query_auth_parse_tests {
    use super::*;
    use crate::config::QueryAuthConfig;

    #[test]
    fn parse_query_auth_impersonate() {
        let blob = serde_json::json!({ "queryAuth": { "type": "impersonate" } });
        let parsed = parse_query_auth_from_config_json(&blob).unwrap().unwrap();
        assert!(matches!(parsed, QueryAuthConfig::Impersonate));
    }

    #[test]
    fn parse_query_auth_omitted_is_none() {
        let blob = serde_json::json!({ "endpoint": "http://t:8080" });
        assert!(parse_query_auth_from_config_json(&blob).unwrap().is_none());
    }

    #[test]
    fn parse_query_auth_invalid_is_err() {
        let blob = serde_json::json!({ "queryAuth": { "type": "notAConfiguredQueryAuth" } });
        assert!(parse_query_auth_from_config_json(&blob).is_err());
    }
}

#[cfg(test)]
mod tls_insecure_skip_verify_tests {
    use super::*;

    #[test]
    fn nested_tls_wins_over_legacy() {
        let json = serde_json::json!({
            "tlsInsecureSkipVerify": true,
            "tls": { "insecureSkipVerify": false }
        });
        assert!(!json_tls_insecure_skip_verify(&json));
    }

    #[test]
    fn legacy_top_level_when_nested_absent() {
        let json = serde_json::json!({ "tlsInsecureSkipVerify": true });
        assert!(json_tls_insecure_skip_verify(&json));
    }

    #[test]
    fn empty_tls_object_falls_back_to_legacy() {
        let json = serde_json::json!({
            "tls": {},
            "tlsInsecureSkipVerify": true
        });
        assert!(json_tls_insecure_skip_verify(&json));
    }
}
