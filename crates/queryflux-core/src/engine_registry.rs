//! Engine registry — types and runtime registry for backend engine descriptors.
//!
//! Core defines only the *types* and the `EngineRegistry` container.
//! The actual descriptor data lives in each engine adapter crate, which calls
//! `EngineRegistry::new(descriptors)` at startup (in `main.rs`).
//!
//! Used for:
//! - Startup validation of `ClusterConfig` (missing endpoint, unsupported auth, …)
//! - Admin API `/admin/engine-registry` so the UI can render forms without hard-coded logic

use serde::Serialize;

use crate::config::{ClusterAuth, ClusterConfig, EngineConfig};
use crate::query::EngineType;

// Re-export JSON parsing helpers from config_json so existing call sites keep working.
pub use crate::config_json::{
    cluster_config_from_persisted_json, json_bool, json_pool_size, json_str,
    json_tls_insecure_skip_verify, parse_auth_from_config_json, parse_query_auth_from_config_json,
};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// How a backend cluster is reached.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionType {
    /// REST/HTTP — Trino protocol, ClickHouse HTTP interface, DuckDB HTTP server
    Http,
    /// MySQL wire protocol — StarRocks front-end
    MySqlWire,
    /// In-process embedded library — DuckDB (no network endpoint)
    Embedded,
    /// SDK or cloud-managed — endpoint is implicit (e.g. Athena, BigQuery, Databricks)
    ManagedApi,
    /// Runtime-loaded shared library driver (ADBC)
    Driver,
}

/// Authentication mechanisms the engine supports.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AuthType {
    /// HTTP Basic auth (`Authorization: Basic …`)
    Basic,
    /// HTTP Bearer token (`Authorization: Bearer …`)
    Bearer,
    /// RSA key-pair (Snowflake, Databricks).
    KeyPair,
    /// AWS static access key (Athena and other AWS backends).
    AccessKey,
    /// AWS IAM role assumption via STS `AssumeRole` (Athena).
    RoleArn,
}

/// Describes a single configuration field that can appear on a `ClusterConfig`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    /// The YAML / JSON field name (camelCase, matches `ClusterConfig`).
    pub key: &'static str,
    /// Human-readable label for the UI.
    pub label: &'static str,
    /// Short description shown as helper text.
    pub description: &'static str,
    /// Field data type for UI rendering and client-side validation.
    pub field_type: FieldType,
    pub required: bool,
    /// Example value shown as placeholder in forms.
    pub example: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum FieldType {
    /// Plain text input
    Text,
    /// URL input (validated as a URL)
    Url,
    /// File system path
    Path,
    /// Password / secret — masked in UI
    Secret,
    /// Boolean toggle
    Boolean,
    /// Unsigned integer
    Number,
    /// Dropdown with a fixed list of allowed values
    Select { options: Vec<&'static str> },
}

/// Full descriptor for one supported backend engine.
///
/// Each implemented adapter provides this via its own `descriptor()` method.
/// Core never hard-codes descriptor data.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineDescriptor {
    /// Value to use for the `engine` YAML key (e.g. `"trino"`, `"duckDb"`).
    pub engine_key: &'static str,
    /// Human-readable name.
    pub display_name: &'static str,
    /// One-line description of the engine.
    pub description: &'static str,
    /// Brand hex color (no `#`) for UI badges.
    pub hex: &'static str,
    /// How the proxy connects to this engine.
    pub connection_type: ConnectionType,
    /// Default port if the user doesn't supply one (informational).
    pub default_port: Option<u16>,
    /// Example endpoint string shown in docs / forms.
    pub endpoint_example: Option<&'static str>,
    /// Auth mechanisms this engine supports.
    pub supported_auth: Vec<AuthType>,
    /// Ordered list of config fields relevant to this engine.
    pub config_fields: Vec<ConfigField>,
    /// Whether a full adapter is implemented in this build.
    pub implemented: bool,
}

impl EngineDescriptor {
    pub fn requires_endpoint(&self) -> bool {
        matches!(
            self.connection_type,
            ConnectionType::Http | ConnectionType::MySqlWire
        )
    }

    pub fn supports_tls(&self) -> bool {
        self.connection_type == ConnectionType::Http
    }

    pub fn supports_database_path(&self) -> bool {
        self.connection_type == ConnectionType::Embedded
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Runtime registry of engine descriptors, built at startup from adapter crates.
///
/// Each adapter supplies its own descriptor via `MyAdapter::descriptor()`.
/// `main.rs` collects them and passes the full list to `EngineRegistry::new`.
pub struct EngineRegistry {
    descriptors: Vec<EngineDescriptor>,
}

impl EngineRegistry {
    pub fn new(descriptors: Vec<EngineDescriptor>) -> Self {
        Self { descriptors }
    }

    /// All registered descriptors (for the admin API list endpoint).
    pub fn all(&self) -> &[EngineDescriptor] {
        &self.descriptors
    }

    /// Look up the descriptor for a given engine config variant.
    pub fn descriptor_for(&self, engine: &EngineConfig) -> Option<&EngineDescriptor> {
        let key = engine_key(engine);
        self.descriptors.iter().find(|d| d.engine_key == key)
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates a single cluster's configuration against the engine registry.
/// Returns a list of human-readable error messages; empty = valid.
pub fn validate_cluster_config(
    registry: &EngineRegistry,
    cluster_name: &str,
    config: &ClusterConfig,
) -> Vec<String> {
    let Some(engine) = &config.engine else {
        return vec![format!(
            "cluster '{cluster_name}': missing required 'engine' field"
        )];
    };

    let Some(desc) = registry.descriptor_for(engine) else {
        return vec![format!(
            "cluster '{cluster_name}': unknown engine '{}'",
            engine_key(engine)
        )];
    };

    let mut errors: Vec<String> = Vec::new();

    if !desc.implemented {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' is defined but not yet implemented",
            desc.display_name
        ));
    }

    if desc.requires_endpoint() && config.endpoint.is_none() {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' requires an 'endpoint' field (e.g. {})",
            desc.display_name,
            desc.endpoint_example.unwrap_or("see docs")
        ));
    }

    if !desc.supports_database_path() && config.database_path.is_some() {
        errors.push(format!(
            "cluster '{cluster_name}': 'databasePath' is only applicable to embedded DuckDB, not '{}'",
            desc.display_name
        ));
    }

    if !desc.supports_tls() && config.tls.is_some() {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' does not support TLS configuration",
            desc.display_name
        ));
    }

    if let Some(auth) = &config.auth {
        let has_auth_type = match auth {
            ClusterAuth::Basic { .. } => desc.supported_auth.contains(&AuthType::Basic),
            ClusterAuth::Bearer { .. } => desc.supported_auth.contains(&AuthType::Bearer),
            // `EngineConfig::Adbc` alone can't tell drivers apart — `supported_auth` on the
            // generic ADBC descriptor advertises KeyPair for every driver, so this needs the
            // same driver-aware narrowing `query_auth_supported` already does for Type 2 auth.
            ClusterAuth::KeyPair { .. } => {
                desc.supported_auth.contains(&AuthType::KeyPair)
                    && (!matches!(engine, crate::config::EngineConfig::Adbc)
                        || config
                            .driver
                            .as_deref()
                            .is_some_and(|d| crate::config::ADBC_KEYPAIR_AUTH_DRIVERS.contains(&d)))
            }
            ClusterAuth::AccessKey { .. } => desc.supported_auth.contains(&AuthType::AccessKey),
            ClusterAuth::RoleArn { .. } => desc.supported_auth.contains(&AuthType::RoleArn),
        };
        if !has_auth_type {
            let auth_label = match auth {
                ClusterAuth::Basic { .. } => "basic",
                ClusterAuth::Bearer { .. } => "bearer",
                ClusterAuth::KeyPair { .. } => "keyPair",
                ClusterAuth::AccessKey { .. } => "accessKey",
                ClusterAuth::RoleArn { .. } => "roleArn",
            };
            if matches!(auth, ClusterAuth::KeyPair { .. })
                && matches!(engine, crate::config::EngineConfig::Adbc)
            {
                let driver_name = config.driver.as_deref().unwrap_or("<unknown>");
                errors.push(format!(
                    "cluster '{cluster_name}': queryAuth type 'keyPair' is not supported for \
                     ADBC driver '{driver_name}' (only {:?})",
                    crate::config::ADBC_KEYPAIR_AUTH_DRIVERS
                ));
            } else {
                errors.push(format!(
                    "cluster '{cluster_name}': engine '{}' does not support '{auth_label}' authentication",
                    desc.display_name
                ));
            }
        }
    }

    errors
}

// ---------------------------------------------------------------------------
// Config JSON helpers
// ---------------------------------------------------------------------------
// (moved to crate::config_json — re-exported above)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps an `EngineConfig` variant to its canonical string key.
/// Must stay in sync with adapter `descriptor().engine_key` values.
pub fn engine_key(engine: &EngineConfig) -> &'static str {
    match engine {
        EngineConfig::Trino => "trino",
        EngineConfig::DuckDb => "duckDb",
        EngineConfig::DuckDbHttp => "duckDbHttp",
        EngineConfig::StarRocks => "starRocks",
        EngineConfig::ClickHouse => "clickHouse",
        EngineConfig::Athena => "athena",
        EngineConfig::Adbc => "adbc",
    }
}

/// Inverse of [`engine_key`]. Used when loading `engine_key` from Postgres / API.
pub fn parse_engine_key(s: &str) -> Result<EngineConfig, String> {
    match s {
        "trino" => Ok(EngineConfig::Trino),
        "duckDb" => Ok(EngineConfig::DuckDb),
        "duckDbHttp" => Ok(EngineConfig::DuckDbHttp),
        "starRocks" => Ok(EngineConfig::StarRocks),
        "clickHouse" => Ok(EngineConfig::ClickHouse),
        "athena" => Ok(EngineConfig::Athena),
        "adbc" => Ok(EngineConfig::Adbc),
        other => Err(format!("Unknown engine key: '{other}'")),
    }
}

impl From<&EngineConfig> for EngineType {
    fn from(cfg: &EngineConfig) -> Self {
        match cfg {
            EngineConfig::Trino => EngineType::Trino,
            EngineConfig::DuckDb => EngineType::DuckDb,
            EngineConfig::DuckDbHttp => EngineType::DuckDbHttp,
            EngineConfig::StarRocks => EngineType::StarRocks,
            EngineConfig::ClickHouse => EngineType::ClickHouse,
            EngineConfig::Athena => EngineType::Athena,
            EngineConfig::Adbc => EngineType::Adbc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;

    fn adbc_descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "adbc",
            display_name: "ADBC",
            description: "test",
            hex: "000000",
            connection_type: ConnectionType::Driver,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::Basic, AuthType::KeyPair],
            implemented: true,
            config_fields: vec![],
        }
    }

    fn key_pair_auth() -> ClusterAuth {
        ClusterAuth::KeyPair {
            username: "svc".to_string(),
            private_key_pem: "pem".to_string(),
            private_key_passphrase: None,
        }
    }

    #[test]
    fn adbc_key_pair_accepted_for_snowflake_driver() {
        let registry = EngineRegistry::new(vec![adbc_descriptor()]);
        let config = ClusterConfig {
            engine: Some(EngineConfig::Adbc),
            driver: Some("snowflake".to_string()),
            auth: Some(key_pair_auth()),
            ..Default::default()
        };
        let errors = validate_cluster_config(&registry, "c", &config);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn adbc_key_pair_rejected_for_non_snowflake_driver() {
        let registry = EngineRegistry::new(vec![adbc_descriptor()]);
        let config = ClusterConfig {
            engine: Some(EngineConfig::Adbc),
            driver: Some("postgresql".to_string()),
            auth: Some(key_pair_auth()),
            ..Default::default()
        };
        let errors = validate_cluster_config(&registry, "c", &config);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("keyPair"), "unexpected: {errors:?}");
        assert!(errors[0].contains("postgresql"), "unexpected: {errors:?}");
    }

    #[test]
    fn adbc_key_pair_rejected_when_descriptor_does_not_advertise_it() {
        let mut desc = adbc_descriptor();
        desc.supported_auth = vec![AuthType::Basic];
        let registry = EngineRegistry::new(vec![desc]);
        let config = ClusterConfig {
            engine: Some(EngineConfig::Adbc),
            driver: Some("snowflake".to_string()),
            auth: Some(key_pair_auth()),
            ..Default::default()
        };
        let errors = validate_cluster_config(&registry, "c", &config);
        assert_eq!(errors.len(), 1, "unexpected errors: {errors:?}");
    }

    #[test]
    fn non_adbc_engine_key_pair_ignores_driver() {
        // KeyPair on a non-ADBC engine (e.g. a hypothetical future Databricks-native
        // adapter) should only check `supported_auth`, never `driver` — the driver-aware
        // narrowing exists specifically because `EngineConfig::Adbc` alone can't tell
        // drivers apart. Use a Trino-keyed descriptor (driver: None) with KeyPair advertised,
        // to exercise the `!matches!(engine, EngineConfig::Adbc)` short-circuit directly.
        let desc = EngineDescriptor {
            engine_key: "trino",
            supported_auth: vec![AuthType::KeyPair],
            ..adbc_descriptor()
        };
        let registry = EngineRegistry::new(vec![desc]);
        let config = ClusterConfig {
            engine: Some(EngineConfig::Trino),
            driver: None,
            auth: Some(key_pair_auth()),
            ..Default::default()
        };
        let errors = validate_cluster_config(&registry, "c", &config);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }
}
