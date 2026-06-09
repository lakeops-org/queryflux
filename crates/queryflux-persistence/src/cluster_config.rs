//! Persisted cluster and cluster-group configuration records.
//!
//! When Postgres persistence is configured, QueryFlux stores the cluster / group
//! config in `cluster_configs` and `cluster_group_configs` and reads from there
//! instead of the YAML file.  The YAML is only used to seed the tables on the
//! very first run (when both tables are empty).
//!
//! Each cluster row has a stable `id` plus an engine-specific `config JSONB`
//! column. All connection details (endpoint, auth, TLS, region, …) live inside
//! that JSON blob so the schema never needs a migration when a new engine field
//! is added.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Cluster config
// ---------------------------------------------------------------------------

/// Full cluster configuration record as stored in Postgres.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfigRecord {
    /// Stable surrogate key; group `members` stores these ids in Postgres.
    pub id: i64,
    pub name: String,
    /// YAML / registry engine key: `"trino"`, `"duckDb"`, `"starRocks"`, `"clickHouse"`, `"athena"`.
    pub engine_key: String,
    pub enabled: bool,
    /// Per-cluster limit; `NULL` means inherit from the cluster group.
    pub max_running_queries: Option<i64>,
    /// All engine-specific connection details (endpoint, auth, TLS, region, …).
    #[schema(value_type = Object)]
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating or fully replacing a cluster config.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClusterConfig {
    pub engine_key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Omit or `null` to inherit the group's `maxRunningQueries`.
    #[serde(default)]
    pub max_running_queries: Option<i64>,
    /// Engine-specific connection details. Schema depends on `engineKey`.
    #[schema(value_type = Object)]
    pub config: serde_json::Value,
}

/// Request body for PATCH rename (`/admin/config/clusters/{name}`, `/admin/config/groups/{name}`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RenameConfigRequest {
    pub new_name: String,
}

// ---------------------------------------------------------------------------
// Cluster group config
// ---------------------------------------------------------------------------

/// Full cluster group configuration record as stored in Postgres.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupConfigRecord {
    /// Stable surrogate key; used by routing rules and foreign keys.
    pub id: i64,
    pub name: String,
    pub enabled: bool,
    /// Ordered member cluster names (resolved from ids stored in Postgres).
    pub members: Vec<String>,
    pub max_running_queries: i64,
    pub max_queued_queries: Option<i64>,
    /// Serialised `StrategyConfig`. `null` means RoundRobin (the default).
    #[schema(value_type = Option<Object>)]
    pub strategy: Option<serde_json::Value>,
    pub allow_groups: Vec<String>,
    pub allow_users: Vec<String>,
    /// Ordered `user_scripts.id` values run as post-sqlglot translation fixups for this group.
    #[serde(default)]
    pub translation_script_ids: Vec<i64>,
    /// Default tags merged into every query routed to this group (session tags win on key conflicts).
    /// Stored as JSONB: `{"team": "eng", "batch": null}` — `null` values are key-only tags.
    #[serde(default = "default_tags_value")]
    #[schema(value_type = Object)]
    pub default_tags: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating or fully replacing a cluster group config.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClusterGroupConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub members: Vec<String>,
    pub max_running_queries: i64,
    pub max_queued_queries: Option<i64>,
    /// `null` = RoundRobin. Set to `{"type":"leastLoaded"}` etc. for other strategies.
    pub strategy: Option<serde_json::Value>,
    #[serde(default)]
    pub allow_groups: Vec<String>,
    #[serde(default)]
    pub allow_users: Vec<String>,
    /// Ordered translation fixup script ids (`user_scripts.kind = translation_fixup`).
    #[serde(default)]
    pub translation_script_ids: Vec<i64>,
    /// Default tags merged into every query in this group. `{"team": "eng", "batch": null}` style.
    /// `null` values are key-only tags (Trino style). Omit or set to `{}` for no defaults.
    #[serde(default = "default_tags_value")]
    #[schema(value_type = Object)]
    pub default_tags: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Conversion helpers: core config types → Upsert types (for YAML seeding)
// ---------------------------------------------------------------------------

use queryflux_core::config::{ClusterAuth, ClusterConfig, ClusterGroupConfig};
use queryflux_core::engine_registry::engine_key;

impl UpsertClusterConfig {
    /// Serializes `ClusterConfig` into the JSONB shape stored in Postgres.
    ///
    /// Returns `Ok(None)` when `engine` is missing. Fails if `queryAuth` cannot be encoded.
    pub fn from_core(cfg: &ClusterConfig) -> Result<Option<Self>, serde_json::Error> {
        let Some(engine) = cfg.engine.as_ref() else {
            return Ok(None);
        };
        let engine_key = engine_key(engine);

        let mut config = serde_json::Map::new();

        if let Some(v) = &cfg.endpoint {
            config.insert("endpoint".into(), v.clone().into());
        }
        if let Some(v) = &cfg.database_path {
            config.insert("databasePath".into(), v.clone().into());
        }
        if cfg
            .tls
            .as_ref()
            .map(|t| t.insecure_skip_verify)
            .unwrap_or(false)
        {
            config.insert("tlsInsecureSkipVerify".into(), true.into());
        }
        if let Some(v) = &cfg.region {
            config.insert("region".into(), v.clone().into());
        }
        if let Some(v) = &cfg.s3_output_location {
            config.insert("s3OutputLocation".into(), v.clone().into());
        }
        if let Some(v) = &cfg.workgroup {
            config.insert("workgroup".into(), v.clone().into());
        }
        if let Some(v) = &cfg.catalog {
            config.insert("catalog".into(), v.clone().into());
        }
        if let Some(n) = cfg.pool_size {
            config.insert("poolSize".into(), serde_json::json!(n));
        }

        match &cfg.auth {
            Some(ClusterAuth::Basic { username, password }) => {
                config.insert("authType".into(), "basic".into());
                config.insert("authUsername".into(), username.clone().into());
                config.insert("authPassword".into(), password.clone().into());
            }
            Some(ClusterAuth::Bearer { token }) => {
                config.insert("authType".into(), "bearer".into());
                config.insert("authToken".into(), token.clone().into());
            }
            Some(ClusterAuth::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                config.insert("authType".into(), "accessKey".into());
                config.insert("authUsername".into(), access_key_id.clone().into());
                config.insert("authPassword".into(), secret_access_key.clone().into());
                if let Some(st) = session_token {
                    config.insert("authToken".into(), st.clone().into());
                }
            }
            // KeyPair: private key material is not persisted to DB.
            Some(ClusterAuth::KeyPair { username, .. }) => {
                config.insert("authType".into(), "keyPair".into());
                config.insert("authUsername".into(), username.clone().into());
            }
            Some(ClusterAuth::RoleArn {
                role_arn,
                external_id,
            }) => {
                config.insert("authType".into(), "roleArn".into());
                config.insert("authUsername".into(), role_arn.clone().into());
                if let Some(eid) = external_id {
                    config.insert("authToken".into(), eid.clone().into());
                }
            }
            None => {}
        }

        if let Some(qa) = &cfg.query_auth {
            config.insert("queryAuth".into(), serde_json::to_value(qa)?);
        }

        Ok(Some(Self {
            engine_key: engine_key.to_owned(),
            enabled: cfg.enabled,
            max_running_queries: cfg.max_running_queries.map(|v| v as i64),
            config: serde_json::Value::Object(config),
        }))
    }
}

impl UpsertClusterGroupConfig {
    pub fn from_core(cfg: &ClusterGroupConfig) -> Self {
        let strategy = cfg
            .strategy
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok());

        let default_tags = serde_json::to_value(&cfg.default_tags)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::default()));

        Self {
            enabled: cfg.enabled,
            members: cfg.members.clone(),
            max_running_queries: cfg.max_running_queries as i64,
            max_queued_queries: cfg.max_queued_queries.map(|v| v as i64),
            strategy,
            allow_groups: cfg.authorization.allow_groups.clone(),
            allow_users: cfg.authorization.allow_users.clone(),
            translation_script_ids: Vec::new(),
            default_tags,
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers: DB records → core config types (for startup loading)
// ---------------------------------------------------------------------------

// NOTE: `ClusterConfigRecord::to_core()` has been removed. Engine adapters are
// built from the JSONB config blob via `try_from_config_json()` on each adapter.
// Type 1 auth uses `parse_auth_from_config_json`; Type 2 (`queryAuth`) uses
// `parse_query_auth_from_config_json` — both in `queryflux_core::engine_registry`.

impl ClusterGroupConfigRecord {
    pub fn to_core(&self) -> ClusterGroupConfig {
        use queryflux_core::config::StrategyConfig;

        let strategy = self
            .strategy
            .as_ref()
            .and_then(|v| serde_json::from_value::<StrategyConfig>(v.clone()).ok());

        let default_tags =
            serde_json::from_value::<queryflux_core::tags::QueryTags>(self.default_tags.clone())
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        group_id = self.id,
                        group_name = %self.name,
                        error = %e,
                        "malformed default_tags in DB — using empty tag set"
                    );
                    Default::default()
                });

        ClusterGroupConfig {
            enabled: self.enabled,
            members: self.members.clone(),
            strategy,
            max_running_queries: self.max_running_queries as u64,
            max_queued_queries: self.max_queued_queries.map(|v| v as u64),
            authorization: queryflux_core::config::ClusterGroupAuthorizationConfig {
                allow_groups: self.allow_groups.clone(),
                allow_users: self.allow_users.clone(),
            },
            default_tags,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_tags_value() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use queryflux_core::tags::QueryTags;

    fn make_record(default_tags: serde_json::Value) -> ClusterGroupConfigRecord {
        ClusterGroupConfigRecord {
            id: 1,
            name: "test-group".to_string(),
            enabled: true,
            members: vec!["c1".to_string()],
            max_running_queries: 10,
            max_queued_queries: None,
            strategy: None,
            allow_groups: vec![],
            allow_users: vec![],
            translation_script_ids: vec![],
            default_tags,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_core_group(tags: QueryTags) -> queryflux_core::config::ClusterGroupConfig {
        queryflux_core::config::ClusterGroupConfig {
            enabled: true,
            members: vec!["c1".to_string()],
            strategy: None,
            max_running_queries: 10,
            max_queued_queries: None,
            authorization: queryflux_core::config::ClusterGroupAuthorizationConfig {
                allow_groups: vec![],
                allow_users: vec![],
            },
            default_tags: tags,
        }
    }

    // --- to_core: JSONB → QueryTags ---

    #[test]
    fn to_core_key_value_tags() {
        let json = serde_json::json!({"team": "eng", "cost_center": "701"});
        let core = make_record(json).to_core();
        assert_eq!(
            core.default_tags.get("team"),
            Some(&Some("eng".to_string()))
        );
        assert_eq!(
            core.default_tags.get("cost_center"),
            Some(&Some("701".to_string()))
        );
    }

    #[test]
    fn to_core_key_only_tags_deserialize_as_none() {
        let json = serde_json::json!({"batch": null, "team": "eng"});
        let core = make_record(json).to_core();
        assert_eq!(core.default_tags.get("batch"), Some(&None));
        assert_eq!(
            core.default_tags.get("team"),
            Some(&Some("eng".to_string()))
        );
    }

    #[test]
    fn to_core_empty_json_gives_empty_tags() {
        let core = make_record(serde_json::json!({})).to_core();
        assert!(core.default_tags.is_empty());
    }

    #[test]
    fn to_core_malformed_json_falls_back_to_empty() {
        // A non-object JSON value cannot deserialize as QueryTags — should not panic.
        let core = make_record(serde_json::json!([1, 2, 3])).to_core();
        assert!(core.default_tags.is_empty());
    }

    // --- from_core: QueryTags → JSONB ---

    #[test]
    fn from_core_key_value_tags_serialize_to_json() {
        let tags: QueryTags = [
            ("env".to_string(), Some("prod".to_string())),
            ("cost_center".to_string(), Some("701".to_string())),
        ]
        .into();
        let upsert = UpsertClusterGroupConfig::from_core(&make_core_group(tags));
        let env = upsert.default_tags.get("env").unwrap();
        assert_eq!(env, &serde_json::Value::String("prod".to_string()));
    }

    #[test]
    fn from_core_key_only_tags_serialize_as_null() {
        let tags: QueryTags = [("batch".to_string(), None)].into();
        let upsert = UpsertClusterGroupConfig::from_core(&make_core_group(tags));
        let batch = upsert.default_tags.get("batch").unwrap();
        assert_eq!(batch, &serde_json::Value::Null);
    }

    #[test]
    fn from_core_empty_tags_gives_empty_object() {
        let upsert = UpsertClusterGroupConfig::from_core(&make_core_group(QueryTags::new()));
        assert!(upsert.default_tags.is_object());
        assert_eq!(upsert.default_tags.as_object().unwrap().len(), 0);
    }

    // --- roundtrip: core → upsert → record → core ---

    #[test]
    fn roundtrip_default_tags() {
        let tags: QueryTags = [
            ("env".to_string(), Some("prod".to_string())),
            ("batch".to_string(), None),
        ]
        .into();
        let core_in = make_core_group(tags);
        let upsert = UpsertClusterGroupConfig::from_core(&core_in);

        // Simulate what the DB would return: use the JSONB stored in upsert as the record's value.
        let record = make_record(upsert.default_tags.clone());
        let core_out = record.to_core();

        assert_eq!(
            core_out.default_tags.get("env"),
            Some(&Some("prod".to_string()))
        );
        assert_eq!(core_out.default_tags.get("batch"), Some(&None));
        assert_eq!(core_out.default_tags.len(), 2);
    }
}
