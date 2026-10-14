//! `WorkerSnapshotV1`: the config a control plane serves to its workers (#255).
//!
//! A snapshot is everything a worker needs to compile its live config (clusters,
//! groups, routing, translation scripts and settings sections) as plain JSON-friendly
//! values. Its `revision` is a content hash, so two snapshots with the same content
//! have the same revision on every control plane, and a worker can tell whether it
//! already has a given config.
//!
//! The wire format is versioned by media type ([`WORKER_SNAPSHOT_MEDIA_TYPE`]) and
//! `schemaVersion`. Fields are added, never repurposed; a breaking change is a new
//! version.
//!
//! Credentials are an explicit choice when a snapshot is built ([`CredentialMode`]):
//! a self-hosted control plane embeds them (served over TLS, held in worker memory
//! only); a lower-trust deployment strips them and resolves secrets on the worker.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

use crate::config::{ClusterGroupConfig, RouterConfig};

/// HTTP media type of a serialized [`WorkerSnapshotV1`].
pub const WORKER_SNAPSHOT_MEDIA_TYPE: &str = "application/vnd.queryflux.worker-config.v1+json";

/// `schemaVersion` of [`WorkerSnapshotV1`].
pub const WORKER_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Whether credential values travel inside the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CredentialMode {
    /// Credential values are included as stored.
    Embedded,
    /// Every credential-like key was removed (see [`is_credential_key`]).
    Stripped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSnapshotV1 {
    pub schema_version: u32,
    /// Content hash of every other field. Set by [`WorkerSnapshotV1::seal`].
    pub revision: String,
    pub credentials: CredentialMode,
    /// Sorted by name.
    pub clusters: Vec<SnapshotCluster>,
    pub groups: BTreeMap<String, ClusterGroupConfig>,
    pub routers: Vec<RouterConfig>,
    pub routing_fallback: String,
    pub group_translation_scripts: BTreeMap<String, Vec<String>>,
    pub settings: SnapshotSettings,
}

/// Serialize `value` with object keys sorted at every level. `serde_json::Value` keeps
/// insertion order when any crate in the build enables `serde_json/preserve_order`, and
/// `HashMap` fields insert in a per-process random order, so plain serialization would give
/// the same config different revisions on different control plane replicas.
fn write_canonical(value: &serde_json::Value, out: &mut Vec<u8>) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push(b'{');
            for (i, (k, v)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(k).unwrap_or_default().as_bytes());
                out.push(b':');
                write_canonical(v, out);
            }
            out.push(b'}');
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(v, out);
            }
            out.push(b']');
        }
        other => out.extend_from_slice(other.to_string().as_bytes()),
    }
}

/// A cluster in its persisted form: engine key plus engine-specific JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotCluster {
    pub name: String,
    pub enabled: bool,
    pub engine_key: String,
    /// `None` inherits the group's limit.
    pub max_running_queries: Option<i64>,
    pub config: serde_json::Value,
    /// Variant definitions; each expands into its own runtime cluster on the worker.
    pub variants: serde_json::Value,
}

/// Settings sections as stored by the control plane. `None` means "not configured".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSettings {
    pub guardrails: Option<serde_json::Value>,
    pub security: Option<serde_json::Value>,
    pub catalog: Option<serde_json::Value>,
    pub access_control: Option<serde_json::Value>,
    /// Script library bodies referenced by guardrails, by script id.
    pub guard_script_bodies: BTreeMap<i64, String>,
}

impl WorkerSnapshotV1 {
    /// Strip credentials if asked, sort clusters and set `revision`. Call once, after
    /// all fields are filled in.
    pub fn seal(mut self) -> Self {
        if self.credentials == CredentialMode::Stripped {
            for c in &mut self.clusters {
                strip_credentials(&mut c.config);
                strip_credentials(&mut c.variants);
            }
            for v in [
                &mut self.settings.guardrails,
                &mut self.settings.security,
                &mut self.settings.catalog,
                &mut self.settings.access_control,
            ]
            .into_iter()
            .flatten()
            {
                strip_credentials(v);
            }
        }
        self.clusters.sort_by(|a, b| a.name.cmp(&b.name));
        self.revision = self.compute_revision();
        self
    }

    /// Content hash of everything except `revision`, as 16 hex characters.
    ///
    /// Serialized through `serde_json::Value`, whose objects are ordered maps, so the
    /// hash does not depend on `HashMap` iteration order inside the config types.
    pub fn compute_revision(&self) -> String {
        let mut value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        if let Some(obj) = value.as_object_mut() {
            obj.remove("revision");
        }
        let mut bytes = Vec::new();
        write_canonical(&value, &mut bytes);
        format!("{:016x}", xxh64(&bytes, 0))
    }

    /// True when `revision` matches the content (the snapshot was not altered after
    /// sealing).
    pub fn is_sealed(&self) -> bool {
        self.revision == self.compute_revision()
    }
}

/// Whether a JSON object key names a credential: passwords, tokens, secrets, private
/// keys and passphrases, matched case-insensitively. Deliberately broad, because some
/// engines (ADBC) accept free-form driver options.
pub fn is_credential_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "secret",
        "token",
        "privatekey",
        "private_key",
        "passphrase",
        "credential",
        "apikey",
        "api_key",
    ]
    .iter()
    .any(|needle| k.contains(needle))
        && !matches!(k.as_str(), "authtype" | "tokenexchange")
}

/// Remove every credential-like key, recursively, from objects inside `value`.
pub fn strip_credentials(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|k, _| !is_credential_key(k));
            for v in map.values_mut() {
                strip_credentials(v);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items {
                strip_credentials(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(json: serde_json::Value) -> ClusterGroupConfig {
        serde_json::from_value(json).expect("valid group fixture")
    }

    fn snapshot(credentials: CredentialMode) -> WorkerSnapshotV1 {
        WorkerSnapshotV1 {
            schema_version: WORKER_SNAPSHOT_SCHEMA_VERSION,
            revision: String::new(),
            credentials,
            clusters: vec![
                SnapshotCluster {
                    name: "trino-b".into(),
                    enabled: true,
                    engine_key: "trino".into(),
                    max_running_queries: None,
                    config: serde_json::json!({
                        "endpoint": "https://trino-b:8443",
                        "authType": "basic",
                        "authUsername": "svc",
                        "authPassword": "s3cret"
                    }),
                    variants: serde_json::json!([]),
                },
                SnapshotCluster {
                    name: "adbc-a".into(),
                    enabled: true,
                    engine_key: "adbc".into(),
                    max_running_queries: Some(4),
                    config: serde_json::json!({
                        "driver": "snowflake",
                        "dbKwargs": { "warehouse": "WH", "privateKeyPassphrase": "pp" },
                        "queryAuth": { "type": "tokenExchange" }
                    }),
                    variants: serde_json::json!([]),
                },
            ],
            groups: BTreeMap::from([(
                "analytics".to_string(),
                group(serde_json::json!({
                    "members": ["trino-b", "adbc-a"],
                    "maxRunningQueries": 10,
                    "strategy": { "type": "weighted", "weights": { "trino-b": 3, "adbc-a": 1 } }
                })),
            )]),
            routers: vec![],
            routing_fallback: "analytics".into(),
            group_translation_scripts: BTreeMap::new(),
            settings: SnapshotSettings {
                security: Some(serde_json::json!({
                    "provider": "oidc",
                    "oidc": { "issuer": "https://idp", "clientSecret": "cs" }
                })),
                ..SnapshotSettings::default()
            },
        }
    }

    #[test]
    fn revision_is_stable_and_independent_of_map_order() {
        let a = snapshot(CredentialMode::Embedded).seal();
        // Same content built again: HashMap fields inside the group config may iterate
        // in a different order, and the clusters arrive in a different order.
        let mut b = snapshot(CredentialMode::Embedded);
        b.clusters.reverse();
        let b = b.seal();
        assert_eq!(a.revision, b.revision);
        assert_eq!(a.revision.len(), 16);
        assert!(a.is_sealed());
    }

    #[test]
    fn canonical_form_ignores_key_order() {
        let a: serde_json::Value =
            serde_json::json!({"b": 1, "a": {"y": [1, {"q": 2, "p": 3}], "x": "s"}});
        let mut m = serde_json::Map::new();
        m.insert(
            "a".into(),
            serde_json::json!({"x": "s", "y": [1, {"p": 3, "q": 2}]}),
        );
        m.insert("b".into(), serde_json::json!(1));
        let (mut x, mut y) = (Vec::new(), Vec::new());
        write_canonical(&a, &mut x);
        write_canonical(&serde_json::Value::Object(m), &mut y);
        assert_eq!(x, y);
        assert_eq!(
            String::from_utf8(x).unwrap(),
            r#"{"a":{"x":"s","y":[1,{"p":3,"q":2}]},"b":1}"#
        );
    }

    #[test]
    fn revision_changes_when_content_changes() {
        let a = snapshot(CredentialMode::Embedded).seal();
        let mut b = snapshot(CredentialMode::Embedded);
        b.routing_fallback = "other".into();
        assert_ne!(a.revision, b.seal().revision);
    }

    #[test]
    fn tampering_after_sealing_is_detected() {
        let mut s = snapshot(CredentialMode::Embedded).seal();
        s.clusters[0].enabled = false;
        assert!(!s.is_sealed());
    }

    #[test]
    fn stripped_snapshots_carry_no_credentials() {
        let s = snapshot(CredentialMode::Stripped).seal();
        let json = serde_json::to_string(&s).unwrap();
        for secret in ["s3cret", "\"pp\"", "\"cs\""] {
            assert!(!json.contains(secret), "{secret} leaked: {json}");
        }
        // Non-secret fields that merely mention a credential type survive.
        let trino = s.clusters.iter().find(|c| c.name == "trino-b").unwrap();
        assert_eq!(trino.config["authType"], "basic");
        assert_eq!(trino.config["authUsername"], "svc");
        let adbc = s.clusters.iter().find(|c| c.name == "adbc-a").unwrap();
        assert_eq!(adbc.config["queryAuth"]["type"], "tokenExchange");
        assert_eq!(adbc.config["dbKwargs"]["warehouse"], "WH");
    }

    #[test]
    fn embedded_snapshots_keep_credentials() {
        let s = snapshot(CredentialMode::Embedded).seal();
        let trino = s.clusters.iter().find(|c| c.name == "trino-b").unwrap();
        assert_eq!(trino.config["authPassword"], "s3cret");
    }

    #[test]
    fn wire_format_round_trips_and_keeps_its_revision() {
        let s = snapshot(CredentialMode::Embedded).seal();
        let json = serde_json::to_string(&s).unwrap();
        let back: WorkerSnapshotV1 = serde_json::from_str(&json).unwrap();
        assert!(back.is_sealed());
        assert_eq!(back.revision, s.revision);
    }

    /// Golden check on the wire field names: renaming one breaks every deployed worker.
    #[test]
    fn wire_field_names_are_stable() {
        let s = snapshot(CredentialMode::Embedded).seal();
        let v = serde_json::to_value(&s).unwrap();
        let mut top: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        top.sort();
        assert_eq!(
            top,
            vec![
                "clusters",
                "credentials",
                "groupTranslationScripts",
                "groups",
                "revision",
                "routers",
                "routingFallback",
                "schemaVersion",
                "settings",
            ]
        );
        let mut cluster: Vec<&str> = v["clusters"][0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        cluster.sort();
        assert_eq!(
            cluster,
            vec![
                "config",
                "enabled",
                "engineKey",
                "maxRunningQueries",
                "name",
                "variants"
            ]
        );
        let mut settings: Vec<&str> = v["settings"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        settings.sort();
        assert_eq!(
            settings,
            vec![
                "accessControl",
                "catalog",
                "guardScriptBodies",
                "guardrails",
                "security"
            ]
        );
        assert_eq!(v["credentials"], "embedded");
    }
}
