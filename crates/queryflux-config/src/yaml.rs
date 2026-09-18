use std::path::{Path, PathBuf};

use async_trait::async_trait;
use queryflux_core::{
    config::ProxyConfig,
    error::{QueryFluxError, Result},
};

use crate::ConfigProvider;

pub struct YamlFileConfigProvider {
    path: PathBuf,
}

impl YamlFileConfigProvider {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl ConfigProvider for YamlFileConfigProvider {
    async fn load(&self) -> Result<ProxyConfig> {
        let content = tokio::fs::read_to_string(&self.path).await.map_err(|e| {
            QueryFluxError::Config(format!(
                "Failed to read config file {}: {e}",
                self.path.display()
            ))
        })?;

        let config: ProxyConfig = serde_yaml::from_str(&content).map_err(|e| {
            QueryFluxError::Config(format!(
                "Failed to parse config file {}: {e}",
                self.path.display()
            ))
        })?;

        if let Some(guardrails) = &config.guardrails {
            guardrails.validate().map_err(|e| {
                QueryFluxError::Config(format!(
                    "Invalid guardrails in {}: {e}",
                    self.path.display()
                ))
            })?;
        }

        if let Some(access_control) = &config.access_control {
            access_control.validate().map_err(|e| {
                QueryFluxError::Config(format!(
                    "Invalid access_control in {}: {e}",
                    self.path.display()
                ))
            })?;
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn load_rejects_invalid_guardrails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
guardrails:
  global:
    - kind: python_script
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let err = provider
            .load()
            .await
            .expect_err("invalid guardrails must fail");
        let msg = err.to_string();
        assert!(msg.contains("script"), "{msg}");
    }

    #[tokio::test]
    async fn load_accepts_valid_guardrails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
guardrails:
  global:
    - kind: python_script
      script: |
        def check(ctx):
            return {"action": "allow"}
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        provider.load().await.expect("valid guardrails should load");
    }

    #[tokio::test]
    async fn load_rejects_invalid_access_control_url() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
accessControl:
  connections:
    default:
      opa:
        url: "not a url"
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let err = provider
            .load()
            .await
            .expect_err("invalid access_control url must fail");
        assert!(err.to_string().contains("url"), "{err}");
    }

    #[tokio::test]
    async fn load_accepts_valid_access_control() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
accessControl:
  connections:
    default:
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
      operations: [table.select]
      failOpen: false
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let cfg = provider
            .load()
            .await
            .expect("valid access_control should load")
            .access_control
            .expect("accessControl block");
        // Assert the *resolved* URL, not just "it parsed" — a config shape drift (e.g. the
        // old flat `opa:`/`operations:` fields moving under `connections.default`) would
        // otherwise still parse "successfully" by silently falling back to
        // `AccessConnectionConfig::default()` (localhost:8181) instead of failing loudly.
        assert_eq!(
            cfg.connections["default"].opa.as_ref().unwrap().url,
            "http://localhost:8181"
        );
    }

    #[tokio::test]
    async fn load_accepts_multiple_connections_with_group_routing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
accessControl:
  defaultConnection: default
  connections:
    default:
      opa:
        url: http://localhost:8181
    eu-sandbox:
      opa:
        url: https://eu-opa.internal
  groups:
    eu-group:
      connection: eu-sandbox
    sandbox:
      enabled: false
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let cfg = provider
            .load()
            .await
            .expect("valid multi-connection access_control should load")
            .access_control
            .expect("accessControl block");
        assert_eq!(
            cfg.connection_name_for_group("eu-group"),
            Some("eu-sandbox")
        );
        assert_eq!(cfg.connection_name_for_group("trino-prod"), Some("default"));
        assert!(cfg.enabled_for_group("trino-prod"));
        assert!(!cfg.enabled_for_group("sandbox"));
    }

    #[tokio::test]
    async fn load_group_with_no_default_connection_resolves_to_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
accessControl:
  connections:
    eu-sandbox:
      opa:
        url: https://eu-opa.internal
  groups:
    eu-group:
      connection: eu-sandbox
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let cfg = provider
            .load()
            .await
            .expect("valid access_control without a default connection should load")
            .access_control
            .expect("accessControl block");
        assert_eq!(
            cfg.connection_name_for_group("eu-group"),
            Some("eu-sandbox")
        );
        // No `defaultConnection` and no per-group override — access control simply
        // doesn't apply to this group.
        assert_eq!(cfg.connection_name_for_group("trino-prod"), None);
    }

    #[tokio::test]
    async fn load_rejects_group_referencing_unknown_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.yaml");
        tokio::fs::write(
            &path,
            r#"
queryflux: {}
accessControl:
  connections:
    default:
      opa:
        url: http://localhost:8181
  groups:
    eu-group:
      connection: does-not-exist
"#,
        )
        .await
        .expect("write config");

        let provider = YamlFileConfigProvider::new(&path);
        let err = provider
            .load()
            .await
            .expect_err("a group referencing an undefined connection must fail validation");
        assert!(err.to_string().contains("does-not-exist"), "{err}");
    }

    #[tokio::test]
    async fn load_accepts_with_opa_example_config() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/with-opa/config.yaml");
        let provider = YamlFileConfigProvider::new(&path);
        let cfg = provider
            .load()
            .await
            .unwrap_or_else(|e| panic!("examples/with-opa/config.yaml must load: {e}"));
        let ac = cfg.access_control.expect("accessControl block");
        ac.validate().expect("accessControl must validate");
        assert_eq!(
            ac.connections["default"].opa.as_ref().unwrap().url,
            "http://127.0.0.1:8182",
            "the example's real OPA URL must survive parsing, not fall back to the default"
        );
        cfg.auth
            .validate_for_required()
            .expect("static auth must validate");
        assert_eq!(cfg.auth.static_users.unwrap().users.len(), 2);
        assert!(cfg.clusters.contains_key("trino-1"));
        assert!(matches!(
            cfg.catalog_provider,
            queryflux_core::config::CatalogProviderConfig::IcebergRest { .. }
        ));
    }
}
