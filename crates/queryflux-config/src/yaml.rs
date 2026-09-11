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
            .expect("valid access_control should load");
        assert!(cfg.access_control.is_some());
    }
}
