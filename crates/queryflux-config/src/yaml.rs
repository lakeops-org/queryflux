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
            if let Err(e) = guardrails.validate() {
                tracing::warn!(
                    "Invalid guardrails in {}: {e} — will be ignored if Postgres config overrides YAML",
                    self.path.display()
                );
            }
        }

        Ok(config)
    }
}
