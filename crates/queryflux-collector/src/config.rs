//! The collector's config file.
//!
//! ```yaml
//! collector:
//!   name: site-a-collector
//!   upstream:
//!     url: https://queryflux-control-plane:9000
//!     auth: { mode: sharedSecret, sharedSecret: "..." }
//!   pollSecs: 30
//! clusters:
//!   trino-a:
//!     engine: trino
//!     endpoint: http://trino:8080
//!     auth: { type: basic, username: monitor, password: "..." }
//! ```
//!
//! Clusters use the same shape as QueryFlux's own config, so the names match the
//! `cluster_name` QueryFlux records and runs can be linked to its queries.

use std::collections::BTreeMap;

use queryflux_core::config::{ClusterConfig, ControlPlaneClientConfig};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectorFile {
    pub collector: CollectorSection,
    #[serde(default)]
    pub clusters: BTreeMap<String, ClusterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectorSection {
    /// Attribution, and the owner of this collector's watermarks upstream.
    pub name: String,
    /// The control plane (standalone, `controlPlane`, or LakeOps). Same shape as a
    /// worker's `controlPlane` block.
    pub upstream: ControlPlaneClientConfig,
    /// How often each cluster is polled. Trino forgets finished queries after
    /// `query.min-expire-age` (15 minutes by default), so keep this well below that.
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
    /// Each poll re-reads this much before the watermark, so runs an engine reports late
    /// are not missed. Re-reads are harmless: ingest upserts.
    #[serde(default = "default_overlap_secs")]
    pub overlap_secs: u64,
    /// How far back the first poll of a cluster with no watermark reads.
    #[serde(default = "default_lookback_secs")]
    pub lookback_secs: u64,
}

fn default_poll_secs() -> u64 {
    30
}
fn default_overlap_secs() -> u64 {
    120
}
fn default_lookback_secs() -> u64 {
    900
}

impl CollectorFile {
    pub fn parse(yaml: &str) -> anyhow::Result<Self> {
        let file: CollectorFile = serde_yaml::from_str(yaml)?;
        file.validate()?;
        Ok(file)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let c = &self.collector;
        if c.name.trim().is_empty() {
            anyhow::bail!("collector.name is required");
        }
        let url = c.upstream.url.trim();
        if !(url.starts_with("https://")
            || (c.upstream.allow_insecure_http && url.starts_with("http://")))
        {
            anyhow::bail!(
                "collector.upstream.url must be https:// (got '{url}'); set allowInsecureHttp: true for development only"
            );
        }
        if c.upstream.auth.bearer().trim().is_empty() {
            anyhow::bail!("collector.upstream.auth.sharedSecret is empty");
        }
        if c.poll_secs == 0 {
            anyhow::bail!("collector.pollSecs must be at least 1");
        }
        if self.clusters.is_empty() {
            anyhow::bail!("no clusters to collect from");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
collector:
  name: c1
  upstream:
    url: http://cp:9000
    allowInsecureHttp: true
    auth: { mode: sharedSecret, sharedSecret: s3cret }
clusters:
  trino-a:
    engine: trino
    endpoint: http://trino:8080
"#;

    #[test]
    fn parses_with_defaults() {
        let file = CollectorFile::parse(YAML).unwrap();
        assert_eq!(file.collector.poll_secs, 30);
        assert_eq!(file.collector.overlap_secs, 120);
        assert!(file.clusters.contains_key("trino-a"));
    }

    #[test]
    fn refuses_plain_http_unless_allowed() {
        let yaml = YAML.replace("    allowInsecureHttp: true\n", "");
        let err = CollectorFile::parse(&yaml).unwrap_err().to_string();
        assert!(err.contains("https://"), "{err}");
    }
}
