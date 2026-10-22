//! The control plane, as the collector sees it: where its watermarks are, and where its
//! runs go. The collector opens every request; nothing connects to it.

use std::time::Duration;

use anyhow::Context;
use queryflux_core::config::ControlPlaneClientConfig;
use queryflux_core::query_runs::{CollectorConfigV1, QueryRunBatchV1};

pub struct Upstream {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl Upstream {
    pub fn new(config: &ControlPlaneClientConfig) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            base: config.url.trim_end_matches('/').to_string(),
            token: config.auth.bearer().to_string(),
        })
    }

    /// Where this collector's clocks last stopped, per cluster.
    pub async fn config(&self, collector: &str) -> anyhow::Result<CollectorConfigV1> {
        let resp = self
            .http
            .get(format!("{}/collector/config", self.base))
            .query(&[("collector", collector)])
            .bearer_auth(&self.token)
            .send()
            .await
            .context("reach the control plane")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!(
                "GET /collector/config: {status}: {}",
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(resp.json().await?)
    }

    /// Send one batch. `Ok` means it is stored and the watermark advanced upstream.
    pub async fn send(&self, batch: &QueryRunBatchV1) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/collector/runs", self.base))
            .bearer_auth(&self.token)
            .json(batch)
            .send()
            .await
            .context("reach the control plane")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!(
                "POST /collector/runs: {status}: {}",
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }
}
