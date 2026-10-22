//! `queryflux-collector --config collector.yaml`

use std::time::Duration;

use anyhow::Context;
use queryflux_collector::config::CollectorFile;
use queryflux_collector::runner::{Cluster, Runner};
use queryflux_collector::trino::TrinoCollector;
use queryflux_collector::upstream::Upstream;
use queryflux_core::config::EngineConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let path = config_path()?;
    let yaml = std::fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
    let file = CollectorFile::parse(&yaml).with_context(|| format!("invalid config {path}"))?;

    let mut clusters = Vec::new();
    for (name, cluster) in &file.clusters {
        match cluster.engine {
            Some(EngineConfig::Trino) => clusters.push(Cluster {
                name: name.clone(),
                collector: Box::new(TrinoCollector::new(name, cluster)?),
            }),
            ref other => {
                tracing::warn!(cluster = %name, engine = ?other, "No collector for this engine yet; skipped")
            }
        }
    }
    if clusters.is_empty() {
        anyhow::bail!("none of the configured clusters has a collector");
    }

    let c = &file.collector;
    let mut runner = Runner::new(
        c.name.clone(),
        Upstream::new(&c.upstream)?,
        clusters,
        Duration::from_secs(c.poll_secs),
        chrono::Duration::seconds(c.overlap_secs as i64),
        chrono::Duration::seconds(c.lookback_secs as i64),
    );
    // Without its watermarks the collector would re-read the lookback window, so wait for
    // the control plane rather than start blind.
    loop {
        match runner.load_watermarks().await {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!("Loading watermarks failed, retrying in 5s: {e:#}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                }
            }
        }
    }
    tracing::info!(collector = %c.name, "Collecting");
    runner
        .run(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    Ok(())
}

/// `--config PATH`, or `QUERYFLUX_COLLECTOR_CONFIG`.
fn config_path() -> anyhow::Result<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().context("--config needs a path");
        }
    }
    std::env::var("QUERYFLUX_COLLECTOR_CONFIG")
        .context("pass --config PATH or set QUERYFLUX_COLLECTOR_CONFIG")
}
