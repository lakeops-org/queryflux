//! Polls every cluster on one clock: read the runs that ended since the cluster's
//! watermark, send them upstream, and move the watermark only once they are stored.
//!
//! The watermark lives upstream (advanced in the same transaction that stores a batch),
//! so a restarted collector resumes where the last stored batch ended and nothing needs
//! to be kept on disk here.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use queryflux_core::query_runs::{QueryRunBatchV1, FAST_CLOCK, QUERY_RUNS_SCHEMA_VERSION};

use crate::upstream::Upstream;
use crate::EngineCollector;

pub struct Cluster {
    pub name: String,
    pub collector: Box<dyn EngineCollector>,
}

pub struct Runner {
    pub name: String,
    pub upstream: Upstream,
    pub clusters: Vec<Cluster>,
    pub poll: Duration,
    pub overlap: chrono::Duration,
    pub lookback: chrono::Duration,
    /// Window end of the last batch stored upstream, per cluster.
    marks: HashMap<String, DateTime<Utc>>,
}

impl Runner {
    pub fn new(
        name: String,
        upstream: Upstream,
        clusters: Vec<Cluster>,
        poll: Duration,
        overlap: chrono::Duration,
        lookback: chrono::Duration,
    ) -> Self {
        Self {
            name,
            upstream,
            clusters,
            poll,
            overlap,
            lookback,
            marks: HashMap::new(),
        }
    }

    /// Resume from the watermarks stored upstream.
    pub async fn load_watermarks(&mut self) -> anyhow::Result<()> {
        let config = self.upstream.config(&self.name).await?;
        for (cluster, clocks) in config.watermarks {
            if let Some(mark) = clocks.get(FAST_CLOCK) {
                self.marks.insert(cluster, *mark);
            }
        }
        Ok(())
    }

    /// One pass over every cluster, reading runs that ended up to `now`. A cluster that
    /// fails is logged and retried from the same watermark on the next pass.
    pub async fn poll_once(&mut self, now: DateTime<Utc>) {
        for cluster in &self.clusters {
            let start = match self.marks.get(&cluster.name) {
                Some(mark) => *mark - self.overlap,
                None => now - self.lookback,
            };
            if start >= now {
                continue;
            }
            let runs = match cluster.collector.list_runs(start, now).await {
                Ok(runs) => runs,
                Err(e) => {
                    tracing::warn!(cluster = %cluster.name, "Collecting runs failed: {e:#}");
                    continue;
                }
            };
            let batch = QueryRunBatchV1 {
                schema_version: QUERY_RUNS_SCHEMA_VERSION,
                collector: self.name.clone(),
                cluster: cluster.name.clone(),
                engine: cluster.collector.engine().to_string(),
                clock: FAST_CLOCK.to_string(),
                window_start: start,
                window_end: now,
                runs,
            };
            match self.upstream.send(&batch).await {
                Ok(()) => {
                    tracing::debug!(cluster = %cluster.name, runs = batch.runs.len(), "Sent runs");
                    self.marks.insert(cluster.name.clone(), now);
                }
                Err(e) => tracing::warn!(cluster = %cluster.name, "Sending runs failed: {e:#}"),
            }
        }
    }

    /// Poll until `shutdown` resolves.
    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            self.poll_once(Utc::now()).await;
            tokio::select! {
                _ = tokio::time::sleep(self.poll) => {}
                _ = &mut shutdown => return,
            }
        }
    }

    pub fn watermark(&self, cluster: &str) -> Option<DateTime<Utc>> {
        self.marks.get(cluster).copied()
    }
}
