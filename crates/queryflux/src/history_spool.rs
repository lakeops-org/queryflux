//! A hub's query history spool (#255): workers' records, on disk until upstream
//! has them.
//!
//! Each batch a worker sends is redacted under the served snapshot's policy, then written
//! as one file (`<seq>-<records>.json`: temp file, fsync, rename) before the worker gets
//! its `202`. So a batch the hub accepted survives a hub restart, and no unredacted
//! SQL is written at the site. A flusher posts the oldest file upstream as a
//! [`QueryRecordBatchV1`] with `site` set, so each record keeps its worker's name, and
//! deletes it once upstream accepts it. When the spool is over `maxBytes`, the oldest
//! files are dropped and counted. Delivery is at least once; upstream ignores repeats
//! (`ON CONFLICT (proxy_query_id)`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_core::worker_snapshot::HistoryPolicy;
use queryflux_persistence::{
    ClusterSnapshot, MetricsStore, QueryRecord, QueryRecordBatchV1, QUERY_RECORDS_SCHEMA_VERSION,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;

use crate::history_uplink::{next_backoff, redact, retry_after, same_origin};

/// How often the flusher looks for files when nothing woke it.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// The served snapshot's history policy (redaction, and the endpoint upstream named).
pub(crate) type PolicyFn = Arc<dyn Fn() -> Option<HistoryPolicy> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
struct SpoolFile {
    bytes: u64,
    records: u64,
}

/// Delivery counters, shown in `/hub/status`.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SpoolStatus {
    pub(crate) endpoint: Option<String>,
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    pub(crate) queued_records: u64,
    pub(crate) sent: u64,
    pub(crate) dropped: u64,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct HistorySpool {
    dir: PathBuf,
    max_bytes: u64,
    next_seq: AtomicU64,
    /// Spooled files by sequence number, oldest first.
    files: Mutex<BTreeMap<u64, SpoolFile>>,
    status: Mutex<SpoolStatus>,
    wake: Notify,
    client: reqwest::Client,
    /// Name the hub reports upstream; `site` qualifies each record's worker.
    hub: String,
    site: String,
    /// The upstream control plane, without a trailing slash.
    upstream: String,
    bearer: crate::bearer::Bearer,
    policy: PolicyFn,
}

/// Write `body` to `path` durably and readable only by this user: temp file, fsync,
/// rename, then fsync the directory so the new name survives a power loss too.
pub(crate) async fn write_private(path: &std::path::Path, body: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    let mut opts = tokio::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut f = opts.open(&tmp).await?;
    f.write_all(body).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp, path).await?;
    #[cfg(unix)]
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        tokio::fs::File::open(dir).await?.sync_all().await?;
    }
    Ok(())
}

fn file_name(seq: u64, records: u64) -> String {
    format!("{seq:020}-{records}.json")
}

/// `(seq, records)` from a spool file name.
fn parse_name(name: &str) -> Option<(u64, u64)> {
    let (seq, records) = name.strip_suffix(".json")?.split_once('-')?;
    Some((seq.parse().ok()?, records.parse().ok()?))
}

impl HistorySpool {
    /// Open (or create) the spool in `dir`, picking up files left by a previous run.
    pub(crate) fn open(
        dir: PathBuf,
        max_bytes: u64,
        hub: String,
        site: String,
        upstream: &queryflux_core::config::UpstreamConfig,
        bearer: crate::bearer::Bearer,
        policy: PolicyFn,
    ) -> anyhow::Result<Arc<Self>> {
        use anyhow::Context;
        let created: Vec<PathBuf> = dir
            .ancestors()
            .take_while(|d| !d.as_os_str().is_empty() && !d.exists())
            .map(PathBuf::from)
            .collect();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create history spool directory {}", dir.display()))?;
        // A new directory's name lives in its parent: sync that too, so files acknowledged
        // into the spool don't vanish with it on a power loss.
        #[cfg(unix)]
        for d in &created {
            if let Some(parent) = d.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::File::open(parent)
                    .and_then(|f| f.sync_all())
                    .with_context(|| format!("sync directory {}", parent.display()))?;
            }
        }
        let mut files = BTreeMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            match parse_name(&name) {
                Some((seq, records)) => {
                    files.insert(
                        seq,
                        SpoolFile {
                            bytes: entry.metadata()?.len(),
                            records,
                        },
                    );
                }
                // A write interrupted before its rename was never acknowledged.
                None if name.ends_with(".tmp") => {
                    let _ = std::fs::remove_file(entry.path());
                }
                None => {}
            }
        }
        let next_seq = files.keys().next_back().map_or(0, |s| s + 1);
        if !files.is_empty() {
            tracing::info!(
                files = files.len(),
                "Query history spool has batches from a previous run; sending them upstream"
            );
        }
        let spool = Arc::new(Self {
            dir,
            max_bytes: max_bytes.max(1),
            next_seq: AtomicU64::new(next_seq),
            files: Mutex::new(files),
            status: Mutex::new(SpoolStatus::default()),
            wake: Notify::new(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            hub,
            site,
            upstream: upstream.url.trim_end_matches('/').to_string(),
            bearer,
            policy,
        });
        spool.refresh_status();
        Ok(spool)
    }

    fn path(&self, seq: u64, f: SpoolFile) -> PathBuf {
        self.dir.join(file_name(seq, f.records))
    }

    pub(crate) fn status(&self) -> SpoolStatus {
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn refresh_status(&self) {
        let (count, bytes, records) = {
            let files = self.files.lock().unwrap_or_else(|e| e.into_inner());
            (
                files.len(),
                files.values().map(|f| f.bytes).sum::<u64>(),
                files.values().map(|f| f.records).sum::<u64>(),
            )
        };
        let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
        st.files = count;
        st.bytes = bytes;
        st.queued_records = records;
        queryflux_metrics::hub::SPOOL_BYTES.set(bytes as i64);
    }

    /// Upstream's history endpoint, or its `/worker/query-records`.
    fn endpoint(&self) -> String {
        (self.policy)()
            .and_then(|p| p.endpoint)
            .unwrap_or_else(|| format!("{}/worker/query-records", self.upstream))
    }

    /// Write one batch durably, then drop the oldest files while over `maxBytes`.
    async fn write(&self, mut records: Vec<QueryRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let redaction = (self.policy)().map(|p| p.redaction).unwrap_or_default();
        for r in &mut records {
            redact(r, redaction);
        }
        let body = serde_json::to_vec(&records)
            .map_err(|e| QueryFluxError::Persistence(format!("history spool: {e}")))?;
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let file = SpoolFile {
            bytes: body.len() as u64,
            records: records.len() as u64,
        };
        // Readable only by this user: with `redaction: full` the batch holds raw SQL.
        write_private(&self.path(seq, file), &body)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("history spool: {e}")))?;

        let evicted: Vec<(u64, SpoolFile)> = {
            let mut files = self.files.lock().unwrap_or_else(|e| e.into_inner());
            files.insert(seq, file);
            let mut total: u64 = files.values().map(|f| f.bytes).sum();
            let mut evicted = Vec::new();
            while total > self.max_bytes && files.len() > 1 {
                let (&oldest, _) = files.iter().next().expect("non-empty");
                let f = files.remove(&oldest).expect("present");
                total -= f.bytes;
                evicted.push((oldest, f));
            }
            evicted
        };
        if !evicted.is_empty() {
            let dropped: u64 = evicted.iter().map(|(_, f)| f.records).sum();
            for (seq, f) in &evicted {
                let _ = tokio::fs::remove_file(self.path(*seq, *f)).await;
            }
            queryflux_metrics::hub::HISTORY_DROPPED.inc_by(dropped);
            self.status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .dropped += dropped;
            tracing::warn!(
                dropped,
                "Query history spool over hub.historySpool.maxBytes; dropped the oldest records"
            );
        }
        self.refresh_status();
        self.wake.notify_one();
        Ok(())
    }

    /// Send every spooled file, oldest first. Stops at the first failure; that file stays.
    pub(crate) async fn flush(
        &self,
    ) -> std::result::Result<(), crate::history_uplink::UploadError> {
        loop {
            let next = {
                let files = self.files.lock().unwrap_or_else(|e| e.into_inner());
                files.iter().next().map(|(s, f)| (*s, *f))
            };
            let Some((seq, file)) = next else {
                return Ok(());
            };
            let path = self.path(seq, file);
            let records: Vec<QueryRecord> = match tokio::fs::read(&path).await {
                Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                    tracing::warn!(file = %path.display(), "Unreadable history spool file, dropping it: {e}");
                    queryflux_metrics::hub::HISTORY_DROPPED.inc_by(file.records);
                    self.status.lock().unwrap_or_else(|e| e.into_inner()).dropped += file.records;
                    Vec::new()
                }),
                // Dropped by eviction meanwhile.
                Err(_) => Vec::new(),
            };
            let endpoint = self.endpoint();
            if !records.is_empty() {
                if let Err(e) = self.send(&endpoint, records).await {
                    let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
                    st.endpoint = Some(endpoint);
                    st.last_error = Some(e.to_string());
                    return Err(e);
                }
                queryflux_metrics::hub::HISTORY_SENT.inc_by(file.records);
                let mut st = self.status.lock().unwrap_or_else(|e| e.into_inner());
                st.endpoint = Some(endpoint);
                st.sent += file.records;
                st.last_error = None;
            }
            self.files
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&seq);
            let _ = tokio::fs::remove_file(&path).await;
            self.refresh_status();
        }
    }

    async fn send(
        &self,
        endpoint: &str,
        records: Vec<QueryRecord>,
    ) -> std::result::Result<(), crate::history_uplink::UploadError> {
        use crate::history_uplink::UploadError;
        let batch = QueryRecordBatchV1 {
            schema_version: QUERY_RECORDS_SCHEMA_VERSION,
            worker: self.hub.clone(),
            records,
            site: Some(self.site.clone()),
        };
        let mut req = self.client.post(endpoint).json(&batch);
        // Upstream's secret goes only to upstream's origin.
        if same_origin(endpoint, &self.upstream) {
            req = req.bearer_auth(self.bearer.get());
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let retry_after = retry_after(resp.headers());
                let body = resp.text().await.unwrap_or_default();
                Err(UploadError::new(
                    format!("history endpoint returned {status}: {body}"),
                    retry_after,
                ))
            }
            Err(e) => Err(UploadError::new(
                format!("history endpoint unreachable: {e}"),
                None,
            )),
        }
    }

    /// Deliver until shutdown.
    pub(crate) fn spawn_flusher(
        self: &Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::ZERO;
            loop {
                tokio::select! {
                    _ = this.wake.notified() => {}
                    _ = tokio::time::sleep(FLUSH_INTERVAL) => {}
                    _ = shutdown.changed() => return,
                }
                if !backoff.is_zero() {
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => return,
                    }
                }
                match this.flush().await {
                    Ok(()) => backoff = Duration::ZERO,
                    Err(e) => {
                        backoff = next_backoff(backoff, e.retry_after());
                        tracing::warn!(retry_in = ?backoff, "Query history relay failed: {e}");
                    }
                }
            }
        });
    }
}

#[async_trait]
impl MetricsStore for HistorySpool {
    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        self.write(vec![record]).await
    }

    /// One file per worker batch, written before the worker's batch is acknowledged.
    async fn record_queries(&self, records: Vec<QueryRecord>) -> Result<()> {
        self.write(records).await
    }

    async fn record_cluster_snapshot(&self, _snapshot: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_file_names_round_trip_and_sort_by_sequence() {
        assert_eq!(parse_name(&file_name(7, 200)), Some((7, 200)));
        assert!(file_name(9, 1) < file_name(10, 1));
        assert_eq!(parse_name("00000000000000000007-3.json.tmp"), None);
        assert_eq!(parse_name("notes.txt"), None);
    }
}
