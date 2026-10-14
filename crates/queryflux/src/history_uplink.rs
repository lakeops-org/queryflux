//! A worker's query history, sent upstream (#255).
//!
//! [`RemoteMetricsStore`] sits in the worker's metrics chain. Each finished query's record
//! is redacted under the policy in the active snapshot and queued in a bounded spool; a
//! flusher task posts batches ([`QueryRecordBatchV1`]) to the history endpoint. The query
//! path never waits on the network: when the endpoint is down the spool fills, the oldest
//! records are dropped and counted, and delivery resumes with backoff.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::config::ControlPlaneClientConfig;
use queryflux_core::error::Result;
use queryflux_core::worker_snapshot::HistoryRedaction;
use queryflux_persistence::{
    ClusterSnapshot, MetricsStore, QueryRecord, QueryRecordBatchV1, QUERY_RECORDS_SCHEMA_VERSION,
};
use tokio::sync::Notify;

use crate::config_source::WorkerSyncState;

/// Records per request.
const BATCH_SIZE: usize = 200;
/// How long a record may wait for a batch to fill.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Delivery counters, shown in `/worker/status`.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoryUplinkStatus {
    /// Where the last batch went (or would go).
    pub(crate) endpoint: Option<String>,
    pub(crate) queued: usize,
    pub(crate) sent: u64,
    /// Records dropped because the spool was full.
    pub(crate) dropped: u64,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct RemoteMetricsStore {
    spool: Mutex<VecDeque<QueryRecord>>,
    max_records: usize,
    wake: Notify,
    client: reqwest::Client,
    worker: String,
    /// `controlPlane.url`, without a trailing slash.
    control_plane: String,
    bearer: String,
    /// `history.endpoint` in the bootstrap config; wins over the snapshot's.
    endpoint_override: Option<String>,
    /// The active snapshot's history policy and the delivery counters.
    state: Arc<Mutex<WorkerSyncState>>,
}

impl RemoteMetricsStore {
    pub(crate) fn new(
        control_plane: &ControlPlaneClientConfig,
        worker: String,
        endpoint_override: Option<String>,
        max_records: usize,
        state: Arc<Mutex<WorkerSyncState>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            spool: Mutex::new(VecDeque::new()),
            max_records: max_records.max(1),
            wake: Notify::new(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            worker,
            control_plane: control_plane.url.trim_end_matches('/').to_string(),
            bearer: control_plane.auth.bearer().to_string(),
            endpoint_override,
            state,
        })
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, WorkerSyncState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The endpoint batches go to: the bootstrap override, else the snapshot's, else the
    /// control plane's own.
    fn endpoint(&self) -> String {
        self.endpoint_override
            .clone()
            .or_else(|| {
                self.lock_state()
                    .history
                    .as_ref()
                    .and_then(|h| h.endpoint.clone())
            })
            .unwrap_or_else(|| format!("{}/worker/query-records", self.control_plane))
    }

    /// Queue records, dropping the oldest beyond the spool size.
    fn enqueue(&self, records: impl IntoIterator<Item = QueryRecord>, front: bool) {
        let mut spool = self.spool.lock().unwrap_or_else(|e| e.into_inner());
        let mut items: Vec<QueryRecord> = records.into_iter().collect();
        if front {
            // A failed batch goes back ahead of newer records, in order.
            for r in items.drain(..).rev() {
                spool.push_front(r);
            }
        } else {
            spool.extend(items);
        }
        let mut dropped = 0u64;
        while spool.len() > self.max_records {
            spool.pop_front();
            dropped += 1;
        }
        let queued = spool.len();
        drop(spool);
        let mut st = self.lock_state();
        st.history_uplink.queued = queued;
        if dropped > 0 {
            st.history_uplink.dropped += dropped;
            tracing::warn!(
                dropped,
                "Query history spool full, dropped the oldest records"
            );
        }
    }

    fn take_batch(&self) -> Vec<QueryRecord> {
        let mut spool = self.spool.lock().unwrap_or_else(|e| e.into_inner());
        let n = spool.len().min(BATCH_SIZE);
        spool.drain(..n).collect()
    }

    /// Post one batch. The control plane's secret is sent only to the control plane.
    async fn send(
        &self,
        endpoint: &str,
        records: Vec<QueryRecord>,
    ) -> std::result::Result<(), (String, Vec<QueryRecord>)> {
        let batch = QueryRecordBatchV1 {
            schema_version: QUERY_RECORDS_SCHEMA_VERSION,
            worker: self.worker.clone(),
            records,
        };
        let mut req = self.client.post(endpoint).json(&batch);
        if endpoint.starts_with(&self.control_plane) {
            req = req.bearer_auth(&self.bearer);
        }
        let result = match req.send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Err(format!("history endpoint returned {status}: {body}"))
            }
            Err(e) => Err(format!("history endpoint unreachable: {e}")),
        };
        result.map_err(|e| (e, batch.records))
    }

    /// Send everything queued. Stops at the first failure, re-queuing that batch.
    async fn flush(&self) -> std::result::Result<(), String> {
        loop {
            let batch = self.take_batch();
            if batch.is_empty() {
                return Ok(());
            }
            let endpoint = self.endpoint();
            let n = batch.len() as u64;
            match self.send(&endpoint, batch).await {
                Ok(()) => {
                    let queued = self.spool.lock().unwrap_or_else(|e| e.into_inner()).len();
                    let mut st = self.lock_state();
                    st.history_uplink.endpoint = Some(endpoint);
                    st.history_uplink.sent += n;
                    st.history_uplink.queued = queued;
                    st.history_uplink.last_error = None;
                }
                Err((e, records)) => {
                    self.enqueue(records, true);
                    let mut st = self.lock_state();
                    st.history_uplink.endpoint = Some(endpoint);
                    st.history_uplink.last_error = Some(e.clone());
                    return Err(e);
                }
            }
        }
    }

    /// Deliver until shutdown, then return. Call [`Self::final_flush`] after the
    /// frontends drain, so queries that finish while draining are sent too.
    pub(crate) fn spawn_flusher(
        self: &Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::ZERO;
            loop {
                tokio::select! {
                    _ = this.wake.notified() => {}
                    _ = tokio::time::sleep(FLUSH_INTERVAL) => {}
                    _ = shutdown_rx.changed() => return,
                }
                if !backoff.is_zero() {
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown_rx.changed() => return,
                    }
                }
                match this.flush().await {
                    Ok(()) => backoff = Duration::ZERO,
                    Err(e) => {
                        backoff = (backoff * 2).clamp(Duration::from_secs(1), MAX_BACKOFF);
                        tracing::warn!(retry_in = ?backoff, "Query history upload failed: {e}");
                    }
                }
            }
        });
    }

    /// One last delivery attempt at shutdown, bounded by `timeout`.
    pub(crate) async fn final_flush(&self, timeout: Duration) {
        match tokio::time::timeout(timeout, self.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("Query history not delivered at shutdown: {e}"),
            Err(_) => tracing::warn!("Query history delivery timed out at shutdown"),
        }
        let left = self.spool.lock().unwrap_or_else(|e| e.into_inner()).len();
        if left > 0 {
            tracing::warn!(records = left, "Query history records lost at shutdown");
        }
    }
}

#[async_trait]
impl MetricsStore for RemoteMetricsStore {
    async fn record_query(&self, mut record: QueryRecord) -> Result<()> {
        let redaction = self
            .lock_state()
            .history
            .as_ref()
            .map(|h| h.redaction)
            .unwrap_or_default();
        redact(&mut record, redaction);
        let full = {
            self.enqueue([record], false);
            self.spool.lock().unwrap_or_else(|e| e.into_inner()).len() >= BATCH_SIZE
        };
        if full {
            self.wake.notify_one();
        }
        Ok(())
    }

    /// The control plane records cluster snapshots itself; with shared capacity its
    /// running counts already include every worker's leases.
    async fn record_cluster_snapshot(&self, _snapshot: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}

/// Apply a redaction policy to a record before it leaves the worker.
pub(crate) fn redact(record: &mut QueryRecord, redaction: HistoryRedaction) {
    match redaction {
        HistoryRedaction::Full => {}
        HistoryRedaction::MaskLiterals => {
            record.sql_preview = record
                .digest_text
                .clone()
                .unwrap_or_else(|| mask_literals(&record.sql_preview));
            record.rewritten_sql = record.rewritten_sql.as_deref().map(mask_literals);
            record.translated_sql = record
                .translated_digest_text
                .clone()
                .or_else(|| record.translated_sql.as_deref().map(mask_literals));
            record.error_message = record.error_message.as_deref().map(mask_literals);
        }
        HistoryRedaction::Metadata => {
            record.sql_preview = String::new();
            record.rewritten_sql = None;
            record.translated_sql = None;
            record.digest_text = None;
            record.translated_digest_text = None;
            record.error_message = None;
            // Router decisions can quote the SQL they matched on.
            record.routing_trace = None;
        }
    }
}

/// Replace quoted strings and numbers with `?`. Identifiers in double quotes or
/// backticks are kept; digits inside identifiers (`t1`, `col_2`) are kept.
pub(crate) fn mask_literals(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut prev: Option<char> = None;
    while let Some(c) = chars.next() {
        if c == '\'' {
            // A quoted string; `''` inside it is an escaped quote.
            loop {
                match chars.next() {
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                    }
                    Some('\'') | None => break,
                    Some(_) => {}
                }
            }
            out.push('?');
            prev = Some('\'');
            continue;
        }
        let in_word = prev.is_some_and(|p| p.is_alphanumeric() || p == '_');
        if c.is_ascii_digit() && !in_word {
            while chars
                .peek()
                .is_some_and(|d| d.is_ascii_digit() || *d == '.' || *d == '_')
            {
                chars.next();
            }
            out.push('?');
            prev = Some('0');
            continue;
        }
        out.push(c);
        prev = Some(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStatus, SqlDialect,
    };
    use queryflux_core::worker_snapshot::HistoryPolicy;

    fn record(id: &str) -> QueryRecord {
        QueryRecord {
            proxy_query_id: id.into(),
            backend_query_id: None,
            cluster_group: ClusterGroupName("g".into()),
            cluster_name: ClusterName("c".into()),
            cluster_group_config_id: None,
            cluster_config_id: None,
            engine_type: EngineType::DuckDb,
            frontend_protocol: FrontendProtocol::TrinoHttp,
            source_dialect: SqlDialect::Trino,
            target_dialect: SqlDialect::DuckDb,
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: true,
            translation: None,
            translated_sql: Some("SELECT * FROM t WHERE id = 42".into()),
            user: Some("u".into()),
            catalog: None,
            database: None,
            sql_preview: "SELECT * FROM t WHERE name = 'bob'".into(),
            status: QueryStatus::Failed,
            routing_trace: Some(serde_json::json!({"matched": "name = 'bob'"})),
            queue_duration_ms: 0,
            execution_duration_ms: 1,
            rows_returned: None,
            error_message: Some("no row with id 42 for 'bob'".into()),
            created_at: chrono::Utc::now(),
            engine_stats: None,
            query_tags: Default::default(),
            query_hash: Some(1),
            query_parameterized_hash: Some(2),
            translated_query_hash: None,
            digest_text: Some("SELECT * FROM t WHERE name = ?".into()),
            translated_digest_text: None,
            agent_id: None,
            conversation_id: None,
            step_index: None,
            tool_call_id: None,
            query_intent: None,
            guard_actions: vec![],
            was_guard_blocked: false,
            cache_hit: false,
            worker: None,
        }
    }

    #[test]
    fn mask_literals_hides_strings_and_numbers_but_not_identifiers() {
        assert_eq!(
            mask_literals("SELECT t1.col_2 FROM t1 WHERE a = 'it''s' AND b > 3.5 LIMIT 10"),
            "SELECT t1.col_2 FROM t1 WHERE a = ? AND b > ? LIMIT ?"
        );
    }

    #[test]
    fn redaction_policies() {
        let mut r = record("q");
        redact(&mut r, HistoryRedaction::MaskLiterals);
        assert_eq!(r.sql_preview, "SELECT * FROM t WHERE name = ?");
        assert_eq!(
            r.translated_sql.as_deref(),
            Some("SELECT * FROM t WHERE id = ?")
        );
        assert_eq!(r.error_message.as_deref(), Some("no row with id ? for ?"));

        let mut r = record("q");
        redact(&mut r, HistoryRedaction::Metadata);
        assert!(r.sql_preview.is_empty());
        assert!(r.translated_sql.is_none() && r.error_message.is_none());
        assert!(r.routing_trace.is_none() && r.digest_text.is_none());
        assert_eq!(r.query_parameterized_hash, Some(2), "fingerprints are kept");
    }

    async fn control_plane(
        store: Arc<queryflux_persistence::in_memory::InMemoryPersistence>,
    ) -> String {
        let app =
            queryflux_frontend::worker_api::router(queryflux_frontend::worker_api::WorkerApi {
                shared_secret: "s3cret".into(),
                snapshot: Arc::new(|| Box::pin(async { Err("unused".to_string()) })),
                registry: None,
                changes: None,
                coordination: None,
                history: Some(store),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn uplink(
        url: &str,
        max: usize,
        state: Arc<Mutex<WorkerSyncState>>,
    ) -> Arc<RemoteMetricsStore> {
        let cp: ControlPlaneClientConfig = serde_yaml::from_str(&format!(
            "url: {url}\nauth:\n  mode: sharedSecret\n  sharedSecret: s3cret\n"
        ))
        .unwrap();
        RemoteMetricsStore::new(&cp, "worker-a".into(), None, max, state)
    }

    async fn stored(
        store: &queryflux_persistence::in_memory::InMemoryPersistence,
    ) -> Vec<queryflux_persistence::query_history::QuerySummary> {
        use queryflux_persistence::QueryHistoryStore;
        store
            .list_queries(&queryflux_persistence::query_history::QueryFilters {
                limit: 1000,
                ..Default::default()
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn records_reach_the_control_plane_redacted_and_attributed() {
        let store = Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let url = control_plane(store.clone()).await;
        let state = Arc::new(Mutex::new(WorkerSyncState {
            history: Some(HistoryPolicy {
                endpoint: None,
                redaction: HistoryRedaction::Metadata,
            }),
            ..Default::default()
        }));
        let up = uplink(&url, 100, state.clone());
        up.record_query(record("q1")).await.unwrap();
        up.record_query(record("q2")).await.unwrap();
        up.flush().await.unwrap();

        let rows = stored(&store).await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.worker.as_deref() == Some("worker-a")));
        assert!(rows.iter().all(|r| r.sql_preview.is_empty()));
        let st = state.lock().unwrap().history_uplink.clone();
        assert_eq!((st.sent, st.queued, st.dropped), (2, 0, 0));
    }

    #[tokio::test]
    async fn an_unreachable_endpoint_spools_and_drops_the_oldest() {
        let state = Arc::new(Mutex::new(WorkerSyncState::default()));
        let up = uplink("http://127.0.0.1:9", 3, state.clone());
        for i in 0..5 {
            up.record_query(record(&format!("q{i}"))).await.unwrap();
        }
        assert!(up.flush().await.is_err());
        let st = state.lock().unwrap().history_uplink.clone();
        assert_eq!((st.queued, st.dropped, st.sent), (3, 2, 0));
        assert!(st.last_error.is_some());
        let ids: Vec<String> = up
            .spool
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.proxy_query_id.clone())
            .collect();
        assert_eq!(ids, ["q2", "q3", "q4"], "the failed batch keeps its place");
    }
}
