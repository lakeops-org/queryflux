//! A worker's query history, sent upstream (#255).
//!
//! [`RemoteMetricsStore`] sits in the worker's metrics chain. Each finished query's record
//! is redacted under the policy in the active snapshot and queued in a bounded spool; a
//! flusher task posts batches ([`QueryRecordBatchV1`]) to the history endpoint. The query
//! path never waits on the network: when the endpoint is down the spool fills, the oldest
//! records are dropped and counted, and delivery resumes with backoff. Any non-2xx answer
//! keeps the batch spooled; a `503` with `Retry-After` (the control plane's ingest is busy)
//! sets the next attempt's delay instead of the exponential backoff.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::config::ControlPlaneClientConfig;
use queryflux_core::error::Result;
use queryflux_core::worker_snapshot::HistoryRedaction;
use queryflux_persistence::{
    ClusterSnapshot, MetricsStore, QueryRecord, QueryRecordBatchV1, QUERY_RECORDS_SCHEMA_VERSION,
};

use crate::config_source::WorkerSyncState;
use crate::spool::{retry_after, Spool, UploadError, Uploader};

/// Records per request.
const BATCH_SIZE: usize = 200;

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
    spool: Spool<QueryRecord>,
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
            spool: Spool::new(max_records),
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
        let (queued, dropped) = self.spool.push(records, front);
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

    /// Post one batch. The control plane's secret is sent only to the control plane.
    async fn send(
        &self,
        endpoint: &str,
        records: Vec<QueryRecord>,
    ) -> std::result::Result<(), (UploadError, Vec<QueryRecord>)> {
        let batch = QueryRecordBatchV1 {
            schema_version: QUERY_RECORDS_SCHEMA_VERSION,
            worker: self.worker.clone(),
            records,
        };
        let mut req = self.client.post(endpoint).json(&batch);
        if same_origin(endpoint, &self.control_plane) {
            req = req.bearer_auth(&self.bearer);
        }
        let result = match req.send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let retry_after = retry_after(resp.headers());
                let body = resp.text().await.unwrap_or_default();
                Err(UploadError {
                    message: format!("history endpoint returned {status}: {body}"),
                    retry_after,
                })
            }
            Err(e) => Err(UploadError {
                message: format!("history endpoint unreachable: {e}"),
                retry_after: None,
            }),
        };
        result.map_err(|e| (e, batch.records))
    }
}

impl Uploader for RemoteMetricsStore {
    type Item = QueryRecord;
    const WHAT: &'static str = "Query history";

    fn spool(&self) -> &Spool<QueryRecord> {
        &self.spool
    }

    async fn flush(&self) -> std::result::Result<(), UploadError> {
        loop {
            let batch = self.spool.take(BATCH_SIZE);
            if batch.is_empty() {
                return Ok(());
            }
            let endpoint = self.endpoint();
            let n = batch.len() as u64;
            match self.send(&endpoint, batch).await {
                Ok(()) => {
                    let queued = self.spool.len();
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
                    st.history_uplink.last_error = Some(e.message.clone());
                    return Err(e);
                }
            }
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
        self.enqueue([record], false);
        if self.spool.len() >= BATCH_SIZE {
            self.spool.wake();
        }
        Ok(())
    }

    /// The control plane asks for datasets when it emits OpenLineage events.
    fn wants_datasets(&self) -> bool {
        self.lock_state()
            .history
            .as_ref()
            .is_some_and(|h| h.datasets)
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
            record.sql = record.sql.as_deref().map(|sql| {
                record
                    .digest_text
                    .clone()
                    .unwrap_or_else(|| mask_literals(sql))
            });
            record.rewritten_sql = record.rewritten_sql.as_deref().map(mask_literals);
            record.translated_sql = record
                .translated_digest_text
                .clone()
                .or_else(|| record.translated_sql.as_deref().map(mask_literals));
            record.error_message = record.error_message.as_deref().map(mask_literals);
            if let Some(trace) = record.routing_trace.as_mut() {
                mask_json_strings(trace);
            }
            // Webhook and script guards return free-form text that can quote the query.
            for g in &mut record.guard_actions {
                g.reason = g.reason.as_deref().map(mask_literals);
                if let Some(metadata) = g.metadata.as_mut() {
                    metadata.values_mut().for_each(|v| *v = mask_literals(v));
                }
            }
        }
        HistoryRedaction::Metadata => {
            record.sql_preview = String::new();
            record.sql = None;
            record.rewritten_sql = None;
            record.translated_sql = None;
            record.digest_text = None;
            record.translated_digest_text = None;
            record.error_message = None;
            // Router decisions can quote the SQL they matched on.
            record.routing_trace = None;
            // Guard text can quote the query or its error; keep which guard did what.
            for g in &mut record.guard_actions {
                g.reason = None;
                g.metadata = None;
            }
        }
    }
}

/// Whether two URLs share scheme, host and port. Decides whether the control plane's
/// secret may be sent; a string prefix would also match `https://cp.example.com.evil.net`
/// or another port on the same host.
pub(crate) fn same_origin(a: &str, b: &str) -> bool {
    match (reqwest::Url::parse(a), reqwest::Url::parse(b)) {
        (Ok(a), Ok(b)) => a.origin() == b.origin(),
        _ => false,
    }
}

/// Mask literals in every string inside `value` (a routing trace can quote the SQL it
/// matched on).
fn mask_json_strings(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(s) => *s = mask_literals(s),
        serde_json::Value::Array(items) => items.iter_mut().for_each(mask_json_strings),
        serde_json::Value::Object(map) => map.values_mut().for_each(mask_json_strings),
        _ => {}
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
    use crate::spool::next_backoff;
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
            guard_actions: vec![queryflux_persistence::GuardAction {
                guard: "pii-webhook".into(),
                action: "warn".into(),
                reason: Some("query filters on name = 'bob'".into()),
                code: Some("PII".into()),
                metadata: Some([("matched".to_string(), "'bob' at 42".to_string())].into()),
            }],
            was_guard_blocked: false,
            cache_hit: false,
            worker: None,
            datasets: vec![],
            sql: None,
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
        assert_eq!(
            r.routing_trace,
            Some(serde_json::json!({"matched": "name = ?"}))
        );
        let g = &r.guard_actions[0];
        assert_eq!(g.reason.as_deref(), Some("query filters on name = ?"));
        assert_eq!(g.metadata.as_ref().unwrap()["matched"], "? at ?");
        assert_eq!(
            (g.guard.as_str(), g.code.as_deref()),
            ("pii-webhook", Some("PII"))
        );

        let mut r = record("q");
        redact(&mut r, HistoryRedaction::Metadata);
        assert!(r.sql_preview.is_empty());
        assert!(r.translated_sql.is_none() && r.error_message.is_none());
        assert!(r.routing_trace.is_none() && r.digest_text.is_none());
        assert_eq!(r.query_parameterized_hash, Some(2), "fingerprints are kept");
        let g = &r.guard_actions[0];
        assert!(g.reason.is_none() && g.metadata.is_none());
        assert_eq!(
            (g.action.as_str(), g.code.as_deref()),
            ("warn", Some("PII"))
        );
    }

    #[test]
    fn the_secret_goes_only_to_the_control_planes_origin() {
        let cp = "https://cp.example.com";
        assert!(same_origin(
            "https://cp.example.com/worker/query-records",
            cp
        ));
        assert!(same_origin("https://cp.example.com:443/x", cp));
        assert!(!same_origin(
            "https://cp.example.com.attacker.net/ingest",
            cp
        ));
        assert!(!same_origin("https://cp.example.com:9443/ingest", cp));
        assert!(!same_origin("http://cp.example.com/ingest", cp));
        assert!(!same_origin("not a url", cp));
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
                quotas: None,
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
                ..Default::default()
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
            .items()
            .iter()
            .map(|r| r.proxy_query_id.clone())
            .collect();
        assert_eq!(ids, ["q2", "q3", "q4"], "the failed batch keeps its place");
    }

    /// A busy control plane answers 503 with `Retry-After`: the batch stays spooled, in
    /// order, the next attempt waits as told, and the retry delivers it.
    #[tokio::test]
    async fn a_busy_control_plane_keeps_the_batch_spooled_for_retry() {
        use axum::http::{header, StatusCode};
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let busy = Arc::new(AtomicBool::new(true));
        let received = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new().route(
            "/worker/query-records",
            axum::routing::post({
                let (busy, received) = (busy.clone(), received.clone());
                move |axum::Json(batch): axum::Json<QueryRecordBatchV1>| async move {
                    if busy.load(Ordering::SeqCst) {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            [(header::RETRY_AFTER, "3")],
                        )
                            .into_response();
                    }
                    received.fetch_add(batch.records.len(), Ordering::SeqCst);
                    StatusCode::ACCEPTED.into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let state = Arc::new(Mutex::new(WorkerSyncState::default()));
        let up = uplink(&format!("http://{addr}"), 100, state.clone());
        up.record_query(record("q1")).await.unwrap();
        up.record_query(record("q2")).await.unwrap();

        let err = up.flush().await.unwrap_err();
        assert!(err.message.contains("503"), "{err}");
        assert_eq!(err.retry_after, Some(Duration::from_secs(3)));
        let wait = next_backoff(Duration::ZERO, err.retry_after);
        assert!(wait >= Duration::from_secs(3) && wait <= Duration::from_millis(4500));
        let ids: Vec<String> = up
            .spool
            .items()
            .iter()
            .map(|r| r.proxy_query_id.clone())
            .collect();
        assert_eq!(ids, ["q1", "q2"]);
        let st = state.lock().unwrap().history_uplink.clone();
        assert_eq!((st.sent, st.queued, st.dropped), (0, 2, 0));

        busy.store(false, Ordering::SeqCst);
        up.flush().await.unwrap();
        assert_eq!(received.load(Ordering::SeqCst), 2);
        let st = state.lock().unwrap().history_uplink.clone();
        assert_eq!((st.sent, st.queued), (2, 0));
        assert!(st.last_error.is_none());
    }
}
