//! The collector against a fake Trino and a fake control plane.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use queryflux_collector::runner::{Cluster, Runner};
use queryflux_collector::trino::TrinoCollector;
use queryflux_collector::upstream::Upstream;
use queryflux_core::config::{ClusterConfig, ControlPlaneClientConfig};
use queryflux_core::query_runs::{CollectorConfigV1, QueryRunBatchV1, FAST_CLOCK};

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn fake_trino() -> String {
    serve(Router::new().route(
        "/v1/query",
        get(|| async {
            (
                [("content-type", "application/json")],
                include_str!("fixtures/trino_v1_query.json"),
            )
        }),
    ))
    .await
}

#[derive(Clone, Default)]
struct ControlPlane {
    batches: Arc<Mutex<Vec<QueryRunBatchV1>>>,
    /// Answer 503 to batches while set.
    down: Arc<Mutex<bool>>,
    config: Arc<Mutex<CollectorConfigV1>>,
}

async fn fake_control_plane(cp: ControlPlane) -> String {
    serve(
        Router::new()
            .route(
                "/collector/config",
                get(|State(cp): State<ControlPlane>| async move {
                    Json(cp.config.lock().unwrap().clone())
                }),
            )
            .route(
                "/collector/runs",
                post(
                    |State(cp): State<ControlPlane>, Json(batch): Json<QueryRunBatchV1>| async move {
                        if *cp.down.lock().unwrap() {
                            return StatusCode::SERVICE_UNAVAILABLE;
                        }
                        cp.batches.lock().unwrap().push(batch);
                        StatusCode::ACCEPTED
                    },
                ),
            )
            .with_state(cp),
    )
    .await
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

async fn runner(cp_url: &str, trino_url: &str) -> Runner {
    let upstream: ControlPlaneClientConfig = serde_json::from_value(serde_json::json!({
        "url": cp_url, "allowInsecureHttp": true, "auth": { "mode": "sharedSecret", "sharedSecret": "s3cret" }
    }))
    .unwrap();
    let cluster: ClusterConfig = serde_json::from_value(serde_json::json!({
        "engine": "trino", "endpoint": trino_url
    }))
    .unwrap();
    Runner::new(
        "c1".into(),
        Upstream::new(&upstream).unwrap(),
        vec![Cluster {
            name: "trino-a".into(),
            collector: Box::new(TrinoCollector::new("trino-a", &cluster).unwrap()),
        }],
        Duration::from_secs(30),
        chrono::Duration::minutes(2),
        chrono::Duration::minutes(30),
    )
}

#[tokio::test]
async fn sends_finished_runs_and_advances_only_after_they_are_stored() {
    let cp = ControlPlane::default();
    // Upstream says this cluster was collected up to 10:10.
    cp.config.lock().unwrap().watermarks.insert(
        "trino-a".into(),
        [(FAST_CLOCK.to_string(), at("2026-10-22T10:10:00Z"))].into(),
    );
    let cp_url = fake_control_plane(cp.clone()).await;
    let mut runner = runner(&cp_url, &fake_trino().await).await;
    runner.load_watermarks().await.unwrap();

    // The control plane is down: nothing is stored and the watermark stays.
    *cp.down.lock().unwrap() = true;
    let now = at("2026-10-22T10:30:00Z");
    runner.poll_once(now).await;
    assert_eq!(
        runner.watermark("trino-a"),
        Some(at("2026-10-22T10:10:00Z"))
    );

    // Back up: the same window is read again (from the watermark less the overlap).
    *cp.down.lock().unwrap() = false;
    runner.poll_once(now).await;
    let batches = cp.batches.lock().unwrap().clone();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.window_start, at("2026-10-22T10:08:00Z"));
    assert_eq!(batch.window_end, now);
    assert_eq!(batch.engine, "Trino");
    // Only the FAILED query ended in 10:08–10:30; the FINISHED one ended at 10:05.
    let ids: Vec<&str> = batch
        .runs
        .iter()
        .map(|r| r.engine_query_id.as_str())
        .collect();
    assert_eq!(ids, ["20261022_102000_00043_abcde"]);
    assert_eq!(runner.watermark("trino-a"), Some(now));
}

#[tokio::test]
async fn a_cluster_with_no_watermark_reads_the_lookback_window() {
    let cp = ControlPlane::default();
    let cp_url = fake_control_plane(cp.clone()).await;
    let mut runner = runner(&cp_url, &fake_trino().await).await;
    runner.load_watermarks().await.unwrap();

    runner.poll_once(at("2026-10-22T10:30:00Z")).await;
    let batches = cp.batches.lock().unwrap().clone();
    // Lookback is 30 minutes: 10:00–10:30 holds the FINISHED and FAILED queries.
    assert_eq!(batches[0].runs.len(), 2);
}
