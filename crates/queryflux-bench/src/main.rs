//! End-to-end overhead benchmark for QueryFlux.
//!
//! Runs two scenarios (mock Trino HTTP backend, mock StarRocks MySQL backend), each
//! comparing direct backend latency vs the same workload through QueryFlux via Trino HTTP.
//! Prints one JSON array in `customSmallerIsBetter` format for
//! `benchmark-action/github-action-benchmark`.
//!
//! Run (after `cargo build --bin queryflux`):
//!   cargo run --bin queryflux-bench
//!
//! `queryflux-bench capacity --help` runs the capacity-admission benchmark instead (lease
//! acquire/release against Postgres, with a cap-violation checker); see `src/capacity`.
//!
//! Configuration (environment):
//!   `QUERYFLUX_BENCH_WARMUP` — warmup rounds per path (default `50`).
//!   `QUERYFLUX_BENCH_ITERATIONS` — timed iterations (default `500`).
//!   `QUERYFLUX_BENCH_TRINO_POLL` — if `1`/`true`, mock Trino uses `POST` + one `GET` on a
//!   Trino-shaped `nextUri` (`/v1/statement/executing/…`, so QueryFlux can rewrite and forward
//!   poll requests). Default `0`.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::response::Json;
use axum::{routing::get, routing::post, Router};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};
use serde_json::{json, Value};
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::time::sleep;

mod capacity;

static QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
struct BenchConfig {
    warmup: usize,
    iterations: usize,
    trino_poll: bool,
}

impl BenchConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            warmup: parse_usize_env("QUERYFLUX_BENCH_WARMUP", 50)?,
            iterations: parse_usize_env("QUERYFLUX_BENCH_ITERATIONS", 500)?,
            trino_poll: env_truthy("QUERYFLUX_BENCH_TRINO_POLL"),
        })
    }
}

fn parse_usize_env(key: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(key) {
        Ok(s) if s.trim().is_empty() => Ok(default),
        Ok(s) => s
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} must be a non-negative integer")),
        Err(_) => Ok(default),
    }
}

fn env_truthy(key: &str) -> bool {
    match std::env::var(key) {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("capacity") {
        let code = capacity::main(args[1..].to_vec()).await?;
        std::process::exit(code);
    }

    let cfg = BenchConfig::from_env()?;
    eprintln!(
        "[queryflux-bench] warmup={} iterations={} trino_poll={}",
        cfg.warmup, cfg.iterations, cfg.trino_poll
    );

    let trino_entries = bench_trino(&cfg).await?;
    let starrocks_entries = bench_starrocks(&cfg).await?;

    let combined: Vec<Value> = trino_entries.into_iter().chain(starrocks_entries).collect();
    println!("{}", serde_json::to_string_pretty(&json!(combined))?);

    Ok(())
}

async fn bench_trino(cfg: &BenchConfig) -> anyhow::Result<Vec<Value>> {
    let mock_port = free_port();
    let qf_port = free_port();
    let admin_port = free_port();

    let mock_cfg = Arc::new(MockTrinoConfig {
        port: mock_port,
        two_phase: cfg.trino_poll,
    });

    let mock_listener = TcpListener::bind(format!("127.0.0.1:{mock_port}")).await?;
    let mock_cfg_serve = mock_cfg.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/statement", post(mock_trino_statement))
            .route(
                "/v1/statement/{*trino_path}",
                get(mock_trino_poll_statement),
            )
            .route("/v1/info", get(mock_trino_info))
            .route("/v1/cluster", get(mock_trino_cluster))
            .with_state(mock_cfg_serve);
        axum::serve(mock_listener, app).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;

    let config_content = format!(
        r#"queryflux:
  externalAddress: "http://127.0.0.1:{qf_port}"
  frontends:
    trinoHttp:
      enabled: true
      port: {qf_port}
  persistence:
    type: inMemory
  adminApi:
    port: {admin_port}

clusters:
  mock-trino-1:
    engine: trino
    endpoint: "http://127.0.0.1:{mock_port}"
    enabled: true

clusterGroups:
  bench-trino-group:
    enabled: true
    maxRunningQueries: 100000
    members: [mock-trino-1]

routers: []
routingFallback: bench-trino-group
"#
    );
    let config_path = std::env::temp_dir().join("queryflux-bench-trino.yaml");
    std::fs::write(&config_path, &config_content)?;

    let queryflux_bin = find_queryflux_bin()?;
    let mut qf_proc = std::process::Command::new(&queryflux_bin)
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    wait_for_health(admin_port).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let engine_label = if cfg.trino_poll {
        "Trino (POST+poll)"
    } else {
        "Trino"
    };

    eprintln!("[Trino] Warming up ({} queries each)...", cfg.warmup);
    for _ in 0..cfg.warmup {
        let _ = send_trino_until_done(&client, mock_port).await;
        let _ = send_trino_until_done(&client, qf_port).await;
    }

    eprintln!(
        "[Trino] Benchmarking direct ({} queries)...",
        cfg.iterations
    );
    let mut direct_ms = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        let t = Instant::now();
        send_trino_until_done(&client, mock_port).await?;
        direct_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    eprintln!(
        "[Trino] Benchmarking through QueryFlux ({} queries)...",
        cfg.iterations
    );
    let mut proxy_ms = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        let t = Instant::now();
        send_trino_until_done(&client, qf_port).await?;
        proxy_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    let _ = qf_proc.kill();
    let _ = std::fs::remove_file(&config_path);

    Ok(build_result_entries(engine_label, &direct_ms, &proxy_ms))
}

async fn bench_starrocks(cfg: &BenchConfig) -> anyhow::Result<Vec<Value>> {
    let mysql_port = spawn_mock_mysql_server().await;
    let qf_port = free_port();
    let admin_port = free_port();

    let mysql_url = format!("mysql://127.0.0.1:{mysql_port}");
    let config_content = format!(
        r#"queryflux:
  externalAddress: "http://127.0.0.1:{qf_port}"
  frontends:
    trinoHttp:
      enabled: true
      port: {qf_port}
  persistence:
    type: inMemory
  adminApi:
    port: {admin_port}

clusters:
  mock-sr-1:
    engine: starRocks
    endpoint: "{mysql_url}"
    enabled: true

clusterGroups:
  bench-sr-group:
    enabled: true
    maxRunningQueries: 100000
    members: [mock-sr-1]

routers: []
routingFallback: bench-sr-group
"#
    );
    let config_path = std::env::temp_dir().join("queryflux-bench-starrocks.yaml");
    std::fs::write(&config_path, &config_content)?;

    let queryflux_bin = find_queryflux_bin()?;
    let mut qf_proc = std::process::Command::new(&queryflux_bin)
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    wait_for_health(admin_port).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mysql_opts = mysql_opts_mock(&mysql_url)?;

    eprintln!("[StarRocks] Warming up ({} queries each)...", cfg.warmup);
    for _ in 0..cfg.warmup {
        let _ = send_mysql_select_one(&mysql_opts).await;
        let _ = send_trino_until_done(&client, qf_port).await;
    }

    eprintln!(
        "[StarRocks] Benchmarking direct MySQL ({} queries)...",
        cfg.iterations
    );
    let mut direct_ms = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        let t = Instant::now();
        send_mysql_select_one(&mysql_opts).await?;
        direct_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    eprintln!(
        "[StarRocks] Benchmarking through QueryFlux ({} queries)...",
        cfg.iterations
    );
    let mut proxy_ms = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        let t = Instant::now();
        send_trino_until_done(&client, qf_port).await?;
        proxy_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    let _ = qf_proc.kill();
    let _ = std::fs::remove_file(&config_path);

    Ok(build_result_entries("StarRocks", &direct_ms, &proxy_ms))
}

fn mysql_opts_mock(url: &str) -> anyhow::Result<Opts> {
    let base = Opts::from_url(url).map_err(|e| anyhow::anyhow!("invalid mysql url: {e}"))?;
    let opts = OptsBuilder::from_opts(base).prefer_socket(false).into();
    Ok(opts)
}

async fn send_mysql_select_one(opts: &Opts) -> anyhow::Result<()> {
    let mut conn = Conn::new(opts.clone()).await?;
    conn.query_drop("SELECT 1").await?;
    Ok(())
}

/// MySQL wire mock sufficient for `mysql_async` + StarRocks adapter (`SELECT 1`, `USE`, settings query).
async fn spawn_mock_mysql_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let (r, w) = socket.into_split();
            let w = BufWriter::new(w);
            tokio::spawn(async move {
                let _ = AsyncMysqlIntermediary::run_on(MockMysqlBackend, r, w).await;
            });
        }
    });

    sleep(Duration::from_millis(50)).await;
    port
}

struct MockMysqlBackend;

#[async_trait]
impl<W: tokio::io::AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MockMysqlBackend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(1, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let u = sql.trim().to_ascii_uppercase();

        if u.starts_with("USE ") {
            return results.completed(OkResponse::default()).await;
        }

        // `mysql_async` reads settings with one round-trip: SELECT @@max_allowed_packet,@@wait_timeout
        if u.contains("@@MAX_ALLOWED_PACKET") && u.contains("@@WAIT_TIMEOUT") {
            let cols = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                },
            ];
            let mut w = results.start(&cols).await?;
            w.write_col(16_777_216i64)?;
            w.write_col(28_800i64)?;
            return w.finish().await;
        }

        if is_select_one_literal(sql) {
            let cols = [Column {
                table: String::new(),
                column: "1".to_owned(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&cols).await?;
            w.write_col(1i64)?;
            return w.finish().await;
        }

        if u.contains("@@") {
            let colname = sql
                .trim()
                .strip_prefix("SELECT")
                .unwrap_or("@@var")
                .split_whitespace()
                .next()
                .unwrap_or("@@var")
                .trim_matches(',')
                .to_string();
            let cols = [Column {
                table: String::new(),
                column: colname,
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&cols).await?;
            w.write_col(0i64)?;
            return w.finish().await;
        }

        results.completed(OkResponse::default()).await
    }
}

fn is_select_one_literal(sql: &str) -> bool {
    let t = sql.trim().trim_end_matches(';').trim();
    t.eq_ignore_ascii_case("SELECT 1")
}

// ── Mock Trino HTTP ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct MockTrinoConfig {
    port: u16,
    /// When true: first `POST` returns `RUNNING` + `nextUri` under `/v1/statement/...` (QueryFlux-compatible).
    two_phase: bool,
}

fn trino_finished_json(id: &str) -> Value {
    json!({
        "id": id,
        "infoUri": "http://mock/ui/query.html",
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "runningDrivers": 0,
            "completedSplits": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "processedRows": 1,
            "processedBytes": 8,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 1,
            "physicalInputBytes": 8,
            "peakUserMemoryBytes": 0,
            "spilledBytes": 0
        },
        "columns": [{"name": "col1", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
        "data": [[1]]
    })
}

async fn mock_trino_statement(State(cfg): State<Arc<MockTrinoConfig>>) -> Json<Value> {
    let n = QUERY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let id = format!("bench_{n:08}");
    if !cfg.two_phase {
        return Json(trino_finished_json(&id));
    }
    let next_uri = format!(
        "http://127.0.0.1:{}/v1/statement/executing/{}/1",
        cfg.port, id
    );
    Json(json!({
        "id": id,
        "infoUri": "http://mock/ui/query.html",
        "nextUri": next_uri,
        "stats": {
            "state": "RUNNING",
            "queued": false,
            "scheduled": true,
            "runningDrivers": 1,
            "completedSplits": 0,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 1,
            "processedRows": 0,
            "processedBytes": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "physicalInputBytes": 0,
            "peakUserMemoryBytes": 0,
            "spilledBytes": 0
        },
        "columns": [{"name": "col1", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
    }))
}

/// Second page of a two-phase query; path matches Trino-style `/v1/statement/executing/{id}/…`.
async fn mock_trino_poll_statement(Path(trino_path): Path<String>) -> Json<Value> {
    let id = trino_path
        .strip_prefix("executing/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("bench_unknown")
        .to_string();
    Json(trino_finished_json(&id))
}

async fn mock_trino_info() -> Json<Value> {
    Json(json!({
        "starting": false,
        "nodeVersion": {"version": "mock-0.1"},
        "coordinator": true,
        "environment": "bench",
        "uptime": "0.00s"
    }))
}

async fn mock_trino_cluster() -> Json<Value> {
    Json(json!({
        "runningQueries": 0,
        "blockedQueries": 0,
        "queuedQueries": 0
    }))
}

/// Full Trino client round-trip: `POST /v1/statement`, then follow `nextUri` until a terminal page.
async fn send_trino_until_done(client: &reqwest::Client, port: u16) -> anyhow::Result<()> {
    let stmt_url = format!("http://127.0.0.1:{port}/v1/statement");
    let mut url = stmt_url;
    let mut is_post = true;

    loop {
        let resp = if is_post {
            client
                .post(&url)
                .header("X-Trino-User", "bench")
                .body("SELECT 1")
                .send()
                .await?
        } else {
            client
                .get(&url)
                .header("X-Trino-User", "bench")
                .send()
                .await?
        };

        if !resp.status().is_success() {
            anyhow::bail!("HTTP {} for {}", resp.status(), url);
        }

        let page: Value = resp.json().await?;
        let state = page
            .get("stats")
            .and_then(|s| s.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let next = page.get("nextUri").and_then(|v| v.as_str());

        if next.is_none() || state == "FINISHED" || state == "FAILED" {
            break;
        }

        url = next.unwrap().to_string();
        is_post = false;
    }

    Ok(())
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn find_queryflux_bin() -> anyhow::Result<std::path::PathBuf> {
    let current_exe = std::env::current_exe()?;
    let bin_dir = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("can't determine binary directory"))?;

    let candidate = bin_dir.join("queryflux");
    if candidate.exists() {
        return Ok(candidate);
    }

    if let Some(parent) = bin_dir.parent() {
        let candidate = parent.join("queryflux");
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    anyhow::bail!(
        "queryflux binary not found near {bin_dir:?}. Build it first with: cargo build --bin queryflux"
    )
}

async fn wait_for_health(admin_port: u16) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{admin_port}/health");
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline {
        if client
            .get(&url)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("QueryFlux did not become healthy within 15s on admin port {admin_port}")
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn stddev_pop(xs: &[f64], m: f64) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let v = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / xs.len() as f64;
    v.sqrt()
}

fn overhead_ratio_pct(direct_p50: f64, proxy_p50: f64) -> f64 {
    if direct_p50 <= f64::EPSILON {
        return 0.0;
    }
    ((proxy_p50 / direct_p50) - 1.0) * 100.0
}

fn build_result_entries(engine: &str, direct_ms: &[f64], proxy_ms: &[f64]) -> Vec<Value> {
    let mut d = direct_ms.to_vec();
    let mut p = proxy_ms.to_vec();
    d.sort_by(|a, b| a.partial_cmp(b).unwrap());
    p.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let d_p50 = percentile(&d, 50.0);
    let d_p95 = percentile(&d, 95.0);
    let d_p99 = percentile(&d, 99.0);
    let p_p50 = percentile(&p, 50.0);
    let p_p95 = percentile(&p, 95.0);
    let p_p99 = percentile(&p, 99.0);

    let d_mean = mean(direct_ms);
    let p_mean = mean(proxy_ms);
    let d_std = stddev_pop(direct_ms, d_mean);
    let p_std = stddev_pop(proxy_ms, p_mean);
    let d_min = direct_ms.iter().cloned().fold(f64::INFINITY, f64::min);
    let d_max = direct_ms.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let p_min = proxy_ms.iter().cloned().fold(f64::INFINITY, f64::min);
    let p_max = proxy_ms.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let oh_ratio = overhead_ratio_pct(d_p50, p_p50);

    eprintln!("\n── QueryFlux overhead — {engine} ─────────────────────────────────────");
    eprintln!("                  p50       p95       p99       mean      stddev");
    eprintln!(
        "  Direct        {d_p50:>6.2} ms  {d_p95:>6.2} ms  {d_p99:>6.2} ms  {:>6.2} ms  {:>6.2} ms",
        round2(d_mean),
        round2(d_std),
    );
    eprintln!(
        "  Via QueryFlux {p_p50:>6.2} ms  {p_p95:>6.2} ms  {p_p99:>6.2} ms  {:>6.2} ms  {:>6.2} ms",
        round2(p_mean),
        round2(p_std),
    );
    eprintln!(
        "  Overhead      {:>6.2} ms  {:>6.2} ms  {:>6.2} ms  {:>6.2} ms        —",
        p_p50 - d_p50,
        p_p95 - d_p95,
        p_p99 - d_p99,
        p_mean - d_mean,
    );
    eprintln!(
        "  Min / max     direct {:>6.2} / {:>6.2} ms   Proxy {:>6.2} / {:>6.2} ms",
        round2(d_min),
        round2(d_max),
        round2(p_min),
        round2(p_max),
    );
    eprintln!("  Overhead vs direct p50: {:+.1}%", round2(oh_ratio));
    eprintln!("────────────────────────────────────────────────────────────────────────\n");

    let prefix = format!("{engine} — ");
    vec![
        json!({"name": format!("{prefix}Direct p50 (ms)"), "value": round2(d_p50), "unit": "ms"}),
        json!({"name": format!("{prefix}Direct p95 (ms)"), "value": round2(d_p95), "unit": "ms"}),
        json!({"name": format!("{prefix}Direct p99 (ms)"), "value": round2(d_p99), "unit": "ms"}),
        json!({"name": format!("{prefix}Direct mean (ms)"), "value": round2(d_mean), "unit": "ms"}),
        json!({"name": format!("{prefix}Direct stddev (ms)"), "value": round2(d_std), "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p50 (ms)"), "value": round2(p_p50), "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p95 (ms)"), "value": round2(p_p95), "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p99 (ms)"), "value": round2(p_p99), "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux mean (ms)"), "value": round2(p_mean), "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux stddev (ms)"), "value": round2(p_std), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p50 (ms)"), "value": round2(p_p50 - d_p50), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p95 (ms)"), "value": round2(p_p95 - d_p95), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p99 (ms)"), "value": round2(p_p99 - d_p99), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead mean (ms)"), "value": round2(p_mean - d_mean), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead vs direct p50 (%)"), "value": round2(oh_ratio), "unit": "%"}),
    ]
}
