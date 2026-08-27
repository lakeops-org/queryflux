//! Minimal demonstration of `QueryFlux::builder()`: an in-memory DuckDB cluster from
//! `queryflux-config.yaml`, plus every kind of compiled-in plugin the builder supports —
//! a custom [`Guard`], a custom [`RouterTrait`], a logging [`QueryHook`], and a tiny
//! extra HTTP frontend that dispatches queries outside the normal Trino/Postgres/MySQL
//! wire protocols.
//!
//! Run with `cargo run -p embed-queryflux`, then either:
//! - point a Trino client at `localhost:8080` (the built-in frontend), or
//! - `curl -X POST localhost:8090/query -d 'SELECT 42'`  (the extra frontend below).
//!
//! Try `curl -X POST localhost:8090/query -d 'DROP TABLE t'` to see `NoDdlGuard` deny it.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use queryflux::QueryFlux;
use queryflux_auth::AuthContext;
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_core::query::{FrontendProtocol, QueryStats};
use queryflux_core::session::SessionContext;
use queryflux_frontend::dispatch::{execute_to_sink, ResultSink};
use queryflux_frontend::hook::{HookContext, HookOutcome, QueryHook};
use queryflux_frontend::state::AppState;
use queryflux_frontend::{FrontendListenerTrait, ShutdownRx};
use queryflux_guardrails::built_in::Guard;
use queryflux_guardrails::{GuardContext, GuardLayer, GuardResult};
use queryflux_routing::{ChainRouteResult, RouterTrait, RoutingDecision};

/// Denies any query whose translated SQL contains a DDL verb. Appended after the
/// YAML-configured guard chain (there isn't one here) via `.guard(...)`.
struct NoDdlGuard;

#[async_trait]
impl Guard for NoDdlGuard {
    fn name(&self) -> &'static str {
        "no_ddl"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        let upper = ctx.translated_sql.to_uppercase();
        let is_ddl = ["DROP ", "CREATE ", "ALTER ", "TRUNCATE "]
            .iter()
            .any(|verb| upper.trim_start().starts_with(verb));
        if is_ddl {
            GuardResult::deny("DDL is disabled in this deployment", "NO_DDL")
        } else {
            GuardResult::allow()
        }
    }
}

/// Logs every query's SQL and protocol, then defers to the YAML-configured router
/// chain by returning `NoMatch`. Registered via `.router_prepend(...)` so it sees
/// every query first, before `queryflux-config.yaml`'s `protocolBased` router picks a group.
struct LoggingRouter;

#[async_trait]
impl RouterTrait for LoggingRouter {
    fn type_name(&self) -> &'static str {
        "Logging"
    }
    async fn route(
        &self,
        sql: &str,
        _session: &SessionContext,
        protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        tracing::info!(
            protocol = ?protocol,
            user = ?auth_ctx.map(|a| a.user.as_str()),
            sql,
            "incoming query"
        );
        Ok(RoutingDecision::NoMatch)
    }
}

/// Logs the lifecycle of every query. Implements only the hooks it cares about —
/// everything else falls back to `QueryHook`'s no-op defaults.
struct AuditHook;

#[async_trait]
impl QueryHook for AuditHook {
    async fn before_execute(&self, ctx: &HookContext<'_>) -> HookOutcome {
        tracing::info!(sql = %ctx.sql, group = ?ctx.group.map(|g| &g.0), "before_execute");
        HookOutcome::Continue
    }

    async fn after_execute(&self, ctx: &HookContext<'_>) {
        tracing::info!(sql = %ctx.sql, "after_execute");
    }

    async fn on_error(&self, ctx: &HookContext<'_>, err: &QueryFluxError) {
        tracing::warn!(sql = %ctx.sql, error = %err, "on_error");
    }
}

/// Collects query results into a small JSON summary instead of the wire-protocol
/// framing a real frontend would use.
#[derive(Default)]
struct CountingSink {
    rows: u64,
    error: Option<String>,
}

#[async_trait]
impl ResultSink for CountingSink {
    async fn on_schema(&mut self, _schema: &arrow::datatypes::Schema) -> Result<()> {
        Ok(())
    }
    async fn on_batch(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        Ok(())
    }
    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.error = Some(message.to_string());
        Ok(())
    }
}

/// A tiny extra frontend: `POST /query` with a raw SQL body, routed through the exact
/// same dispatch pipeline (routing, translation, guardrails, hooks, execution) as the
/// built-in Trino/Postgres/MySQL frontends — just with a much simpler wire format.
struct TinyHttpFrontend {
    state: Arc<AppState>,
}

async fn handle_query(State(state): State<Arc<AppState>>, body: String) -> Response {
    let auth = AuthContext {
        user: "tiny-http".to_string(),
        ..Default::default()
    };
    let session = SessionContext::default();
    let protocol = FrontendProtocol::Custom {
        name: "tiny-http".to_string(),
        dialect: queryflux_core::query::SqlDialect::DuckDb,
    };

    // Route first, same as every built-in frontend — this is what runs LoggingRouter
    // and the before_route/after_route hooks, and picks up a RoutingTrace.
    let (sql, group) = match state
        .route_query(body, &session, &protocol, Some(&auth))
        .await
    {
        Ok((sql, ChainRouteResult::Routed(group), _trace)) => (sql, group),
        Ok((_, ChainRouteResult::Denied { message }, _)) => {
            return (
                axum::http::StatusCode::FORBIDDEN,
                serde_json::json!({ "error": message }).to_string(),
            )
                .into_response();
        }
        Err(e) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": e.to_string() }).to_string(),
            )
                .into_response();
        }
    };

    let mut sink = CountingSink::default();
    let _ = execute_to_sink(
        &state,
        sql,
        vec![],
        session,
        protocol,
        group,
        &mut sink,
        &auth,
    )
    .await;

    match sink.error {
        Some(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            serde_json::json!({ "error": e }).to_string(),
        )
            .into_response(),
        None => serde_json::json!({ "rows": sink.rows })
            .to_string()
            .into_response(),
    }
}

#[async_trait]
impl FrontendListenerTrait for TinyHttpFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let app = Router::new()
            .route("/query", post(handle_query))
            .with_state(self.state.clone());
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8090")
            .await
            .map_err(|e| QueryFluxError::Engine(format!("tiny-http bind failed: {e}")))?;
        tracing::info!("tiny-http frontend listening on :8090");
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.wait_for(|v| *v).await;
            })
            .await
            .map_err(|e| QueryFluxError::Engine(format!("tiny-http serve failed: {e}")))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    QueryFlux::builder()
        .config_path(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/queryflux-config.yaml"
        ))
        .with_builtin_plugins()
        .guard(Box::new(NoDdlGuard))
        .router_prepend(Box::new(LoggingRouter))
        .hook(Arc::new(AuditHook))
        .frontend(|state| Box::new(TinyHttpFrontend { state }))
        .build()
        .await?
        .serve()
        .await?;
    Ok(())
}
