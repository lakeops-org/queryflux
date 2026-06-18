use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_core::{
    error::Result,
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::QueryTags,
};
use serde_json::Value;
use tracing::warn;
use uuid::Uuid;

use crate::dispatch::{execute_to_sink, ResultSink};
use crate::snowflake::http::format::sf_query_response;
use crate::state::AppState;

use super::bindings::bindings_to_params;
use super::common::{extract_snowflake_token, parse_snowflake_json_body, sf_error};

// ---------------------------------------------------------------------------
// ResultSink that accumulates Arrow batches into Snowflake JSON format
// ---------------------------------------------------------------------------

struct SnowflakeSink {
    schema: Option<Arc<Schema>>,
    batches: Vec<RecordBatch>,
    total_rows: u64,
    error: Option<String>,
}

impl SnowflakeSink {
    fn new() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            total_rows: 0,
            error: None,
        }
    }
}

#[async_trait]
impl ResultSink for SnowflakeSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.schema = Some(Arc::new(schema.clone()));
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows() as u64;
        self.batches.push(batch.clone());
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

impl SnowflakeSink {
    fn into_response(self, query_id: &str, database: &str, schema_name: &str) -> Response {
        if let Some(err) = self.error {
            return (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "data": {
                        "errorCode": "100183",
                        "sqlState": "P0001"
                    },
                    "code": "100183",
                    "message": err,
                    "success": false
                })),
            )
                .into_response();
        }

        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::empty()));

        let body = match sf_query_response(
            &schema,
            &self.batches,
            self.total_rows,
            query_id,
            database,
            schema_name,
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to serialize Arrow IPC response: {e}");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({
                        "data": {"errorCode": "100183", "sqlState": "P0001"},
                        "code": "100183",
                        "message": format!("Failed to serialize result: {e}"),
                        "success": false
                    })),
                )
                    .into_response();
            }
        };
        (StatusCode::OK, axum::Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /queries/v1/query-request  — execute SQL
pub async fn query_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let qf_token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return sf_error(StatusCode::UNAUTHORIZED, 390001, "Missing token"),
    };

    // Parse SQL from body (body may be gzip per Snowflake Python connector).
    let body_json: Value = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => {
            return sf_error(
                StatusCode::BAD_REQUEST,
                390000,
                "Invalid query request body",
            );
        }
    };
    let sql = match body_json
        .get("sqlText")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        Some(s) => s.to_string(),
        None => {
            return sf_error(
                StatusCode::BAD_REQUEST,
                390000,
                "Missing or invalid sqlText",
            );
        }
    };
    let params = bindings_to_params(body_json.get("parameterBindings"));

    // Validate TTL/idle, bump last_seen, clone fields (must not hold DashMap guard across await).
    let snapshot = match state
        .snowflake_sessions
        .validate_snowflake_session(&qf_token)
    {
        Some(v) => v.snapshot,
        None => {
            return sf_error(
                StatusCode::UNAUTHORIZED,
                390390,
                "Session not found or expired",
            );
        }
    };
    let auth_ctx = snapshot.auth_ctx;
    let group = snapshot.group;
    let user = snapshot.user;
    let database = snapshot.database.unwrap_or_default();
    let schema = snapshot.schema.unwrap_or_default();

    // Collect request headers into `extra` (lowercase keys) so that agent headers
    // (x-agent-id, x-conversation-id, etc.) are resolved lazily in dispatch.
    let extra: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_lowercase(), s.to_string()))
        })
        .collect();
    let session_ctx = SessionContext {
        user,
        database: if !database.is_empty() {
            Some(database.clone())
        } else if !schema.is_empty() {
            Some(schema.clone())
        } else {
            None
        },
        tags: QueryTags::default(),
        extra,
        agent_context: None,
    };

    let query_id = Uuid::new_v4().to_string();
    let mut sink = SnowflakeSink::new();

    if let Err(e) = execute_to_sink(
        &state,
        sql,
        params,
        session_ctx,
        FrontendProtocol::SnowflakeHttp,
        group,
        None,
        &mut sink,
        &auth_ctx,
    )
    .await
    {
        warn!(query_id = %query_id, "execute_to_sink error: {e}");
        sink.error = Some(e.to_string());
    }

    sink.into_response(&query_id, &database, &schema)
}

/// GET /queries/v1/query-monitoring-request  — async status poll (stub)
///
/// The Snowflake Python connector polls this when `asyncExec: true`.
/// For now all queries are executed synchronously; this returns a not-found
/// body (empty queries array) which causes the connector to stop polling.
pub async fn query_monitoring_request(
    State(_state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Response {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "data": {"queries": []},
            "success": true
        })),
    )
        .into_response()
}

/// DELETE /queries/v1/:query_id  — cancel query (no-op for sync execution)
pub async fn cancel_query(State(_state): State<Arc<AppState>>, _headers: HeaderMap) -> Response {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"success": true, "data": null})),
    )
        .into_response()
}
