//! Snowflake HTTP wire v1 query handlers.
//!
//! POST /queries/v1/query-request            — execute SQL, return Arrow IPC
//! GET  /queries/v1/query-monitoring-request — list in-flight queries for session
//! DELETE /queries/v1/:query_id              — cancel in-flight query

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_core::{
    error::Result,
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::QueryTags,
};
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

use crate::dispatch::ResultSink;
use crate::snowflake::http::format::sf_query_response;
use crate::snowflake::http::handlers::bindings::bindings_to_params;
use crate::snowflake::http::handlers::common::{
    extract_snowflake_token, parse_snowflake_json_body,
};
use crate::snowflake::http::SnowflakeWireState;
use crate::snowflake::in_flight::{CancelOutcome, SnowflakeExecParams, SpawnExecuteResult};

// ---------------------------------------------------------------------------
// SnowflakeSink — accumulates Arrow batches, serialises via sf_query_response
// ---------------------------------------------------------------------------

struct SnowflakeSink {
    schema: Option<Arc<Schema>>,
    batches: Vec<RecordBatch>,
    error: Option<String>,
}

impl SnowflakeSink {
    fn new() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            error: None,
        }
    }

    fn into_response(self, query_id: &str, database: &str, schema_name: &str) -> Response {
        if let Some(err) = self.error {
            return (
                StatusCode::OK,
                axum::Json(json!({
                    "data": null,
                    "message": err,
                    "success": false,
                    "code": "002043"
                })),
            )
                .into_response();
        }

        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let total_rows = self.batches.iter().map(|b| b.num_rows() as u64).sum();

        match sf_query_response(
            &schema,
            &self.batches,
            total_rows,
            query_id,
            database,
            schema_name,
        ) {
            Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
            Err(e) => (
                StatusCode::OK,
                axum::Json(json!({
                    "data": null,
                    "message": format!("Arrow serialisation error: {e}"),
                    "success": false,
                    "code": "002043"
                })),
            )
                .into_response(),
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

// ---------------------------------------------------------------------------
// POST /queries/v1/query-request
// ---------------------------------------------------------------------------

pub async fn query_request(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };

    // Validate session and extract stored context.
    let (auth_ctx, group, database, schema_name) = {
        match state.sessions.validate_session(&token) {
            Some((_, session)) => (
                session.auth_ctx.clone(),
                session.group.clone(),
                session.database.clone().unwrap_or_default(),
                session.schema.clone().unwrap_or_default(),
            ),
            None => return unauthorized(),
        }
    };

    let body_json = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => return sf_error("390000", "Invalid request body"),
    };

    let sql = match body_json["sqlText"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
    {
        Some(s) => s.to_string(),
        None => return sf_error("390000", "Missing or empty sqlText"),
    };

    // Wire v1 uses "parameterBindings" (SQL API v2 uses "bindings").
    let params = bindings_to_params(body_json.get("parameterBindings"));

    let session_ctx = SessionContext {
        user: Some(auth_ctx.user.clone()),
        database: Some(database.clone()),
        tags: QueryTags::default(),
        extra: Default::default(),
        agent_context: None,
    };

    let query_id = Uuid::new_v4().to_string();

    let exec = SnowflakeExecParams {
        sql,
        params,
        session_ctx,
        protocol: FrontendProtocol::SnowflakeHttp,
        group,
        auth_ctx: auth_ctx.clone(),
    };

    match crate::snowflake::in_flight::spawn_execute(
        &state.app,
        &state.in_flight,
        query_id.clone(),
        auth_ctx.user.clone(),
        exec,
        SnowflakeSink::new,
    )
    .await
    {
        SpawnExecuteResult::Completed(Ok(()), sink) => {
            sink.into_response(&query_id, &database, &schema_name)
        }
        SpawnExecuteResult::Completed(Err(e), mut sink) => {
            warn!(query_id = %query_id, "Snowflake wire query error: {e}");
            sink.error = Some(e.to_string());
            sink.into_response(&query_id, &database, &schema_name)
        }
        SpawnExecuteResult::Cancelled => (
            StatusCode::OK,
            axum::Json(json!({
                "data": null,
                "message": "Query cancelled.",
                "success": false,
                "code": "000630"
            })),
        )
            .into_response(),
        SpawnExecuteResult::JoinFailed(e) => {
            warn!(query_id = %query_id, "Snowflake wire query task failed: {e}");
            sf_error("390000", &format!("Query execution failed: {e}"))
        }
    }
}

// ---------------------------------------------------------------------------
// GET /queries/v1/query-monitoring-request  — async poll stub
// ---------------------------------------------------------------------------

pub async fn query_monitoring_request(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };

    let user = match state.sessions.validate_session(&token) {
        Some((_, session)) => session.auth_ctx.user.clone(),
        None => return unauthorized(),
    };

    let ids = state.in_flight.ids_for_owner(&user);
    let queries: Vec<_> = ids
        .into_iter()
        .map(|id| {
            json!({
                "id": id,
                "status": "RUNNING",
            })
        })
        .collect();

    (
        StatusCode::OK,
        axum::Json(json!({
            "data": {"queries": queries},
            "message": null,
            "success": true,
            "code": null
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// DELETE /queries/v1/:query_id  — cancel stub
// ---------------------------------------------------------------------------

pub async fn cancel_query(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
    Path(query_id): Path<String>,
) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };

    let auth_ctx = match state.sessions.validate_session(&token) {
        Some((_, session)) => session.auth_ctx.clone(),
        None => return unauthorized(),
    };

    match state.in_flight.cancel(&query_id, &auth_ctx) {
        CancelOutcome::Aborted | CancelOutcome::NotFound => (
            StatusCode::OK,
            axum::Json(json!({
                "data": null,
                "message": null,
                "success": true,
                "code": null
            })),
        )
            .into_response(),
        CancelOutcome::Forbidden => (
            StatusCode::FORBIDDEN,
            axum::Json(json!({
                "data": null,
                "message": "Query belongs to a different user.",
                "success": false,
                "code": "390403"
            })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sf_error(code: &str, message: &str) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({
            "data": null,
            "message": message,
            "success": false,
            "code": code
        })),
    )
        .into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({
            "data": null,
            "message": "Session token is invalid or has expired.",
            "success": false,
            "code": "390111"
        })),
    )
        .into_response()
}
