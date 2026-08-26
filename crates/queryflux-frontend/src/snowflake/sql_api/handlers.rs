use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::cast as arrow_cast;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_auth::Credentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::QueryTags,
};
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::dispatch::ResultSink;
use crate::snowflake::http::format::schema_to_rowtype;
use crate::snowflake::http::handlers::bindings::bindings_to_params;
use crate::snowflake::http::handlers::common::parse_snowflake_json_body;
use crate::snowflake::http::SnowflakeWireState;
use crate::snowflake::in_flight::{CancelOutcome, SnowflakeExecParams, SpawnExecuteResult};
use queryflux_routing::ChainRouteResult;

// ---------------------------------------------------------------------------
// ResultSink that accumulates Arrow batches into SQL API v2 jsonv2 format
// ---------------------------------------------------------------------------

struct SqlApiSink {
    schema: Option<Arc<Schema>>,
    rows: Vec<Vec<Value>>,
    error: Option<String>,
}

impl SqlApiSink {
    fn new() -> Self {
        Self {
            schema: None,
            rows: Vec::new(),
            error: None,
        }
    }
}

#[async_trait]
impl ResultSink for SqlApiSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.schema = Some(Arc::new(schema.clone()));
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let cast_columns: Vec<CastColumn> = (0..batch.num_columns())
            .map(|col_idx| CastColumn::new(batch.column(col_idx)))
            .collect();

        for row_idx in 0..batch.num_rows() {
            let row: Vec<Value> = cast_columns
                .iter()
                .map(|col| col.value_at(row_idx))
                .collect();
            self.rows.push(row);
        }
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

impl SqlApiSink {
    fn into_response(self, handle: &str) -> Response {
        if let Some(err) = self.error {
            return (
                StatusCode::OK,
                axum::Json(json!({
                    "code": "002043",
                    "message": err,
                    "sqlState": "P0001",
                    "statementHandle": handle
                })),
            )
                .into_response();
        }

        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let num_rows = self.rows.len() as u64;
        let rowtype = schema_to_rowtype(&schema);

        (
            StatusCode::OK,
            axum::Json(json!({
                "statementHandle": handle,
                "message": "Statement executed successfully.",
                "createdOn": chrono::Utc::now().timestamp_millis(),
                "statementStatusUrl": format!("/api/v2/statements/{handle}"),
                "resultSetMetaData": {
                    "numRows": num_rows,
                    "format": "jsonv2",
                    "rowType": rowtype,
                    "partitionInfo": [{"rowCount": num_rows, "uncompressedSize": 0}]
                },
                "data": self.rows
            })),
        )
            .into_response()
    }
}

/// A column pre-cast to Utf8 so the conversion happens once per batch, not once per cell.
enum CastColumn {
    Strings(Arc<StringArray>),
    /// Values we cannot stringify for JSON without corrupting data.
    Unsupported,
}

impl CastColumn {
    fn new(arr: &Arc<dyn Array>) -> Self {
        let utf8 = if *arr.data_type() == DataType::Utf8 {
            Some(Arc::clone(arr))
        } else {
            arrow_cast(arr, &DataType::Utf8).ok()
        };
        match utf8.and_then(|a| {
            a.as_any().downcast_ref::<StringArray>().map(|s| {
                // SAFETY: we just confirmed the array is DataType::Utf8, so StringArray is the
                // correct concrete type. The downcast cannot fail here.
                Arc::new(s.clone())
            })
        }) {
            Some(sa) => Self::Strings(sa),
            None => Self::Unsupported,
        }
    }

    fn value_at(&self, row: usize) -> Value {
        match self {
            Self::Strings(arr) => {
                if arr.is_null(row) {
                    Value::Null
                } else {
                    Value::String(arr.value(row).to_string())
                }
            }
            Self::Unsupported => Value::Null,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-statement database/role/warehouse/schema — SQL API v2 is stateless, so real
// Snowflake accepts these as top-level request-body fields instead of a session `USE`.
// ---------------------------------------------------------------------------

fn statement_database(body_json: &Value) -> Option<String> {
    body_json["database"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Namespaced (`snowflake.*`) so these never collide with an unrelated `extra` key some other
/// frontend might have set if ever routed to a Snowflake ADBC cluster — see the identical
/// concern for wire v1's `USE ROLE`/`USE WAREHOUSE` handling in `handlers::query`.
fn statement_session_overrides(body_json: &Value) -> HashMap<String, String> {
    let mut overrides = HashMap::new();
    for (field, extra_key) in [
        ("role", "snowflake.role"),
        ("warehouse", "snowflake.warehouse"),
        ("schema", "snowflake.schema"),
    ] {
        if let Some(value) = body_json[field].as_str().filter(|s| !s.is_empty()) {
            overrides.insert(extra_key.to_string(), value.to_string());
        }
    }
    overrides
}

// ---------------------------------------------------------------------------
// SQL API v2 error helper — preserves the real HTTP status code
// ---------------------------------------------------------------------------

fn sql_api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "code": code,
            "message": message,
            "sqlState": "P0001",
            "statementHandle": ""
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v2/statements  — submit SQL, execute synchronously, return jsonv2
pub async fn submit_statement(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_json: Value = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => return sql_api_error(StatusCode::BAD_REQUEST, "390000", "Invalid JSON body"),
    };
    let Some(sql) = body_json["statement"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
    else {
        return sql_api_error(
            StatusCode::BAD_REQUEST,
            "390000",
            "Missing or empty statement",
        );
    };
    // SQL API v2 uses "bindings" (not "parameterBindings").
    let sql = sql.to_string();
    let params = bindings_to_params(body_json.get("bindings"));

    // Stateless auth: Bearer token in Authorization header.
    let auth_ctx = match authenticate(&state.app, &headers).await {
        Ok(ctx) => ctx,
        Err(e) => return sql_api_error(StatusCode::UNAUTHORIZED, "390002", &e.to_string()),
    };

    // Collect request headers into `extra` (lowercase keys) so that agent headers
    // (x-agent-id, x-conversation-id, etc.) are resolved lazily in dispatch.
    let mut extra: HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_lowercase(), s.to_string()))
        })
        .collect();

    // SQL API v2 is stateless — there is no session to intercept a mid-stream `USE ROLE`/
    // `USE WAREHOUSE` into (unlike wire v1's session-based login), so real Snowflake accepts
    // these as per-statement fields on the request body instead. Read them the same way.
    let database = statement_database(&body_json);
    extra.extend(statement_session_overrides(&body_json));

    let session_ctx = SessionContext {
        user: Some(auth_ctx.user.clone()),
        database,
        tags: QueryTags::default(),
        extra,
        agent_context: None,
    };
    let routing_result = state
        .app
        .route_query(
            sql,
            &session_ctx,
            &FrontendProtocol::SnowflakeSqlApi,
            Some(&auth_ctx),
        )
        .await;
    let (sql, chain_result, routing_trace) = match routing_result {
        Ok(r) => r,
        Err(QueryFluxError::Unauthorized(msg)) => {
            return sql_api_error(StatusCode::FORBIDDEN, "390201", &msg);
        }
        Err(e) => return sql_api_error(StatusCode::BAD_GATEWAY, "390000", &e.to_string()),
    };
    // AppState::route_query already resolved the authorization-aware fallback group
    // (if the chain used one), so `g` here is final — no separate call needed.
    let group = match chain_result {
        ChainRouteResult::Routed(g) => g,
        ChainRouteResult::Denied { message } => {
            state.app.record_routing_deny(
                &sql,
                &session_ctx,
                FrontendProtocol::SnowflakeSqlApi,
                &message,
                Some(routing_trace),
            );
            return sql_api_error(StatusCode::FORBIDDEN, "390201", &message);
        }
    };

    let handle = Uuid::new_v4().to_string();

    let exec = SnowflakeExecParams {
        sql,
        params,
        session_ctx,
        protocol: FrontendProtocol::SnowflakeSqlApi,
        group,
        auth_ctx: auth_ctx.clone(),
    };

    match crate::snowflake::in_flight::spawn_execute(
        &state.app,
        &state.in_flight,
        handle.clone(),
        auth_ctx.user.clone(),
        exec,
        SqlApiSink::new,
    )
    .await
    {
        SpawnExecuteResult::Completed(Ok(()), sink) => sink.into_response(&handle),
        SpawnExecuteResult::Completed(Err(e), mut sink) => {
            warn!(handle = %handle, "SQL API execute_to_sink error: {e}");
            sink.error = Some(e.to_string());
            sink.into_response(&handle)
        }
        SpawnExecuteResult::Cancelled => (
            StatusCode::OK,
            axum::Json(json!({
                "code": "000630",
                "message": "Statement aborted.",
                "sqlState": "57014",
                "statementHandle": handle
            })),
        )
            .into_response(),
        SpawnExecuteResult::JoinFailed(e) => {
            warn!(handle = %handle, "SQL API query task failed: {e}");
            sql_api_error(StatusCode::INTERNAL_SERVER_ERROR, "390000", &e)
        }
    }
}

/// GET /api/v2/statements/:handle  — stub (sync execution, nothing to poll)
pub async fn get_statement(
    State(_state): State<SnowflakeWireState>,
    _headers: HeaderMap,
    axum::extract::Path(handle): axum::extract::Path<String>,
    _raw_query: axum::extract::RawQuery,
) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({
            "code": "390142",
            "message": format!("Statement handle {handle} not found or already complete."),
            "sqlState": "02000",
            "statementHandle": handle
        })),
    )
        .into_response()
}

/// DELETE /api/v2/statements/:handle  — abort an in-flight statement
pub async fn cancel_statement(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
    axum::extract::Path(handle): axum::extract::Path<String>,
) -> Response {
    let auth_ctx = match authenticate(&state.app, &headers).await {
        Ok(ctx) => ctx,
        Err(e) => return sql_api_error(StatusCode::UNAUTHORIZED, "390002", &e.to_string()),
    };

    match state.in_flight.cancel(&handle, &auth_ctx) {
        CancelOutcome::Aborted | CancelOutcome::NotFound => (
            StatusCode::OK,
            axum::Json(json!({
                "statementHandle": handle,
                "message": "Statement aborted.",
            })),
        )
            .into_response(),
        CancelOutcome::Forbidden => sql_api_error(
            StatusCode::FORBIDDEN,
            "390403",
            "Statement belongs to a different user.",
        ),
    }
}

// ---------------------------------------------------------------------------
// Auth helper
// ---------------------------------------------------------------------------

async fn authenticate(
    state: &std::sync::Arc<crate::state::AppState>,
    headers: &HeaderMap,
) -> std::result::Result<queryflux_auth::AuthContext, String> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(crate::strip_bearer_prefix)
        .map(|s| s.to_string());

    let auth_provider = state.live.read().await.auth_provider.clone();
    auth_provider
        .authenticate(&Credentials {
            username: None,
            password: None,
            bearer_token: bearer,
        })
        .await
        .map_err(|e| {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::SnowflakeSqlApi));
            e.to_string()
        })
}

#[cfg(test)]
mod tests {
    use super::{statement_database, statement_session_overrides};
    use serde_json::json;

    #[test]
    fn statement_database_reads_top_level_field() {
        let body = json!({"statement": "SELECT 1", "database": "PROD"});
        assert_eq!(statement_database(&body).as_deref(), Some("PROD"));
    }

    #[test]
    fn statement_database_absent_when_missing_or_empty() {
        assert_eq!(statement_database(&json!({"statement": "SELECT 1"})), None);
        assert_eq!(
            statement_database(&json!({"statement": "SELECT 1", "database": ""})),
            None
        );
    }

    #[test]
    fn statement_session_overrides_reads_role_warehouse_schema_namespaced() {
        let body = json!({
            "statement": "SELECT 1",
            "role": "ANALYST",
            "warehouse": "ANALYTICS_WH",
            "schema": "PUBLIC",
        });
        let overrides = statement_session_overrides(&body);
        assert_eq!(
            overrides.get("snowflake.role").map(String::as_str),
            Some("ANALYST")
        );
        assert_eq!(
            overrides.get("snowflake.warehouse").map(String::as_str),
            Some("ANALYTICS_WH")
        );
        assert_eq!(
            overrides.get("snowflake.schema").map(String::as_str),
            Some("PUBLIC")
        );
        // Bare (unnamespaced) keys must never appear — that's what avoids the cross-protocol
        // collision risk if this ever shared an `extra` map convention with another frontend.
        assert!(!overrides.contains_key("role"));
    }

    #[test]
    fn statement_session_overrides_empty_when_no_fields_present() {
        let body = json!({"statement": "SELECT 1"});
        assert!(statement_session_overrides(&body).is_empty());
    }

    #[test]
    fn statement_session_overrides_skips_empty_strings() {
        let body = json!({"statement": "SELECT 1", "role": ""});
        assert!(statement_session_overrides(&body).is_empty());
    }
}
