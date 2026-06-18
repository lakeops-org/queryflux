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
    error::Result,
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::QueryTags,
};
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::dispatch::{route_and_execute, ResultSink};
use crate::snowflake::http::format::schema_to_rowtype;
use crate::snowflake::http::handlers::bindings::bindings_to_params;
use crate::snowflake::http::handlers::common::parse_snowflake_json_body;
use crate::state::AppState;

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
    Strings(Arc<dyn Array>),
    /// Values we cannot stringify for JSON without corrupting data.
    Unsupported,
}

impl CastColumn {
    fn new(arr: &Arc<dyn Array>) -> Self {
        if *arr.data_type() == DataType::Utf8 {
            return Self::Strings(Arc::clone(arr));
        }
        match arrow_cast(arr, &DataType::Utf8) {
            Ok(casted) => Self::Strings(casted),
            Err(_) => Self::Unsupported,
        }
    }

    fn value_at(&self, row: usize) -> Value {
        match self {
            Self::Strings(arr) => {
                if arr.is_null(row) {
                    return Value::Null;
                }
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(str_arr.value(row).to_string())
            }
            Self::Unsupported => Value::Null,
        }
    }
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
    State(state): State<Arc<AppState>>,
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
    let auth_ctx = match authenticate(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(e) => return sql_api_error(StatusCode::UNAUTHORIZED, "390002", &e.to_string()),
    };

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
        user: Some(auth_ctx.user.clone()),
        database: None,
        tags: QueryTags::default(),
        extra,
        agent_context: None,
    };
    let handle = Uuid::new_v4().to_string();
    let mut sink = SqlApiSink::new();

    if let Err(e) = route_and_execute(
        &state,
        sql,
        params,
        session_ctx,
        FrontendProtocol::SnowflakeSqlApi,
        &mut sink,
        &auth_ctx,
    )
    .await
    {
        warn!(handle = %handle, "SQL API route_and_execute error: {e}");
        sink.error = Some(e.to_string());
    }

    sink.into_response(&handle)
}

/// GET /api/v2/statements/:handle  — stub (sync execution, nothing to poll)
pub async fn get_statement(
    State(_state): State<Arc<AppState>>,
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

/// DELETE /api/v2/statements/:handle  — stub (sync execution, nothing to cancel)
pub async fn cancel_statement(
    State(_state): State<Arc<AppState>>,
    _headers: HeaderMap,
    axum::extract::Path(handle): axum::extract::Path<String>,
) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({
            "statementHandle": handle,
            "message": "Statement aborted.",
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Auth helper
// ---------------------------------------------------------------------------

async fn authenticate(
    state: &Arc<AppState>,
    headers: &HeaderMap,
) -> std::result::Result<queryflux_auth::AuthContext, String> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    state
        .auth_provider
        .authenticate(&Credentials {
            username: None,
            password: None,
            bearer_token: bearer,
        })
        .await
        .map_err(|e| e.to_string())
}
