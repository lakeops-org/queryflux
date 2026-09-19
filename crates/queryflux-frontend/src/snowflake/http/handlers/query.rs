//! Snowflake HTTP wire v1 query handlers.
//!
//! POST /queries/v1/query-request            — execute SQL, return Arrow IPC
//! GET  /queries/v1/query-monitoring-request — list in-flight queries for session
//! DELETE /queries/v1/:query_id              — cancel in-flight query

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use polyglot_sql::{
    dialects::Dialect,
    tokens::{Token, TokenType},
    DialectType,
};
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

/// A completed sink carrying a single "Statement executed successfully." row, matching what
/// real Snowflake returns for session-scoping statements like `USE ROLE`/`USE WAREHOUSE`.
/// Reuses `SnowflakeSink::into_response` for the actual envelope rather than hand-rolling a
/// new response shape.
fn synthetic_ok_sink() -> SnowflakeSink {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "Statement executed successfully.",
        ]))],
    )
    .expect("synthetic OK batch is always valid");
    SnowflakeSink {
        schema: Some(schema),
        batches: vec![batch],
        error: None,
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
    let (auth_ctx, group, mut database, mut schema_name, role, warehouse, password) = {
        match state.sessions.validate_session(&token) {
            Some((_, session)) => (
                session.auth_ctx.clone(),
                session.group.clone(),
                session.database.clone(),
                session.schema.clone(),
                session.role.clone(),
                session.warehouse.clone(),
                session.password.clone(),
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

    // Fast path: USE ROLE/WAREHOUSE/DATABASE/SCHEMA update tracked session state and ack
    // locally — never reaching the ADBC adapter, mirroring the MySQL wire frontend's `USE db`
    // handling. This is what makes these statements safe: forwarding them as literal SQL to a
    // pooled connection shared across unrelated sessions would leak state between them (see
    // `AdbcAdapter`'s session-scoped sub-pool, which is what a *real* query after one of these
    // statements routes through instead).
    if let Some((target, mut names)) = try_parse_snowflake_use(&sql) {
        // Parsing guarantees one identifier, or two for a qualified schema.
        let value = names.pop().expect("USE has an identifier");
        match target {
            // The response envelope only echoes database/schema (see `into_response` below),
            // so only those two need a local update — the setter call is what actually matters
            // for warehouse/role, since it's what the *next* query's dispatch reads back.
            SnowflakeUseTarget::Warehouse => {
                state.sessions.set_warehouse(&token, Some(value));
            }
            SnowflakeUseTarget::Role => {
                state.sessions.set_role(&token, Some(value));
            }
            SnowflakeUseTarget::Database | SnowflakeUseTarget::Schema => {
                let (new_database, new_schema) = if target == SnowflakeUseTarget::Database {
                    (Some(value), None)
                } else {
                    (names.pop(), Some(value))
                };
                let Some(namespace) =
                    state
                        .sessions
                        .set_namespace(&token, new_database, new_schema)
                else {
                    return unauthorized();
                };
                (database, schema_name) = namespace;
            }
        }
        let query_id = Uuid::new_v4().to_string();
        return synthetic_ok_sink().into_response(
            &query_id,
            database.as_deref().unwrap_or_default(),
            schema_name.as_deref().unwrap_or_default(),
        );
    }

    // Wire v1 uses "parameterBindings" (SQL API v2 uses "bindings").
    let params = bindings_to_params(body_json.get("parameterBindings"));

    let mut extra = std::collections::HashMap::new();
    if let Some(role) = &role {
        extra.insert("snowflake.role".to_string(), role.clone());
    }
    if let Some(warehouse) = &warehouse {
        extra.insert("snowflake.warehouse".to_string(), warehouse.clone());
    }
    if let Some(schema) = schema_name.as_ref().filter(|s| !s.is_empty()) {
        extra.insert("snowflake.schema".to_string(), schema.clone());
    }
    // Only meaningful for `queryAuth: passthrough` clusters — `AdbcAdapter` fails closed if
    // these are absent and the resolved credentials are `Passthrough`; harmless to include
    // otherwise (any other credential mode just ignores them).
    if let Some(password) = &password {
        extra.insert(
            "snowflake.passthrough_username".to_string(),
            auth_ctx.user.clone(),
        );
        extra.insert(
            "snowflake.passthrough_password".to_string(),
            password.clone(),
        );
    }

    let session_ctx = SessionContext {
        user: Some(auth_ctx.user.clone()),
        // Keep the shared schema hint in sync with the Snowflake backend override.
        database: extra.get("snowflake.schema").cloned(),
        // Snowflake's database is the catalog in catalog.database.table.
        catalog: database.clone(),
        tags: QueryTags::default(),
        extra,
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
        SpawnExecuteResult::Completed(Ok(()), sink) => sink.into_response(
            &query_id,
            database.as_deref().unwrap_or_default(),
            schema_name.as_deref().unwrap_or_default(),
        ),
        SpawnExecuteResult::Completed(Err(e), mut sink) => {
            warn!(query_id = %query_id, "Snowflake wire query error: {e}");
            sink.error = Some(e.to_string());
            sink.into_response(
                &query_id,
                database.as_deref().unwrap_or_default(),
                schema_name.as_deref().unwrap_or_default(),
            )
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
// USE ROLE / USE WAREHOUSE / USE DATABASE / USE SCHEMA fast path
// ---------------------------------------------------------------------------

/// What a `USE ...` statement targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnowflakeUseTarget {
    Warehouse,
    Role,
    Database,
    Schema,
}

/// `keyword` must be followed by whitespace (or nothing before the value) — a simple
/// `starts_with` would wrongly match `usex` as `use`, or `use warehouse` as bare `use`.
fn strip_use_keyword<'a>(s_lower: &str, s: &'a str, keyword_lower: &str) -> Option<&'a str> {
    // An exact match (e.g. bare "USE ROLE" with no value) must return the empty remainder
    // here, not `None` — `None` would let it fall through to a shorter keyword match (bare
    // "use"), which would misparse "USE ROLE" as `USE DATABASE` with value "ROLE". The caller
    // rejects an empty value on its own, so returning `Some("")` here is what makes that
    // rejection actually happen instead of silently matching the wrong, shorter keyword.
    if s_lower == keyword_lower {
        return Some(&s[keyword_lower.len()..]);
    }
    if s_lower.starts_with(keyword_lower)
        && s_lower[keyword_lower.len()..].starts_with(|c: char| c.is_whitespace())
    {
        Some(&s[keyword_lower.len()..])
    } else {
        None
    }
}

/// Parse `USE WAREHOUSE <x>` / `USE ROLE <x>` / `USE DATABASE <x>` / bare `USE <x>` (= database)
/// / `USE SCHEMA <x>` (including qualified `db.schema`) sent as `sqlText`. Returns `None` for
/// anything else so the caller falls through to normal dispatch.
///
/// Reuse polyglot-sql's Snowflake tokenizer for identifier boundaries and unescaping.
/// Its USE AST flattens qualified names, losing the distinction between `db.schema`
/// and `"db.schema"`, so retain the identifier tokens instead. Non-USE queries avoid
/// tokenization entirely. Identifier case is preserved, as in the existing wire path.
///
/// Order matters: the multi-word forms (`use warehouse`/`use role`/`use schema`/`use database`)
/// must be checked before bare `use`, since `use` is itself a whitespace-terminated prefix of
/// all of them — checking bare `use` first would misparse `USE WAREHOUSE X` as `USE <database>`
/// with value `"warehouse x"`.
fn try_parse_snowflake_use(sql: &str) -> Option<(SnowflakeUseTarget, Vec<String>)> {
    let s = sql.trim().trim_end_matches(';');
    let s_lower = s.to_lowercase();

    let (target, rest) = if let Some(r) = strip_use_keyword(&s_lower, s, "use warehouse") {
        (SnowflakeUseTarget::Warehouse, r)
    } else if let Some(r) = strip_use_keyword(&s_lower, s, "use role") {
        (SnowflakeUseTarget::Role, r)
    } else if let Some(r) = strip_use_keyword(&s_lower, s, "use schema") {
        (SnowflakeUseTarget::Schema, r)
    } else if let Some(r) = strip_use_keyword(&s_lower, s, "use database") {
        (SnowflakeUseTarget::Database, r)
    } else if let Some(r) = strip_use_keyword(&s_lower, s, "use") {
        (SnowflakeUseTarget::Database, r)
    } else {
        return None;
    };

    let tokens = Dialect::get(DialectType::Snowflake).tokenize(rest).ok()?;
    let names = match tokens.as_slice() {
        [name] if is_use_identifier(name) => vec![name.text.clone()],
        [database, dot, schema]
            if target == SnowflakeUseTarget::Schema
                && dot.token_type == TokenType::Dot
                && is_use_identifier(database)
                && is_use_identifier(schema) =>
        {
            vec![database.text.clone(), schema.text.clone()]
        }
        _ => return None,
    };
    Some((target, names))
}

fn is_use_identifier(token: &Token) -> bool {
    if token.token_type == TokenType::QuotedIdentifier {
        return !token.text.is_empty();
    }
    (token.token_type == TokenType::Var || token.token_type.is_keyword())
        && token
            .text
            .starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && token
            .text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
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

#[cfg(test)]
mod tests {
    use super::{try_parse_snowflake_use, SnowflakeUseTarget};

    #[test]
    fn parses_use_warehouse() {
        let (target, value) = try_parse_snowflake_use("USE WAREHOUSE ANALYTICS_WH").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Warehouse);
        assert_eq!(value, ["ANALYTICS_WH"]);
    }

    #[test]
    fn parses_use_role() {
        let (target, value) = try_parse_snowflake_use("use role sysadmin").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Role);
        assert_eq!(value, ["sysadmin"]);
    }

    #[test]
    fn parses_use_database_keyword() {
        let (target, value) = try_parse_snowflake_use("USE DATABASE PROD").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Database);
        assert_eq!(value, ["PROD"]);
    }

    #[test]
    fn bare_use_is_database() {
        let (target, value) = try_parse_snowflake_use("USE PROD").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Database);
        assert_eq!(value, ["PROD"]);
    }

    #[test]
    fn bare_use_does_not_misparse_multi_word_forms() {
        // Bare `USE` must be checked *after* the multi-word forms — otherwise `USE WAREHOUSE X`
        // would misparse as `USE <database>` with value "warehouse x".
        let (target, value) = try_parse_snowflake_use("USE WAREHOUSE X").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Warehouse);
        assert_eq!(value, ["X"]);
    }

    #[test]
    fn parses_use_schema_qualified() {
        let (target, value) = try_parse_snowflake_use("USE SCHEMA mydb.myschema").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Schema);
        assert_eq!(value, ["mydb", "myschema"]);
    }

    #[test]
    fn strips_double_quoted_identifiers() {
        let (_, value) = try_parse_snowflake_use(r#"USE ROLE "My Role""#).unwrap();
        assert_eq!(value, ["My Role"]);
    }

    #[test]
    fn strips_double_quotes_on_each_qualified_segment() {
        let (_, value) = try_parse_snowflake_use(r#"USE SCHEMA "My Db"."My Schema""#).unwrap();
        assert_eq!(value, ["My Db", "My Schema"]);
    }

    #[test]
    fn preserves_quoted_dots_and_unescapes_quotes() {
        for (sql, expected) in [
            (r#"USE SCHEMA "ARCHIVE"."SALES""#, vec!["ARCHIVE", "SALES"]),
            (
                r#"USE SCHEMA "Archive.Db" . "Sa""les.Data""#,
                vec!["Archive.Db", "Sa\"les.Data"],
            ),
            (r#"USE SCHEMA "ARCHIVE.SALES""#, vec!["ARCHIVE.SALES"]),
            (
                r#"USE SCHEMA ARCHIVE."My Sales""#,
                vec!["ARCHIVE", "My Sales"],
            ),
            ("USE SCHEMA SALES", vec!["SALES"]),
        ] {
            let (target, names) = try_parse_snowflake_use(sql).unwrap();
            assert_eq!(target, SnowflakeUseTarget::Schema);
            assert_eq!(names, expected, "{sql}");
        }
    }

    #[test]
    fn rejects_malformed_or_nonliteral_names() {
        for sql in [
            "USE SCHEMA .SALES",
            "USE SCHEMA ARCHIVE.",
            "USE SCHEMA ARCHIVE..SALES",
            "USE SCHEMA A.B.C",
            "USE DATABASE ARCHIVE.SALES",
            r#"USE SCHEMA "unclosed"#,
            r#"USE SCHEMA "SALES" trailing"#,
            r#"USE SCHEMA """#,
            "USE SCHEMA 'SALES'",
            "USE SCHEMA SALES; SELECT 1",
            "USE SCHEMA IDENTIFIER($schema)",
        ] {
            assert!(try_parse_snowflake_use(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn handles_trailing_semicolon_and_whitespace() {
        let (target, value) = try_parse_snowflake_use("  USE WAREHOUSE X  ;  ").unwrap();
        assert_eq!(target, SnowflakeUseTarget::Warehouse);
        assert_eq!(value, ["X"]);
    }

    #[test]
    fn rejects_bare_use_with_no_target() {
        assert!(try_parse_snowflake_use("USE").is_none());
        assert!(try_parse_snowflake_use("USE ").is_none());
    }

    #[test]
    fn rejects_non_use_statements() {
        assert!(try_parse_snowflake_use("SELECT 1").is_none());
        assert!(try_parse_snowflake_use("SELECT * FROM USE_LOG").is_none());
        assert!(try_parse_snowflake_use("USEROLE X").is_none());
    }

    #[test]
    fn rejects_use_role_with_no_target() {
        assert!(try_parse_snowflake_use("USE ROLE").is_none());
        assert!(try_parse_snowflake_use("USE ROLE ").is_none());
    }

    #[test]
    fn rejects_use_secondary_roles_as_a_bare_database_name() {
        // A real, different Snowflake statement (activates secondary roles) that also starts
        // with `USE` — must fall through to normal dispatch, not be misparsed as `USE
        // <database>` naming the literal unquoted, multi-word "SECONDARY ROLES ALL".
        assert!(try_parse_snowflake_use("USE SECONDARY ROLES ALL").is_none());
        assert!(try_parse_snowflake_use("USE SECONDARY ROLES NONE").is_none());
        assert!(try_parse_snowflake_use("USE SECONDARY ROLES analyst, auditor").is_none());
    }

    #[test]
    fn bare_use_still_accepts_a_quoted_database_identifier_with_spaces() {
        let (target, value) = try_parse_snowflake_use(r#"USE "My Db""#).unwrap();
        assert_eq!(target, SnowflakeUseTarget::Database);
        assert_eq!(value, ["My Db"]);
    }

    #[test]
    fn rejects_unquoted_multi_word_value_for_every_keyword_form() {
        // An unquoted identifier can never legitimately contain whitespace — this applies
        // uniformly, not just to the bare-USE fallback that collides with `USE SECONDARY
        // ROLES`.
        assert!(try_parse_snowflake_use("USE ROLE MY ROLE").is_none());
        assert!(try_parse_snowflake_use("USE WAREHOUSE MY WH").is_none());
        assert!(try_parse_snowflake_use("USE DATABASE MY DB").is_none());
        assert!(try_parse_snowflake_use("USE SCHEMA MY SCHEMA").is_none());
    }
}
