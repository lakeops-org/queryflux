use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_auth::Credentials;
use queryflux_core::{query::FrontendProtocol, session::SessionContext, tags::QueryTags};
use serde_json::{json, Value};
use tracing::{info, warn};
use uuid::Uuid;

use crate::dispatch::resolve_route;
use crate::snowflake::http::session_store::SnowflakeSession;
use crate::state::AppState;

use super::common::{extract_snowflake_token, parse_snowflake_json_body, sf_error};

/// POST /session/v1/login-request
///
/// Authenticates the client against QueryFlux's auth provider, resolves a cluster
/// group via the router chain, and creates a local QF session. No backend Snowflake
/// account is contacted — QueryFlux terminates the Snowflake wire protocol and
/// dispatches SQL to any configured engine (Trino, StarRocks, DuckDB, etc.).
pub async fn login_request(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_json: Value = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => return sf_error(StatusCode::BAD_REQUEST, 390000, "Invalid JSON body"),
    };

    let data = &body_json["data"];
    let username = data["LOGIN_NAME"].as_str().unwrap_or("").to_string();
    let password = data["PASSWORD"].as_str().map(|s| s.to_string());

    // Database/schema hints from query params or session parameters.
    let database = params.get("databaseName").cloned().or_else(|| {
        data["SESSION_PARAMETERS"]["DATABASE"]
            .as_str()
            .map(|s| s.to_string())
    });
    let schema = params.get("schemaName").cloned().or_else(|| {
        data["SESSION_PARAMETERS"]["SCHEMA"]
            .as_str()
            .map(|s| s.to_string())
    });

    // Authenticate via QueryFlux auth provider.
    let creds = Credentials {
        username: Some(username.clone()),
        password: password.clone(),
        bearer_token: None,
    };
    let auth_ctx = match state.auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            warn!(user = %username, "Snowflake HTTP login auth failed: {e}");
            return sf_error(
                StatusCode::UNAUTHORIZED,
                390100,
                "Incorrect username or password",
            );
        }
    };

    // Route to find the cluster group for this session.
    let session_ctx = SessionContext {
        user: Some(username.clone()),
        database: database.clone().or_else(|| schema.clone()),
        tags: QueryTags::default(),
        extra: HashMap::new(),
        agent_context: None,
    };
    let (group, _routing_trace) = match resolve_route(
        &state,
        "",
        &session_ctx,
        &FrontendProtocol::SnowflakeHttp,
        &auth_ctx,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(user = %username, "Snowflake HTTP routing failed at login: {e}");
            return sf_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                390000,
                &format!("Routing error: {e}"),
            );
        }
    };

    let qf_token = Uuid::new_v4().to_string();
    let now = Instant::now();
    state.snowflake_sessions.insert(
        qf_token.clone(),
        SnowflakeSession {
            qf_token: qf_token.clone(),
            user: Some(username.clone()),
            auth_ctx,
            group,
            database: database.clone(),
            schema: schema.clone(),
            created_at: now,
            last_seen: now,
        },
    );

    info!(user = %username, token = %&qf_token[..8], "Snowflake HTTP session created");

    (
        StatusCode::OK,
        axum::Json(json!({
            "data": {
                "token": qf_token,
                "masterToken": qf_token,
                "parameters": [
                    {"name": "AUTOCOMMIT", "value": true},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
                    {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
                    {"name": "QUERY_RESULT_FORMAT", "value": "ARROW_FORCE"},
                    {"name": "TIMEZONE", "value": "Etc/UTC"}
                ],
                "sessionInfo": {
                    "databaseName": database.unwrap_or_default(),
                    "schemaName": schema.unwrap_or_default(),
                    "warehouseName": "",
                    "roleName": "PUBLIC"
                }
            },
            "success": true,
            "code": null,
            "message": null
        })),
    )
        .into_response()
}

/// DELETE /session  — log out
pub async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(token) = extract_snowflake_token(&headers) {
        state.snowflake_sessions.remove(&token);
    }
    (
        StatusCode::OK,
        axum::Json(json!({"success": true, "code": null, "message": null, "data": null})),
    )
        .into_response()
}

/// GET /session/heartbeat  — keep-alive check
pub async fn heartbeat(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => {
            return sf_error(
                StatusCode::UNAUTHORIZED,
                390101,
                "Authorization header not found",
            )
        }
    };
    if state
        .snowflake_sessions
        .validate_snowflake_session(&token)
        .is_none()
    {
        return sf_error(
            StatusCode::UNAUTHORIZED,
            390104,
            "Session not found or expired",
        );
    }
    (
        StatusCode::OK,
        axum::Json(json!({"success": true, "code": null, "message": null, "data": null})),
    )
        .into_response()
}
