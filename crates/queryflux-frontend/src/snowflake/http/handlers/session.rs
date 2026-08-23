//! Snowflake HTTP wire v1 session handlers.
//!
//! POST /session/v1/login-request  — authenticate and create a session
//! DELETE /session                 — logout / invalidate session
//! GET  /session/heartbeat         — validate session and bump idle timer

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_auth::Credentials;
use queryflux_core::{
    error::QueryFluxError, query::FrontendProtocol, session::SessionContext, tags::QueryTags,
};
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

use crate::snowflake::http::handlers::common::{
    extract_snowflake_token, parse_snowflake_json_body,
};
use crate::snowflake::http::SnowflakeWireState;
use queryflux_routing::ChainRouteResult;

// ---------------------------------------------------------------------------
// POST /session/v1/login-request
// ---------------------------------------------------------------------------

pub async fn login_request(
    State(state): State<SnowflakeWireState>,
    Query(query_params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_json = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => return sf_error("390000", "Invalid request body"),
    };

    let data = &body_json["data"];
    let login_name = match data["LOGIN_NAME"].as_str() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return sf_error("390000", "Missing LOGIN_NAME"),
    };
    let password = data["PASSWORD"].as_str().unwrap_or("").to_string();
    // Retained for `passthrough` mode (see `SnowflakeSession::password`) — unconditionally,
    // since a login handler has no way to know yet whether the query group it's about to
    // resolve will route to a `queryAuth: passthrough` cluster. Not verified by queryflux
    // itself; the actual verification is the real Snowflake connection attempt this later
    // enables (`AdbcAdapter`'s passthrough sub-pool), same as connecting to Snowflake directly.
    let passthrough_password = (!password.is_empty()).then(|| password.clone());
    let database = data["DATABASE_NAME"].as_str().map(|s| s.to_string());
    let schema = data["SCHEMA_NAME"].as_str().map(|s| s.to_string());
    // Unlike database/schema, real Snowflake drivers send role/warehouse as query-string
    // parameters on the login-request URL itself (`?roleName=X&warehouse=Y`), not JSON body
    // fields — confirmed against the actual login URL the Go driver constructs.
    let role = query_params
        .get("roleName")
        .filter(|s| !s.is_empty())
        .cloned();
    let warehouse = query_params
        .get("warehouse")
        .filter(|s| !s.is_empty())
        .cloned();

    let auth_provider = state.app.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider
        .authenticate(&Credentials {
            username: Some(login_name.clone()),
            password: Some(password),
            bearer_token: None,
        })
        .await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .app
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::SnowflakeHttp));
            warn!(user = %login_name, "Snowflake wire login failed: {e}");
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({
                    "data": null,
                    "message": "Incorrect username or password was specified.",
                    "success": false,
                    "code": "390100"
                })),
            )
                .into_response();
        }
    };

    // Resolve routing group at login time — stored in session so every subsequent
    // query in this session lands on the same cluster group.
    let session_ctx = SessionContext {
        user: Some(auth_ctx.user.clone()),
        database: database.clone(),
        tags: QueryTags::default(),
        extra: Default::default(),
        agent_context: None,
    };
    let routing_result = {
        let live = state.app.live.read().await;
        live.router_chain
            .route_with_trace(
                "",
                &session_ctx,
                &FrontendProtocol::SnowflakeHttp,
                Some(&auth_ctx),
            )
            .await
    };
    let (chain_result, mut routing_trace) = match routing_result {
        Ok(r) => r,
        Err(e) => return sf_error("390000", &format!("Routing error: {e}")),
    };
    let mut group = match chain_result {
        ChainRouteResult::Routed(g) => g,
        ChainRouteResult::Denied { message } => {
            state.app.record_routing_deny(
                "",
                &session_ctx,
                FrontendProtocol::SnowflakeHttp,
                &message,
                Some(routing_trace),
            );
            return sf_error("390201", &message);
        }
    };
    group = match state
        .app
        .resolve_routed_group(group, &mut routing_trace, &auth_ctx)
        .await
    {
        Ok(g) => g,
        Err(QueryFluxError::Unauthorized(msg)) => return sf_error("390201", &msg),
        Err(e) => return sf_error("390000", &format!("Routing error: {e}")),
    };

    let token = state.sessions.create_session(
        auth_ctx.user.clone(),
        auth_ctx,
        group,
        database.clone(),
        schema.clone(),
        role.clone(),
        warehouse.clone(),
        passthrough_password,
    );

    let master_token = Uuid::new_v4().to_string();

    (
        StatusCode::OK,
        axum::Json(json!({
            "data": {
                "token": token,
                "masterToken": master_token,
                "validityInSeconds": 3600,
                "masterValidityInSeconds": 14400,
                "displayUserName": login_name,
                "serverVersion": "QueryFlux",
                "firstLogin": false,
                "remMeToken": null,
                "remMeValidityInSeconds": 0,
                "healthCheckInterval": 45,
                "newClientForUpgrade": null,
                "sessionId": 0,
                "parameters": [
                    {"name": "TIMEZONE", "value": "Etc/UTC"},
                    {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600}
                ],
                "sessionInfo": {
                    "databaseName": database.unwrap_or_default(),
                    "schemaName": schema.unwrap_or_default(),
                    "warehouseName": warehouse.unwrap_or_default(),
                    "roleName": role.unwrap_or_else(|| "PUBLIC".to_string())
                },
                "idToken": null,
                "idTokenValidityInSeconds": 0,
                "responseData": null,
                "mfaToken": null,
                "mfaTokenValidityInSeconds": 0
            },
            "message": null,
            "success": true,
            "code": null
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// DELETE /session  — logout
// ---------------------------------------------------------------------------

pub async fn logout(State(state): State<SnowflakeWireState>, headers: HeaderMap) -> Response {
    if let Some(token) = extract_snowflake_token(&headers) {
        state.sessions.remove_session(&token);
    }
    (
        StatusCode::OK,
        axum::Json(json!({"success": true, "code": null, "message": null, "data": null})),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// GET /session/heartbeat
// ---------------------------------------------------------------------------

pub async fn heartbeat(State(state): State<SnowflakeWireState>, headers: HeaderMap) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };
    match state.sessions.validate_session(&token) {
        Some((remaining, _)) => (
            StatusCode::OK,
            axum::Json(json!({
                "data": {"validityInSeconds": remaining},
                "message": null,
                "success": true,
                "code": null
            })),
        )
            .into_response(),
        None => unauthorized(),
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
