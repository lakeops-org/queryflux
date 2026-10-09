//! Snowflake HTTP wire v1 token-request handler.
//!
//! POST /session/token-request — renew a session token (extend TTL)

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::snowflake::http::handlers::common::extract_snowflake_token;
use crate::snowflake::http::SnowflakeWireState;

pub async fn token_request(
    State(state): State<SnowflakeWireState>,
    headers: HeaderMap,
) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };
    if super::common::authenticated_session(&state, &token)
        .await
        .is_none()
    {
        return unauthorized();
    }
    match state.sessions.validate_session(&token) {
        Some((remaining, _)) => (
            StatusCode::OK,
            axum::Json(json!({
                "data": {
                    "sessionToken": token,
                    "validityInSecondsST": remaining,
                    "masterToken": null,
                    "validityInSecondsMT": 0
                },
                "message": null,
                "success": true,
                "code": null
            })),
        )
            .into_response(),
        None => unauthorized(),
    }
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
