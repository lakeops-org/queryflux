//! HTTPS control endpoints for native connection authorization leases.
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
pub use queryflux_auth::lease::LeaseStore;
use queryflux_auth::lease::MAX_LIFETIME;
use queryflux_auth::{provider::WireAuthKind, AuthContext, Credentials};
use queryflux_core::error::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn authenticate(state: &AppState, credentials: &Credentials) -> Result<AuthContext> {
    let provider = state.live.read().await.auth_provider.clone();
    state
        .native_leases
        .authenticate(provider.as_ref(), credentials)
        .await
}
pub async fn authenticate_initial(
    state: &AppState,
    credentials: &mut Credentials,
) -> Result<AuthContext> {
    let provider = state.live.read().await.auth_provider.clone();
    state
        .native_leases
        .authenticate_initial(provider.as_ref(), credentials)
        .await
}
#[derive(Deserialize)]
pub struct LeaseRequest {
    token: String,
}
fn credentials(request: LeaseRequest) -> std::result::Result<Credentials, StatusCode> {
    if request.token.len() > 10240 || request.token.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Credentials {
        bearer_token: Some(request.token),
        ..Default::default()
    })
}
fn secret(headers: &HeaderMap) -> std::result::Result<&str, StatusCode> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(crate::strip_bearer_prefix)
        .filter(|s| s.len() == 64)
        .ok_or(StatusCode::UNAUTHORIZED)
}
async fn verified(
    state: &AppState,
    credentials: &Credentials,
) -> std::result::Result<AuthContext, StatusCode> {
    let provider = state.live.read().await.auth_provider.clone();
    if provider.wire_auth_kind() != WireAuthKind::Jwt {
        return Err(StatusCode::FORBIDDEN);
    }
    provider
        .authenticate(credentials)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)
}
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(request): Json<LeaseRequest>,
) -> std::result::Result<
    (
        [(axum::http::header::HeaderName, &'static str); 1],
        Json<Value>,
    ),
    StatusCode,
> {
    let c = credentials(request)?;
    let identity = verified(&state, &c).await?;
    let (id, secret, connection_token) = state
        .native_leases
        .register(c, &identity)
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(
            json!({"id":id, "secret":secret, "connection_token":connection_token, "refresh_before_seconds":300, "max_lifetime_seconds":MAX_LIFETIME}),
        ),
    ))
}
pub async fn renew(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<LeaseRequest>,
) -> std::result::Result<StatusCode, StatusCode> {
    let c = credentials(request)?;
    let identity = verified(&state, &c).await?;
    state
        .native_leases
        .renew(&id, secret(&headers)?, c, &identity)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(StatusCode::NO_CONTENT)
}
pub async fn revoke(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<StatusCode, StatusCode> {
    state
        .native_leases
        .revoke(&id, secret(&headers)?)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(StatusCode::NO_CONTENT)
}
