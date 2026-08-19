use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Response, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use chrono::Utc;
use queryflux_auth::{Credentials, QueryAction, QueryAuthz};
use queryflux_core::{
    error::QueryFluxError,
    query::{BackendQueryId, FrontendProtocol, ProxyQueryId, QueryPollResult, QueryStatus},
    session::SessionContext,
    tags::{parse_query_tags, QueryTags},
};
use queryflux_engine_adapters::trino::api::{
    queued_response, TrinoError, TrinoResponse, TrinoStats,
};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use super::result_sink::TrinoHttpResultSink;
use crate::dispatch::{dispatch_query, execute_to_sink, rewrite_trino_uri, DispatchOutcome};
use crate::state::{AppState, QueryContext, QueryOutcome};
use queryflux_persistence::QueueCoordinator;
use queryflux_routing::ChainRouteResult;

/// Stale-claim cutoff. Heartbeats must run more often than this while dispatch
/// is in progress so a slow submit cannot look like a crashed replica.
const QUEUE_CLAIM_TIMEOUT_SECS: i64 = 60;
const QUEUE_CLAIM_HEARTBEAT_SECS: u64 = 15;

/// Refreshes `claimed_at` until dropped so another replica cannot take over.
struct QueueClaimHeartbeat {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl QueueClaimHeartbeat {
    fn start(qc: Arc<dyn QueueCoordinator>, query_id: String, instance_id: String) -> Self {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(QUEUE_CLAIM_HEARTBEAT_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    _ = interval.tick() => {
                        match qc.refresh_claim(&query_id, &instance_id).await {
                            Ok(true) => {}
                            Ok(false) => {
                                warn!(id = %query_id, "Queue claim lost during dispatch");
                                break;
                            }
                            Err(e) => {
                                warn!(id = %query_id, "Queue claim refresh failed: {e}");
                            }
                        }
                    }
                }
            }
        });
        Self { stop: Some(tx) }
    }
}

impl Drop for QueueClaimHeartbeat {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }
}

async fn allow_query_action_or_forbid(
    state: &AppState,
    auth_ctx: &queryflux_auth::AuthContext,
    action: QueryAction,
    submitted_by: &str,
    group: &str,
) -> Option<Response<Body>> {
    let authz = state.live.read().await.authorization.clone();
    let query = QueryAuthz {
        submitted_by: submitted_by.to_string(),
        group: group.to_string(),
    };
    if authz.check_query(auth_ctx, action, &query).await {
        return None;
    }
    warn!(
        action = ?action,
        user = %auth_ctx.user,
        owner = %submitted_by,
        group,
        "Query action denied"
    );
    Some(StatusCode::FORBIDDEN.into_response())
}

/// Restore backend auth stripped from persisted queued sessions.
///
/// Called only after the dequeue caller is verified as the query owner.
fn session_for_queued_dispatch(
    mut session: SessionContext,
    headers: &HeaderMap,
    auth_ctx: &queryflux_auth::AuthContext,
) -> SessionContext {
    if let Some(v) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        session
            .extra
            .insert("authorization".to_string(), v.to_string());
    } else if let Some(token) = &auth_ctx.raw_token {
        session
            .extra
            .insert("authorization".to_string(), format!("Bearer {token}"));
    }
    session
}

fn trino_error_response(query_id: &str, message: &str) -> Response<Body> {
    let resp = queryflux_engine_adapters::trino::api::TrinoResponse {
        id: query_id.to_string(),
        next_uri: None,
        info_uri: "http://queryflux/ui/query.html".to_string(),
        partial_cancel_uri: None,
        stats: queryflux_engine_adapters::trino::api::TrinoStats {
            state: "FAILED".to_string(),
            queued: false,
            scheduled: false,
            ..Default::default()
        },
        error: Some(queryflux_engine_adapters::trino::api::TrinoError {
            message: message.to_string(),
            error_code: Some(0),
            error_name: Some("QUERY_FAILED".to_string()),
            error_type: Some("USER_ERROR".to_string()),
            failure_info: Default::default(),
        }),
        columns: None,
        data: None,
        update_type: None,
        update_count: None,
        warnings: vec![],
    };
    json_response(&resp)
}

/// Returns a client-safe error message for the given error, or an empty string
/// if the original message is already safe to expose (e.g. auth/not-found errors).
fn client_safe_message(e: &QueryFluxError) -> &'static str {
    use queryflux_core::error::QueryFluxError::*;
    match e {
        Persistence(_) => "Internal service error",
        Engine(_) => "Backend engine error",
        Routing(_) | NoClusterGroupAvailable(_) => "Query routing failed",
        Config(_) => "Configuration error",
        // Empty → caller forwards Display (QueueFull / CapacityWaitTimeout detail).
        Auth(_)
        | Unauthorized(_)
        | QueryNotFound(_)
        | ClusterNotFound(_)
        | Denied(_)
        | QueueFull { .. }
        | CapacityWaitTimeout { .. } => "",
        _ => "Internal error",
    }
}

fn json_response(body: impl serde::Serialize) -> Response<Body> {
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

/// Rewrite the `"nextUri":"..."` field in a raw JSON byte slice without a full parse/serialize.
fn raw_response_with_rewritten_next_uri(
    body_bytes: Bytes,
    proxy_next_uri: Option<String>,
) -> Response<Body> {
    let out = rewrite_next_uri_bytes(&body_bytes, proxy_next_uri.as_deref());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(out))
        .unwrap()
}

/// Trino sometimes omits `failureInfo` or sets it to `null`; `trino-rust-client` requires an object.
fn normalize_trino_error_failure_info_json(bytes: &[u8]) -> Bytes {
    if !bytes.windows(7).any(|w| w == b"\"error\"") {
        return Bytes::copy_from_slice(bytes);
    }
    let mut v: Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(_) => return Bytes::copy_from_slice(bytes),
    };
    let err_obj = match v.get_mut("error") {
        Some(Value::Object(o)) => o,
        _ => return Bytes::copy_from_slice(bytes),
    };
    let needs_default = matches!(err_obj.get("failureInfo"), None | Some(Value::Null));
    if !needs_default {
        return Bytes::copy_from_slice(bytes);
    }
    err_obj.insert(
        "failureInfo".to_string(),
        json!({
            "type": "io.trino.spi.TrinoException",
            "suppressed": [],
            "stack": [],
        }),
    );
    Bytes::from(serde_json::to_vec(&v).unwrap_or_else(|_| bytes.to_vec()))
}

fn rewrite_next_uri_bytes(src: &[u8], new_uri: Option<&str>) -> Bytes {
    let core = rewrite_next_uri_bytes_core(src, new_uri);
    normalize_trino_error_failure_info_json(core.as_ref())
}

fn rewrite_next_uri_bytes_core(src: &[u8], new_uri: Option<&str>) -> Bytes {
    const KEY: &[u8] = b"\"nextUri\"";
    if let Some(key_pos) = find_subsequence(src, KEY) {
        let after_key = &src[key_pos + KEY.len()..];
        let colon_offset = after_key.iter().position(|&b| b == b':').unwrap_or(0);
        let after_colon = &after_key[colon_offset + 1..];
        let value_start_offset = after_colon
            .iter()
            .position(|&b| !b.is_ascii_whitespace())
            .unwrap_or(0);
        let value_start = key_pos + KEY.len() + colon_offset + 1 + value_start_offset;

        if src[value_start] == b'"' {
            if let Some(end_offset) = src[value_start + 1..].iter().position(|&b| b == b'"') {
                let value_end = value_start + 1 + end_offset + 1;
                let before = &src[..key_pos];
                let after = &src[value_end..];

                return match new_uri {
                    Some(uri) => {
                        let mut out = Vec::with_capacity(src.len() + uri.len());
                        out.extend_from_slice(before);
                        out.extend_from_slice(KEY);
                        out.extend_from_slice(b":");
                        out.push(b'"');
                        out.extend_from_slice(uri.as_bytes());
                        out.push(b'"');
                        out.extend_from_slice(after);
                        Bytes::from(out)
                    }
                    None => {
                        let mut out = Vec::with_capacity(src.len());
                        let trim_end = before
                            .iter()
                            .rposition(|&b| b == b',')
                            .unwrap_or(before.len());
                        let has_preceding_comma = trim_end < before.len();
                        if has_preceding_comma {
                            out.extend_from_slice(&before[..trim_end]);
                        } else {
                            out.extend_from_slice(before);
                        }
                        let after_trimmed = if !has_preceding_comma {
                            let skip = after
                                .iter()
                                .position(|&b| b != b',' && !b.is_ascii_whitespace())
                                .unwrap_or(0);
                            &after[skip..]
                        } else {
                            after
                        };
                        out.extend_from_slice(after_trimmed);
                        Bytes::from(out)
                    }
                };
            }
        }
    }

    // Fallback: full serde parse/serialize.
    let mut json: Value = match serde_json::from_slice(src) {
        Ok(v) => v,
        Err(_) => return Bytes::copy_from_slice(src),
    };
    if let Some(uri) = new_uri {
        json["nextUri"] = Value::String(uri.to_string());
    } else {
        json.as_object_mut().map(|o| o.remove("nextUri"));
    }
    Bytes::from(serde_json::to_vec(&json).unwrap_or_default())
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Exponential backoff delay for queued query polling.
async fn queued_backoff_delay(sequence: u64) {
    if sequence > 0 {
        let ms = (2u64.saturating_pow((sequence + 7) as u32)).min(3000);
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

fn extract_session(headers: &HeaderMap) -> SessionContext {
    let mut h = std::collections::HashMap::new();
    for (k, v) in headers {
        if let Ok(s) = v.to_str() {
            h.insert(k.as_str().to_lowercase(), s.to_string());
        }
    }
    let tags = extract_trino_tags(&h);
    let user = h.get("x-trino-user").cloned();
    let database = h.get("x-trino-catalog").cloned();
    // Agent context is NOT extracted here — it is derived lazily from `extra` in
    // dispatch.rs via `session.resolved_agent_context()`. All HTTP frontends that
    // store headers in `extra` (lowercase) automatically support agent headers.
    SessionContext {
        user,
        database,
        tags,
        extra: h,
        agent_context: None,
    }
}

/// Percent-encode a session property value for [`set_session_response`] (`X-Trino-Set-Session`),
/// matching Trino's Java client (`URLEncoder.encode` / `URLDecoder.decode` in `StatementClientV1`).
/// Commas and other delimiters in the value must not appear raw, because `X-Trino-Session` uses
/// comma-separated `name=value` pairs on subsequent requests.
fn encode_trino_session_property_value(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Decode a `query_tags` / `query_tag` value from `X-Trino-Session` (best-effort; invalid escapes
/// fall back to the raw substring so older unencoded clients keep working).
fn decode_trino_session_property_value(raw: &str) -> String {
    urlencoding::decode(raw)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| raw.to_string())
}

fn extract_trino_tags(headers: &std::collections::HashMap<String, String>) -> QueryTags {
    let mut tags = QueryTags::new();
    // X-Trino-Client-Tags: comma-separated key-only strings.
    if let Some(raw) = headers.get("x-trino-client-tags") {
        for tag in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            tags.insert(tag.to_string(), None);
        }
    }
    // X-Trino-Session: comma-separated `name=value` pairs (Trino client protocol). Values are
    // percent-encoded when they contain commas; split on commas only separates properties, not
    // characters inside an encoded value.
    if let Some(session_props) = headers.get("x-trino-session") {
        for prop in session_props
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            let Some(eq) = prop.find('=') else {
                continue;
            };
            let (key, raw_val) = prop.split_at(eq);
            let raw_val = &raw_val[1..];
            if key.eq_ignore_ascii_case("query_tags") || key.eq_ignore_ascii_case("query_tag") {
                let val = decode_trino_session_property_value(raw_val);
                let (parsed, _) = parse_query_tags(&val);
                tags.extend(parsed);
                break;
            }
        }
    }
    tags
}

fn outcome_to_response(
    _state: &Arc<AppState>,
    query_id: &ProxyQueryId,
    outcome: DispatchOutcome,
) -> Response<Body> {
    match outcome {
        DispatchOutcome::Queued { queued_next_uri } => {
            let resp = queued_response(&query_id.0, 0, queued_next_uri);
            json_response(&resp).into_response()
        }
        DispatchOutcome::Async {
            initial_body,
            proxy_next_uri,
        } => match initial_body {
            Some(body) => {
                raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response()
            }
            None => {
                let resp = queued_response(&query_id.0, 0, proxy_next_uri.unwrap_or_default());
                json_response(&resp).into_response()
            }
        },
    }
}

/// Detect `SET SESSION query_tags = '...'` (and the singular `query_tag` variant).
/// Returns `Some((header_key, raw_value))` on match, e.g. `("query_tags", "team:eng,batch")`.
/// Case-insensitive, tolerant of extra whitespace and a trailing semicolon.
fn try_parse_set_session_tags(sql: &str) -> Option<(String, String)> {
    let s = sql.trim().trim_end_matches(';').trim();
    let mut words = s.splitn(4, |c: char| c.is_ascii_whitespace());
    let w1 = words.next()?;
    if !w1.eq_ignore_ascii_case("set") {
        return None;
    }
    // skip empty tokens from multiple spaces
    let w2 = words.by_ref().find(|w| !w.is_empty())?;
    if !w2.eq_ignore_ascii_case("session") {
        return None;
    }
    let rest = s
        .get(w1.len()..)?
        .trim_start()
        .get(w2.len()..)?
        .trim_start();
    // rest is now something like: query_tags = 'team:eng,batch'
    let rest: &str = if rest.to_lowercase().starts_with("query_tags") {
        &rest["query_tags".len()..]
    } else if rest.to_lowercase().starts_with("query_tag") {
        &rest["query_tag".len()..]
    } else {
        return None;
    };
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    // Strip surrounding single quotes.
    let value = rest
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(rest);
    Some(("query_tags".to_string(), value.to_string()))
}

/// Synthetic response for an intercepted `SET SESSION query_tags = '...'`.
///
/// Returns HTTP 200 with `X-Trino-Set-Session` header so the Trino CLI includes
/// the property in `X-Trino-Session` on subsequent requests.
fn set_session_response(query_id: &str, prop_key: &str, prop_val: &str) -> Response<Body> {
    use queryflux_engine_adapters::trino::api::{TrinoResponse, TrinoStats};
    let resp = TrinoResponse {
        id: query_id.to_string(),
        next_uri: None,
        info_uri: "http://queryflux/ui/query.html".to_string(),
        partial_cancel_uri: None,
        stats: TrinoStats {
            state: "FINISHED".to_string(),
            scheduled: true,
            completed_splits: 1,
            total_splits: 1,
            ..Default::default()
        },
        error: None,
        columns: None,
        data: None,
        update_type: Some("SET SESSION".to_string()),
        update_count: Some(0),
        warnings: vec![],
    };
    let json = serde_json::to_vec(&resp).unwrap_or_default();
    Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("content-type", "application/json")
        .header(
            "X-Trino-Set-Session",
            format!(
                "{}={}",
                prop_key,
                encode_trino_session_property_value(prop_val)
            ),
        )
        .body(Body::from(json))
        .unwrap()
}

/// POST /v1/statement — client submits a new query.
pub async fn post_statement(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let sql = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Intercept SET SESSION query_tags/query_tag before routing to backend.
    // Trino doesn't know these properties; QueryFlux handles them locally and
    // returns X-Trino-Set-Session so the CLI carries the value in subsequent requests.
    let session = extract_session(&headers);
    let protocol = FrontendProtocol::TrinoHttp;

    // 1. Authenticate — derive AuthContext from request credentials.
    let creds = extract_credentials(&headers);
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::TrinoHttp));
            warn!("Authentication failed: {e}");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Auth-complete fast path: `SET SESSION query_tag(s)` is handled locally by QueryFlux,
    // but must still require authentication when `auth.required=true`.
    if let Some((prop_key, prop_val)) = try_parse_set_session_tags(&sql) {
        let query_id = ProxyQueryId::new();
        return set_session_response(&query_id.0, &prop_key, &prop_val).into_response();
    }

    // 2. Route — first matching router wins.
    // `route_with_trace` is CPU-bound (regex match, header lookup); holding the read lock
    // across this call is fine since it's brief and read-locks don't block each other.
    let routing_result = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(&sql, &session, &protocol, Some(&auth_ctx))
            .await
    };
    let (chain_result, mut routing_trace) = match routing_result {
        Ok(r) => r,
        Err(e) => {
            warn!("Routing error: {e}");
            let tmp_id = ProxyQueryId::new();
            let safe = client_safe_message(&e);
            let fallback = e.to_string();
            let msg = if safe.is_empty() {
                fallback.as_str()
            } else {
                safe
            };
            return trino_error_response(&tmp_id.0, msg).into_response();
        }
    };
    let mut group = match chain_result {
        ChainRouteResult::Routed(g) => g,
        ChainRouteResult::Denied { message } => {
            warn!(%message, user = %auth_ctx.user, "Query denied by routing rule");
            let query_id =
                state.record_routing_deny(&sql, &session, protocol, &message, Some(routing_trace));
            return trino_error_response(&query_id.0, &message).into_response();
        }
    };
    group = match state
        .resolve_routed_group(group, &mut routing_trace, &auth_ctx)
        .await
    {
        Ok(g) => g,
        Err(QueryFluxError::Unauthorized(msg)) => {
            warn!(user = %auth_ctx.user, "{msg}");
            let tmp_id = ProxyQueryId::new();
            return trino_error_response(&tmp_id.0, &msg).into_response();
        }
        Err(e) => {
            warn!("Routing resolution error: {e}");
            let tmp_id = ProxyQueryId::new();
            return trino_error_response(&tmp_id.0, &e.to_string()).into_response();
        }
    };

    let query_id = ProxyQueryId::new();
    info!(id = %query_id, group = %group, user = %auth_ctx.user, "New query submitted");

    // When caching is enabled for this group and the query is cacheable,
    // force through the sync execute_to_sink path so the cache intercept works.
    let use_cache_path = {
        let live = state.live.read().await;
        let caching_requested = live.group_cache_settings.contains_key(&group.0)
            || queryflux_cache::extract_cache_hint(&sql, &session).is_some();
        caching_requested
            && queryflux_cache::is_deterministic(
                &sql,
                &queryflux_fingerprint::polyglot_dialect(&protocol.default_dialect()),
            )
    };

    if !use_cache_path && state.group_supports_async(&group.0).await {
        match dispatch_query(
            &state,
            query_id.clone(),
            sql.clone(),
            vec![],
            session.clone(),
            protocol.clone(),
            group.clone(),
            false,
            None,
            0,
            &auth_ctx,
        )
        .await
        {
            Ok(outcome) => outcome_to_response(&state, &query_id, outcome),
            Err(QueryFluxError::SyncEngineRequired(_)) => {
                let mut sink = TrinoHttpResultSink::new(&query_id.0);
                if let Err(e) = execute_to_sink(
                    &state,
                    sql,
                    vec![],
                    session,
                    protocol,
                    group,
                    &mut sink,
                    &auth_ctx,
                )
                .await
                {
                    warn!(id = %query_id, "execute_to_sink error: {e}");
                }
                sink.into_response()
            }
            Err(QueryFluxError::Unauthorized(msg)) => {
                warn!(id = %query_id, "Unauthorized: {msg}");
                StatusCode::FORBIDDEN.into_response()
            }
            Err(e) => {
                warn!(id = %query_id, "Dispatch error: {e}");
                let msg = client_safe_message(&e);
                let msg = if msg.is_empty() {
                    e.to_string()
                } else {
                    msg.to_string()
                };
                trino_error_response(&query_id.0, &msg).into_response()
            }
        }
    } else {
        let mut sink = TrinoHttpResultSink::new(&query_id.0);
        if let Err(e) = execute_to_sink(
            &state,
            sql,
            vec![],
            session,
            protocol,
            group,
            &mut sink,
            &auth_ctx,
        )
        .await
        {
            warn!(id = %query_id, "execute_to_sink error: {e}");
        }
        sink.into_response()
    }
}

/// Extract raw credentials from Trino HTTP headers for authentication.
/// Supports `Authorization: Basic` and `Authorization: Bearer`.
/// Falls back to `X-Trino-User` as username when no Authorization header is present.
fn extract_credentials(headers: &HeaderMap) -> Credentials {
    use axum::http::header::AUTHORIZATION;

    if let Some(auth) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(encoded) = auth.strip_prefix("Basic ") {
            if let Ok(decoded) = base64_decode(encoded) {
                if let Some((user, pass)) = decoded.split_once(':') {
                    return Credentials {
                        username: Some(user.to_string()),
                        password: Some(pass.to_string()),
                        bearer_token: None,
                    };
                }
            }
        }
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return Credentials {
                username: None,
                password: None,
                bearer_token: Some(token.to_string()),
            };
        }
    }

    // No Authorization header — fall back to X-Trino-User (NoneAuthProvider path).
    let username = headers
        .get("x-trino-user")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    Credentials {
        username,
        password: None,
        bearer_token: None,
    }
}

/// Decode standard base64 without a dependency — sufficient for Phase 1 Basic auth parsing.
/// Returns the decoded string on success, or Err(()) on invalid input.
fn base64_decode(encoded: &str) -> Result<String, ()> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut lookup = [0xffu8; 256];
    for (i, &b) in TABLE.iter().enumerate() {
        lookup[b as usize] = i as u8;
    }
    let encoded = encoded.trim_end_matches('=');
    let mut out = Vec::with_capacity((encoded.len() * 3) / 4 + 1);
    let bytes: Vec<u8> = encoded.bytes().collect();
    let mut i = 0;
    while i + 3 < bytes.len() {
        let (a, b, c, d) = (
            lookup[bytes[i] as usize],
            lookup[bytes[i + 1] as usize],
            lookup[bytes[i + 2] as usize],
            lookup[bytes[i + 3] as usize],
        );
        if a == 0xff || b == 0xff || c == 0xff || d == 0xff {
            return Err(());
        }
        out.push((a << 2) | (b >> 4));
        out.push((b << 4) | (c >> 2));
        out.push((c << 6) | d);
        i += 4;
    }
    match bytes.len() - i {
        2 => {
            let (a, b) = (lookup[bytes[i] as usize], lookup[bytes[i + 1] as usize]);
            if a == 0xff || b == 0xff {
                return Err(());
            }
            out.push((a << 2) | (b >> 4));
        }
        3 => {
            let (a, b, c) = (
                lookup[bytes[i] as usize],
                lookup[bytes[i + 1] as usize],
                lookup[bytes[i + 2] as usize],
            );
            if a == 0xff || b == 0xff || c == 0xff {
                return Err(());
            }
            out.push((a << 2) | (b >> 4));
            out.push((b << 4) | (c >> 2));
        }
        _ => {}
    }
    String::from_utf8(out).map_err(|_| ())
}

/// GET /v1/statement/qf/queued/{id}/{seq} — poll a query queued in QueryFlux.
pub async fn get_queued_statement(
    State(state): State<Arc<AppState>>,
    Path((id, seq)): Path<(String, u64)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let creds = extract_credentials(&headers);
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::TrinoHttp));
            warn!("Queued poll auth failed: {e}");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let query_id = ProxyQueryId(id);

    // Load first so we can reject a non-owner *before* taking a distributed
    // claim (otherwise an attacker could pin the row for the claim TTL).
    let queued = match state.persistence.get_queued(&query_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!("Persistence error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Fail on wall-clock wait from enqueue (`creation_time`), not `last_accessed`
    // (polls refresh last_accessed and would otherwise wait forever).
    let wait_timeout_secs = {
        let live = state.live.read().await;
        live.group_capacity_wait_timeout_secs
            .get(&queued.cluster_group.0)
            .copied()
            .unwrap_or(queryflux_core::config::DEFAULT_CAPACITY_WAIT_TIMEOUT_SECS)
    };
    let waited_secs = (Utc::now() - queued.creation_time).num_seconds().max(0) as u64;
    if waited_secs >= wait_timeout_secs {
        let err = QueryFluxError::CapacityWaitTimeout {
            group: queued.cluster_group.0.clone(),
            timeout_secs: wait_timeout_secs,
        };
        if let Ok(Some(taken)) = state.persistence.take_queued(&query_id).await {
            state.record_queued_terminal(&taken, QueryStatus::Failed, &err.to_string());
        }
        if let Some(qc) = &state.queue_coordinator {
            let _ = qc.release_claim(&query_id.0).await;
        }
        return trino_error_response(&query_id.0, &err.to_string()).into_response();
    }

    if let Some(resp) = allow_query_action_or_forbid(
        &state,
        &auth_ctx,
        QueryAction::Dequeue,
        &queued.submitted_by,
        &queued.cluster_group.0,
    )
    .await
    {
        return resp;
    }

    // In distributed mode, try to claim ownership of this queued query so only
    // one replica dispatches it. If another replica already claimed it, return
    // a "still queued" response and let the client poll again.
    //
    // Claims are held only for the duration of one dispatch attempt. A claim
    // older than the timeout means the claiming replica crashed mid-dispatch,
    // so it is treated as abandoned and taken over. Heartbeat while dispatch
    // runs so a slow submit cannot look like a crash.
    let mut claim_heartbeat = None;
    if let Some(qc) = &state.queue_coordinator {
        let stale_before = chrono::Utc::now() - chrono::Duration::seconds(QUEUE_CLAIM_TIMEOUT_SECS);
        match qc
            .try_claim(&query_id.0, &state.instance_id, stale_before)
            .await
        {
            Ok(Some(_)) => {
                claim_heartbeat = Some(QueueClaimHeartbeat::start(
                    qc.clone(),
                    query_id.0.clone(),
                    state.instance_id.clone(),
                ));
            }
            Ok(None) => {
                // `None` means either claimed by another replica or the row no
                // longer exists (finished/cleaned up). Distinguish them: a
                // deleted query must 404 instead of polling "queued" forever.
                match state.persistence.get_queued(&query_id).await {
                    Ok(Some(_)) => {
                        // Claimed by another replica — tell client to keep polling.
                        let next_uri = format!(
                            "{}/v1/statement/qf/queued/{}/{}",
                            state.external_address,
                            query_id.0,
                            seq + 1
                        );
                        return json_response(queued_response(&query_id.0, seq, next_uri))
                            .into_response();
                    }
                    Ok(None) => return StatusCode::NOT_FOUND.into_response(),
                    Err(e) => {
                        warn!("Persistence error checking claimed queued query: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                }
            }
            Err(e) => {
                state.metrics.on_coordination_failure("queue_claim");
                warn!("QueueCoordinator try_claim error: {e}");
                let resp = queryflux_engine_adapters::trino::api::TrinoResponse {
                    id: query_id.0.clone(),
                    next_uri: None,
                    info_uri: "http://queryflux/ui/query.html".to_string(),
                    partial_cancel_uri: None,
                    stats: queryflux_engine_adapters::trino::api::TrinoStats {
                        state: "FAILED".to_string(),
                        queued: false,
                        scheduled: false,
                        ..Default::default()
                    },
                    error: Some(queryflux_engine_adapters::trino::api::TrinoError {
                        message: "Query coordination temporarily unavailable, please retry"
                            .to_string(),
                        error_code: Some(0),
                        error_name: Some("TEMPORARILY_UNAVAILABLE".to_string()),
                        error_type: Some("INTERNAL_ERROR".to_string()),
                        failure_info: Default::default(),
                    }),
                    columns: None,
                    data: None,
                    update_type: None,
                    update_count: None,
                    warnings: vec![],
                };
                let json = serde_json::to_vec(&resp).unwrap_or_default();
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("content-type", "application/json")
                    .body(Body::from(json))
                    .unwrap()
                    .into_response();
            }
        }
    }
    let _claim_heartbeat = claim_heartbeat;

    if let Err(e) = state
        .persistence
        .touch_queued_last_accessed(&query_id)
        .await
    {
        warn!(id = %query_id, "Failed to refresh queued last_accessed: {e}");
    }

    queued_backoff_delay(seq).await;

    let sql = queued.sql.clone();
    let session = session_for_queued_dispatch(queued.session.clone(), &headers, &auth_ctx);
    let protocol = queued.frontend_protocol.clone();
    let group = queued.cluster_group.clone();

    let release_claim = |state: &Arc<AppState>, qid: &str| {
        let qc = state.queue_coordinator.clone();
        let qid = qid.to_string();
        async move {
            if let Some(qc) = qc {
                let _ = qc.release_claim(&qid).await;
            }
        }
    };

    // Admin cancel deletes the queued row. Abort if it disappeared after claim
    // so dequeue cannot dispatch a query that already returned 204.
    match state.persistence.get_queued(&query_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            release_claim(&state, &query_id.0).await;
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(e) => {
            warn!("Persistence error re-checking queued query before dispatch: {e}");
            release_claim(&state, &query_id.0).await;
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    let use_cache_path = {
        let live = state.live.read().await;
        let caching_requested = live.group_cache_settings.contains_key(&group.0)
            || queryflux_cache::extract_cache_hint(&sql, &session).is_some();
        caching_requested
            && queryflux_cache::is_deterministic(
                &sql,
                &queryflux_fingerprint::polyglot_dialect(&protocol.default_dialect()),
            )
    };

    if !use_cache_path && state.group_supports_async(&group.0).await {
        match dispatch_query(
            &state,
            query_id.clone(),
            sql.clone(),
            vec![],
            session.clone(),
            protocol.clone(),
            group.clone(),
            true,
            Some(queued.creation_time),
            seq,
            &auth_ctx,
        )
        .await
        {
            Ok(outcome) => {
                if matches!(&outcome, DispatchOutcome::Queued { .. }) {
                    // Re-queued (no capacity yet) — release claim so other replicas can try.
                    release_claim(&state, &query_id.0).await;
                }
                outcome_to_response(&state, &query_id, outcome)
            }
            Err(QueryFluxError::SyncEngineRequired(_)) => {
                if let Err(e) = state.persistence.delete_queued(&query_id).await {
                    warn!(id = %query_id, "Failed to delete queued record on sync fallback: {e}");
                }
                let mut sink = TrinoHttpResultSink::new(&query_id.0);
                if let Err(e) = execute_to_sink(
                    &state,
                    sql,
                    vec![],
                    session,
                    protocol,
                    group,
                    &mut sink,
                    &auth_ctx,
                )
                .await
                {
                    warn!(id = %query_id, "execute_to_sink error: {e}");
                }
                sink.into_response()
            }
            Err(e) => {
                release_claim(&state, &query_id.0).await;
                warn!(id = %query_id, "Dispatch error: {e}");
                let msg = client_safe_message(&e);
                let msg = if msg.is_empty() {
                    e.to_string()
                } else {
                    msg.to_string()
                };
                trino_error_response(&query_id.0, &msg).into_response()
            }
        }
    } else {
        if let Err(e) = state.persistence.delete_queued(&query_id).await {
            warn!(id = %query_id, "Failed to delete queued record before sync dispatch: {e}");
        }
        let mut sink = TrinoHttpResultSink::new(&query_id.0);
        if let Err(e) = execute_to_sink(
            &state,
            sql,
            vec![],
            session,
            protocol,
            group,
            &mut sink,
            &auth_ctx,
        )
        .await
        {
            warn!(id = %query_id, "execute_to_sink error: {e}");
        }
        sink.into_response()
    }
}

/// GET /v1/statement/{*trino_path} — poll any Trino statement URL (queued or executing).
///
/// Trino's query lifecycle uses two path prefixes: `/v1/statement/queued/...` initially,
/// then `/v1/statement/executing/...` once running. Both are handled identically here.
///
/// The path is embedded verbatim in the client-facing URL. Any QueryFlux instance looks up
/// the stored `poll_base_url` by trino_id (second path segment) and reconstructs the full
/// Trino URL — no persistence write needed between polls.
pub async fn get_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(trino_path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Authenticate the polling request when auth is enabled.
    let creds = extract_credentials(&headers);
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::TrinoHttp));
            warn!("Poll auth failed: {e}");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // trino_path = e.g. "queued/20260319_084733_00386_kqwci/1/token"
    //                 or "executing/20260319_084733_00386_kqwci/1/token"

    // Reject path traversal: a ".." segment would escape /v1/statement/ on the backend.
    if trino_path.split('/').any(|seg| seg == "..") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // Extract the Trino query ID (always the second segment).
    let trino_id = match trino_path.split('/').nth(1) {
        Some(id) => id.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let backend_id = BackendQueryId(trino_id.clone());

    let executing = match state.persistence.get(&backend_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!("Persistence error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if let Some(resp) = allow_query_action_or_forbid(
        &state,
        &auth_ctx,
        QueryAction::Poll,
        &executing.submitted_by,
        &executing.cluster_group.0,
    )
    .await
    {
        return resp;
    }

    let adapter = match state.adapter(&executing.cluster_name.0).await {
        Some(a) => match a.as_async() {
            Some(async_adapter) => async_adapter,
            None => {
                warn!(
                    "Adapter for cluster {}/{} is not async",
                    executing.cluster_group, executing.cluster_name
                );
                state
                    .release_query_slot(
                        &executing.cluster_group,
                        &executing.cluster_name,
                        &executing.id.0,
                    )
                    .await;
                if let Err(e) = state.persistence.delete(&backend_id).await {
                    warn!(id = %executing.id, "Failed to delete executing record: {e}");
                }
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        },
        None => {
            warn!(
                "No adapter for cluster {}/{}",
                executing.cluster_group, executing.cluster_name
            );
            state
                .release_query_slot(
                    &executing.cluster_group,
                    &executing.cluster_name,
                    &executing.id.0,
                )
                .await;
            if let Err(e) = state.persistence.delete(&backend_id).await {
                warn!(id = %executing.id, "Failed to delete executing record: {e}");
            }
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Reconstruct the full Trino poll URL: stored base URL + /v1/statement/ + captured path.
    let trino_url = format!(
        "{}/v1/statement/{}",
        executing
            .poll_base_url
            .as_deref()
            .unwrap_or_default()
            .trim_end_matches('/'),
        trino_path
    );

    // Forward session headers to Trino.
    let session = extract_session(&headers);

    // Tags were captured at submit time (includes client tags from the original POST).
    // Poll requests don't repeat client headers, so we use the stored value.
    let effective_tags = executing.query_tags.clone();

    // Throttled last_accessed refresh: write to persistence at most every 120s per query.
    // This keeps the record "alive" for the zombie-cleanup task across all proxy instances,
    // without adding a persistence write on every poll.
    const LAST_ACCESSED_UPDATE_INTERVAL: i64 = 120;
    let now = Utc::now();
    if (now - executing.last_accessed).num_seconds() >= LAST_ACCESSED_UPDATE_INTERVAL {
        let mut refreshed = executing.clone();
        refreshed.last_accessed = now;
        let _ = state.persistence.upsert(refreshed).await;
    }

    let elapsed_ms = (Utc::now() - executing.creation_time)
        .num_milliseconds()
        .max(0) as u64;

    // Build query context once — reused for success, failure, and poll-error record_query calls.
    let was_translated = executing.translated_sql.is_some();
    let ctx = QueryContext {
        query_id: executing.id.clone(),
        // original SQL: when translated, translated_sql holds it; otherwise sql is original
        sql: executing
            .translated_sql
            .as_deref()
            .unwrap_or(&executing.sql)
            .to_string(),
        session: session.clone(),
        protocol: FrontendProtocol::TrinoHttp,
        group: executing.cluster_group.clone(),
        cluster: executing.cluster_name.clone(),
        cluster_group_config_id: executing.cluster_group_config_id,
        cluster_config_id: executing.cluster_config_id,
        engine_type: adapter.engine_type(),
        src_dialect: FrontendProtocol::TrinoHttp.default_dialect(),
        tgt_dialect: adapter.translation_target_dialect(),
        was_translated,
        translated_sql: if was_translated {
            Some(executing.sql.clone())
        } else {
            None
        },
        query_tags: effective_tags,
        query_params: vec![],
        agent_context: executing.agent_context.clone(),
    };

    // Guard actions captured at submit time — injected into the final record_query call.
    let submit_guard_actions: Vec<queryflux_persistence::GuardAction> = match serde_json::from_value(
        serde_json::Value::Array(executing.submitted_guard_actions.clone()),
    ) {
        Ok(actions) => actions,
        Err(e) => {
            warn!(id = %executing.id, "Failed to deserialize stored guard actions — audit record will be incomplete: {e}");
            vec![]
        }
    };
    let submit_was_guard_blocked = executing.was_guard_blocked;

    let poll_result = match adapter
        .poll_query(&backend_id, Some(&trino_url), executing.wire_auth.as_ref())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            if e.is_transient() {
                warn!(id = %executing.id, "Transient poll error (will retry): {e}");
                let next_uri = format!("{}/v1/statement/{}", state.external_address, trino_path);
                let resp = queued_response(&executing.id.0, 0, next_uri);
                return json_response(&resp).into_response();
            }
            error!(id = %executing.id, "Permanent poll error: {e}");
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id: Some(backend_id.0.clone()),
                    status: QueryStatus::Failed,
                    execution_ms: elapsed_ms,
                    rows: None,
                    error: Some(e.to_string()),
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: submit_guard_actions,
                    was_guard_blocked: submit_was_guard_blocked,
                    queue_duration_ms: 0,
                    cache_hit: false,
                },
            );
            state
                .release_query_slot(
                    &executing.cluster_group,
                    &executing.cluster_name,
                    &executing.id.0,
                )
                .await;
            if let Err(del_err) = state.persistence.delete(&backend_id).await {
                warn!(id = %executing.id, "Failed to delete executing record after poll error: {del_err}");
            }
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match poll_result {
        QueryPollResult::Raw {
            body,
            poll_token,
            engine_stats,
        } => {
            if poll_token.is_none() {
                // Final page — query complete.
                state.record_query(
                    &ctx,
                    QueryOutcome {
                        backend_query_id: Some(backend_id.0.clone()),
                        status: QueryStatus::Success,
                        execution_ms: elapsed_ms,
                        rows: None,
                        error: None,
                        routing_trace: None,
                        engine_stats,
                        guard_actions: submit_guard_actions,
                        was_guard_blocked: submit_was_guard_blocked,
                        queue_duration_ms: 0,
                        cache_hit: false,
                    },
                );
                state
                    .release_query_slot(
                        &executing.cluster_group,
                        &executing.cluster_name,
                        &executing.id.0,
                    )
                    .await;
                if let Err(e) = state.persistence.delete(&backend_id).await {
                    warn!(id = %executing.id, "Failed to delete executing record on completion: {e}");
                }
                return raw_response_with_rewritten_next_uri(body, None).into_response();
            }

            // Intermediate page — rewrite nextUri (swap Trino host → QueryFlux), no persistence write.
            let proxy_next_uri = poll_token
                .as_deref()
                .map(|uri| rewrite_trino_uri(uri, &state.external_address));
            raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response()
        }

        QueryPollResult::Failed { message, .. } => {
            state.record_query(
                &ctx,
                QueryOutcome {
                    backend_query_id: Some(backend_id.0.clone()),
                    status: QueryStatus::Failed,
                    execution_ms: elapsed_ms,
                    rows: None,
                    error: Some(message.clone()),
                    routing_trace: None,
                    engine_stats: None,
                    guard_actions: submit_guard_actions,
                    was_guard_blocked: submit_was_guard_blocked,
                    queue_duration_ms: 0,
                    cache_hit: false,
                },
            );
            state
                .release_query_slot(
                    &executing.cluster_group,
                    &executing.cluster_name,
                    &executing.id.0,
                )
                .await;
            warn!(id = %executing.id, "Query failed: {message}");
            if let Err(e) = state.persistence.delete(&backend_id).await {
                warn!(id = %executing.id, "Failed to delete executing record on failure: {e}");
            }
            let error_resp = TrinoResponse {
                id: executing.id.0.clone(),
                next_uri: None,
                info_uri: format!("{}/ui/query.html", state.external_address),
                partial_cancel_uri: None,
                stats: TrinoStats {
                    state: "FAILED".to_string(),
                    queued: false,
                    scheduled: false,
                    elapsed_time_millis: elapsed_ms,
                    ..Default::default()
                },
                error: Some(TrinoError {
                    message: message.clone(),
                    error_code: Some(0),
                    error_name: Some("QUERY_FAILED".to_string()),
                    error_type: Some("USER_ERROR".to_string()),
                    failure_info: Default::default(),
                }),
                columns: None,
                data: None,
                update_type: None,
                update_count: None,
                warnings: vec![],
            };
            json_response(&error_resp).into_response()
        }

        QueryPollResult::Pending { poll_token, .. } => {
            // Still running — rewrite poll URL, no persistence write needed.
            let proxy_next_uri = poll_token
                .as_deref()
                .map(|uri| rewrite_trino_uri(uri, &state.external_address))
                .unwrap_or_else(|| {
                    format!("{}/v1/statement/{}", state.external_address, trino_path)
                });
            let resp = queued_response(&executing.id.0, 0, proxy_next_uri);
            json_response(&resp).into_response()
        }
    }
}

/// DELETE /v1/statement/{*trino_path} — cancel a running query.
pub async fn delete_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(trino_path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let creds = extract_credentials(&headers);
    let auth_provider = state.live.read().await.auth_provider.clone();
    let auth_ctx = match auth_provider.authenticate(&creds).await {
        Ok(ctx) => ctx,
        Err(e) => {
            state
                .metrics
                .on_auth_failure(&format!("{:?}", FrontendProtocol::TrinoHttp));
            warn!("Cancel auth failed: {e}");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Reject path traversal before constructing the backend cancel URL.
    if trino_path.split('/').any(|seg| seg == "..") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let trino_id = match trino_path.split('/').nth(1) {
        Some(id) => id.to_string(),
        None => return StatusCode::NO_CONTENT.into_response(),
    };
    let backend_id = BackendQueryId(trino_id);

    let executing = match state.persistence.get(&backend_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            warn!("Persistence error on cancel: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if let Some(resp) = allow_query_action_or_forbid(
        &state,
        &auth_ctx,
        QueryAction::Cancel,
        &executing.submitted_by,
        &executing.cluster_group.0,
    )
    .await
    {
        return resp;
    }

    let Some(adapter) = state.adapter(&executing.cluster_name.0).await else {
        warn!(
            id = %executing.id,
            cluster = %executing.cluster_name,
            "No adapter for cluster; refusing to release slot without backend cancel"
        );
        return (StatusCode::BAD_GATEWAY, "failed to cancel query on backend").into_response();
    };

    if let Err(e) = adapter
        .cancel_query(&executing.backend_query_id, executing.wire_auth.as_ref())
        .await
    {
        warn!(
            id = %executing.id,
            backend = %executing.backend_query_id,
            "Cancel with cluster credentials failed: {e}"
        );
        return (StatusCode::BAD_GATEWAY, "failed to cancel query on backend").into_response();
    }

    state.record_executing_cancelled(
        &executing,
        FrontendProtocol::TrinoHttp,
        adapter.engine_type(),
        adapter.translation_target_dialect(),
        "client cancelled",
    );
    state
        .release_query_slot(
            &executing.cluster_group,
            &executing.cluster_name,
            &executing.id.0,
        )
        .await;
    if let Err(e) = state.persistence.delete(&backend_id).await {
        warn!(id = %executing.id, "Failed to delete executing record on cancel: {e}");
    }

    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod queue_claim_heartbeat_tests {
    use super::{QUEUE_CLAIM_HEARTBEAT_SECS, QUEUE_CLAIM_TIMEOUT_SECS};

    #[test]
    fn heartbeat_interval_is_well_below_stale_claim_timeout() {
        assert!(
            QUEUE_CLAIM_HEARTBEAT_SECS * 2 <= QUEUE_CLAIM_TIMEOUT_SECS as u64,
            "heartbeat must run at least twice within the stale-claim window"
        );
    }
}

#[cfg(test)]
mod cancel_executing_statement_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use chrono::Utc;
    use queryflux_auth::{
        AllowAllAuthorization, AuthProvider, AuthorizationChecker, BackendIdentityResolver,
        NoneAuthProvider,
    };
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
    };
    use queryflux_core::{
        error::{QueryFluxError, Result},
        query::{
            BackendQueryId, ClusterGroupName, ClusterName, EngineType, ExecutingQuery,
            ProxyQueryId, QueryExecution, QueryPollResult, SqlDialect,
        },
    };
    use queryflux_engine_adapters::{AdapterKind, AsyncAdapter};
    use queryflux_metrics::NoopMetricsStore;
    use queryflux_persistence::in_memory::InMemoryPersistence;
    use queryflux_routing::chain::RouterChain;
    use queryflux_translation::TranslationService;
    use tokio::sync::RwLock;

    use super::delete_executing_statement;
    use crate::state::{AppState, LiveConfig};

    struct CancelStubAdapter {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl AsyncAdapter for CancelStubAdapter {
        async fn submit_query(
            &self,
            _sql: &str,
            _session: &queryflux_core::session::SessionContext,
            _credentials: &queryflux_auth::QueryCredentials,
            _tags: &queryflux_core::tags::QueryTags,
            _params: &queryflux_core::params::QueryParams,
        ) -> Result<QueryExecution> {
            Err(QueryFluxError::Engine("not used in cancel tests".into()))
        }

        async fn poll_query(
            &self,
            _backend_id: &BackendQueryId,
            _poll_token: Option<&str>,
            _wire_auth: Option<&queryflux_core::query::StoredWireAuth>,
        ) -> Result<QueryPollResult> {
            Err(QueryFluxError::Engine("not used in cancel tests".into()))
        }

        async fn cancel_query(
            &self,
            _backend_id: &BackendQueryId,
            _wire_auth: Option<&queryflux_core::query::StoredWireAuth>,
        ) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(QueryFluxError::Engine("cancel rejected".into()))
            } else {
                Ok(())
            }
        }

        fn engine_type(&self) -> EngineType {
            EngineType::Trino
        }

        fn translation_target_dialect(&self) -> SqlDialect {
            SqlDialect::Trino
        }

        async fn health_check(&self) -> bool {
            true
        }

        async fn list_catalogs(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn describe_table(
            &self,
            _catalog: &str,
            _database: &str,
            _table: &str,
        ) -> Result<Option<queryflux_core::catalog::TableSchema>> {
            Ok(None)
        }
    }

    fn test_cluster_names() -> (ClusterGroupName, ClusterName) {
        (
            ClusterGroupName("default".into()),
            ClusterName("trino".into()),
        )
    }

    async fn test_state(adapter: Option<AdapterKind>) -> Arc<AppState> {
        let (group_name, cluster_name) = test_cluster_names();
        let cluster_state = Arc::new(ClusterState::new(
            cluster_name.clone(),
            group_name.clone(),
            None,
            None,
            EngineType::Trino,
            Some("http://trino.test:8080".into()),
            10,
            true,
        ));
        let mut adapters = HashMap::new();
        if let Some(adapter) = adapter {
            adapters.insert(cluster_name.0.clone(), adapter);
        }
        let mut group_members = HashMap::new();
        group_members.insert(group_name.0.clone(), vec![cluster_name.0.clone()]);
        let mut groups = HashMap::new();
        groups.insert(
            group_name.clone(),
            (
                vec![cluster_state],
                Arc::new(queryflux_cluster_manager::strategy::RoundRobinStrategy::new())
                    as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
            ),
        );
        let live = LiveConfig {
            router_chain: RouterChain::new(vec![], group_name.clone()),
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager: Arc::new(SimpleClusterGroupManager::new(groups)),
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members,
            group_order: vec![group_name.0.clone()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_max_queued_queries: HashMap::new(),
            group_capacity_wait_timeout_secs: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization::default())
                as Arc<dyn AuthorizationChecker>,
        };
        Arc::new(AppState {
            external_address: "http://127.0.0.1:8080".into(),
            live: Arc::new(RwLock::new(live)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation: Arc::new(TranslationService::disabled()),
            metrics: Arc::new(NoopMetricsStore),
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            capacity_store: None,
            queue_coordinator: None,
            instance_id: "test".into(),
            http_client: reqwest::Client::new(),
            result_cache: Arc::new(queryflux_cache::noop::NoopResultCache),
        })
    }

    async fn seed_executing(state: &AppState, backend_id: &str) {
        let (group_name, cluster_name) = test_cluster_names();
        let executing = ExecutingQuery {
            id: ProxyQueryId("proxy-1".into()),
            sql: "SELECT 1".into(),
            translated_sql: None,
            cluster_group: group_name,
            cluster_name,
            cluster_group_config_id: None,
            cluster_config_id: None,
            backend_query_id: BackendQueryId(backend_id.into()),
            poll_base_url: None,
            creation_time: Utc::now(),
            last_accessed: Utc::now(),
            query_tags: HashMap::new(),
            agent_context: None,
            submitted_guard_actions: vec![],
            was_guard_blocked: false,
            submitted_by: "anonymous".into(),
            wire_auth: None,
        };
        state
            .persistence
            .upsert(executing)
            .await
            .expect("seed executing query");
    }

    #[tokio::test]
    async fn cancel_removes_executing_record_after_adapter_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let adapter = AdapterKind::Async(Arc::new(CancelStubAdapter {
            calls: calls.clone(),
            fail: false,
        }));
        let state = test_state(Some(adapter)).await;
        seed_executing(&state, "trino-q-ok").await;

        let resp = delete_executing_statement(
            State(state.clone()),
            Path("executing/trino-q-ok".into()),
            HeaderMap::new(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            state
                .persistence
                .get(&BackendQueryId("trino-q-ok".into()))
                .await
                .expect("get")
                .is_none(),
            "executing record must be deleted only after backend cancel succeeds"
        );
    }

    #[tokio::test]
    async fn cancel_keeps_executing_record_when_adapter_fails() {
        let adapter = AdapterKind::Async(Arc::new(CancelStubAdapter {
            calls: Arc::new(AtomicUsize::new(0)),
            fail: true,
        }));
        let state = test_state(Some(adapter)).await;
        seed_executing(&state, "trino-q-fail").await;

        let resp = delete_executing_statement(
            State(state.clone()),
            Path("executing/trino-q-fail".into()),
            HeaderMap::new(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert!(
            state
                .persistence
                .get(&BackendQueryId("trino-q-fail".into()))
                .await
                .expect("get")
                .is_some(),
            "slot must not be released when backend cancel fails"
        );
    }

    #[tokio::test]
    async fn cancel_refuses_when_cluster_adapter_missing() {
        let state = test_state(None).await;
        seed_executing(&state, "trino-q-no-adapter").await;

        let resp = delete_executing_statement(
            State(state.clone()),
            Path("executing/trino-q-no-adapter".into()),
            HeaderMap::new(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert!(state
            .persistence
            .get(&BackendQueryId("trino-q-no-adapter".into()))
            .await
            .expect("get")
            .is_some());
    }
}

#[cfg(test)]
mod auth_fast_path_tests {
    use axum::extract::State;
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use bytes::Bytes;

    use super::post_statement;
    use crate::state::test_fixtures;

    #[tokio::test]
    async fn set_session_fast_path_rejects_unauthenticated_when_required() {
        let state = test_fixtures::app_state(true);
        let resp = post_statement(
            State(state),
            HeaderMap::new(),
            Bytes::from("SET SESSION query_tags = 'team=eng'"),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn set_session_fast_path_allows_authenticated_user() {
        let state = test_fixtures::app_state(true);
        let mut headers = HeaderMap::new();
        headers.insert("X-Trino-User", HeaderValue::from_static("alice"));
        let resp = post_statement(
            State(state),
            headers,
            Bytes::from("SET SESSION query_tags = 'team=eng'"),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[cfg(test)]
mod trino_session_property_encoding_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn encode_decode_roundtrip_commas_and_colons() {
        let raw = "team:eng,cost_center:701";
        let enc = encode_trino_session_property_value(raw);
        assert!(
            enc.contains("%2C") || enc.contains("%2c"),
            "comma should be percent-encoded, got {enc:?}"
        );
        assert_eq!(decode_trino_session_property_value(&enc), raw);
    }

    #[test]
    fn extract_trino_tags_decodes_query_tags_session_value() {
        let mut h = HashMap::new();
        h.insert(
            "x-trino-session".to_string(),
            format!(
                "query_tags={}",
                encode_trino_session_property_value("team:eng,cost_center:701")
            ),
        );
        let tags = extract_trino_tags(&h);
        assert_eq!(tags.get("team"), Some(&Some("eng".to_string())));
        assert_eq!(tags.get("cost_center"), Some(&Some("701".to_string())));
    }

    #[test]
    fn extract_trino_tags_plain_ascii_still_works() {
        let mut h = HashMap::new();
        h.insert(
            "x-trino-session".to_string(),
            "query_tag=team:eng".to_string(),
        );
        let tags = extract_trino_tags(&h);
        assert_eq!(tags.get("team"), Some(&Some("eng".to_string())));
    }
}
