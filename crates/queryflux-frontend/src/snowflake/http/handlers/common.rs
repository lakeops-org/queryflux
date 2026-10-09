use std::io::Read;

use axum::http::HeaderMap;
use bytes::Bytes;
use flate2::read::GzDecoder;
use serde_json::Value;

/// Snowflake clients (Python connector, JDBC, etc.) send `Content-Encoding: gzip` and gzip the
/// JSON body for most POSTs. Axum does not decompress automatically — decode before `serde_json`.
pub fn decode_snowflake_request_body(headers: &HeaderMap, body: &Bytes) -> Result<Vec<u8>, String> {
    let gzip = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            let s = s.trim();
            s.eq_ignore_ascii_case("gzip") || s.eq_ignore_ascii_case("x-gzip")
        })
        .unwrap_or(false);
    if !gzip {
        return Ok(body.to_vec());
    }
    let mut decoder = GzDecoder::new(std::io::Cursor::new(body.as_ref()));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| format!("gzip decompress: {e}"))?;
    Ok(out)
}

/// Parse JSON from a request body, after optional gzip decompression.
pub fn parse_snowflake_json_body(headers: &HeaderMap, body: &Bytes) -> Result<Value, String> {
    let decoded = decode_snowflake_request_body(headers, body)?;
    serde_json::from_slice(&decoded).map_err(|e| e.to_string())
}

/// Extract the session token from `Authorization: Snowflake Token="<token>"`.
/// Returns `None` if the header is absent or does not match the expected format.
pub fn extract_snowflake_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    let stripped = value.trim().strip_prefix("Snowflake Token=")?;
    Some(stripped.trim_matches('"').to_string())
}

/// Revalidate the original JWT on every session operation. A gateway session
/// must not extend the lifetime of the identity-provider access token.
pub async fn authenticated_session(
    state: &crate::snowflake::http::SnowflakeWireState,
    token: &str,
) -> Option<queryflux_auth::AuthContext> {
    let identity = state.sessions.validate_session(token)?.1.auth_ctx.clone();
    let provider = state.app.live.read().await.auth_provider.clone();
    if identity.raw_token.is_some()
        || provider.wire_auth_kind() == queryflux_auth::provider::WireAuthKind::Jwt
    {
        let credentials = queryflux_auth::Credentials {
            username: Some(identity.user.clone()),
            bearer_token: identity.raw_token.clone(),
            password: None,
        };
        match crate::lease::authenticate(&state.app, &credentials).await {
            Ok(current) if current.user == identity.user => Some(current),
            _ => {
                state.sessions.remove_session(token);
                None
            }
        }
    } else {
        Some(identity)
    }
}

/// OAuth is the Snowflake CLI transport for an IdP access token. Key-pair
/// SNOWFLAKE_JWT authentication is a different protocol and is not accepted here.
pub fn oauth_login_token(data: &Value) -> Result<Option<String>, &'static str> {
    match data["AUTHENTICATOR"].as_str() {
        Some(kind) if kind.eq_ignore_ascii_case("OAUTH") => {
            let token = data["TOKEN"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 16384)
                .ok_or("Missing or invalid OAuth TOKEN")?;
            Ok(Some(token.to_owned()))
        }
        Some(kind) if kind.eq_ignore_ascii_case("SNOWFLAKE_JWT") => {
            Err("Use OAUTH for an identity-provider access token")
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    #[test]
    fn oauth_login_uses_token_instead_of_password() {
        assert_eq!(
            oauth_login_token(
                &serde_json::json!({"AUTHENTICATOR":"oauth","TOKEN":"jwt","PASSWORD":"ignored"})
            )
            .unwrap(),
            Some("jwt".into())
        );
        assert!(oauth_login_token(&serde_json::json!({"AUTHENTICATOR":"oauth"})).is_err());
        assert!(oauth_login_token(
            &serde_json::json!({"AUTHENTICATOR":"SNOWFLAKE_JWT","TOKEN":"key-pair-jwt"})
        )
        .is_err());
        assert_eq!(
            oauth_login_token(&serde_json::json!({"PASSWORD":"legacy"})).unwrap(),
            None
        );
    }

    #[test]
    fn decodes_gzip_json_body() {
        let json = br#"{"data":{"LOGIN_NAME":"u"}}"#;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(json).unwrap();
        let gz = enc.finish().unwrap();
        let bytes = Bytes::from(gz);
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        let out = decode_snowflake_request_body(&headers, &bytes).unwrap();
        assert_eq!(out.as_slice(), json);
    }

    #[test]
    fn passthrough_plain_json_without_gzip_header() {
        let raw = br#"{"a":1}"#;
        let bytes = Bytes::from_static(raw);
        let headers = HeaderMap::new();
        let out = decode_snowflake_request_body(&headers, &bytes).unwrap();
        assert_eq!(out.as_slice(), raw);
    }
}
