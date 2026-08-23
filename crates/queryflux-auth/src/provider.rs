use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use queryflux_core::config::{OidcConfig, StaticUserEntry};
use queryflux_core::error::{QueryFluxError, Result};
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::credentials::{AuthContext, Credentials};

/// Verifies client credentials and produces a canonical `AuthContext`.
///
/// Implementations:
/// - `NoneAuthProvider`   — network-trust only; identity from session (no crypto verification)
/// - `StaticAuthProvider` — user/password map in config (Phase 2)
/// - `OidcAuthProvider`   — JWT validation via JWKS (Phase 2)
/// - `LdapAuthProvider`   — LDAP bind + group lookup (Phase 5)
#[async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext>;
}

// ---------------------------------------------------------------------------
// NoneAuthProvider
// ---------------------------------------------------------------------------

/// No-op auth provider. Derives identity from the session username with no
/// cryptographic verification. Suitable for trusted networks (VPC, mTLS at LB).
///
/// Behavior:
/// - `username` present → `AuthContext { user: username, groups: [], roles: [], raw_token: bearer_token }`
/// - `username` absent → user = `"anonymous"` (even if `bearer_token` is present; token is passed through as `raw_token`)
///
/// `auth.required: true` with this provider rejects requests that have no username,
/// but provides no JWT signature checks. Document clearly for operators.
pub struct NoneAuthProvider {
    pub required: bool,
}

impl NoneAuthProvider {
    pub fn new(required: bool) -> Self {
        Self { required }
    }
}

#[async_trait]
impl AuthProvider for NoneAuthProvider {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext> {
        let user = match &creds.username {
            Some(u) if !u.is_empty() => u.clone(),
            _ => {
                if self.required {
                    return Err(QueryFluxError::Auth(
                        "authentication required: no username provided".into(),
                    ));
                }
                "anonymous".to_string()
            }
        };

        Ok(AuthContext {
            user,
            groups: vec![],
            roles: vec![],
            raw_token: creds.bearer_token.clone(),
            raw_password: None,
        })
    }
}

// ---------------------------------------------------------------------------
// StaticAuthProvider
// ---------------------------------------------------------------------------

/// Config-driven username/password map. For dev and simple deployments.
///
/// - Validates `credentials.username` + `credentials.password` against the map.
/// - Bearer tokens are rejected (use `OidcAuthProvider` for JWT auth).
/// - Passwords stored in plain text in config; suitable for dev only.
pub struct StaticAuthProvider {
    users: HashMap<String, StaticUserEntry>,
    required: bool,
}

impl StaticAuthProvider {
    pub fn new(users: HashMap<String, StaticUserEntry>, required: bool) -> Self {
        Self { users, required }
    }
}

#[async_trait]
impl AuthProvider for StaticAuthProvider {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext> {
        // Static provider does not handle JWTs — if a bearer token is present
        // without a username, reject or fall back to anonymous.
        if creds.bearer_token.is_some() && creds.username.is_none() {
            return Err(QueryFluxError::Auth(
                "static auth provider does not accept bearer tokens without a username".into(),
            ));
        }

        let username = match &creds.username {
            Some(u) if !u.is_empty() => u.as_str(),
            _ => {
                if self.required {
                    return Err(QueryFluxError::Auth(
                        "authentication required: no username provided".into(),
                    ));
                }
                return Ok(AuthContext {
                    user: "anonymous".to_string(),
                    groups: vec![],
                    roles: vec![],
                    raw_token: None,
                    raw_password: None,
                });
            }
        };

        let entry = self.users.get(username).ok_or_else(|| {
            QueryFluxError::Auth(format!("authentication failed for user '{username}'"))
        })?;

        // Verify password when provided.
        if let Some(provided) = &creds.password {
            if provided != &entry.password {
                return Err(QueryFluxError::Auth(format!(
                    "authentication failed for user '{username}'"
                )));
            }
        } else if self.required {
            return Err(QueryFluxError::Auth(format!(
                "authentication required: no password provided for user '{username}'"
            )));
        }

        // `raw_password` deliberately stays None: this password only proves the caller
        // knows QueryFlux's own local static-user entry, not any credential a backend
        // would recognize. See the field doc on `AuthContext::raw_password`.
        Ok(AuthContext {
            user: username.to_string(),
            groups: entry.groups.clone(),
            roles: entry.roles.clone(),
            raw_token: None,
            raw_password: None,
        })
    }
}

// ---------------------------------------------------------------------------
// OidcAuthProvider
// ---------------------------------------------------------------------------

/// OIDC JWT authentication provider. Validates `Authorization: Bearer <token>`
/// against a JWKS endpoint (Keycloak, Auth0, Okta, etc.).
///
/// - Validates signature, expiry, and (optionally) audience and issuer.
/// - Extracts `sub` as the user identity.
/// - Extracts groups from `config.groups_claim` (default: `"groups"`).
/// - Extracts roles from `config.roles_claim` if set (supports dot-notation
///   for nested claims, e.g. `"realm_access.roles"` for Keycloak).
/// - JWKS are cached for 1 hour and refreshed on next request.
pub struct OidcAuthProvider {
    config: OidcConfig,
    http_client: reqwest::Client,
    jwks_cache: Arc<RwLock<Option<(JwkSet, Instant)>>>,
    required: bool,
}

const JWKS_CACHE_TTL: Duration = Duration::from_secs(3600);

impl OidcAuthProvider {
    pub fn new(config: OidcConfig, required: bool) -> Self {
        Self {
            config,
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("build OIDC http client"),
            jwks_cache: Arc::new(RwLock::new(None)),
            required,
        }
    }

    async fn get_jwks(&self) -> Result<JwkSet> {
        // Fast path: read from cache.
        {
            let guard = self.jwks_cache.read().await;
            if let Some((jwks, fetched_at)) = guard.as_ref() {
                if fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(jwks.clone());
                }
            }
        }

        // Fetch fresh JWKS.
        debug!(jwks_uri = %self.config.jwks_uri, "Fetching JWKS");
        let jwks: JwkSet = self
            .http_client
            .get(&self.config.jwks_uri)
            .send()
            .await
            .map_err(|e| QueryFluxError::Auth(format!("failed to fetch JWKS: {e}")))?
            .json()
            .await
            .map_err(|e| QueryFluxError::Auth(format!("failed to parse JWKS: {e}")))?;

        *self.jwks_cache.write().await = Some((jwks.clone(), Instant::now()));
        Ok(jwks)
    }
}

#[async_trait]
impl AuthProvider for OidcAuthProvider {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext> {
        let token = match &creds.bearer_token {
            Some(t) => t.as_str(),
            None => {
                if self.required {
                    return Err(QueryFluxError::Auth(
                        "OIDC authentication required: no bearer token provided".into(),
                    ));
                }
                // No token — anonymous
                return Ok(AuthContext {
                    user: creds
                        .username
                        .clone()
                        .unwrap_or_else(|| "anonymous".to_string()),
                    groups: vec![],
                    roles: vec![],
                    raw_token: None,
                    raw_password: None,
                });
            }
        };

        // Audience validation is security-critical for production deployments.
        // If the operator marked auth as required but did not configure `audience`,
        // fail closed instead of skipping audience checks.
        if self.required && self.config.audience.is_none() {
            return Err(QueryFluxError::Auth(
                "OIDC audience validation required: configure auth.oidc.audience when auth.required=true"
                    .into(),
            ));
        }

        // Decode the header to get kid + algorithm.
        let header = decode_header(token)
            .map_err(|e| QueryFluxError::Auth(format!("invalid JWT header: {e}")))?;

        let jwks = self.get_jwks().await?;

        // Find the matching JWK by kid; fall back to first key if no kid in token.
        let jwk = match &header.kid {
            Some(kid) => jwks.find(kid),
            None => jwks.keys.first(),
        }
        .ok_or_else(|| QueryFluxError::Auth("no matching JWK found for token kid".into()))?;

        let decoding_key = DecodingKey::from_jwk(jwk)
            .map_err(|e| QueryFluxError::Auth(format!("failed to build decoding key: {e}")))?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.config.issuer]);
        if let Some(aud) = &self.config.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let token_data = decode::<Value>(token, &decoding_key, &validation)
            .map_err(|e| QueryFluxError::Auth(format!("JWT validation failed: {e}")))?;

        let claims = &token_data.claims;

        // Extract user from `sub`.
        let user = claims
            .get("sub")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                warn!("JWT missing 'sub' claim; falling back to preferred_username");
                claims
                    .get("preferred_username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string()
            });

        // Extract groups.
        let groups = extract_string_array(claims, &self.config.groups_claim);

        // Extract roles (optional, supports dot-notation).
        let roles = self
            .config
            .roles_claim
            .as_deref()
            .map(|claim| extract_string_array(claims, claim))
            .unwrap_or_default();

        Ok(AuthContext {
            user,
            groups,
            roles,
            raw_token: Some(token.to_string()),
            raw_password: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a `Vec<String>` from a JWT claim by dot-notation path.
/// E.g. `"realm_access.roles"` → `claims["realm_access"]["roles"]`.
fn extract_string_array(claims: &Value, claim_path: &str) -> Vec<String> {
    let value = resolve_dot_path(claims, claim_path);
    match value {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => vec![],
    }
}

/// Walk a dot-separated path through nested JSON objects.
fn resolve_dot_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::config::OidcConfig;

    #[tokio::test]
    async fn oidc_audience_required_fails_fast_when_missing() {
        let provider = OidcAuthProvider::new(
            OidcConfig {
                issuer: "https://idp".to_string(),
                jwks_uri: "https://idp/jwks".to_string(),
                audience: None,
                groups_claim: "groups".to_string(),
                roles_claim: None,
            },
            true,
        );

        let creds = Credentials {
            username: None,
            password: None,
            bearer_token: Some("not-a-real-jwt".to_string()),
        };

        let err = provider.authenticate(&creds).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("OIDC audience validation required"),
            "unexpected error: {err}"
        );
    }
}
