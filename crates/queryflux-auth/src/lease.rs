//! Bounded renewable authorization for already authenticated native connections.
use crate::{
    provider::{AuthProvider, WireAuthKind},
    AuthContext, Credentials,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use queryflux_core::error::{QueryFluxError, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

const PREFIX: &str = "queryflux-internal-lease:";
pub const MAX_LIFETIME: i64 = 8 * 3600;
#[derive(Default)]
pub struct LeaseStore {
    entries: std::sync::Mutex<HashMap<String, Lease>>,
}
struct Lease {
    secret_hash: Vec<u8>,
    connection_key: String,
    binding: Value,
    credentials: Credentials,
    user: String,
    deadline: i64,
    token_expiry: i64,
    bound: bool,
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn hash(s: &str) -> Vec<u8> {
    Sha256::digest(s.as_bytes()).to_vec()
}
fn claims(identity: &AuthContext) -> Result<(Value, i64)> {
    // Decode only AFTER the configured OIDC provider has verified this exact token.
    let token = identity
        .raw_token
        .as_deref()
        .ok_or_else(|| denied("JWT required"))?;
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| denied("JWT required"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| denied("invalid JWT"))?;
    let c: Value = serde_json::from_slice(&bytes).map_err(|_| denied("invalid JWT"))?;
    let exp = c["exp"]
        .as_i64()
        .filter(|e| *e > now())
        .ok_or_else(|| denied("expired JWT"))?;
    if c["token_type"] != "cluster_access" || c["tenant_id"].is_null() || c["cluster_id"].is_null()
    {
        return Err(denied("Knorket cluster_access JWT required"));
    }
    let keys = [
        "iss",
        "aud",
        "sub",
        "tenant_id",
        "cluster_id",
        "cluster_ref",
        "token_type",
        "actor_id",
        "actor_email",
        "actor",
        "policy_test",
        "context",
        "agent_manifest_id",
        "agent_session_id",
        "capability_attachment_id",
        "agent_id",
        "conversation_id",
    ];
    let binding = keys
        .into_iter()
        .map(|k| (k.to_owned(), c[k].clone()))
        .collect::<serde_json::Map<_, _>>();
    Ok((Value::Object(binding), exp))
}
fn denied(message: &str) -> QueryFluxError {
    QueryFluxError::Auth(message.into())
}

const CONNECTION_KID: &str = "qf-native-lease:";
#[derive(serde::Serialize, serde::Deserialize)]
struct ConnectionClaims {
    sub: String,
    exp: i64,
    iss: String,
    aud: String,
}
fn random_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}
fn secret_matches(expected: &[u8], secret: &str) -> bool {
    let supplied = hash(secret);
    expected.len() == supplied.len()
        && expected
            .iter()
            .zip(supplied)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}
impl LeaseStore {
    /// `identity` MUST be the just-verified result for these exact credentials.
    pub fn register(
        &self,
        credentials: Credentials,
        identity: &AuthContext,
    ) -> Result<(String, String, String)> {
        let (binding, exp) = claims(identity)?;
        if identity.raw_token != credentials.bearer_token {
            return Err(denied("verified credential mismatch"));
        }
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, l| l.deadline > now() && l.token_expiry > now());
        if entries.len() >= 10000 {
            return Err(denied("session lease capacity exceeded"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let secret = random_secret();
        let connection_key = random_secret();
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
        header.kid = Some(format!("{CONNECTION_KID}{id}"));
        let connection_token = jsonwebtoken::encode(
            &header,
            &ConnectionClaims {
                sub: identity.user.clone(),
                exp,
                iss: "queryflux-native-session".into(),
                aud: "native-session".into(),
            },
            &jsonwebtoken::EncodingKey::from_secret(connection_key.as_bytes()),
        )
        .map_err(|_| denied("unable to issue connection JWT"))?;
        entries.insert(
            id.clone(),
            Lease {
                secret_hash: hash(&secret),
                connection_key,
                binding,
                credentials,
                user: identity.user.clone(),
                deadline: now() + MAX_LIFETIME,
                token_expiry: exp,
                bound: false,
            },
        );
        Ok((id, secret, connection_token))
    }
    /// The launcher exchanges its Knorket JWT over HTTPS for a one-use connection
    /// JWT and a SEPARATE renewal secret. A Knorket JWT alone cannot acquire an
    /// existing lease, and replaying a used connection JWT cannot open a new session.
    pub async fn authenticate_initial(
        &self,
        provider: &dyn AuthProvider,
        credentials: &mut Credentials,
    ) -> Result<AuthContext> {
        let token = credentials.bearer_token.as_deref().unwrap_or("");
        let header = jsonwebtoken::decode_header(token).ok();
        let Some(id) = header
            .as_ref()
            .and_then(|h| h.kid.as_deref())
            .and_then(|k| k.strip_prefix(CONNECTION_KID))
        else {
            return provider.authenticate(credentials).await;
        };
        let id = id.to_owned();
        if provider.wire_auth_kind() != WireAuthKind::Jwt {
            return Err(denied("OIDC provider required"));
        }
        let (current, expected, user) = {
            let entries = self.entries.lock().unwrap();
            let l = entries
                .get(&id)
                .filter(|l| !l.bound && l.token_expiry > now() && l.deadline > now())
                .ok_or_else(|| denied("connection JWT is expired or already used"))?;
            let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
            validation.leeway = 0;
            validation.set_issuer(&["queryflux-native-session"]);
            validation.set_audience(&["native-session"]);
            let ticket = jsonwebtoken::decode::<ConnectionClaims>(
                token,
                &jsonwebtoken::DecodingKey::from_secret(l.connection_key.as_bytes()),
                &validation,
            )
            .map_err(|_| denied("invalid connection JWT"))?;
            if ticket.claims.sub != l.user {
                return Err(denied("connection identity mismatch"));
            }
            (l.credentials.clone(), l.binding.clone(), l.user.clone())
        };
        let identity = provider.authenticate(&current).await?;
        if claims(&identity)?.0 != expected || identity.user != user {
            return Err(denied("session identity changed"));
        }
        // Claim atomically AFTER async verification: two competing logins cannot
        // both bind the same ticket. Never hold the mutex across an await.
        let mut entries = self.entries.lock().unwrap();
        let l = entries
            .get_mut(&id)
            .filter(|l| !l.bound && l.token_expiry > now() && l.deadline > now())
            .ok_or_else(|| denied("connection JWT is expired or already used"))?;
        l.bound = true;
        credentials.bearer_token = Some(format!("{PREFIX}{id}"));
        Ok(identity)
    }
    fn resolve(&self, credentials: &Credentials) -> Result<(Credentials, Option<(Value, String)>)> {
        let Some(id) = credentials
            .bearer_token
            .as_deref()
            .and_then(|s| s.strip_prefix(PREFIX))
        else {
            return Ok((credentials.clone(), None));
        };
        let entries = self.entries.lock().unwrap();
        let l = entries
            .get(id)
            .filter(|l| l.bound && l.deadline > now() && l.token_expiry > now())
            .ok_or_else(|| denied("session authorization lease expired; reconnect"))?;
        Ok((
            l.credentials.clone(),
            Some((l.binding.clone(), l.user.clone())),
        ))
    }
    pub fn renew(
        &self,
        id: &str,
        secret: &str,
        credentials: Credentials,
        identity: &AuthContext,
    ) -> Result<()> {
        let (binding, exp) = claims(identity)?;
        if identity.raw_token != credentials.bearer_token {
            return Err(denied("verified credential mismatch"));
        }
        let mut entries = self.entries.lock().unwrap();
        let l = entries
            .get_mut(id)
            .filter(|l| l.deadline > now() && l.token_expiry > now())
            .ok_or_else(|| denied("session lease expired; reconnect"))?;
        if !secret_matches(&l.secret_hash, secret)
            || l.binding != binding
            || l.user != identity.user
        {
            return Err(denied("session renewal identity or secret mismatch"));
        }
        l.credentials = credentials;
        l.token_expiry = exp;
        Ok(())
    }
    pub fn revoke(&self, id: &str, secret: &str) -> Result<()> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(l) = entries.get(id) {
            if !secret_matches(&l.secret_hash, secret) {
                return Err(denied("invalid session secret"));
            }
            entries.remove(id);
        }
        Ok(())
    }
    pub async fn authenticate(
        &self,
        provider: &dyn AuthProvider,
        credentials: &Credentials,
    ) -> Result<AuthContext> {
        let (current, binding) = self.resolve(credentials)?;
        if binding.is_some() && provider.wire_auth_kind() != WireAuthKind::Jwt {
            return Err(denied("OIDC provider required"));
        }
        let identity = provider.authenticate(&current).await?;
        if let Some((expected, user)) = binding {
            if claims(&identity)?.0 != expected || identity.user != user {
                return Err(denied("session identity changed"));
            }
        }
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct MockProvider;
    #[async_trait::async_trait]
    impl AuthProvider for MockProvider {
        fn wire_auth_kind(&self) -> WireAuthKind {
            WireAuthKind::Jwt
        }
        async fn authenticate(&self, c: &Credentials) -> Result<AuthContext> {
            let token = c
                .bearer_token
                .clone()
                .ok_or_else(|| denied("missing token"))?;
            let payload = URL_SAFE_NO_PAD
                .decode(token.split('.').nth(1).unwrap())
                .unwrap();
            let claims: Value = serde_json::from_slice(&payload).unwrap();
            Ok(AuthContext {
                user: claims["sub"].as_str().unwrap().into(),
                raw_token: Some(token),
                ..Default::default()
            })
        }
    }

    fn identity(
        user: &str,
        tenant: &str,
        cluster: &str,
        nonce: i64,
        role: &str,
    ) -> (Credentials, AuthContext) {
        let payload = serde_json::json!({"sub":user,"iss":"https://issuer.test","aud":cluster,
            "tenant_id":tenant,"cluster_id":cluster,"token_type":"cluster_access", "exp":now()+1800,"jti":nonce,"role":role});
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap())
        );
        (
            Credentials {
                bearer_token: Some(token.clone()),
                ..Default::default()
            },
            AuthContext {
                user: user.into(),
                raw_token: Some(token),
                roles: vec![role.into()],
                ..Default::default()
            },
        )
    }
    #[tokio::test]
    async fn renewal_replaces_credentials_for_bound_session_only() {
        let store = LeaseStore::default();
        let (initial, identity) = identity("u", "tenant", "cluster", 1, "reader");
        let (id, secret, connection_token) = store.register(initial.clone(), &identity).unwrap();
        let mut established = initial.clone();
        established.bearer_token = Some(connection_token.clone());
        store
            .authenticate_initial(&MockProvider, &mut established)
            .await
            .unwrap();
        let (fresh, current) = self::identity("u", "tenant", "cluster", 2, "writer");
        store.renew(&id, &secret, fresh.clone(), &current).unwrap();
        assert_eq!(
            store.resolve(&established).unwrap().0.bearer_token,
            fresh.bearer_token
        );
        // Replaying the original on a new login cannot obtain the established lease.
        let mut replay = initial.clone();
        assert!(store
            .authenticate_initial(&MockProvider, &mut replay)
            .await
            .is_ok());
        let mut ticket_replay = Credentials {
            bearer_token: Some(connection_token),
            ..Default::default()
        };
        assert!(store
            .authenticate_initial(&MockProvider, &mut ticket_replay)
            .await
            .is_err());
        assert_eq!(replay.bearer_token, initial.bearer_token);
        store.revoke(&id, &secret).unwrap();
        assert!(store.resolve(&established).is_err());
    }
    #[tokio::test]
    async fn forged_connection_ticket_and_plain_knorket_jwt_cannot_bind_lease() {
        let store = LeaseStore::default();
        let (initial, verified) = identity("u", "t", "c", 1, "reader");
        let (_, _, ticket) = store.register(initial.clone(), &verified).unwrap();
        let mut ordinary = initial.clone();
        store
            .authenticate_initial(&MockProvider, &mut ordinary)
            .await
            .unwrap();
        assert_eq!(ordinary.bearer_token, initial.bearer_token);
        assert!(store.resolve(&ordinary).unwrap().1.is_none());
        let mut parts = ticket.split('.').map(str::to_owned).collect::<Vec<_>>();
        let replacement = if parts[2].starts_with('A') { "B" } else { "A" };
        parts[2].replace_range(0..1, replacement);
        let mut forged = Credentials {
            bearer_token: Some(parts.join(".")),
            ..Default::default()
        };
        assert!(store
            .authenticate_initial(&MockProvider, &mut forged)
            .await
            .is_err());
        let mut legitimate = Credentials {
            bearer_token: Some(ticket),
            ..Default::default()
        };
        store
            .authenticate_initial(&MockProvider, &mut legitimate)
            .await
            .unwrap();
    }

    #[test]
    fn renewal_rejects_secret_and_identity_scope_changes() {
        let store = LeaseStore::default();
        let (initial, identity) = identity("u", "tenant", "cluster", 1, "reader");
        let (id, secret, connection_token) = store.register(initial, &identity).unwrap();
        for (user, tenant, cluster) in [
            ("attacker", "tenant", "cluster"),
            ("u", "other", "cluster"),
            ("u", "tenant", "other"),
        ] {
            let (token, identity) = self::identity(user, tenant, cluster, 2, "reader");
            assert!(store.renew(&id, &secret, token, &identity).is_err());
        }
        let (fresh, current) = self::identity("u", "tenant", "cluster", 3, "admin");
        assert!(store
            .renew(&id, "wrong secret", fresh.clone(), &current)
            .is_err());
        assert!(store.revoke(&id, "wrong secret").is_err());
        store.renew(&id, &secret, fresh, &current).unwrap();
    }
    #[tokio::test]
    async fn expired_token_and_maximum_age_cannot_be_resurrected() {
        for hard_limit in [false, true] {
            let store = LeaseStore::default();
            let (initial, identity) = identity("u", "t", "c", 1, "r");
            let (id, secret, connection_token) =
                store.register(initial.clone(), &identity).unwrap();
            let mut established = initial;
            established.bearer_token = Some(connection_token.clone());
            store
                .authenticate_initial(&MockProvider, &mut established)
                .await
                .unwrap();
            {
                let mut entries = store.entries.lock().unwrap();
                let l = entries.get_mut(&id).unwrap();
                if hard_limit {
                    l.deadline = now() - 1
                } else {
                    l.token_expiry = now() - 1
                }
            }
            let (fresh, current) = self::identity("u", "t", "c", 2, "r");
            assert!(store.renew(&id, &secret, fresh, &current).is_err());
            assert!(store.resolve(&established).is_err());
        }
    }
    #[test]
    fn registration_rejects_expiry_mismatch_and_wrong_token_type() {
        let store = LeaseStore::default();
        let (initial, identity) = identity("u", "t", "c", 1, "r");
        store.register(initial.clone(), &identity).unwrap();
        assert!(store.register(initial, &identity).is_ok()); // Each registration issues a distinct one-use connection JWT.
        let mut expired = identity.clone();
        expired.raw_token = Some(format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&serde_json::json!({"exp":now()-1})).unwrap())
        ));
        assert!(store.register(Credentials::default(), &expired).is_err());
        let mut other = identity;
        other.raw_token=Some(format!("header.{}.signature",URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({"exp":now()+1800,"token_type":"other","tenant_id":"t","cluster_id":"c"})).unwrap())));
        assert!(store.register(Credentials::default(), &other).is_err());
    }
}
