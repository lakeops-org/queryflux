//! A hub's credential toward its control plane (#255): an Ed25519 key the hub generates
//! at enrollment, and short-lived tokens it signs with it.
//!
//! A token is a compact JWT (`alg: EdDSA`) whose `kid` is the hub's id. The control plane
//! holds only the public key, so nothing it stores can be replayed, and a token that
//! leaks expires within [`MAX_LIFETIME_SECS`].

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use ed25519_dalek::{Signature, Signer, Verifier};
pub use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

/// `aud` of every hub token: it is only good for a control plane's worker API.
pub const AUDIENCE: &str = "queryflux-worker-api";
/// How long a hub's token is valid.
pub const TOKEN_LIFETIME_SECS: i64 = 300;
/// The longest lifetime the control plane accepts, whatever the hub signed.
pub const MAX_LIFETIME_SECS: i64 = 600;
/// Allowed clock difference between hub and control plane.
const LEEWAY_SECS: i64 = 60;

#[derive(Serialize, Deserialize)]
struct Header {
    alg: String,
    typ: String,
    kid: String,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    aud: String,
    iat: i64,
    exp: i64,
}

/// A new random key.
pub fn generate_key() -> SigningKey {
    let mut seed = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut seed);
    SigningKey::from_bytes(&seed)
}

/// A new one-time enrollment token. The prefix lets secret scanners recognize it.
pub fn new_enrollment_token() -> String {
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
    format!("qfe_{}", B64.encode(bytes))
}

/// The key's 32-byte seed, base64url: how a hub stores it.
pub fn encode_key(key: &SigningKey) -> String {
    B64.encode(key.to_bytes())
}

pub fn decode_key(encoded: &str) -> Option<SigningKey> {
    let bytes: [u8; 32] = B64.decode(encoded.trim()).ok()?.try_into().ok()?;
    Some(SigningKey::from_bytes(&bytes))
}

/// The public key, base64url: what the control plane stores.
pub fn encode_public_key(key: &VerifyingKey) -> String {
    B64.encode(key.as_bytes())
}

pub fn decode_public_key(encoded: &str) -> Option<VerifyingKey> {
    let bytes: [u8; 32] = B64.decode(encoded.trim()).ok()?.try_into().ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

fn segment<T: Serialize>(value: &T) -> String {
    B64.encode(serde_json::to_vec(value).expect("token parts serialize"))
}

/// A token for hub `hub_id`, valid from `now` (unix seconds) for [`TOKEN_LIFETIME_SECS`].
pub fn sign(key: &SigningKey, hub_id: &str, now: i64) -> String {
    let header = segment(&Header {
        alg: "EdDSA".into(),
        typ: "JWT".into(),
        kid: hub_id.into(),
    });
    let claims = segment(&Claims {
        sub: hub_id.into(),
        aud: AUDIENCE.into(),
        iat: now,
        exp: now + TOKEN_LIFETIME_SECS,
    });
    let signed = format!("{header}.{claims}");
    let signature = B64.encode(key.sign(signed.as_bytes()).to_bytes());
    format!("{signed}.{signature}")
}

/// The hub id a token claims, unverified: which public key to check it with. `None`
/// for anything that is not a hub token.
pub fn key_id(token: &str) -> Option<String> {
    let header = token.split('.').next()?;
    let header: Header = serde_json::from_slice(&B64.decode(header).ok()?).ok()?;
    (header.alg == "EdDSA").then_some(header.kid)
}

/// Check `token` against the hub's public key at `now` (unix seconds).
pub fn verify(token: &str, key: &VerifyingKey, now: i64) -> Result<(), &'static str> {
    let mut parts = token.split('.');
    let (Some(header), Some(claims), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("malformed token");
    };
    let signature: [u8; 64] = B64
        .decode(signature)
        .ok()
        .and_then(|s| s.try_into().ok())
        .ok_or("malformed signature")?;
    key.verify(
        format!("{header}.{claims}").as_bytes(),
        &Signature::from_bytes(&signature),
    )
    .map_err(|_| "bad signature")?;
    let claims: Claims = B64
        .decode(claims)
        .ok()
        .and_then(|c| serde_json::from_slice(&c).ok())
        .ok_or("malformed claims")?;
    let kid = key_id(token).ok_or("malformed header")?;
    if claims.sub != kid || claims.aud != AUDIENCE {
        return Err("token is not for this API");
    }
    if claims.exp - claims.iat > MAX_LIFETIME_SECS {
        return Err("token lifetime too long");
    }
    if now > claims.exp + LEEWAY_SECS || now + LEEWAY_SECS < claims.iat {
        return Err("token expired or not yet valid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_verifies_only_with_its_key_and_while_fresh() {
        let key = generate_key();
        let token = sign(&key, "hub-1", 1_000);
        assert_eq!(key_id(&token).as_deref(), Some("hub-1"));
        assert_eq!(verify(&token, &key.verifying_key(), 1_100), Ok(()));

        let other = generate_key();
        assert_eq!(
            verify(&token, &other.verifying_key(), 1_100),
            Err("bad signature")
        );
        assert!(verify(&token, &key.verifying_key(), 1_000 + 300 + 61).is_err());
        assert!(verify(&token, &key.verifying_key(), 1_000 - 61).is_err());

        // Changing any claim breaks the signature.
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = segment(&Claims {
            sub: "hub-1".into(),
            aud: AUDIENCE.into(),
            iat: 1_000,
            exp: 99_999,
        });
        parts[1] = &forged;
        assert_eq!(
            verify(&parts.join("."), &key.verifying_key(), 1_100),
            Err("bad signature")
        );
    }

    #[test]
    fn keys_round_trip_and_long_lived_tokens_are_refused() {
        let key = generate_key();
        let back = decode_key(&encode_key(&key)).unwrap();
        assert_eq!(back.to_bytes(), key.to_bytes());
        let public = decode_public_key(&encode_public_key(&key.verifying_key())).unwrap();
        assert_eq!(public, key.verifying_key());

        // A hub holds its key, so it could sign a long-lived token; the control plane
        // still refuses one.
        let header = segment(&Header {
            alg: "EdDSA".into(),
            typ: "JWT".into(),
            kid: "hub-1".into(),
        });
        let claims = segment(&Claims {
            sub: "hub-1".into(),
            aud: AUDIENCE.into(),
            iat: 1_000,
            exp: 1_000 + 86_400,
        });
        let signed = format!("{header}.{claims}");
        let token = format!(
            "{signed}.{}",
            B64.encode(key.sign(signed.as_bytes()).to_bytes())
        );
        assert_eq!(
            verify(&token, &key.verifying_key(), 1_100),
            Err("token lifetime too long")
        );
        assert_eq!(key_id("not-a-token"), None);
    }
}
