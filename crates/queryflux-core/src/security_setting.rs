//! Parse the persisted `security_settings.config` JSON blob.
//!
//! `PUT /admin/config/security` stores the flat Studio `UpsertSecurityConfig` shape
//! (`auth_provider`, `static_users` as a username map, snake_case nested objects).
//! Older rows wrap typed configs under `authConfig` / `authorizationConfig`.
//! The seeded empty object `{}` means "no Studio override" — callers must keep YAML.

use serde_json::{json, Map, Value};

use crate::config::{AuthConfig, AuthorizationConfig};

/// Seeded / deleted row: no provider keys, so YAML (or the last-good live config) wins.
pub fn is_blank_security_setting(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        _ => false,
    }
}

/// Accept Studio's flat `{ "alice": { password, groups } }` and the typed
/// `{ "users": { "alice": ... } }` shape used by [`AuthConfig`].
pub fn normalize_static_users(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Object(map) if map.contains_key("users") => v.clone(),
        Value::Object(map) => json!({ "users": map }),
        _ => Value::Null,
    }
}

/// Parse persisted security JSON into typed configs.
///
/// Returns `None` for a section that is absent or fails to parse so a reload
/// can keep the previous provider instead of falling back to allow-all.
pub fn parse_security_setting(v: &Value) -> (Option<AuthConfig>, Option<AuthorizationConfig>) {
    if is_blank_security_setting(v) {
        return (None, None);
    }

    let auth = if let Some(wrapped) = v.get("authConfig") {
        serde_json::from_value(wrapped.clone()).ok()
    } else if v.get("auth_provider").is_some() || v.get("provider").is_some() {
        serde_json::from_value(json!({
            "provider": v.get("auth_provider").or_else(|| v.get("provider")).cloned().unwrap_or(Value::Null),
            "required": v.get("auth_required").or_else(|| v.get("required")).cloned().unwrap_or(Value::Bool(false)),
            "oidc": camelize_value(v.get("oidc").unwrap_or(&Value::Null)),
            "ldap": camelize_value(v.get("ldap").unwrap_or(&Value::Null)),
            "staticUsers": normalize_static_users(v.get("static_users").or_else(|| v.get("staticUsers")).unwrap_or(&Value::Null)),
        }))
        .ok()
    } else {
        None
    };

    let authz = if let Some(wrapped) = v.get("authorizationConfig") {
        serde_json::from_value(wrapped.clone()).ok()
    } else if v.get("authorization_provider").is_some() {
        serde_json::from_value(json!({
            "provider": v.get("authorization_provider").cloned().unwrap_or(Value::Null),
            "openfga": camelize_value(v.get("openfga").unwrap_or(&Value::Null)),
        }))
        .ok()
    } else {
        None
    };

    (auth, authz)
}

/// Copy secrets from `existing` when the incoming Studio payload left them blank
/// (password fields are never pre-filled in the UI).
pub fn merge_security_setting(existing: Option<&Value>, mut incoming: Value) -> Value {
    let Some(prev) = existing.filter(|v| !is_blank_security_setting(v)) else {
        if let Some(users) = incoming.get_mut("static_users") {
            *users = normalize_static_users(users);
        }
        return incoming;
    };

    if let Some(merged) = merge_static_users(
        prev.get("static_users").or_else(|| prev.get("staticUsers")),
        incoming.get("static_users"),
    ) {
        incoming
            .as_object_mut()
            .map(|m| m.insert("static_users".to_string(), merged));
    }

    merge_secret_field(
        &mut incoming,
        prev,
        "ldap",
        &["bind_password", "bindPassword"],
    );
    merge_openfga_secrets(&mut incoming, prev);

    incoming
}

fn merge_static_users(existing: Option<&Value>, incoming: Option<&Value>) -> Option<Value> {
    let incoming = incoming?;
    if incoming.is_null() {
        return Some(Value::Null);
    }
    let mut users = match incoming {
        Value::Object(map) if map.get("users").map(|u| u.is_object()).unwrap_or(false) => map
            .get("users")
            .and_then(|u| u.as_object())
            .cloned()
            .unwrap_or_default(),
        Value::Object(map) => map.clone(),
        _ => return Some(incoming.clone()),
    };

    let prev_users = existing.and_then(static_users_map);
    if let Some(prev_users) = prev_users {
        for (name, entry) in users.iter_mut() {
            let Some(obj) = entry.as_object_mut() else {
                continue;
            };
            let blank = obj
                .get("password")
                .and_then(|p| p.as_str())
                .map(|s| s.is_empty())
                .unwrap_or(true);
            if !blank {
                continue;
            }
            if let Some(old_pw) = prev_users
                .get(name)
                .and_then(|e| e.get("password"))
                .cloned()
            {
                obj.insert("password".to_string(), old_pw);
            }
        }
    }

    Some(json!({ "users": users }))
}

fn static_users_map(v: &Value) -> Option<Map<String, Value>> {
    let obj = v.as_object()?;
    if let Some(users) = obj.get("users").and_then(|u| u.as_object()) {
        return Some(users.clone());
    }
    Some(obj.clone())
}

fn merge_secret_field(incoming: &mut Value, prev: &Value, section: &str, keys: &[&str]) {
    let Some(inc_section) = incoming.get_mut(section) else {
        return;
    };
    if inc_section.is_null() {
        return;
    }
    let Some(inc_obj) = inc_section.as_object_mut() else {
        return;
    };
    let blank = keys.iter().any(|k| match inc_obj.get(*k) {
        None => true,
        Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        _ => false,
    });
    if !blank {
        return;
    }
    let Some(prev_obj) = prev.get(section).and_then(|s| s.as_object()) else {
        return;
    };
    for k in keys {
        if let Some(val) = prev_obj.get(*k) {
            if !val.is_null() {
                inc_obj.insert((*k).to_string(), val.clone());
                return;
            }
        }
    }
}

fn merge_openfga_secrets(incoming: &mut Value, prev: &Value) {
    let Some(inc_fga) = incoming.get_mut("openfga") else {
        return;
    };
    if inc_fga.is_null() {
        return;
    }
    let Some(inc_creds) = inc_fga.get_mut("credentials") else {
        return;
    };
    if inc_creds.is_null() {
        if let Some(prev_creds) = prev
            .get("openfga")
            .and_then(|o| o.get("credentials"))
            .cloned()
        {
            if let Some(obj) = inc_fga.as_object_mut() {
                obj.insert("credentials".to_string(), prev_creds);
            }
        }
        return;
    }
    let Some(inc_obj) = inc_creds.as_object_mut() else {
        return;
    };
    let Some(prev_obj) = prev
        .get("openfga")
        .and_then(|o| o.get("credentials"))
        .and_then(|c| c.as_object())
    else {
        return;
    };
    for (snake, camel) in [("api_key", "apiKey"), ("client_secret", "clientSecret")] {
        let blank = match inc_obj.get(snake).or_else(|| inc_obj.get(camel)) {
            None | Some(Value::Null) => true,
            Some(Value::String(s)) => s.is_empty(),
            _ => false,
        };
        if blank {
            if let Some(val) = prev_obj.get(snake).or_else(|| prev_obj.get(camel)) {
                inc_obj.insert(snake.to_string(), val.clone());
            }
        }
    }
}

fn snake_to_camel(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut up = false;
    for c in s.chars() {
        if c == '_' {
            up = true;
        } else if up {
            out.extend(c.to_uppercase());
            up = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn camelize_value(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                out.insert(snake_to_camel(k), camelize_value(val));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(camelize_value).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthProviderConfig, AuthorizationProviderConfig};

    #[test]
    fn blank_object_is_not_an_override() {
        assert!(is_blank_security_setting(&json!({})));
        assert!(is_blank_security_setting(&Value::Null));
        let (auth, authz) = parse_security_setting(&json!({}));
        assert!(auth.is_none());
        assert!(authz.is_none());
    }

    #[test]
    fn wrapped_users_shape() {
        let v = json!({
            "auth_provider": "static",
            "auth_required": true,
            "static_users": { "users": { "alice": { "password": "pw" } } },
            "authorization_provider": "none",
        });
        let (auth, authz) = parse_security_setting(&v);
        let auth = auth.expect("auth");
        assert!(matches!(auth.provider, AuthProviderConfig::Static));
        assert_eq!(auth.static_users.unwrap().users["alice"].password, "pw");
        assert!(matches!(
            authz.unwrap().provider,
            AuthorizationProviderConfig::None
        ));
    }

    #[test]
    fn studio_flat_users_shape() {
        let v = json!({
            "auth_provider": "static",
            "auth_required": true,
            "static_users": { "alex.alves": { "password": "s3cret", "groups": ["Doris"] } },
            "authorization_provider": "none",
        });
        let (auth, _) = parse_security_setting(&v);
        let users = auth.expect("flat map must parse").static_users.unwrap();
        assert_eq!(users.users["alex.alves"].password, "s3cret");
        assert_eq!(users.users["alex.alves"].groups, vec!["Doris"]);
    }

    #[test]
    fn studio_snake_case_oidc_parses() {
        let v = json!({
            "auth_provider": "oidc",
            "auth_required": true,
            "oidc": {
                "issuer": "https://idp",
                "jwks_uri": "https://idp/jwks",
                "groups_claim": "groups",
                "roles_claim": "roles"
            },
            "authorization_provider": "none",
        });
        let (auth, _) = parse_security_setting(&v);
        let oidc = auth.expect("oidc").oidc.unwrap();
        assert_eq!(oidc.issuer, "https://idp");
        assert_eq!(oidc.jwks_uri, "https://idp/jwks");
        assert_eq!(oidc.groups_claim, "groups");
    }

    #[test]
    fn merge_keeps_existing_password_when_blank() {
        let existing = json!({
            "auth_provider": "static",
            "static_users": { "users": { "alice": { "password": "kept", "groups": ["g1"] } } },
        });
        let incoming = json!({
            "auth_provider": "static",
            "auth_required": true,
            "static_users": { "alice": { "password": "", "groups": ["g1", "g2"] } },
            "authorization_provider": "none",
        });
        let merged = merge_security_setting(Some(&existing), incoming);
        let (auth, _) = parse_security_setting(&merged);
        let alice = &auth.unwrap().static_users.unwrap().users["alice"];
        assert_eq!(alice.password, "kept");
        assert_eq!(alice.groups, vec!["g1", "g2"]);
    }

    #[test]
    fn wrapped_legacy_shape() {
        let v = json!({
            "authConfig": { "provider": "none", "required": true },
            "authorizationConfig": { "provider": "openfga", "openfga": {
                "url": "http://fga:8080", "storeId": "s1", "model": null
            }},
        });
        let (auth, authz) = parse_security_setting(&v);
        assert!(matches!(auth.unwrap().provider, AuthProviderConfig::None));
        assert!(matches!(
            authz.unwrap().provider,
            AuthorizationProviderConfig::OpenFga
        ));
    }

    #[test]
    fn unrecognized_yields_none() {
        let (auth, authz) = parse_security_setting(&json!({ "something": "else" }));
        assert!(auth.is_none());
        assert!(authz.is_none());
    }
}
