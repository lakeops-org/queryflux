//! LDAP authentication provider.
//!
//! Supports two bind patterns:
//!
//! **Search-then-bind** (enterprise default):
//! 1. Connect and bind with the service account (`bindDn` + `bindPassword`).
//! 2. Search `userSearchBase` using `userSearchFilter` to locate the user's DN.
//! 3. Re-bind with the found DN + user's password to verify credentials.
//! 4. Read `memberOf` from the user entry (or search `groupSearchBase`) for group membership.
//!
//! **Direct bind** (simpler, when DN structure is known):
//! - Set `userDnTemplate: "cn={},ou=users,dc=example,dc=com"`.
//! - QueryFlux formats the DN and binds directly — no search needed.
//! - Groups are still read from `memberOf` on the entry after binding.
//!
//! A new LDAP connection is opened per `authenticate()` call. Connection pooling
//! can be added later if latency becomes a concern.

use ldap3::{Ldap, LdapConnAsync, Scope, SearchEntry};
use queryflux_core::config::LdapConfig;
use queryflux_core::error::{QueryFluxError, Result};
use tracing::{debug, warn};

use crate::credentials::{AuthContext, Credentials};
use crate::provider::AuthProvider;

pub struct LdapAuthProvider {
    config: LdapConfig,
    required: bool,
}

impl LdapAuthProvider {
    pub fn new(config: LdapConfig, required: bool) -> Self {
        Self { config, required }
    }

    async fn connect(&self) -> Result<Ldap> {
        let (conn, ldap) = LdapConnAsync::new(&self.config.url)
            .await
            .map_err(|e| QueryFluxError::Auth(format!("LDAP connect failed: {e}")))?;
        ldap3::drive!(conn);
        Ok(ldap)
    }

    /// Resolve the full DN for `username`, either via template or service-account search.
    async fn resolve_user_dn(&self, ldap: &mut Ldap, username: &str) -> Result<String> {
        // Direct-bind template takes priority.
        if let Some(template) = &self.config.user_dn_template {
            return Ok(template.replace("{}", &ldap_escape(username)));
        }

        // Service account bind → search for user DN.
        if !self.config.bind_dn.is_empty() {
            let bind_pw = self.config.bind_password.as_deref().unwrap_or("");
            ldap.simple_bind(&self.config.bind_dn, bind_pw)
                .await
                .map_err(|e| QueryFluxError::Auth(format!("LDAP service bind failed: {e}")))?
                .success()
                .map_err(|e| QueryFluxError::Auth(format!("LDAP service bind rejected: {e}")))?;
        }

        let filter = self
            .config
            .user_search_filter
            .replace("{}", &ldap_escape(username));

        debug!(base = %self.config.user_search_base, filter = %filter, "LDAP user search");

        let (entries, _) = ldap
            .search(
                &self.config.user_search_base,
                Scope::Subtree,
                &filter,
                vec!["dn", "memberOf"],
            )
            .await
            .map_err(|e| QueryFluxError::Auth(format!("LDAP search failed: {e}")))?
            .success()
            .map_err(|e| QueryFluxError::Auth(format!("LDAP search error: {e}")))?;

        let entry = entries
            .into_iter()
            .next()
            .ok_or_else(|| QueryFluxError::Auth(format!("user '{username}' not found in LDAP")))?;

        Ok(SearchEntry::construct(entry).dn)
    }

    /// Read group names from `memberOf` on the user entry, or search `groupSearchBase`.
    async fn resolve_groups(&self, ldap: &mut Ldap, user_dn: &str) -> Vec<String> {
        match &self.config.group_search_base {
            Some(base) => {
                // Explicit group search: find groups where `member` = user_dn.
                let filter = format!("(member={})", ldap_escape(user_dn));
                let attr = &self.config.group_name_attribute;
                match ldap
                    .search(base, Scope::Subtree, &filter, vec![attr.as_str()])
                    .await
                    .and_then(|r| r.success())
                {
                    Ok((entries, _)) => entries
                        .into_iter()
                        .filter_map(|e| {
                            let entry = SearchEntry::construct(e);
                            entry.attrs.get(attr.as_str())?.first().cloned()
                        })
                        .collect(),
                    Err(e) => {
                        warn!(error = %e, "LDAP group search failed — returning empty groups");
                        vec![]
                    }
                }
            }
            None => {
                // Read `memberOf` from the user entry (already fetched if search path was used;
                // re-fetch via a base search on the user DN when direct-bind was used).
                match ldap
                    .search(user_dn, Scope::Base, "(objectClass=*)", vec!["memberOf"])
                    .await
                    .and_then(|r| r.success())
                {
                    Ok((entries, _)) => {
                        let attr = self.config.group_name_attribute.as_str();
                        entries
                            .into_iter()
                            .flat_map(|e| {
                                let entry = SearchEntry::construct(e);
                                entry
                                    .attrs
                                    .get("memberOf")
                                    .cloned()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .filter_map(|dn| extract_cn(&dn, attr))
                            })
                            .collect()
                    }
                    Err(e) => {
                        warn!(error = %e, "LDAP memberOf fetch failed — returning empty groups");
                        vec![]
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for LdapAuthProvider {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext> {
        let username = match &creds.username {
            Some(u) if !u.is_empty() => u.clone(),
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

        let password = creds.password.as_deref().unwrap_or("");

        let mut ldap = self.connect().await?;

        // Resolve user DN (search or template).
        let user_dn = self.resolve_user_dn(&mut ldap, &username).await?;

        debug!(user_dn = %user_dn, "LDAP: binding as user to verify password");

        // Verify password by binding as the user.
        ldap.simple_bind(&user_dn, password)
            .await
            .map_err(|e| QueryFluxError::Auth(format!("LDAP bind failed: {e}")))?
            .success()
            .map_err(|_| {
                QueryFluxError::Auth(format!("authentication failed for user '{username}'"))
            })?;

        // Read group memberships.
        let groups = self.resolve_groups(&mut ldap, &user_dn).await;
        debug!(user = %username, groups = ?groups, "LDAP: authenticated");

        let _ = ldap.unbind().await;

        // This password was just verified by a real LDAP bind above — safe to carry as
        // `raw_password` for `queryAuth: passthrough` on a MySQL-wire cluster configured
        // against the same LDAP directory (StarRocks `authentication_ldap_simple`).
        Ok(AuthContext {
            user: username,
            groups,
            roles: vec![],
            raw_token: None,
            raw_password: Some(password.to_string()),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal LDAP DN/filter special-character escaping (RFC 4515 for filters).
fn ldap_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '*' => out.push_str("\\2a"),
            '(' => out.push_str("\\28"),
            ')' => out.push_str("\\29"),
            '\\' => out.push_str("\\5c"),
            '\0' => out.push_str("\\00"),
            _ => out.push(ch),
        }
    }
    out
}

/// Extract the value of a named RDN attribute from a DN string.
/// E.g. `extract_cn("cn=admins,ou=groups,dc=example,dc=com", "cn")` → `Some("admins")`.
fn extract_cn(dn: &str, attr: &str) -> Option<String> {
    let prefix = format!("{attr}=");
    dn.split(',')
        .find(|rdn| {
            rdn.trim()
                .to_lowercase()
                .starts_with(&prefix.to_lowercase())
        })
        .map(|rdn| {
            rdn.trim()
                .split_once('=')
                .map(|x| x.1)
                .unwrap_or("")
                .to_string()
        })
}
