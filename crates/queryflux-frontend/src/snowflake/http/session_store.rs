//! Process-local session store for the Snowflake HTTP wire protocol v1.
//!
//! Sessions are keyed by an opaque UUID token issued at login. Each session carries
//! the authenticated user's `AuthContext` and the cluster group resolved at login time,
//! so subsequent queries in the same session use the same routing decision.
//!
//! **Multi-replica note**: this store is process-local. When running multiple QueryFlux
//! replicas, the load balancer must be configured for sticky session affinity so that all
//! requests from a given client land on the same instance.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use std::time::Instant;
use uuid::Uuid;

use queryflux_auth::AuthContext;
use queryflux_core::query::ClusterGroupName;

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SnowflakeHttpSessionPolicy {
    /// Maximum session lifetime. `Duration::ZERO` = no limit.
    pub max_session_age: Duration,
    /// Idle timeout (time since last activity). `Duration::ZERO` = no limit.
    pub idle_timeout: Duration,
}

impl Default for SnowflakeHttpSessionPolicy {
    fn default() -> Self {
        Self {
            max_session_age: Duration::from_secs(86400),
            idle_timeout: Duration::from_secs(14400),
        }
    }
}

// ---------------------------------------------------------------------------
// Session record
// ---------------------------------------------------------------------------

pub struct SnowflakeSession {
    pub user: String,
    pub auth_ctx: AuthContext,
    pub group: ClusterGroupName,
    pub database: Option<String>,
    pub schema: Option<String>,
    /// Captured at login (`roleName` login-request query param) or updated mid-session by
    /// `USE ROLE`. `None` means "whatever role the backend connection defaults to" — never
    /// forwarded as a session-scoped override in that case.
    pub role: Option<String>,
    /// Captured at login (`warehouse` login-request query param) or updated mid-session by
    /// `USE WAREHOUSE`.
    pub warehouse: Option<String>,
    /// The plaintext password submitted at login, retained for the session's lifetime —
    /// **not verified by queryflux itself** — used only to build a `passthrough`-mode ADBC
    /// sub-pool (`AdbcAdapter`'s `PoolScopeKey::passthrough`), where the real Snowflake
    /// connection attempt is the actual verification. Harmless to capture unconditionally:
    /// unused unless the session's queries route to a `queryAuth: passthrough` cluster.
    /// Deliberately distinct from `AuthContext::raw_password`, which carries an implicit
    /// "verified by a provider that checked the same backend" guarantee this does not have.
    pub password: Option<String>,
    created_at: Instant,
    last_seen: Instant,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

pub struct SnowflakeSessionStore {
    sessions: Arc<DashMap<String, SnowflakeSession>>,
    policy: SnowflakeHttpSessionPolicy,
}

impl SnowflakeSessionStore {
    pub fn new(policy: SnowflakeHttpSessionPolicy) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            policy,
        }
    }

    /// Spawn the background GC task. Call once after construction when the Tokio
    /// runtime is running. The task runs until the store is dropped.
    pub fn spawn_gc(&self) {
        let sessions = Arc::clone(&self.sessions);
        let policy = self.policy.clone();
        let has_limit =
            policy.max_session_age != Duration::ZERO || policy.idle_timeout != Duration::ZERO;
        if !has_limit {
            return;
        }
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let now = Instant::now();
                sessions.retain(|_, s| !is_expired(s, &policy, now));
            }
        });
    }

    /// Insert a new session and return the opaque token.
    #[allow(clippy::too_many_arguments)]
    pub fn create_session(
        &self,
        user: String,
        auth_ctx: AuthContext,
        group: ClusterGroupName,
        database: Option<String>,
        schema: Option<String>,
        role: Option<String>,
        warehouse: Option<String>,
        password: Option<String>,
    ) -> String {
        let token = Uuid::new_v4().to_string();
        let now = Instant::now();
        self.sessions.insert(
            token.clone(),
            SnowflakeSession {
                user,
                auth_ctx,
                group,
                database,
                schema,
                role,
                warehouse,
                password,
                created_at: now,
                last_seen: now,
            },
        );
        token
    }

    /// Update the tracked database for `token` (e.g. from a `USE DATABASE` statement).
    /// No-op if the token is unknown (session expired/removed concurrently).
    pub fn set_database(&self, token: &str, database: Option<String>) {
        if let Some(mut entry) = self.sessions.get_mut(token) {
            entry.database = database;
        }
    }

    /// Update the tracked schema for `token` (e.g. from a `USE SCHEMA` statement).
    pub fn set_schema(&self, token: &str, schema: Option<String>) {
        if let Some(mut entry) = self.sessions.get_mut(token) {
            entry.schema = schema;
        }
    }

    /// Update the tracked role for `token` (e.g. from a `USE ROLE` statement).
    pub fn set_role(&self, token: &str, role: Option<String>) {
        if let Some(mut entry) = self.sessions.get_mut(token) {
            entry.role = role;
        }
    }

    /// Update the tracked warehouse for `token` (e.g. from a `USE WAREHOUSE` statement).
    pub fn set_warehouse(&self, token: &str, warehouse: Option<String>) {
        if let Some(mut entry) = self.sessions.get_mut(token) {
            entry.warehouse = warehouse;
        }
    }

    /// Validate a session token. On success bumps `last_seen` and returns remaining
    /// validity in seconds (min of age-remaining and idle-remaining; u64::MAX if unlimited).
    /// Returns `None` if the token is unknown or the session has expired (also removes it).
    pub fn validate_session(&self, token: &str) -> Option<(u64, SessionRef<'_>)> {
        let now = Instant::now();
        // Check expiry first without holding a write ref.
        {
            let entry = self.sessions.get(token)?;
            if is_expired(&entry, &self.policy, now) {
                drop(entry);
                self.sessions.remove(token);
                return None;
            }
        }
        // Bump last_seen.
        let mut entry = self.sessions.get_mut(token)?;
        entry.last_seen = now;
        let remaining = remaining_secs(&entry, &self.policy, now);
        // SAFETY: `entry` borrows from `self.sessions` which lives as long as `self`.
        // We return a `SessionRef` that carries the DashMap guard lifetime.
        Some((remaining, SessionRef { _guard: entry }))
    }

    /// Explicitly remove a session (logout).
    pub fn remove_session(&self, token: &str) {
        self.sessions.remove(token);
    }
}

// ---------------------------------------------------------------------------
// SessionRef — holds the DashMap read guard so the caller can access fields
// ---------------------------------------------------------------------------

pub struct SessionRef<'a> {
    _guard: dashmap::mapref::one::RefMut<'a, String, SnowflakeSession>,
}

impl<'a> std::ops::Deref for SessionRef<'a> {
    type Target = SnowflakeSession;
    fn deref(&self) -> &Self::Target {
        &self._guard
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_expired(s: &SnowflakeSession, policy: &SnowflakeHttpSessionPolicy, now: Instant) -> bool {
    if policy.max_session_age != Duration::ZERO
        && now.duration_since(s.created_at) >= policy.max_session_age
    {
        return true;
    }
    if policy.idle_timeout != Duration::ZERO
        && now.duration_since(s.last_seen) >= policy.idle_timeout
    {
        return true;
    }
    false
}

fn remaining_secs(s: &SnowflakeSession, policy: &SnowflakeHttpSessionPolicy, now: Instant) -> u64 {
    let age_remaining = if policy.max_session_age != Duration::ZERO {
        policy
            .max_session_age
            .saturating_sub(now.duration_since(s.created_at))
            .as_secs()
    } else {
        u64::MAX
    };
    let idle_remaining = if policy.idle_timeout != Duration::ZERO {
        policy
            .idle_timeout
            .saturating_sub(now.duration_since(s.last_seen))
            .as_secs()
    } else {
        u64::MAX
    };
    age_remaining.min(idle_remaining)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_auth::AuthContext;

    fn make_auth() -> AuthContext {
        AuthContext {
            user: "test_user".to_string(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        }
    }

    fn store_with_policy(max_age_secs: u64, idle_secs: u64) -> SnowflakeSessionStore {
        SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: if max_age_secs == 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(max_age_secs)
            },
            idle_timeout: if idle_secs == 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(idle_secs)
            },
        })
    }

    #[test]
    fn create_and_validate() {
        let store = store_with_policy(3600, 900);
        let token = store.create_session(
            "alice".into(),
            make_auth(),
            ClusterGroupName("prod".into()),
            Some("mydb".into()),
            None,
            None,
            None,
            None,
        );
        let result = store.validate_session(&token);
        assert!(result.is_some());
        let (remaining, session) = result.unwrap();
        assert!(remaining > 0);
        assert_eq!(session.user, "alice");
        assert_eq!(session.database.as_deref(), Some("mydb"));
    }

    #[test]
    fn unknown_token_returns_none() {
        let store = store_with_policy(3600, 900);
        assert!(store.validate_session("no-such-token").is_none());
    }

    #[test]
    fn logout_invalidates_session() {
        let store = store_with_policy(3600, 900);
        let token = store.create_session(
            "bob".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        store.remove_session(&token);
        assert!(store.validate_session(&token).is_none());
    }

    #[test]
    fn expired_by_max_age() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: Duration::from_millis(1),
            idle_timeout: Duration::ZERO,
        });
        let token = store.create_session(
            "charlie".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        // Give time to elapse past the 1ms max age.
        std::thread::sleep(Duration::from_millis(10));
        assert!(store.validate_session(&token).is_none());
    }

    #[test]
    fn expired_by_idle_timeout() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: Duration::ZERO,
            idle_timeout: Duration::from_millis(1),
        });
        let token = store.create_session(
            "dave".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        std::thread::sleep(Duration::from_millis(10));
        assert!(store.validate_session(&token).is_none());
    }

    #[test]
    fn validate_bumps_idle_timer() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: Duration::ZERO,
            idle_timeout: Duration::from_millis(20),
        });
        let token = store.create_session(
            "eve".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        // Still within idle timeout — validate bumps last_seen.
        std::thread::sleep(Duration::from_millis(10));
        assert!(store.validate_session(&token).is_some());
        // Another 10ms since last validate — total 20ms but last_seen was just bumped.
        std::thread::sleep(Duration::from_millis(10));
        assert!(store.validate_session(&token).is_some());
        // Now wait past idle timeout without any activity.
        std::thread::sleep(Duration::from_millis(30));
        assert!(store.validate_session(&token).is_none());
    }

    #[test]
    fn no_limits_never_expires() {
        let store = store_with_policy(0, 0);
        let token = store.create_session(
            "frank".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        let (remaining, _) = store.validate_session(&token).unwrap();
        assert_eq!(remaining, u64::MAX);
    }

    #[test]
    fn create_session_captures_role_and_warehouse() {
        let store = store_with_policy(3600, 900);
        let token = store.create_session(
            "grace".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            Some("ANALYST".into()),
            Some("ANALYTICS_WH".into()),
            None,
        );
        let (_, session) = store.validate_session(&token).unwrap();
        assert_eq!(session.role.as_deref(), Some("ANALYST"));
        assert_eq!(session.warehouse.as_deref(), Some("ANALYTICS_WH"));
    }

    #[test]
    fn setters_update_tracked_fields() {
        let store = store_with_policy(3600, 900);
        let token = store.create_session(
            "heidi".into(),
            make_auth(),
            ClusterGroupName("g".into()),
            None,
            None,
            None,
            None,
            None,
        );
        store.set_role(&token, Some("SYSADMIN".into()));
        store.set_warehouse(&token, Some("ETL_WH".into()));
        store.set_schema(&token, Some("PUBLIC".into()));
        store.set_database(&token, Some("PROD".into()));
        let (_, session) = store.validate_session(&token).unwrap();
        assert_eq!(session.role.as_deref(), Some("SYSADMIN"));
        assert_eq!(session.warehouse.as_deref(), Some("ETL_WH"));
        assert_eq!(session.schema.as_deref(), Some("PUBLIC"));
        assert_eq!(session.database.as_deref(), Some("PROD"));
    }

    #[test]
    fn setters_are_a_no_op_for_unknown_tokens() {
        let store = store_with_policy(3600, 900);
        // Must not panic when the session was already removed/expired concurrently.
        store.set_role("no-such-token", Some("X".into()));
        store.set_warehouse("no-such-token", Some("X".into()));
    }
}
