//! In-process registry of running Snowflake HTTP/SQL-API queries.
//!
//! Snowflake wire v1 and SQL API v2 execute synchronously on the HTTP request
//! thread, but the work runs in a spawned task so explicit DELETE cancel (and
//! client disconnect via task abort) can stop the backend query through
//! [`SyncCancelGuard`](crate::dispatch::SyncCancelGuard).

use std::sync::Arc;

use dashmap::DashMap;
use queryflux_auth::{require_query_owner, AuthContext};
use queryflux_core::{error::Result, query::FrontendProtocol, session::SessionContext};
use tokio::task::AbortHandle;

/// Outcome of attempting to cancel an in-flight Snowflake query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The query was found and abort was signalled.
    Aborted,
    /// No in-flight query with this id (already finished or unknown id).
    NotFound,
    /// Authenticated user is not the query owner.
    Forbidden,
}

/// Result of spawning a Snowflake query task.
pub enum SpawnExecuteResult<S> {
    Completed(Result<()>, S),
    Cancelled,
    JoinFailed(String),
}

/// A single in-flight Snowflake query handle.
struct InFlightEntry {
    owner: String,
    abort: AbortHandle,
}

/// Process-local registry keyed by wire `queryId` / SQL API `statementHandle`.
#[derive(Default)]
pub struct SnowflakeInFlightRegistry {
    entries: DashMap<String, InFlightEntry>,
}

impl SnowflakeInFlightRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
        })
    }

    pub fn register(&self, id: String, owner: String, abort: AbortHandle) {
        self.entries.insert(id, InFlightEntry { owner, abort });
    }

    pub fn unregister(&self, id: &str) {
        self.entries.remove(id);
    }

    /// Abort an in-flight query when the requester owns it.
    pub fn cancel(&self, id: &str, auth: &AuthContext) -> CancelOutcome {
        let Some(entry) = self.entries.get(id) else {
            return CancelOutcome::NotFound;
        };
        if require_query_owner(auth, &entry.owner).is_err() {
            return CancelOutcome::Forbidden;
        }
        entry.abort.abort();
        CancelOutcome::Aborted
    }

    /// In-flight query ids owned by `user` (wire monitoring / tests).
    pub fn ids_for_owner(&self, owner: &str) -> Vec<String> {
        self.entries
            .iter()
            .filter(|e| e.owner == owner)
            .map(|e| e.key().clone())
            .collect()
    }
}

/// Parameters shared by wire v1 and SQL API v2 synchronous execute paths.
pub struct SnowflakeExecParams {
    pub sql: String,
    pub params: queryflux_core::params::QueryParams,
    pub session_ctx: SessionContext,
    pub protocol: FrontendProtocol,
    pub group: queryflux_core::query::ClusterGroupName,
    pub auth_ctx: AuthContext,
}

/// Spawn `execute_to_sink` so explicit cancel can abort the task (and trigger
/// sync cancel on the backend).
pub async fn spawn_execute<S, F>(
    app: &Arc<crate::state::AppState>,
    registry: &Arc<SnowflakeInFlightRegistry>,
    query_id: String,
    owner: String,
    exec: SnowflakeExecParams,
    make_sink: F,
) -> SpawnExecuteResult<S>
where
    S: crate::dispatch::ResultSink + Send + 'static,
    F: FnOnce() -> S + Send + 'static,
{
    let app = app.clone();
    let registry = registry.clone();
    let query_id_for_task = query_id.clone();

    let join = tokio::spawn(async move {
        let mut sink = make_sink();
        let result = crate::dispatch::execute_to_sink(
            &app,
            exec.sql,
            exec.params,
            exec.session_ctx,
            exec.protocol,
            exec.group,
            &mut sink,
            &exec.auth_ctx,
        )
        .await;
        (result, sink)
    });

    registry.register(query_id, owner, join.abort_handle());

    match join.await {
        Ok((result, sink)) => {
            registry.unregister(&query_id_for_task);
            SpawnExecuteResult::Completed(result, sink)
        }
        Err(e) if e.is_cancelled() => {
            registry.unregister(&query_id_for_task);
            SpawnExecuteResult::Cancelled
        }
        Err(e) => {
            registry.unregister(&query_id_for_task);
            SpawnExecuteResult::JoinFailed(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ctx(user: &str) -> AuthContext {
        AuthContext {
            user: user.to_string(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn cancel_aborts_registered_task() {
        let registry = SnowflakeInFlightRegistry::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            struct SignalOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
            impl Drop for SignalOnDrop {
                fn drop(&mut self) {
                    if let Some(tx) = self.0.take() {
                        let _ = tx.send(());
                    }
                }
            }
            let _guard = SignalOnDrop(Some(tx));
            std::future::pending::<()>().await
        });
        registry.register("q1".to_string(), "alice".to_string(), handle.abort_handle());

        assert_eq!(registry.cancel("q1", &ctx("alice")), CancelOutcome::Aborted);

        let _ = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("task should abort");
    }

    #[tokio::test]
    async fn cancel_rejects_other_user() {
        let registry = SnowflakeInFlightRegistry::new();
        let handle = tokio::spawn(async { std::future::pending::<()>().await });
        registry.register("q1".to_string(), "alice".to_string(), handle.abort_handle());

        assert_eq!(registry.cancel("q1", &ctx("bob")), CancelOutcome::Forbidden);
        handle.abort();
    }

    #[test]
    fn cancel_unknown_returns_not_found() {
        let registry = SnowflakeInFlightRegistry::new();
        assert_eq!(
            registry.cancel("missing", &ctx("alice")),
            CancelOutcome::NotFound
        );
    }
}
