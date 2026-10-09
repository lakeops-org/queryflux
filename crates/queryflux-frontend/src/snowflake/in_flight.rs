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
    Unsupported,
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
    cancellable: bool,
    binding: Option<String>,
    group: Option<String>,
}

/// Process-local registry keyed by wire `queryId` / SQL API `statementHandle`.
#[derive(Default)]
pub struct SnowflakeInFlightRegistry {
    entries: DashMap<String, InFlightEntry>,
    results: DashMap<String, AsyncResult>,
    result_budget: std::sync::Mutex<()>,
}

struct AsyncResult {
    owner: String,
    group: String,
    created: std::time::Instant,
    body: Option<serde_json::Value>,
    bytes: usize,
}
impl SnowflakeInFlightRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            results: DashMap::new(),
            result_budget: std::sync::Mutex::new(()),
        })
    }

    pub fn reserve_result(&self, id: &str, identity: &AuthContext, group: &str) -> bool {
        self.results
            .retain(|_, r| r.created.elapsed() < std::time::Duration::from_secs(900));
        if self.results.len() >= 128 {
            return false;
        }
        self.results.insert(
            id.into(),
            AsyncResult {
                owner: identity.session_owner(),
                group: group.into(),
                created: std::time::Instant::now(),
                body: None,
                bytes: 0,
            },
        );
        true
    }
    pub fn complete_result(&self, id: &str, mut body: serde_json::Value) {
        let _budget = self.result_budget.lock().unwrap();
        let used: usize = self.results.iter().map(|r| r.bytes).sum();
        let mut bytes = body.to_string().len();
        if used.saturating_add(bytes) > 64 * 1024 * 1024 {
            body = serde_json::json!({"success": false, "code": "390000", "message": "Async result storage capacity exceeded"});
            bytes = body.to_string().len();
        }
        if let Some(mut entry) = self.results.get_mut(id) {
            entry.body = Some(body);
            entry.bytes = bytes;
        }
    }
    pub fn result_group(
        &self,
        id: &str,
        identity: &AuthContext,
    ) -> std::result::Result<Option<String>, ()> {
        let Some(entry) = self.results.get(id) else {
            return Ok(None);
        };
        if entry.owner != identity.session_owner() {
            return Err(());
        }
        Ok(Some(entry.group.clone()))
    }
    /// None: unknown or expired; Some(None): running; Some(Some): terminal.
    pub fn result(
        &self,
        id: &str,
        auth: &AuthContext,
    ) -> std::result::Result<Option<Option<serde_json::Value>>, ()> {
        let Some(entry) = self.results.get(id) else {
            return Ok(None);
        };
        if auth.session_owner() != entry.owner {
            return Err(());
        }
        if entry.created.elapsed() >= std::time::Duration::from_secs(900) {
            return Ok(None);
        }
        Ok(Some(entry.body.clone()))
    }
    pub fn result_statuses(&self, identity: &AuthContext) -> Vec<serde_json::Value> {
        self.results.iter().filter(|r| r.owner == identity.session_owner() && r.created.elapsed() < std::time::Duration::from_secs(900)).map(|r| {
            serde_json::json!({"id": r.key(), "status": match &r.body { None => "RUNNING", Some(v) if v["success"] == true || v.get("resultSetMetaData").is_some() => "SUCCESS", Some(_) => "FAILED_WITH_ERROR" }})
        }).collect()
    }

    pub fn register(&self, id: String, owner: String, abort: AbortHandle) {
        self.entries.insert(
            id,
            InFlightEntry {
                owner,
                abort,
                cancellable: true,
                binding: None,
                group: None,
            },
        );
    }

    pub fn unregister(&self, id: &str) {
        self.entries.remove(id);
    }

    /// Abort an in-flight query when the requester owns it.
    pub fn cancel(&self, id: &str, auth: &AuthContext) -> CancelOutcome {
        let Some(entry) = self.entries.get(id) else {
            return CancelOutcome::NotFound;
        };
        if require_query_owner(auth, &entry.owner).is_err()
            || entry
                .binding
                .as_ref()
                .is_some_and(|b| b != &auth.session_owner())
        {
            return CancelOutcome::Forbidden;
        }
        if !entry.cancellable {
            return CancelOutcome::Unsupported;
        }
        entry.abort.abort();
        CancelOutcome::Aborted
    }

    pub fn query_group(
        &self,
        id: &str,
        identity: &AuthContext,
    ) -> std::result::Result<Option<String>, ()> {
        let Some(entry) = self.entries.get(id) else {
            return Ok(None);
        };
        if require_query_owner(identity, &entry.owner).is_err()
            || entry
                .binding
                .as_ref()
                .is_some_and(|b| b != &identity.session_owner())
        {
            return Err(());
        }
        Ok(entry.group.clone())
    }
    pub fn ids_for_identity(&self, identity: &AuthContext) -> Vec<String> {
        self.entries
            .iter()
            .filter(|e| {
                e.owner == identity.user
                    && e.binding
                        .as_ref()
                        .is_none_or(|b| b == &identity.session_owner())
            })
            .map(|e| e.key().clone())
            .collect()
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
    let cancellable = {
        let live = app.live.read().await;
        live.group_members
            .get(&exec.group.0)
            .is_some_and(|members| {
                !members.is_empty()
                    && members.iter().all(|name| match live.adapters.get(name) {
                        Some(queryflux_engine_adapters::AdapterKind::Sync(a)) => {
                            a.supports_cancellation()
                        }
                        Some(queryflux_engine_adapters::AdapterKind::Async(_)) => true,
                        None => false,
                    })
            })
    };
    let binding = exec.auth_ctx.session_owner();
    let group = exec.group.0.clone();
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

    registry.entries.insert(
        query_id.clone(),
        InFlightEntry {
            owner,
            abort: join.abort_handle(),
            cancellable,
            binding: Some(binding),
            group: Some(group),
        },
    );

    let join = crate::abort::AbortOnDrop::new(join);
    let _unregister = Unregister {
        registry: registry.clone(),
        id: query_id_for_task.clone(),
    };
    match join.join().await {
        Ok((result, sink)) => SpawnExecuteResult::Completed(result, sink),
        Err(e) if e.is_cancelled() => SpawnExecuteResult::Cancelled,
        Err(e) => SpawnExecuteResult::JoinFailed(e.to_string()),
    }
}
/// Detached async results must terminate before their in-memory retention expires.
pub async fn spawn_async_execute<S, F>(
    app: &Arc<crate::state::AppState>,
    registry: &Arc<SnowflakeInFlightRegistry>,
    id: String,
    owner: String,
    exec: SnowflakeExecParams,
    make_sink: F,
) -> SpawnExecuteResult<S>
where
    S: crate::dispatch::ResultSink + Send + 'static,
    F: FnOnce() -> S + Send + 'static,
{
    match tokio::time::timeout(
        std::time::Duration::from_secs(840),
        spawn_execute(app, registry, id, owner, exec, make_sink),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => SpawnExecuteResult::JoinFailed(
            "async query exceeded the 14 minute gateway deadline".into(),
        ),
    }
}

struct Unregister {
    registry: Arc<SnowflakeInFlightRegistry>,
    id: String,
}
impl Drop for Unregister {
    fn drop(&mut self) {
        self.registry.unregister(&self.id);
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
    fn async_result_owner_and_completion() {
        let registry = SnowflakeInFlightRegistry::new();
        assert!(registry.reserve_result("async1", &ctx("alice"), "default"));
        assert!(matches!(
            registry.result("async1", &ctx("alice")),
            Ok(Some(None))
        ));
        assert!(registry.result("async1", &ctx("bob")).is_err());
        registry.complete_result(
            "async1",
            serde_json::json!({"success": true, "data": ["done"]}),
        );
        assert_eq!(
            registry
                .result("async1", &ctx("alice"))
                .unwrap()
                .unwrap()
                .unwrap()["data"][0],
            "done"
        );
    }
    #[test]
    fn sql_api_terminal_result_reports_success() {
        let registry = SnowflakeInFlightRegistry::new();
        assert!(registry.reserve_result("sql-api", &ctx("alice"), "default"));
        registry.complete_result("sql-api", serde_json::json!({"statementHandle":"sql-api", "resultSetMetaData":{"numRows":0}, "data":[]}));
        assert_eq!(
            registry.result_statuses(&ctx("alice"))[0]["status"],
            "SUCCESS"
        );
    }
    #[tokio::test]
    async fn unsupported_cancellation_does_not_abort_work() {
        let registry = SnowflakeInFlightRegistry::new();
        let task = tokio::spawn(std::future::pending::<()>());
        registry.register("uncancellable".into(), "alice".into(), task.abort_handle());
        registry
            .entries
            .get_mut("uncancellable")
            .unwrap()
            .cancellable = false;
        assert_eq!(
            registry.cancel("uncancellable", &ctx("alice")),
            CancelOutcome::Unsupported
        );
        assert!(!task.is_finished());
        task.abort();
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
