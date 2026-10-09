//! Process-local owner-bound transactions. Queries keep using the shared dispatch
//! pipeline, but admission and execution are pinned to the original cluster/connection.
use crate::state::AppState;
use dashmap::DashMap;
use queryflux_auth::AuthContext;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName, FrontendProtocol},
};
use queryflux_engine_adapters::{AdapterKind, SyncAdapter};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
pub const SESSION_KEY: &str = "queryflux.transaction_session";
#[derive(Default)]
pub struct Transactions {
    entries: DashMap<String, Arc<PinnedTransaction>>,
    expired: DashMap<String, (String, FrontendProtocol, Instant)>,
}
pub struct PinnedTransaction {
    pub group: ClusterGroupName,
    pub cluster: ClusterName,
    pub adapter: Arc<dyn SyncAdapter>,
    owner: String,
    protocol: FrontendProtocol,
    pub operation: tokio::sync::Mutex<()>,
    pub failed: std::sync::atomic::AtomicBool,
    touched: std::sync::Mutex<Instant>,
    created: Instant,
}
impl Drop for PinnedTransaction {
    fn drop(&mut self) {
        let adapter = self.adapter.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = adapter.finish_transaction(false).await;
            });
        }
    }
}
fn error(message: &str) -> QueryFluxError {
    QueryFluxError::Engine(message.into())
}
impl Transactions {
    pub fn expire_idle(&self) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|t| {
                t.created.elapsed() >= Duration::from_secs(8 * 3600)
                    || t.touched.lock().unwrap().elapsed() >= Duration::from_secs(1800)
            })
            .map(|t| t.key().clone())
            .collect();
        for id in expired {
            if let Some((_, t)) = self.entries.remove(&id) {
                self.expired
                    .insert(id, (t.owner.clone(), t.protocol.clone(), Instant::now()));
            }
        }
    }
    pub fn lookup(
        &self,
        id: &str,
        identity: &AuthContext,
        protocol: &FrontendProtocol,
    ) -> Result<Option<Arc<PinnedTransaction>>> {
        if self.expired.contains_key(id) {
            return Err(error("transaction expired; ROLLBACK before continuing"));
        }
        let Some(entry) = self.entries.get(id) else {
            return Ok(None);
        };
        if entry.owner != identity.session_owner() || &entry.protocol != protocol {
            return Err(QueryFluxError::Unauthorized(
                "transaction belongs to another session identity or protocol".into(),
            ));
        }
        if entry.created.elapsed() > Duration::from_secs(8 * 3600)
            || entry.touched.lock().unwrap().elapsed() > Duration::from_secs(1800)
        {
            drop(entry);
            self.expire_idle();
            return Err(error(
                "transaction expired and was rolled back; ROLLBACK before continuing",
            ));
        }
        *entry.touched.lock().unwrap() = Instant::now();
        Ok(Some(entry.clone()))
    }
    pub async fn begin(
        &self,
        state: &AppState,
        id: &str,
        group: ClusterGroupName,
        identity: &AuthContext,
        protocol: FrontendProtocol,
        session: &queryflux_core::session::SessionContext,
    ) -> Result<()> {
        if self.lookup(id, identity, &protocol)?.is_some() {
            return Err(error("transaction already active"));
        }
        self.expire_idle();
        if self.entries.len() + self.expired.len() >= 1024 {
            return Err(error("transaction capacity exceeded"));
        }
        let live = state.live.read().await;
        let authorization = live.authorization.clone();
        let manager = live.cluster_manager.clone();
        let adapters = live.adapters.clone();
        let configs = live.cluster_configs.clone();
        drop(live);
        if !authorization.check(identity, &group.0).await {
            return Err(QueryFluxError::Unauthorized(
                "transaction group denied".into(),
            ));
        }
        let cluster = manager
            .acquire_cluster(&group)
            .await?
            .ok_or_else(|| error("no transaction backend capacity"))?;
        let _slot = BeginSlot {
            manager: manager.clone(),
            group: group.clone(),
            cluster: cluster.clone(),
        };
        let result = async {
            if configs
                .get(&cluster.0)
                .and_then(|c| c.query_auth.as_ref())
                .is_some_and(|a| {
                    !matches!(a, queryflux_core::config::QueryAuthConfig::ServiceAccount)
                })
            {
                return Err(error(
                    "transactions require configured service-account backend authentication",
                ));
            }
            let adapter = match adapters.get(&cluster.0) {
                Some(AdapterKind::Sync(a)) => a.begin_transaction(session).await?,
                Some(AdapterKind::Async(a)) => a.begin_transaction(session).await?,
                None => return Err(error("backend adapter not found")),
            };
            let transaction = Arc::new(PinnedTransaction {
                group: group.clone(),
                cluster: cluster.clone(),
                adapter,
                owner: identity.session_owner(),
                protocol,
                operation: tokio::sync::Mutex::new(()),
                failed: std::sync::atomic::AtomicBool::new(false),
                touched: std::sync::Mutex::new(Instant::now()),
                created: Instant::now(),
            });
            match self.entries.entry(id.into()) {
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(transaction);
                    Ok(())
                }
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    Err(error("concurrent transaction already started"))
                }
            }
        }
        .await;
        result
    }
    pub async fn finish(
        &self,
        id: &str,
        identity: &AuthContext,
        protocol: &FrontendProtocol,
        commit: bool,
    ) -> Result<()> {
        if let Some(expired) = self.expired.get(id) {
            if expired.0 != identity.session_owner() || &expired.1 != protocol {
                return Err(QueryFluxError::Unauthorized(
                    "transaction belongs to another user or protocol".into(),
                ));
            }
            drop(expired);
            self.expired.remove(id);
            return Ok(());
        }
        let Some(transaction) = self.lookup(id, identity, protocol)? else {
            return Ok(());
        };
        let _operation = transaction.operation.lock().await;
        self.entries.remove(id);
        transaction
            .adapter
            .finish_transaction(
                commit
                    && !transaction
                        .failed
                        .load(std::sync::atomic::Ordering::Acquire),
            )
            .await
    }
    pub fn rollback_on_disconnect(&self, id: &str) {
        self.entries.remove(id);
        self.expired.remove(id);
    }
}
/// Drop on native disconnect; the registry releases its connection and rolls back.
pub struct DisconnectRollback {
    pub state: Arc<AppState>,
    pub id: String,
}
impl Drop for DisconnectRollback {
    fn drop(&mut self) {
        self.state.transactions.rollback_on_disconnect(&self.id);
    }
}
pub fn transaction_command(sql: &str) -> Option<bool> {
    let words = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    match words.as_slice() {
        [word] if word == "BEGIN" => Some(true),
        [a, b]
            if (a == "BEGIN" && (b == "WORK" || b == "TRANSACTION"))
                || (a == "START" && b == "TRANSACTION") =>
        {
            Some(true)
        }
        _ => None,
    }
}
pub fn transaction_end(sql: &str) -> Option<bool> {
    match sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "COMMIT" | "END" => Some(true),
        "ROLLBACK" | "ABORT" => Some(false),
        _ => None,
    }
}

struct BeginSlot {
    manager: Arc<dyn queryflux_cluster_manager::ClusterGroupManager>,
    group: ClusterGroupName,
    cluster: ClusterName,
}
impl Drop for BeginSlot {
    fn drop(&mut self) {
        let manager = self.manager.clone();
        let group = self.group.clone();
        let cluster = self.cluster.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = manager.release_cluster(&group, &cluster).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn transaction_owner_protocol_and_expiry_cannot_fall_back_to_autocommit() {
        use queryflux_engine_adapters::duckdb::{DuckDbAdapter, DuckDbConfig};
        let state = crate::state::test_fixtures::app_state(false);
        let adapter = Arc::new(
            DuckDbAdapter::new(
                ClusterName("trino".into()),
                ClusterGroupName("default".into()),
                DuckDbConfig {
                    database_path: None,
                    motherduck_token: None,
                    pool_size: 1,
                    max_result_buffer_bytes: 1024 * 1024,
                },
            )
            .unwrap(),
        );
        state
            .live
            .write()
            .await
            .adapters
            .insert("trino".into(), AdapterKind::Sync(adapter));
        let identity = AuthContext {
            user: "alice".into(),
            ..Default::default()
        };
        state
            .transactions
            .begin(
                &state,
                "session",
                ClusterGroupName("default".into()),
                &identity,
                FrontendProtocol::PostgresWire,
                &queryflux_core::session::SessionContext::default(),
            )
            .await
            .unwrap();
        assert!(state
            .transactions
            .lookup(
                "session",
                &AuthContext {
                    user: "bob".into(),
                    ..Default::default()
                },
                &FrontendProtocol::PostgresWire
            )
            .is_err());
        assert!(state
            .transactions
            .lookup("session", &identity, &FrontendProtocol::FlightSql)
            .is_err());
        let transaction = state
            .transactions
            .lookup("session", &identity, &FrontendProtocol::PostgresWire)
            .unwrap()
            .unwrap();
        *transaction.touched.lock().unwrap() = Instant::now() - Duration::from_secs(1801);
        drop(transaction);
        state.transactions.expire_idle();
        assert!(state
            .transactions
            .lookup("session", &identity, &FrontendProtocol::PostgresWire)
            .is_err());
        state
            .transactions
            .finish("session", &identity, &FrontendProtocol::PostgresWire, false)
            .await
            .unwrap();
        assert!(state
            .transactions
            .lookup("session", &identity, &FrontendProtocol::PostgresWire)
            .unwrap()
            .is_none());
    }
}
