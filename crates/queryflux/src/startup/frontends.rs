//! Frontend listeners: the set of running listener tasks, and spawning the query
//! frontends a config enables.

use std::collections::HashMap;
use std::sync::Arc;

use queryflux_core::config::ProxyConfig;
use queryflux_frontend::{
    flight_sql::FlightSqlFrontend, mcp::McpFrontend, mysql_wire::MysqlWireFrontend,
    postgres_wire::PostgresWireFrontend, snowflake::SnowflakeFrontend, state::AppState,
    trino_http::TrinoHttpFrontend, FrontendListenerTrait,
};
use tokio::sync::watch;

pub(crate) type FrontendTaskResult = queryflux_core::error::Result<()>;

/// Owns only the frontend listeners that are actually running.
///
/// Keeping disabled frontends out of the task set is important for graceful
/// shutdown: a placeholder future that never completes would make the drain
/// timeout the only way to leave the process.
#[derive(Default)]
pub(crate) struct FrontendTasks {
    tasks: tokio::task::JoinSet<FrontendTaskResult>,
    names: HashMap<tokio::task::Id, &'static str>,
}

pub(crate) struct FrontendTaskExit {
    pub(crate) name: &'static str,
    pub(crate) result: std::result::Result<FrontendTaskResult, tokio::task::JoinError>,
}

impl FrontendTasks {
    pub(crate) fn spawn<F>(&mut self, name: &'static str, task: F)
    where
        F: std::future::Future<Output = FrontendTaskResult> + Send + 'static,
    {
        let handle = self.tasks.spawn(task);
        self.names.insert(handle.id(), name);
    }

    pub(crate) fn spawn_if_enabled<F, Fut>(&mut self, name: &'static str, enabled: bool, task: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = FrontendTaskResult> + Send + 'static,
    {
        if enabled {
            self.spawn(name, task());
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<FrontendTaskExit> {
        let result = self.tasks.join_next_with_id().await?;
        Some(match result {
            Ok((id, result)) => FrontendTaskExit {
                name: self.names.remove(&id).unwrap_or("unknown frontend"),
                result: Ok(result),
            },
            Err(error) => FrontendTaskExit {
                name: self.names.remove(&error.id()).unwrap_or("unknown frontend"),
                result: Err(error),
            },
        })
    }

    pub(crate) async fn drain(&mut self) {
        while let Some(exit) = self.join_next().await {
            match exit.result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(
                        frontend = exit.name,
                        "Frontend failed while draining: {error}"
                    )
                }
                Err(error) => tracing::warn!(
                    frontend = exit.name,
                    "Frontend task failed while draining: {error}"
                ),
            }
        }
    }

    pub(crate) fn outstanding_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.names.values().copied().collect();
        names.sort_unstable();
        names
    }
}

/// Spawn every query frontend the config enables (Trino HTTP, MySQL wire, Postgres wire,
/// Flight SQL, Snowflake, MCP). Each observes `shutdown_rx` and drains on shutdown.
pub(crate) fn spawn_query_frontends(
    frontend_tasks: &mut FrontendTasks,
    app_state: &Arc<AppState>,
    config: &ProxyConfig,
    trino_cfg: queryflux_core::config::FrontendConfig,
    shutdown_rx: &watch::Receiver<bool>,
) {
    frontend_tasks.spawn_if_enabled("Trino HTTP", trino_cfg.enabled, || {
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = trino_cfg;
        async move {
            TrinoHttpFrontend::new(state, cfg.port, cfg.max_connections)
                .listen(rx)
                .await
        }
    });
    if let Some(cfg) = config.queryflux.frontends.mysql_wire.clone() {
        frontend_tasks.spawn_if_enabled("MySQL wire", cfg.enabled, || {
            let state = app_state.clone();
            let rx = shutdown_rx.clone();
            async move {
                MysqlWireFrontend::new(state, cfg.port, cfg.max_connections)
                    .listen(rx)
                    .await
            }
        });
    }
    if let Some(cfg) = config.queryflux.frontends.postgres_wire.clone() {
        frontend_tasks.spawn_if_enabled("Postgres wire", cfg.enabled, || {
            let state = app_state.clone();
            let rx = shutdown_rx.clone();
            async move {
                PostgresWireFrontend::new(state, cfg.port, cfg.max_connections)
                    .listen(rx)
                    .await
            }
        });
    }
    if let Some(cfg) = config.queryflux.frontends.flight_sql.clone() {
        frontend_tasks.spawn_if_enabled("Flight SQL", cfg.enabled, || {
            let state = app_state.clone();
            let rx = shutdown_rx.clone();
            async move {
                FlightSqlFrontend::new(state, cfg.port, cfg.max_connections)
                    .listen(rx)
                    .await
            }
        });
    }
    if let Some(cfg) = config.queryflux.frontends.snowflake_http.clone() {
        frontend_tasks.spawn_if_enabled("Snowflake", cfg.enabled, || {
            let state = app_state.clone();
            let rx = shutdown_rx.clone();
            async move { SnowflakeFrontend::new(state, cfg).listen(rx).await }
        });
    }
    if let Some(cfg) = config.queryflux.frontends.mcp.clone() {
        frontend_tasks.spawn_if_enabled("MCP", cfg.enabled, || {
            let state = app_state.clone();
            let rx = shutdown_rx.clone();
            async move {
                McpFrontend::new(state, cfg.port, cfg.max_connections)
                    .listen(rx)
                    .await
            }
        });
    }
}
