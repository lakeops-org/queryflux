//! A transaction owns its ADBC connection; it is never returned to the shared pool.
use super::params_to_record_batch;
use crate::{BackendQueryIdSlot, SyncAdapter, SyncExecution};
use adbc_core::{
    options::{OptionConnection, OptionValue},
    Connection, Database, Optionable, Statement,
};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use queryflux_auth::QueryCredentials;
use queryflux_core::{
    catalog::TableSchema,
    error::{QueryFluxError, Result},
    params::QueryParams,
    query::{EngineType, SqlDialect},
    session::SessionContext,
    tags::QueryTags,
};
use std::sync::{Arc, Mutex};
fn error(e: impl std::fmt::Display) -> QueryFluxError {
    QueryFluxError::Engine(format!("ADBC transaction: {e}"))
}
pub async fn open(
    database: ManagedDatabase,
    engine: EngineType,
    dialect: SqlDialect,
) -> Result<Arc<dyn SyncAdapter>> {
    let connection = tokio::task::spawn_blocking(move || {
        // Dedicated connection: it never enters the shared service pool.
        let mut connection = database.new_connection().map_err(error)?;
        connection
            .set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("false".into()),
            )
            .map_err(error)?;
        Ok::<_, QueryFluxError>(connection)
    })
    .await
    .map_err(error)??;
    Ok(Arc::new(Transaction {
        connection: Arc::new(Mutex::new(Some(connection))),
        engine,
        dialect,
    }))
}
struct Transaction {
    connection: Arc<Mutex<Option<ManagedConnection>>>,
    engine: EngineType,
    dialect: SqlDialect,
}
impl Drop for Transaction {
    fn drop(&mut self) {
        let connection = self.connection.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || {
                if let Some(mut c) = connection.lock().unwrap().take() {
                    let _ = c.rollback();
                }
            });
        }
        // Without a runtime, dropping the detached connection closes it; it never
        // reenters a pool with an uncommitted transaction.
    }
}
#[async_trait]
impl SyncAdapter for Transaction {
    fn engine_type(&self) -> EngineType {
        self.engine.clone()
    }
    fn translation_target_dialect(&self) -> SqlDialect {
        self.dialect.clone()
    }
    fn supports_native_params(&self) -> bool {
        true
    }
    async fn finish_transaction(&self, commit: bool) -> Result<()> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = connection
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| error("transaction closed"))?;
            if commit {
                c.commit().map_err(error)
            } else {
                c.rollback().map_err(error)
            }
        })
        .await
        .map_err(error)?
    }
    async fn describe_query(&self, sql: &str) -> Result<Arc<Schema>> {
        let connection = self.connection.clone();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut guard = connection.lock().unwrap();
            let c = guard.as_mut().ok_or_else(|| error("transaction closed"))?;
            let mut stmt = c.new_statement().map_err(error)?;
            stmt.set_sql_query(&sql).map_err(error)?;
            stmt.prepare().map_err(error)?;
            stmt.execute_schema().map(Arc::new).map_err(error)
        })
        .await
        .map_err(error)?
    }
    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        credentials: &QueryCredentials,
        _tags: &QueryTags,
        params: &QueryParams,
        hints: queryflux_core::sql_classify::ExecutionHints,
        _id: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        if !matches!(credentials, QueryCredentials::ServiceAccount) {
            return Err(error(
                "transaction requires service-account backend authentication",
            ));
        }
        let connection = self.connection.clone();
        let sql = sql.to_owned();
        let read = hints
            .is_read_like
            .unwrap_or_else(|| queryflux_core::sql_classify::is_read_like_sql(&sql, &self.dialect));
        let parameters = if params.is_empty() {
            None
        } else {
            Some(params_to_record_batch(params)?)
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(8);
        let (stats_tx, stats) = tokio::sync::oneshot::channel();
        let affected_rows = if read {
            tokio::task::spawn_blocking(move || {
                let result = (|| -> Result<()> {
                    let mut guard = connection.lock().unwrap();
                    let c = guard.as_mut().ok_or_else(|| error("transaction closed"))?;
                    let mut statement = c.new_statement().map_err(error)?;
                    statement.set_sql_query(&sql).map_err(error)?;
                    if let Some(parameters) = parameters {
                        statement.bind(parameters).map_err(error)?;
                    }
                    let reader = statement.execute().map_err(error)?;
                    let schema = reader.schema();
                    let mut any = false;
                    for batch in reader {
                        any = true;
                        if tx.blocking_send(batch.map_err(error)).is_err() {
                            return Ok(());
                        }
                    }
                    if !any {
                        let _ = tx.blocking_send(Ok(RecordBatch::new_empty(schema)));
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    let _ = tx.blocking_send(Err(e));
                }
                let _ = stats_tx.send(None);
            });
            None
        } else {
            let rows = tokio::task::spawn_blocking(move || {
                let mut guard = connection.lock().unwrap();
                let c = guard.as_mut().ok_or_else(|| error("transaction closed"))?;
                let mut statement = c.new_statement().map_err(error)?;
                statement.set_sql_query(&sql).map_err(error)?;
                if let Some(parameters) = parameters {
                    statement.bind(parameters).map_err(error)?;
                }
                statement
                    .execute_update()
                    .map(|n| n.filter(|n| *n >= 0).map(|n| n as u64))
                    .map_err(error)
            })
            .await
            .map_err(error)??;
            drop(tx);
            let _ = stats_tx.send(None);
            rows
        };
        Ok(SyncExecution {
            stream: Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)),
            stats,
            affected_rows,
        })
    }
    async fn health_check(&self) -> bool {
        self.connection.lock().unwrap().is_some()
    }
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Err(error("use catalog metadata outside a transaction"))
    }
    async fn list_databases(&self, _: &str) -> Result<Vec<String>> {
        Err(error("use catalog metadata outside a transaction"))
    }
    async fn list_tables(&self, _: &str, _: &str) -> Result<Vec<String>> {
        Err(error("use catalog metadata outside a transaction"))
    }
    async fn describe_table(&self, _: &str, _: &str, _: &str) -> Result<Option<TableSchema>> {
        Err(error("use catalog metadata outside a transaction"))
    }
}
