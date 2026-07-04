use async_trait::async_trait;
use queryflux_core::query::ClusterName;

use super::introspection::AdbcIntrospection;
use super::sql_helpers;
use super::AdbcPool;

const RECONCILE_SQL: &str = "SELECT COUNT(*) FROM stv_recents WHERE status = 'Running'";

/// Redshift introspection via `stv_recents` (leader-node system view).
pub struct RedshiftIntrospection {
    pool: AdbcPool,
}

pub fn try_from_adbc_config(
    cluster_name: &ClusterName,
    _uri: &str,
    _db_kwargs: &[(String, String)],
    pool: AdbcPool,
) -> Option<RedshiftIntrospection> {
    tracing::info!(
        cluster = %cluster_name.0,
        "Redshift introspection initialized (stv_recents)"
    );
    Some(RedshiftIntrospection { pool })
}

#[async_trait]
impl AdbcIntrospection for RedshiftIntrospection {
    async fn health_check(&self) -> bool {
        true
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let batches = sql_helpers::query_batches(&pool, RECONCILE_SQL)?;
            sql_helpers::first_cell_u64(&batches)
        })
        .await
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_sql_is_stv_recents() {
        assert_eq!(
            RECONCILE_SQL,
            "SELECT COUNT(*) FROM stv_recents WHERE status = 'Running'"
        );
    }
}
