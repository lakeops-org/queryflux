use async_trait::async_trait;
use queryflux_core::query::ClusterName;

use super::introspection::AdbcIntrospection;
use super::sql_helpers;
use super::AdbcPool;

/// BigQuery introspection via `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (metadata only, no slot cost).
pub struct BigQueryIntrospection {
    pool: AdbcPool,
    project_id: String,
    region: Option<String>,
}

pub fn try_from_adbc_config(
    cluster_name: &ClusterName,
    uri: &str,
    db_kwargs: &[(String, String)],
    pool: AdbcPool,
) -> Option<BigQueryIntrospection> {
    let project_id = sql_helpers::db_kwarg(db_kwargs, "project_id")
        .or_else(|| sql_helpers::db_kwarg(db_kwargs, "project"))
        .or_else(|| uri.split('/').next().map(str::to_string))
        .filter(|p| !p.is_empty())?;
    let region = sql_helpers::db_kwarg(db_kwargs, "location")
        .or_else(|| sql_helpers::db_kwarg(db_kwargs, "region"));

    tracing::info!(
        cluster = %cluster_name.0,
        project_id = %project_id,
        region = ?region,
        "BigQuery introspection initialized (INFORMATION_SCHEMA.JOBS_BY_PROJECT)"
    );

    Some(BigQueryIntrospection {
        pool,
        project_id,
        region,
    })
}

impl BigQueryIntrospection {
    pub(crate) fn build_reconcile_sql(project_id: &str, region: Option<&str>) -> String {
        if let Some(region) = region {
            format!(
                "SELECT COUNT(*) FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
                 WHERE state = 'RUNNING' AND project_id = '{}'",
                sql_helpers::escape_sql_literal(project_id)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM `{project}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
                 WHERE state = 'RUNNING'",
                project = project_id
            )
        }
    }

    fn reconcile_sql(&self) -> String {
        Self::build_reconcile_sql(&self.project_id, self.region.as_deref())
    }
}

#[async_trait]
impl AdbcIntrospection for BigQueryIntrospection {
    async fn health_check(&self) -> bool {
        // Metadata queries are cheap; no warehouse to wake.
        true
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        let pool = self.pool.clone();
        let sql = self.reconcile_sql();
        tokio::task::spawn_blocking(move || {
            let batches = sql_helpers::query_batches(&pool, &sql)?;
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
    fn reconcile_sql_without_region_uses_project_dataset() {
        let sql = BigQueryIntrospection::build_reconcile_sql("my-proj", None);
        assert!(sql.contains("`my-proj`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"));
        assert!(sql.contains("state = 'RUNNING'"));
        assert!(!sql.contains("project_id ="));
    }

    #[test]
    fn reconcile_sql_with_region_filters_project() {
        let sql = BigQueryIntrospection::build_reconcile_sql("my-proj", Some("region-us"));
        assert!(sql.contains("`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"));
        assert!(sql.contains("project_id = 'my-proj'"));
    }

    #[test]
    fn reconcile_sql_escapes_project_id_quotes() {
        let sql = BigQueryIntrospection::build_reconcile_sql("proj'x", Some("region-us"));
        assert!(sql.contains("project_id = 'proj''x'"));
    }

    #[test]
    fn project_id_from_db_kwargs_or_uri() {
        let kwargs = vec![("project_id".into(), "from-kw".into())];
        assert_eq!(
            sql_helpers::db_kwarg(&kwargs, "project_id").as_deref(),
            Some("from-kw")
        );
        assert_eq!(
            "my-proj/dataset"
                .split('/')
                .next()
                .map(str::to_string)
                .as_deref(),
            Some("my-proj")
        );
    }
}
