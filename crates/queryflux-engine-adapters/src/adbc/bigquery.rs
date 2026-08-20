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

/// Extracts the project ID from a BigQuery ADBC URI
/// (`bigquery://[host[:port]]/<project_id>[?params]` — the project ID is the
/// last path segment; the host is optional, e.g. `bigquery:///my-project`).
/// A bare `<project_id>` with no scheme or slashes is also accepted.
fn project_id_from_uri(uri: &str) -> Option<String> {
    let without_query = uri.split('?').next().unwrap_or(uri);
    let path = without_query
        .strip_prefix("bigquery://")
        .unwrap_or(without_query);
    let project = path.rsplit('/').find(|s| !s.is_empty())?;
    (!project.is_empty()).then(|| project.to_string())
}

pub fn try_from_adbc_config(
    cluster_name: &ClusterName,
    uri: &str,
    db_kwargs: &[(String, String)],
    pool: AdbcPool,
) -> Option<BigQueryIntrospection> {
    let project_id = sql_helpers::db_kwarg(db_kwargs, "project_id")
        .or_else(|| sql_helpers::db_kwarg(db_kwargs, "project"))
        .or_else(|| project_id_from_uri(uri))
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
        // A metadata-only probe — no slot cost, no warehouse to wake — but
        // still validates connectivity/auth/project access, unlike an
        // unconditional `true` which would never catch an unreachable project.
        let pool = self.pool.clone();
        let sql = format!(
            "SELECT 1 FROM `{}`.INFORMATION_SCHEMA.SCHEMATA LIMIT 1",
            self.project_id
        );
        tokio::task::spawn_blocking(move || sql_helpers::query_batches(&pool, &sql).is_some())
            .await
            .unwrap_or(false)
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
    }

    #[test]
    fn project_id_from_uri_parses_official_adbc_format() {
        // bigquery://[host[:port]]/<project_id>[?params] — project ID is the
        // last path segment, not the first (the host is optional).
        assert_eq!(
            project_id_from_uri("bigquery:///my-project-123").as_deref(),
            Some("my-project-123")
        );
        assert_eq!(
            project_id_from_uri(
                "bigquery://bigquery.googleapis.com/my-project-123?OAuthType=0&DatasetId=analytics"
            )
            .as_deref(),
            Some("my-project-123")
        );
        assert_eq!(
            project_id_from_uri("my-project-123").as_deref(),
            Some("my-project-123")
        );
    }

    #[test]
    fn project_id_from_uri_does_not_return_scheme() {
        // Regression: `uri.split('/').next()` on "bigquery://host/project"
        // used to return "bigquery:" (the scheme) instead of the project ID.
        let project = project_id_from_uri("bigquery://bigquery.googleapis.com/real-project");
        assert_ne!(project.as_deref(), Some("bigquery:"));
        assert_eq!(project.as_deref(), Some("real-project"));
    }
}
