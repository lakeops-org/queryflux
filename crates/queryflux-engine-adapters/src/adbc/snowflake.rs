use async_trait::async_trait;
use queryflux_core::query::ClusterName;

use super::introspection::AdbcIntrospection;
use super::sql_helpers;
use super::AdbcPool;

/// Snowflake introspection via `SHOW WAREHOUSES` (cloud services layer — no warehouse resume).
pub struct SnowflakeIntrospection {
    pool: AdbcPool,
    warehouse: String,
}

pub fn try_from_adbc_config(
    cluster_name: &ClusterName,
    uri: &str,
    db_kwargs: &[(String, String)],
    pool: AdbcPool,
) -> Option<SnowflakeIntrospection> {
    let warehouse = sql_helpers::db_kwarg(db_kwargs, "adbc.snowflake.sql.warehouse")
        .or_else(|| sql_helpers::db_kwarg(db_kwargs, "warehouse"))
        .or_else(|| sql_helpers::uri_query_param(uri, "warehouse"))?;
    if warehouse.is_empty() {
        tracing::warn!(cluster = %cluster_name.0, "Snowflake introspection: no warehouse in config");
        return None;
    }

    tracing::info!(
        cluster = %cluster_name.0,
        warehouse = %warehouse,
        "Snowflake introspection initialized (SHOW WAREHOUSES)"
    );

    Some(SnowflakeIntrospection { pool, warehouse })
}

impl SnowflakeIntrospection {
    /// `SHOW ... LIKE` has no `ESCAPE` clause support (unlike the standard SQL
    /// `LIKE` predicate), so `_`/`%` in `warehouse` remain live wildcards —
    /// e.g. `LIKE 'ETL_WH'` can also match a warehouse literally named
    /// `ETLXWH`. A backslash here would be taken literally rather than as an
    /// escape character, so trying to escape wildcards would make the query
    /// match *nothing*, including the intended warehouse — worse than the
    /// over-matching it would prevent. [`find_named_row`] compensates by
    /// preferring an exact `name` match among whatever rows come back.
    pub(crate) fn show_warehouses_sql(warehouse: &str) -> String {
        format!(
            "SHOW WAREHOUSES LIKE '{}'",
            sql_helpers::escape_sql_literal(warehouse)
        )
    }

    /// `LIKE` can return multiple rows (see [`show_warehouses_sql`]); prefer
    /// the row whose `name` exactly matches (case-insensitive — Snowflake
    /// identifiers are case-insensitive unless quoted) over blindly taking
    /// the first row.
    fn find_named_row<'a>(
        batches: &'a [arrow::record_batch::RecordBatch],
        warehouse: &str,
    ) -> Option<(&'a arrow::record_batch::RecordBatch, usize)> {
        for batch in batches {
            for row in 0..batch.num_rows() {
                if let Some(name) = sql_helpers::cell_str(batch, "name", row) {
                    if name.eq_ignore_ascii_case(warehouse) {
                        return Some((batch, row));
                    }
                }
            }
        }
        batches.iter().find(|b| b.num_rows() > 0).map(|b| (b, 0))
    }

    pub(crate) fn parse_show_warehouses_health(
        batches: &[arrow::record_batch::RecordBatch],
        warehouse: &str,
    ) -> bool {
        let Some((batch, row)) = Self::find_named_row(batches, warehouse) else {
            return false;
        };
        let state = match sql_helpers::cell_str(batch, "state", row) {
            Some(s) => s.to_ascii_uppercase(),
            None => return false,
        };
        matches!(
            state.as_str(),
            "STARTED" | "SUSPENDED" | "RESUMING" | "RUNNING"
        )
    }

    pub(crate) fn parse_show_warehouses_running(
        batches: &[arrow::record_batch::RecordBatch],
        warehouse: &str,
    ) -> Option<u64> {
        let (batch, row) = Self::find_named_row(batches, warehouse)?;
        sql_helpers::cell_u64(batch, "running", row)
    }

    fn instance_show_warehouses_sql(&self) -> String {
        Self::show_warehouses_sql(&self.warehouse)
    }
}

#[async_trait]
impl AdbcIntrospection for SnowflakeIntrospection {
    async fn health_check(&self) -> bool {
        let pool = self.pool.clone();
        let sql = self.instance_show_warehouses_sql();
        let warehouse = self.warehouse.clone();
        tokio::task::spawn_blocking(move || {
            let batches = sql_helpers::query_batches(&pool, &sql)?;
            Some(SnowflakeIntrospection::parse_show_warehouses_health(
                &batches, &warehouse,
            ))
        })
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
    }

    async fn fetch_running_query_count(&self) -> Option<u64> {
        let pool = self.pool.clone();
        let sql = self.instance_show_warehouses_sql();
        let warehouse = self.warehouse.clone();
        tokio::task::spawn_blocking(move || {
            let batches = sql_helpers::query_batches(&pool, &sql)?;
            SnowflakeIntrospection::parse_show_warehouses_running(&batches, &warehouse)
        })
        .await
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adbc::test_fixtures::snowflake_show_warehouses_batch;
    use arrow::record_batch::RecordBatch;
    use queryflux_core::query::ClusterName;

    #[test]
    fn warehouse_from_db_kwargs_precedence() {
        let kwargs = vec![
            ("adbc.snowflake.sql.warehouse".into(), "FROM_ADBC".into()),
            ("warehouse".into(), "OTHER".into()),
        ];
        // try_from needs pool — only test extraction logic via first kwarg path
        assert_eq!(
            sql_helpers::db_kwarg(&kwargs, "adbc.snowflake.sql.warehouse").as_deref(),
            Some("FROM_ADBC")
        );
    }

    #[test]
    fn try_from_requires_non_empty_warehouse() {
        let name = ClusterName("c".into());
        // Cannot call without pool — test uri/db empty returns None by testing warehouse resolution
        assert!(sql_helpers::uri_query_param("acct/db", "warehouse").is_none());
        let kwargs: Vec<(String, String)> = vec![];
        assert!(
            sql_helpers::db_kwarg(&kwargs, "adbc.snowflake.sql.warehouse")
                .or_else(|| sql_helpers::db_kwarg(&kwargs, "warehouse"))
                .or_else(|| sql_helpers::uri_query_param("acct/db", "warehouse"))
                .is_none()
        );
        let _ = name;
    }

    #[test]
    fn show_warehouses_sql_escapes_quotes() {
        assert_eq!(
            SnowflakeIntrospection::show_warehouses_sql("WH'A"),
            "SHOW WAREHOUSES LIKE 'WH''A'"
        );
    }

    #[test]
    fn show_warehouses_sql_does_not_backslash_escape_wildcards() {
        // SHOW ... LIKE has no ESCAPE support — a backslash would be taken
        // literally, so escaping here would make the query match nothing at
        // all (worse than the over-matching escaping would have prevented).
        assert_eq!(
            SnowflakeIntrospection::show_warehouses_sql("ETL_WH"),
            "SHOW WAREHOUSES LIKE 'ETL_WH'"
        );
    }

    #[test]
    fn health_accepts_suspended_and_started_states() {
        for state in ["SUSPENDED", "STARTED", "Resuming", "running"] {
            let batch = snowflake_show_warehouses_batch(state, 0);
            assert!(
                SnowflakeIntrospection::parse_show_warehouses_health(&[batch], "ANALYTICS_WH"),
                "state {state} should be healthy"
            );
        }
    }

    #[test]
    fn health_rejects_missing_row_and_bad_state() {
        assert!(!SnowflakeIntrospection::parse_show_warehouses_health(
            &[],
            "ANALYTICS_WH"
        ));
        let stopped = snowflake_show_warehouses_batch("STOPPED", 0);
        assert!(!SnowflakeIntrospection::parse_show_warehouses_health(
            &[stopped],
            "ANALYTICS_WH"
        ));
    }

    #[test]
    fn running_count_from_show_warehouses() {
        let batch = snowflake_show_warehouses_batch("STARTED", 5);
        assert_eq!(
            SnowflakeIntrospection::parse_show_warehouses_running(&[batch], "ANALYTICS_WH"),
            Some(5)
        );
    }

    #[test]
    fn health_and_running_prefer_exact_name_match_over_first_row() {
        // LIKE 'ETL_WH' can also match "ETLXWH" (or, alphabetically, sort
        // before the intended warehouse) — the exact-name row must win over
        // whatever row happens to come back first.
        let decoy = snowflake_show_warehouses_batch("SUSPENDED", 99);
        let mut target = snowflake_show_warehouses_batch("STARTED", 3);
        {
            use arrow::array::StringArray;
            use std::sync::Arc;
            let schema = target.schema();
            let mut cols = target.columns().to_vec();
            cols[0] = Arc::new(StringArray::from(vec!["ETL_WH"]));
            target = RecordBatch::try_new(schema, cols).unwrap();
        }
        let batches = vec![decoy, target];
        assert_eq!(
            SnowflakeIntrospection::parse_show_warehouses_running(&batches, "ETL_WH"),
            Some(3)
        );
        assert!(SnowflakeIntrospection::parse_show_warehouses_health(
            &batches, "ETL_WH"
        ));
    }

    #[test]
    fn running_count_missing_column_returns_none() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "state",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["STARTED"]))])
                .unwrap();
        assert!(
            SnowflakeIntrospection::parse_show_warehouses_running(&[batch], "ANALYTICS_WH")
                .is_none()
        );
    }
}
