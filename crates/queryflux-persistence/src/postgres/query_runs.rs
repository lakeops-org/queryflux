//! `query_runs`: runs reported by `queryflux-collector`, and its watermarks.

use std::sync::LazyLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::{
    error::{QueryFluxError, Result},
    query_runs::{links_by_engine_id, CollectorConfigV1, QueryRunBatchV1},
};
use serde_json::{Map, Value};

use super::PostgresStore;
use crate::QueryRunStore;

/// Columns a later batch may fill: a new value replaces a stored null, never the other
/// way round (the fast and slow clocks report different parts of one run).
const FILLED: &[&str] = &[
    "user_name",
    "started_at",
    "ended_at",
    "status",
    "statement_type",
    "sql_hash",
    "fast_hash",
    "elapsed_ms",
    "queued_ms",
    "provisioning_ms",
    "planning_ms",
    "execution_ms",
    "cpu_ms",
    "bytes_scanned",
    "rows_scanned",
    "peak_memory_bytes",
    "spilled_bytes",
    "network_bytes",
    "rows_returned",
    "bytes_returned",
    "rows_written",
    "bytes_written",
    "result_cache_hit",
];

/// Rows arrive as one JSON array, mapped onto the table's columns by name
/// (`jsonb_populate_recordset`), so a new metric column needs a migration and a field,
/// not new bind code.
static UPSERT_RUNS_SQL: LazyLock<String> = LazyLock::new(|| {
    let cols = FILLED.join(", ");
    let updates: Vec<String> = FILLED
        .iter()
        .map(|c| format!("{c} = COALESCE(EXCLUDED.{c}, q.{c})"))
        .collect();
    format!(
        "INSERT INTO query_runs AS q \
           (cluster, engine_query_id, engine, link_by_id, collector, {cols}, extra) \
         SELECT cluster, engine_query_id, engine, link_by_id, collector, {cols}, \
                COALESCE(extra, '{{}}'::jsonb) \
         FROM jsonb_populate_recordset(NULL::query_runs, $1) \
         ON CONFLICT (cluster, engine_query_id) DO UPDATE SET {}, \
           extra = q.extra || EXCLUDED.extra, \
           collector = EXCLUDED.collector, \
           collected_at = now()",
        updates.join(", ")
    )
});

/// Link the batch's unlinked runs to the records QueryFlux wrote for them.
const LINK_RUNS_SQL: &str = "\
UPDATE query_runs q SET proxy_query_id = r.proxy_query_id, matched_by = 'engine_id'
FROM query_records r
WHERE q.cluster = $1 AND q.engine_query_id = ANY($2)
  AND q.proxy_query_id IS NULL AND q.link_by_id
  AND r.cluster_name = q.cluster AND r.backend_query_id = q.engine_query_id";

const ADVANCE_WATERMARK_SQL: &str = "\
INSERT INTO collector_watermarks (collector, cluster, clock, watermark)
VALUES ($1, $2, $3, $4)
ON CONFLICT (collector, cluster, clock) DO UPDATE SET
  watermark = GREATEST(collector_watermarks.watermark, EXCLUDED.watermark),
  updated_at = now()";

/// `engineQueryId` → `engine_query_id`.
fn snake_case(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 4);
    for c in key.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The batch's runs as table rows: the wire's camelCase keys renamed to column names,
/// plus the columns every run in the batch shares.
fn run_rows(batch: &QueryRunBatchV1) -> Result<Value> {
    let link_by_id = links_by_engine_id(&batch.engine);
    let rows = batch
        .runs
        .iter()
        .map(|run| {
            let Value::Object(fields) = serde_json::to_value(run)
                .map_err(|e| QueryFluxError::Persistence(format!("encode run: {e}")))?
            else {
                unreachable!("a struct serializes to an object")
            };
            let mut row: Map<String, Value> = fields
                .into_iter()
                .map(|(k, v)| (snake_case(&k), v))
                .collect();
            row.insert("cluster".into(), batch.cluster.clone().into());
            row.insert("engine".into(), batch.engine.clone().into());
            row.insert("link_by_id".into(), link_by_id.into());
            row.insert("collector".into(), batch.collector.clone().into());
            Ok(Value::Object(row))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Array(rows))
}

#[async_trait]
impl QueryRunStore for PostgresStore {
    async fn ingest_runs(&self, batch: &QueryRunBatchV1) -> Result<()> {
        let err = |what: &str, e: sqlx::Error| QueryFluxError::Persistence(format!("{what}: {e}"));
        let mut tx = self
            .admin_pool
            .begin()
            .await
            .map_err(|e| err("ingest runs (begin)", e))?;
        if !batch.runs.is_empty() {
            sqlx::query(&UPSERT_RUNS_SQL)
                .bind(run_rows(batch)?)
                .execute(&mut *tx)
                .await
                .map_err(|e| err("upsert query_runs", e))?;
            let ids: Vec<&str> = batch
                .runs
                .iter()
                .map(|r| r.engine_query_id.as_str())
                .collect();
            sqlx::query(LINK_RUNS_SQL)
                .bind(&batch.cluster)
                .bind(&ids)
                .execute(&mut *tx)
                .await
                .map_err(|e| err("link query_runs", e))?;
        }
        sqlx::query(ADVANCE_WATERMARK_SQL)
            .bind(&batch.collector)
            .bind(&batch.cluster)
            .bind(&batch.clock)
            .bind(batch.window_end)
            .execute(&mut *tx)
            .await
            .map_err(|e| err("advance collector watermark", e))?;
        tx.commit()
            .await
            .map_err(|e| err("ingest runs (commit)", e))
    }

    async fn collector_watermarks(&self, collector: &str) -> Result<CollectorConfigV1> {
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT cluster, clock, watermark FROM collector_watermarks WHERE collector = $1",
        )
        .bind(collector)
        .fetch_all(&self.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("read collector watermarks: {e}")))?;
        let mut config = CollectorConfigV1::default();
        for (cluster, clock, watermark) in rows {
            config
                .watermarks
                .entry(cluster)
                .or_default()
                .insert(clock, watermark);
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query_runs::{
        ExecMetrics, QueryRun, FAST_CLOCK, QUERY_RUNS_SCHEMA_VERSION,
    };

    #[test]
    fn wire_keys_become_column_names() {
        assert_eq!(snake_case("engineQueryId"), "engine_query_id");
        assert_eq!(snake_case("peakMemoryBytes"), "peak_memory_bytes");
        assert_eq!(snake_case("status"), "status");
    }

    fn batch(window_end: DateTime<Utc>, runs: Vec<QueryRun>) -> QueryRunBatchV1 {
        QueryRunBatchV1 {
            schema_version: QUERY_RUNS_SCHEMA_VERSION,
            collector: "c1".into(),
            cluster: "trino-a".into(),
            engine: "Trino".into(),
            clock: FAST_CLOCK.into(),
            window_start: window_end - chrono::Duration::minutes(5),
            window_end,
            runs,
        }
    }

    #[test]
    fn rows_carry_the_shared_columns_and_snake_case_metrics() {
        let run = QueryRun {
            engine_query_id: "q1".into(),
            metrics: ExecMetrics {
                peak_memory_bytes: Some(7),
                ..Default::default()
            },
            ..Default::default()
        };
        let rows = run_rows(&batch(Utc::now(), vec![run])).unwrap();
        let row = &rows[0];
        assert_eq!(row["engine_query_id"], "q1");
        assert_eq!(row["peak_memory_bytes"], 7);
        assert_eq!(row["cluster"], "trino-a");
        assert_eq!(row["link_by_id"], true);
    }

    async fn test_store() -> PostgresStore {
        let url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for Postgres integration tests");
        let store = PostgresStore::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    #[tokio::test]
    #[ignore]
    async fn pg_runs_upsert_fill_nulls_link_and_advance_the_watermark() {
        let store = test_store().await;
        let id = format!("q-{}", Utc::now().timestamp_nanos_opt().unwrap());
        // A record QueryFlux wrote for this run, before the run arrives.
        let mut record = crate::postgres::history_batch::tests::record(&id, None);
        record.cluster_name = queryflux_core::query::ClusterName("trino-a".into());
        record.backend_query_id = Some(id.clone());
        crate::MetricsStore::record_queries(&store, vec![record.clone()])
            .await
            .unwrap();

        let t1 = Utc::now();
        let fast = QueryRun {
            engine_query_id: id.clone(),
            status: Some("FINISHED".into()),
            metrics: ExecMetrics {
                cpu_ms: Some(10),
                ..Default::default()
            },
            ..Default::default()
        };
        store
            .ingest_runs(&batch(t1, vec![fast.clone()]))
            .await
            .unwrap();
        // Sent again with a new number and a missing one: the new fills, the null keeps.
        let again = QueryRun {
            status: None,
            metrics: ExecMetrics {
                bytes_scanned: Some(99),
                ..Default::default()
            },
            ..fast
        };
        let t0 = t1 - chrono::Duration::minutes(1);
        store.ingest_runs(&batch(t0, vec![again])).await.unwrap();

        let (status, cpu, bytes, linked): (
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT status, cpu_ms, bytes_scanned, proxy_query_id FROM query_runs \
                 WHERE cluster = 'trino-a' AND engine_query_id = $1",
        )
        .bind(&id)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(status.as_deref(), Some("FINISHED"));
        assert_eq!(cpu, Some(10));
        assert_eq!(bytes, Some(99));
        assert_eq!(linked.as_deref(), Some(record.proxy_query_id.as_str()));

        // The watermark never moves back.
        let config = store.collector_watermarks("c1").await.unwrap();
        let mark = config.watermarks["trino-a"][FAST_CLOCK];
        assert_eq!(mark.timestamp_micros(), t1.timestamp_micros());
    }

    #[tokio::test]
    #[ignore]
    async fn pg_a_record_arriving_after_its_run_links_it() {
        let store = test_store().await;
        let id = format!("q-{}", Utc::now().timestamp_nanos_opt().unwrap());
        let run = QueryRun {
            engine_query_id: id.clone(),
            ..Default::default()
        };
        store
            .ingest_runs(&batch(Utc::now(), vec![run]))
            .await
            .unwrap();

        let mut record = crate::postgres::history_batch::tests::record(&id, None);
        record.cluster_name = queryflux_core::query::ClusterName("trino-a".into());
        record.backend_query_id = Some(id.clone());
        crate::MetricsStore::record_queries(&store, vec![record.clone()])
            .await
            .unwrap();

        let linked: Option<String> = sqlx::query_scalar(
            "SELECT proxy_query_id FROM query_runs WHERE cluster = 'trino-a' AND engine_query_id = $1",
        )
        .bind(&id)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(linked.as_deref(), Some(record.proxy_query_id.as_str()));
    }
}
