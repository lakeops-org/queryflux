//! Batched query-history writes for [`PostgresStore`].
//!
//! A batch of [`QueryRecord`]s becomes one `INSERT ... SELECT FROM UNNEST(...)` into
//! `query_records` (array binds, so the parameter count does not grow with the batch)
//! and one multi-row upsert into `query_digest_stats`, in one transaction. Only records
//! the insert actually stored count toward digest stats, so a retried batch does not
//! count twice.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use queryflux_core::{
    error::{QueryFluxError, Result},
    tags::tags_to_json,
};

use super::PostgresStore;
use crate::metrics_store::QueryRecord;

/// Records per statement. Binds are arrays, so this bounds statement size and how long
/// one transaction holds its locks, not the parameter count.
pub(super) const MAX_RECORDS_PER_STATEMENT: usize = 1000;

const INSERT_RECORDS_SQL: &str = r#"
INSERT INTO query_records
    (proxy_query_id, backend_query_id, cluster_group, cluster_name, engine_type,
     frontend_protocol, source_dialect, target_dialect, was_rewritten, was_translated,
     username, catalog, db_name, sql_preview, rewritten_sql, translated_sql, status,
     routing_trace, queue_duration_ms, execution_duration_ms, rows_returned,
     error_message, created_at, engine_elapsed_time_ms, cpu_time_ms, processed_rows,
     processed_bytes, physical_input_bytes, peak_memory_bytes, spilled_bytes,
     total_splits, cluster_group_id, cluster_id, query_tags,
     query_hash, query_parameterized_hash, translated_query_hash,
     agent_id, conversation_id, step_index, tool_call_id, query_intent,
     guard_actions, was_guard_blocked, cache_hit, translation, worker)
SELECT
    u.proxy_query_id, u.backend_query_id, u.cluster_group, u.cluster_name, u.engine_type,
    u.frontend_protocol, u.source_dialect, u.target_dialect, u.was_rewritten, u.was_translated,
    u.username, u.catalog, u.db_name, u.sql_preview, u.rewritten_sql, u.translated_sql, u.status,
    u.routing_trace, u.queue_duration_ms, u.execution_duration_ms, u.rows_returned,
    u.error_message, u.created_at, u.engine_elapsed_time_ms, u.cpu_time_ms, u.processed_rows,
    u.processed_bytes, u.physical_input_bytes, u.peak_memory_bytes, u.spilled_bytes,
    u.total_splits,
    -- Records from workers (#255) carry names, not this database's ids, and may arrive
    -- after a cluster was deleted: resolve by name for those, and store a missing row as
    -- NULL rather than failing the foreign key.
    COALESCE((SELECT id FROM cluster_group_configs WHERE id = u.cluster_group_config_id),
             (SELECT id FROM cluster_group_configs
               WHERE u.worker IS NOT NULL AND name = u.cluster_group)),
    COALESCE((SELECT id FROM cluster_configs WHERE id = u.cluster_config_id),
             (SELECT id FROM cluster_configs
               WHERE u.worker IS NOT NULL AND name = u.cluster_name)),
    u.query_tags,
    u.query_hash, u.query_parameterized_hash, u.translated_query_hash,
    u.agent_id, u.conversation_id, u.step_index, u.tool_call_id, u.query_intent,
    u.guard_actions, u.was_guard_blocked, u.cache_hit, u.translation, u.worker
FROM UNNEST(
    $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::text[], $7::text[], $8::text[], $9::bool[], $10::bool[],
    $11::text[], $12::text[], $13::text[], $14::text[], $15::text[], $16::text[], $17::text[],
    $18::jsonb[], $19::int8[], $20::int8[], $21::int8[],
    $22::text[], $23::timestamptz[], $24::int8[], $25::int8[], $26::int8[],
    $27::int8[], $28::int8[], $29::int8[], $30::int8[],
    $31::int4[], $32::int8[], $33::int8[], $34::jsonb[],
    $35::int8[], $36::int8[], $37::int8[],
    $38::text[], $39::text[], $40::int4[], $41::text[], $42::text[],
    $43::jsonb[], $44::bool[], $45::bool[], $46::jsonb[], $47::text[]
) WITH ORDINALITY AS u(
    proxy_query_id, backend_query_id, cluster_group, cluster_name, engine_type,
    frontend_protocol, source_dialect, target_dialect, was_rewritten, was_translated,
    username, catalog, db_name, sql_preview, rewritten_sql, translated_sql, status,
    routing_trace, queue_duration_ms, execution_duration_ms, rows_returned,
    error_message, created_at, engine_elapsed_time_ms, cpu_time_ms, processed_rows,
    processed_bytes, physical_input_bytes, peak_memory_bytes, spilled_bytes,
    total_splits, cluster_group_config_id, cluster_config_id, query_tags,
    query_hash, query_parameterized_hash, translated_query_hash,
    agent_id, conversation_id, step_index, tool_call_id, query_intent,
    guard_actions, was_guard_blocked, cache_hit, translation, worker, ord)
-- Ids follow batch order, as if the records were inserted one at a time.
ORDER BY u.ord
ON CONFLICT (proxy_query_id) DO NOTHING
RETURNING proxy_query_id
"#;

/// Same per-row semantics as the single-record upsert it replaces, with counts and sums
/// added from the folded delta instead of one at a time.
const UPSERT_DIGESTS_SQL: &str = r#"
INSERT INTO query_digest_stats
    (query_parameterized_hash, digest_text,
     translated_query_hash, translated_digest_text,
     first_seen, last_seen, call_count, sum_execution_ms, sum_rows_returned,
     cluster_group)
SELECT * FROM UNNEST(
    $1::int8[], $2::text[], $3::int8[], $4::text[], $5::timestamptz[], $6::timestamptz[],
    $7::int8[], $8::int8[], $9::int8[], $10::text[])
ON CONFLICT (query_parameterized_hash) DO UPDATE SET
    last_seen = EXCLUDED.last_seen,
    call_count = query_digest_stats.call_count + EXCLUDED.call_count,
    sum_execution_ms = query_digest_stats.sum_execution_ms + EXCLUDED.sum_execution_ms,
    sum_rows_returned = query_digest_stats.sum_rows_returned + EXCLUDED.sum_rows_returned,
    cluster_group = EXCLUDED.cluster_group,
    digest_text = CASE WHEN query_digest_stats.digest_text = '' THEN EXCLUDED.digest_text
                       ELSE query_digest_stats.digest_text END,
    translated_digest_text = COALESCE(query_digest_stats.translated_digest_text,
                                      EXCLUDED.translated_digest_text)
"#;

/// One fingerprint's share of a batch, folded so that applying it once gives the same
/// row as applying each record's upsert in order.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DigestDelta {
    pub hash: i64,
    /// First non-empty digest text (the upsert replaces only an empty one).
    pub digest_text: String,
    /// The first record's: it is written only when the row is created.
    pub translated_query_hash: Option<i64>,
    /// First non-null (the upsert keeps an existing value).
    pub translated_digest_text: Option<String>,
    /// The first record's: written only when the row is created.
    pub first_seen: DateTime<Utc>,
    /// The last record's: each upsert overwrites it.
    pub last_seen: DateTime<Utc>,
    pub call_count: i64,
    pub sum_execution_ms: i64,
    pub sum_rows_returned: i64,
    /// The last record's: each upsert overwrites it.
    pub cluster_group: String,
}

/// Fold records into one delta per parameterized hash, sorted by hash so concurrent
/// batches lock digest rows in the same order.
pub(super) fn fold_digests<'a>(
    records: impl IntoIterator<Item = &'a QueryRecord>,
) -> Vec<DigestDelta> {
    let mut by_hash: HashMap<i64, DigestDelta> = HashMap::new();
    for r in records {
        let Some(hash) = r.query_parameterized_hash else {
            continue;
        };
        let digest = r.digest_text.as_deref().unwrap_or("");
        let rows = r.rows_returned.map(|v| v as i64).unwrap_or(0);
        let exec_ms = r.execution_duration_ms as i64;
        match by_hash.get_mut(&hash) {
            None => {
                by_hash.insert(
                    hash,
                    DigestDelta {
                        hash,
                        digest_text: digest.to_string(),
                        translated_query_hash: r.translated_query_hash,
                        translated_digest_text: r.translated_digest_text.clone(),
                        first_seen: r.created_at,
                        last_seen: r.created_at,
                        call_count: 1,
                        sum_execution_ms: exec_ms,
                        sum_rows_returned: rows,
                        cluster_group: r.cluster_group.0.clone(),
                    },
                );
            }
            Some(d) => {
                if d.digest_text.is_empty() {
                    d.digest_text = digest.to_string();
                }
                if d.translated_digest_text.is_none() {
                    d.translated_digest_text = r.translated_digest_text.clone();
                }
                d.last_seen = r.created_at;
                d.call_count += 1;
                d.sum_execution_ms += exec_ms;
                d.sum_rows_returned += rows;
                d.cluster_group = r.cluster_group.0.clone();
            }
        }
    }
    let mut deltas: Vec<DigestDelta> = by_hash.into_values().collect();
    deltas.sort_by_key(|d| d.hash);
    deltas
}

/// Keep the first record per `proxy_query_id`, in order.
pub(super) fn dedupe_first(records: Vec<QueryRecord>) -> Vec<QueryRecord> {
    let mut seen = HashSet::with_capacity(records.len());
    records
        .into_iter()
        .filter(|r| seen.insert(r.proxy_query_id.clone()))
        .collect()
}

impl PostgresStore {
    /// Write records in chunks of [`MAX_RECORDS_PER_STATEMENT`]: per chunk, one insert
    /// and one digest upsert in one transaction. Returns how many records were new.
    ///
    /// If Postgres rejects a chunk's data, the chunk is retried one record at a time and
    /// only the records it still rejects are dropped, so one bad record cannot block a
    /// batch forever. Other errors (connection, timeout) are returned for a retry.
    pub(super) async fn insert_query_records(&self, records: Vec<QueryRecord>) -> Result<u64> {
        let records = dedupe_first(records);
        let mut inserted = 0u64;
        for chunk in records.chunks(MAX_RECORDS_PER_STATEMENT) {
            match self.insert_chunk(chunk).await {
                Ok(n) => inserted += n,
                Err(e) if is_rejected_data(&e) => {
                    tracing::warn!(
                        records = chunk.len(),
                        "Postgres rejected a query history batch, retrying per record: {e}"
                    );
                    for r in chunk {
                        match self.insert_chunk(std::slice::from_ref(r)).await {
                            Ok(n) => inserted += n,
                            Err(e) if is_rejected_data(&e) => tracing::warn!(
                                proxy_query_id = %r.proxy_query_id,
                                "Dropping query record Postgres rejected: {e}"
                            ),
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(inserted)
    }

    async fn insert_chunk(&self, chunk: &[QueryRecord]) -> std::result::Result<u64, ChunkError> {
        if chunk.is_empty() {
            return Ok(0);
        }
        let err = |what: &'static str, e: sqlx::Error| ChunkError(what, e);
        let mut tx = self
            .admin_pool
            .begin()
            .await
            .map_err(|e| err("Insert query_records (begin)", e))?;

        let new_ids: Vec<String> = bind_records(sqlx::query_scalar(INSERT_RECORDS_SQL), chunk)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| err("Insert query_records", e))?;

        if !new_ids.is_empty() {
            let new_ids: HashSet<&str> = new_ids.iter().map(String::as_str).collect();
            let deltas = fold_digests(
                chunk
                    .iter()
                    .filter(|r| new_ids.contains(r.proxy_query_id.as_str())),
            );
            if !deltas.is_empty() {
                bind_digests(sqlx::query(UPSERT_DIGESTS_SQL), &deltas)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| err("Upsert query_digest_stats", e))?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| err("Insert query_records (commit)", e))?;
        Ok(new_ids.len() as u64)
    }
}

/// A failed chunk write: what was being done, and the driver error.
#[derive(Debug)]
struct ChunkError(&'static str, sqlx::Error);

impl std::fmt::Display for ChunkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0, self.1)
    }
}

impl From<ChunkError> for QueryFluxError {
    fn from(e: ChunkError) -> Self {
        QueryFluxError::Persistence(e.to_string())
    }
}

/// Postgres refused the data itself (SQLSTATE class 22 data exception, e.g. a NUL byte in
/// text, or class 23 constraint violation). Retrying the same record will not succeed.
fn is_rejected_data(e: &ChunkError) -> bool {
    match &e.1 {
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|c| c.starts_with("22") || c.starts_with("23")),
        sqlx::Error::Encode(_) => true,
        _ => false,
    }
}

type PgArgs = sqlx::postgres::PgArguments;

/// Bind the 47 column arrays of [`INSERT_RECORDS_SQL`], one element per record.
fn bind_records<'q, O>(
    q: sqlx::query::QueryScalar<'q, sqlx::Postgres, O, PgArgs>,
    rs: &'q [QueryRecord],
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, O, PgArgs> {
    fn col<'a, T>(rs: &'a [QueryRecord], f: impl Fn(&'a QueryRecord) -> T) -> Vec<T> {
        rs.iter().map(f).collect()
    }
    fn stat(
        r: &QueryRecord,
        f: impl Fn(&queryflux_core::query::QueryEngineStats) -> Option<u64>,
    ) -> Option<i64> {
        r.engine_stats.as_ref().and_then(f).map(|v| v as i64)
    }
    q.bind(col(rs, |r| r.proxy_query_id.as_str()))
        .bind(col(rs, |r| r.backend_query_id.as_deref()))
        .bind(col(rs, |r| r.cluster_group.0.as_str()))
        .bind(col(rs, |r| r.cluster_name.0.as_str()))
        .bind(col(rs, |r| format!("{:?}", r.engine_type)))
        .bind(col(rs, |r| format!("{:?}", r.frontend_protocol)))
        .bind(col(rs, |r| format!("{:?}", r.source_dialect)))
        .bind(col(rs, |r| format!("{:?}", r.target_dialect)))
        .bind(col(rs, |r| r.was_rewritten))
        .bind(col(rs, |r| r.was_translated))
        .bind(col(rs, |r| r.user.as_deref()))
        .bind(col(rs, |r| r.catalog.as_deref()))
        .bind(col(rs, |r| r.database.as_deref()))
        .bind(col(rs, |r| r.sql_preview.as_str()))
        .bind(col(rs, |r| r.rewritten_sql.as_deref()))
        .bind(col(rs, |r| r.translated_sql.as_deref()))
        .bind(col(rs, |r| format!("{:?}", r.status)))
        .bind(col(rs, |r| r.routing_trace.clone()))
        .bind(col(rs, |r| r.queue_duration_ms as i64))
        .bind(col(rs, |r| r.execution_duration_ms as i64))
        .bind(col(rs, |r| r.rows_returned.map(|v| v as i64)))
        .bind(col(rs, |r| r.error_message.as_deref()))
        .bind(col(rs, |r| r.created_at))
        .bind(col(rs, |r| stat(r, |s| s.engine_elapsed_time_ms)))
        .bind(col(rs, |r| stat(r, |s| s.cpu_time_ms)))
        .bind(col(rs, |r| stat(r, |s| s.processed_rows)))
        .bind(col(rs, |r| stat(r, |s| s.processed_bytes)))
        .bind(col(rs, |r| stat(r, |s| s.physical_input_bytes)))
        .bind(col(rs, |r| stat(r, |s| s.peak_memory_bytes)))
        .bind(col(rs, |r| stat(r, |s| s.spilled_bytes)))
        .bind(col(rs, |r| {
            r.engine_stats
                .as_ref()
                .and_then(|s| s.total_splits)
                .map(|v| v as i32)
        }))
        .bind(col(rs, |r| r.cluster_group_config_id))
        .bind(col(rs, |r| r.cluster_config_id))
        .bind(col(rs, |r| tags_to_json(&r.query_tags)))
        .bind(col(rs, |r| r.query_hash))
        .bind(col(rs, |r| r.query_parameterized_hash))
        .bind(col(rs, |r| r.translated_query_hash))
        .bind(col(rs, |r| r.agent_id.as_deref()))
        .bind(col(rs, |r| r.conversation_id.as_deref()))
        .bind(col(rs, |r| r.step_index))
        .bind(col(rs, |r| r.tool_call_id.as_deref()))
        .bind(col(rs, |r| r.query_intent.as_deref()))
        .bind(col(rs, |r| {
            serde_json::to_value(&r.guard_actions).unwrap_or(serde_json::Value::Array(vec![]))
        }))
        .bind(col(rs, |r| r.was_guard_blocked))
        .bind(col(rs, |r| r.cache_hit))
        .bind(col(rs, |r| {
            r.translation.map(|outcome| {
                serde_json::to_value(outcome).expect("serializable translation outcome")
            })
        }))
        .bind(col(rs, |r| r.worker.as_deref()))
}

fn bind_digests<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, PgArgs>,
    ds: &'q [DigestDelta],
) -> sqlx::query::Query<'q, sqlx::Postgres, PgArgs> {
    q.bind(ds.iter().map(|d| d.hash).collect::<Vec<_>>())
        .bind(
            ds.iter()
                .map(|d| d.digest_text.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            ds.iter()
                .map(|d| d.translated_query_hash)
                .collect::<Vec<_>>(),
        )
        .bind(
            ds.iter()
                .map(|d| d.translated_digest_text.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(ds.iter().map(|d| d.first_seen).collect::<Vec<_>>())
        .bind(ds.iter().map(|d| d.last_seen).collect::<Vec<_>>())
        .bind(ds.iter().map(|d| d.call_count).collect::<Vec<_>>())
        .bind(ds.iter().map(|d| d.sum_execution_ms).collect::<Vec<_>>())
        .bind(ds.iter().map(|d| d.sum_rows_returned).collect::<Vec<_>>())
        .bind(
            ds.iter()
                .map(|d| d.cluster_group.as_str())
                .collect::<Vec<_>>(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStatus, SqlDialect,
    };

    pub(crate) fn record(id: &str, hash: Option<i64>) -> QueryRecord {
        QueryRecord {
            proxy_query_id: id.into(),
            backend_query_id: None,
            cluster_group: ClusterGroupName("g".into()),
            cluster_name: ClusterName("c".into()),
            cluster_group_config_id: None,
            cluster_config_id: None,
            engine_type: EngineType::DuckDb,
            frontend_protocol: FrontendProtocol::TrinoHttp,
            source_dialect: SqlDialect::Trino,
            target_dialect: SqlDialect::DuckDb,
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: false,
            translation: None,
            translated_sql: None,
            user: None,
            catalog: None,
            database: None,
            sql_preview: "SELECT 1".into(),
            status: QueryStatus::Success,
            routing_trace: None,
            queue_duration_ms: 0,
            execution_duration_ms: 1,
            rows_returned: Some(1),
            error_message: None,
            created_at: Utc::now(),
            engine_stats: None,
            query_tags: Default::default(),
            query_hash: None,
            query_parameterized_hash: hash,
            translated_query_hash: None,
            digest_text: None,
            translated_digest_text: None,
            agent_id: None,
            conversation_id: None,
            step_index: None,
            tool_call_id: None,
            query_intent: None,
            guard_actions: vec![],
            was_guard_blocked: false,
            cache_hit: false,
            worker: None,
        }
    }

    #[test]
    fn fold_matches_applying_each_upsert_in_order() {
        let t0 = Utc::now();
        let mut a = record("a", Some(7));
        a.created_at = t0;
        a.digest_text = None;
        a.translated_query_hash = Some(70);
        a.execution_duration_ms = 5;
        a.rows_returned = None;
        let mut b = record("b", Some(7));
        b.created_at = t0 + chrono::Duration::seconds(1);
        b.digest_text = Some("select ?".into());
        b.translated_query_hash = Some(71);
        b.translated_digest_text = Some("SELECT ?".into());
        b.execution_duration_ms = 10;
        b.rows_returned = Some(3);
        b.cluster_group = ClusterGroupName("g2".into());
        let mut c = record("c", Some(7));
        // Older than b: last_seen still follows batch order, as sequential upserts did.
        c.created_at = t0 - chrono::Duration::seconds(5);
        c.digest_text = Some("other".into());
        c.translated_digest_text = Some("OTHER".into());
        let none = record("n", None);
        let other = record("o", Some(-3));

        let deltas = fold_digests([&a, &b, &none, &c, &other]);
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].hash, -3, "sorted by hash");
        let d = &deltas[1];
        assert_eq!(d.digest_text, "select ?");
        assert_eq!(d.translated_query_hash, Some(70));
        assert_eq!(d.translated_digest_text.as_deref(), Some("SELECT ?"));
        assert_eq!((d.first_seen, d.last_seen), (a.created_at, c.created_at));
        assert_eq!(d.call_count, 3);
        assert_eq!(d.sum_execution_ms, 16);
        assert_eq!(d.sum_rows_returned, 4);
        assert_eq!(d.cluster_group, "g");
    }

    #[test]
    fn dedupe_keeps_the_first_record_per_id() {
        let mut first = record("x", None);
        first.status = QueryStatus::Failed;
        let kept = dedupe_first(vec![first, record("y", None), record("x", None)]);
        let ids: Vec<&str> = kept.iter().map(|r| r.proxy_query_id.as_str()).collect();
        assert_eq!(ids, ["x", "y"]);
        assert!(matches!(kept[0].status, QueryStatus::Failed));
    }

    // -- Postgres integration tests (need DATABASE_URL) ---------------------
    // Run with: cargo test -p queryflux-persistence history_batch -- --ignored

    use crate::metrics_store::{GuardAction, MetricsStore};
    use queryflux_core::query::{
        QueryEngineStats, TranslationOutcome, TranslationReason, TranslationStatus,
    };

    async fn test_store() -> PostgresStore {
        let url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for Postgres integration tests");
        let store = PostgresStore::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    /// A per-run prefix and hash base, so runs against the same database do not collide.
    fn run_id() -> (String, i64) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        (
            format!("hb-{nanos}"),
            (nanos as i64 & 0x0fff_ffff_ffff) << 16,
        )
    }

    /// Records that exercise every column: engine stats, tags, guard actions, routing
    /// traces, translation outcomes, worker names (resolving cluster ids by name) and
    /// ids that no longer exist. `hashes` distinct fingerprints, some records without one.
    fn rich_records(
        prefix: &str,
        hash_base: i64,
        n: usize,
        hashes: i64,
        cluster: &str,
        group: &str,
    ) -> Vec<QueryRecord> {
        let t0 = Utc::now();
        (0..n)
            .map(|i| {
                let mut r = record(&format!("{prefix}-{i}"), None);
                r.created_at = t0 + chrono::Duration::milliseconds((i as i64 * 37) % 1000);
                r.cluster_group = ClusterGroupName(group.into());
                r.cluster_name = ClusterName(cluster.into());
                r.backend_query_id = (i % 2 == 0).then(|| format!("backend-{i}"));
                r.status = match i % 3 {
                    0 => QueryStatus::Success,
                    1 => QueryStatus::Failed,
                    _ => QueryStatus::Cancelled,
                };
                r.worker = (i % 4 != 0).then(|| format!("worker-{}", i % 3));
                if i % 5 == 0 {
                    // A local record pointing at a deleted cluster: stored as NULL.
                    r.cluster_group_config_id = Some(i64::MAX - 1);
                    r.cluster_config_id = Some(i64::MAX - 1);
                }
                r.was_rewritten = i % 6 == 0;
                r.rewritten_sql = r.was_rewritten.then(|| format!("SELECT {i} WHERE t = 1"));
                r.was_translated = i % 2 == 1;
                r.translated_sql = r.was_translated.then(|| format!("select {i}"));
                r.translation = match i % 4 {
                    0 => None,
                    1 => Some(TranslationOutcome {
                        status: TranslationStatus::Yes,
                        reason: None,
                    }),
                    _ => Some(TranslationOutcome {
                        status: TranslationStatus::Fallback,
                        reason: Some(TranslationReason::NoSchema),
                    }),
                };
                r.user = Some(format!("user-{}", i % 7));
                r.catalog = (i % 3 == 0).then(|| "cat".to_string());
                r.database = (i % 3 == 1).then(|| "db".to_string());
                r.sql_preview = format!("SELECT {i} FROM t WHERE name = 'x'");
                r.routing_trace = (i % 2 == 0).then(|| serde_json::json!({"router": "r", "i": i}));
                r.queue_duration_ms = i as u64 % 13;
                r.execution_duration_ms = (i as u64 * 7) % 101;
                r.rows_returned = (i % 5 != 1).then_some(i as u64 * 3);
                r.error_message = matches!(r.status, QueryStatus::Failed).then(|| "boom".into());
                r.engine_stats = (i % 3 != 2).then(|| QueryEngineStats {
                    engine_elapsed_time_ms: Some(i as u64),
                    cpu_time_ms: Some(i as u64 * 2),
                    processed_rows: (i % 2 == 0).then_some(100),
                    processed_bytes: Some(1 << 20),
                    physical_input_bytes: None,
                    peak_memory_bytes: Some(4096),
                    spilled_bytes: Some(0),
                    total_splits: Some(i as u32 % 9),
                });
                if i % 4 == 1 {
                    r.query_tags.insert("team".into(), Some("data".into()));
                    r.query_tags.insert("adhoc".into(), None);
                }
                r.query_hash = Some(hash_base + 100_000 + i as i64);
                if i % 7 != 3 {
                    let h = (i as i64) % hashes;
                    r.query_parameterized_hash = Some(hash_base + h);
                    // Some fingerprints start with no digest text and get one later.
                    r.digest_text = (i % 3 != 0).then(|| format!("select ? -- {h}"));
                    r.translated_query_hash = (i % 2 == 1).then_some(hash_base + 50_000 + h);
                    r.translated_digest_text = (i % 4 == 3).then(|| format!("SELECT ? -- {h}"));
                }
                r.agent_id = (i % 5 == 2).then(|| "agent".to_string());
                r.conversation_id = (i % 5 == 2).then(|| format!("{prefix}-conv"));
                r.step_index = (i % 5 == 2).then_some(i as i32);
                r.tool_call_id = (i % 5 == 2).then(|| format!("tool-{i}"));
                r.query_intent = (i % 5 == 2).then(|| "explore".to_string());
                if i % 6 == 1 {
                    r.guard_actions = vec![GuardAction {
                        guard: "g1".into(),
                        action: "warn".into(),
                        reason: Some("r".into()),
                        code: None,
                        metadata: Some([("k".to_string(), "v".to_string())].into()),
                    }];
                    r.was_guard_blocked = i % 12 == 1;
                }
                r.cache_hit = i % 8 == 0;
                r
            })
            .collect()
    }

    /// Re-key a record set under another prefix and hash base (same everything else).
    fn rekey(records: &[QueryRecord], from: (&str, i64), to: (&str, i64)) -> Vec<QueryRecord> {
        let shift = |h: Option<i64>| h.map(|h| h - from.1 + to.1);
        records
            .iter()
            .cloned()
            .map(|mut r| {
                r.proxy_query_id = r.proxy_query_id.replacen(from.0, to.0, 1);
                r.conversation_id = r.conversation_id.map(|c| c.replacen(from.0, to.0, 1));
                r.query_hash = shift(r.query_hash);
                r.query_parameterized_hash = shift(r.query_parameterized_hash);
                r.translated_query_hash = shift(r.translated_query_hash);
                r
            })
            .collect()
    }

    /// Every stored column except the surrogate id, with run-specific keys normalized.
    async fn stored_rows(
        store: &PostgresStore,
        prefix: &str,
        hash_base: i64,
    ) -> Vec<serde_json::Value> {
        sqlx::query_scalar::<_, serde_json::Value>(
            r#"SELECT to_jsonb(q) - 'id'
                   || jsonb_build_object(
                        'proxy_query_id', substr(q.proxy_query_id, length($1) + 1),
                        'conversation_id', substr(q.conversation_id, length($1) + 1),
                        'query_hash', q.query_hash - $2,
                        'query_parameterized_hash', q.query_parameterized_hash - $2,
                        'translated_query_hash', q.translated_query_hash - $2)
               FROM query_records q
               WHERE q.proxy_query_id LIKE $1 || '-%'
               ORDER BY q.id"#,
        )
        .bind(prefix)
        .bind(hash_base)
        .fetch_all(&store.admin_pool)
        .await
        .unwrap()
    }

    async fn stored_digests(store: &PostgresStore, hash_base: i64) -> Vec<serde_json::Value> {
        sqlx::query_scalar::<_, serde_json::Value>(
            r#"SELECT to_jsonb(d) - 'id'
                   || jsonb_build_object(
                        'query_parameterized_hash', d.query_parameterized_hash - $1,
                        'translated_query_hash', d.translated_query_hash - $1)
               FROM query_digest_stats d
               WHERE d.query_parameterized_hash BETWEEN $1 AND $1 + 49999
               ORDER BY d.query_parameterized_hash"#,
        )
        .bind(hash_base)
        .fetch_all(&store.admin_pool)
        .await
        .unwrap()
    }

    async fn seed_cluster(store: &PostgresStore, cluster: &str, group: &str) {
        let upsert: crate::cluster_config::UpsertClusterConfig =
            serde_json::from_value(serde_json::json!({
                "engineKey": "duckDb", "maxRunningQueries": 1, "config": {}
            }))
            .unwrap();
        crate::ClusterConfigStore::upsert_cluster_config(store, cluster, &upsert)
            .await
            .unwrap();
        let g: crate::cluster_config::UpsertClusterGroupConfig =
            serde_json::from_value(serde_json::json!({
                "members": [cluster], "maxRunningQueries": 1
            }))
            .unwrap();
        crate::ClusterConfigStore::upsert_group_config(store, group, &g)
            .await
            .unwrap();
    }

    /// One batch call stores exactly what the old per-record writes stored, including
    /// digest stats, with repeats of a proxy query id inside the batch ignored.
    #[tokio::test]
    #[ignore]
    async fn pg_batch_matches_per_record_writes() {
        let store = test_store().await;
        let (run, base) = run_id();
        let (cluster, group) = (format!("{run}-c"), format!("{run}-g"));
        seed_cluster(&store, &cluster, &group).await;

        let (pa, pb) = (format!("{run}a"), format!("{run}b"));
        let (ba, bb) = (base, base + (1 << 40));
        let mut a = rich_records(&pa, ba, 120, 9, &cluster, &group);
        // A repeat of an earlier id with a different outcome: the first one wins.
        let mut repeat = a[4].clone();
        repeat.status = QueryStatus::Cancelled;
        repeat.execution_duration_ms = 9_999;
        a.push(repeat);
        let b = rekey(&a, (&pa, ba), (&pb, bb));

        for r in a {
            legacy_record_query(&store, r).await.unwrap();
        }
        store.record_queries(b).await.unwrap();

        let (rows_a, rows_b) = (
            stored_rows(&store, &pa, ba).await,
            stored_rows(&store, &pb, bb).await,
        );
        assert_eq!(rows_a.len(), 120);
        assert_eq!(rows_a, rows_b);
        assert!(
            rows_b.iter().any(|r| r["cluster_id"].is_i64()),
            "worker records resolve cluster ids by name"
        );
        let (dig_a, dig_b) = (
            stored_digests(&store, ba).await,
            stored_digests(&store, bb).await,
        );
        assert_eq!(dig_a.len(), 9);
        assert_eq!(dig_a, dig_b);
    }

    /// A retried batch, and a later batch overlapping it, add only their new records to
    /// the digest stats.
    #[tokio::test]
    #[ignore]
    async fn pg_retried_batches_are_not_double_counted() {
        let store = test_store().await;
        let (run, base) = run_id();
        let mk = |range: std::ops::Range<usize>| -> Vec<QueryRecord> {
            range
                .map(|i| {
                    let mut r = record(&format!("{run}-{i}"), Some(base + (i % 2) as i64));
                    r.execution_duration_ms = 10;
                    r.rows_returned = Some(2);
                    r
                })
                .collect()
        };
        store.record_queries(mk(0..10)).await.unwrap();
        store.record_queries(mk(0..10)).await.unwrap();
        store.record_queries(mk(5..15)).await.unwrap();

        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM query_records WHERE proxy_query_id LIKE $1 || '-%'",
        )
        .bind(&run)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(rows, 15);
        let stats: Vec<(i64, i64, i64)> = sqlx::query_as(
            "SELECT call_count, sum_execution_ms, sum_rows_returned FROM query_digest_stats
             WHERE query_parameterized_hash IN ($1, $1 + 1) ORDER BY query_parameterized_hash",
        )
        .bind(base)
        .fetch_all(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(stats, vec![(8, 80, 16), (7, 70, 14)]);
    }

    /// A record Postgres rejects is dropped alone; the rest of its batch is stored.
    #[tokio::test]
    #[ignore]
    async fn pg_rejected_record_does_not_fail_its_batch() {
        let store = test_store().await;
        let (run, base) = run_id();
        let mut records: Vec<QueryRecord> = (0..3)
            .map(|i| record(&format!("{run}-{i}"), Some(base)))
            .collect();
        // Postgres rejects NUL bytes in text columns (SQLSTATE 22021).
        records[1].sql_preview = "select \0".into();
        store.record_queries(records).await.unwrap();

        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT proxy_query_id FROM query_records WHERE proxy_query_id LIKE $1 || '-%'
             ORDER BY proxy_query_id",
        )
        .bind(&run)
        .fetch_all(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(ids, [format!("{run}-0"), format!("{run}-2")]);
        let calls: i64 = sqlx::query_scalar(
            "SELECT call_count FROM query_digest_stats WHERE query_parameterized_hash = $1",
        )
        .bind(base)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(calls, 2);
    }

    /// A batch larger than one statement is split, and a repeat in a later chunk is
    /// still ignored.
    #[tokio::test]
    #[ignore]
    async fn pg_large_batches_are_chunked() {
        let store = test_store().await;
        let (run, base) = run_id();
        let n = MAX_RECORDS_PER_STATEMENT * 2 + 500;
        let mut records: Vec<QueryRecord> = (0..n)
            .map(|i| record(&format!("{run}-{i}"), Some(base + (i % 50) as i64)))
            .collect();
        records[MAX_RECORDS_PER_STATEMENT + 7] = record(&format!("{run}-3"), Some(base));
        records[n - 1] = record(&format!("{run}-3"), Some(base));
        store.record_queries(records).await.unwrap();

        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM query_records WHERE proxy_query_id LIKE $1 || '-%'",
        )
        .bind(&run)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(rows as usize, n - 2);
        let calls: i64 = sqlx::query_scalar(
            "SELECT SUM(call_count)::int8 FROM query_digest_stats
             WHERE query_parameterized_hash BETWEEN $1 AND $1 + 49",
        )
        .bind(base)
        .fetch_one(&store.admin_pool)
        .await
        .unwrap();
        assert_eq!(calls as usize, n - 2);
    }

    /// Ingest throughput, per-record (before) vs batched (after), in worker-sized batches.
    /// `QF_BENCH_RECORDS` (default 100000) and `QF_BENCH_BATCH` (default 200).
    ///
    /// cargo test --release -p queryflux-persistence pg_history_ingest_benchmark -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn pg_history_ingest_benchmark() {
        let env = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let total = env("QF_BENCH_RECORDS", 100_000);
        let batch = env("QF_BENCH_BATCH", 200);
        let store = test_store().await;
        let (run, base) = run_id();
        let (cluster, group) = (format!("{run}-c"), format!("{run}-g"));
        seed_cluster(&store, &cluster, &group).await;

        async fn xacts(store: &PostgresStore) -> i64 {
            sqlx::query_scalar(
                "SELECT (xact_commit + xact_rollback)::int8 FROM pg_stat_database
                 WHERE datname = current_database()",
            )
            .fetch_one(&store.admin_pool)
            .await
            .unwrap()
        }
        async fn settle() {
            // pg_stat_database is updated when backends flush their stats.
            tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        }

        for (label, legacy) in [("before (per record)", true), ("after (batched)", false)] {
            let prefix = format!("{run}{}", if legacy { "l" } else { "b" });
            let hash_base = if legacy { base } else { base + (1 << 40) };
            // 500 distinct fingerprints, like a busy but repetitive workload.
            let records = rich_records(&prefix, hash_base, total, 500, &cluster, &group);
            settle().await;
            let x0 = xacts(&store).await;
            let started = std::time::Instant::now();
            let mut it = records.into_iter().peekable();
            while it.peek().is_some() {
                let chunk: Vec<QueryRecord> = it.by_ref().take(batch).collect();
                if legacy {
                    for r in chunk {
                        legacy_record_query(&store, r).await.unwrap();
                    }
                } else {
                    store.record_queries(chunk).await.unwrap();
                }
            }
            let secs = started.elapsed().as_secs_f64();
            settle().await;
            let x = xacts(&store).await - x0;
            let batches = total.div_ceil(batch);
            println!(
                "{label}: {total} records in {secs:.2}s = {:.0} records/s; \
                 {:.1} transactions per {batch}-record batch",
                total as f64 / secs,
                x as f64 / batches as f64
            );
        }
    }

    /// The per-record write this module replaced, kept as the reference for
    /// equivalence tests and the "before" benchmark.
    async fn legacy_record_query(store: &PostgresStore, r: QueryRecord) -> Result<()> {
        let (
            engine_elapsed_ms,
            cpu_ms,
            proc_rows,
            proc_bytes,
            phys_bytes,
            peak_mem,
            spilled,
            splits,
        ) = match &r.engine_stats {
            Some(s) => (
                s.engine_elapsed_time_ms.map(|v| v as i64),
                s.cpu_time_ms.map(|v| v as i64),
                s.processed_rows.map(|v| v as i64),
                s.processed_bytes.map(|v| v as i64),
                s.physical_input_bytes.map(|v| v as i64),
                s.peak_memory_bytes.map(|v| v as i64),
                s.spilled_bytes.map(|v| v as i64),
                s.total_splits.map(|v| v as i32),
            ),
            None => (None, None, None, None, None, None, None, None),
        };

        let query_tags_json = tags_to_json(&r.query_tags);
        let guard_actions_json =
            serde_json::to_value(&r.guard_actions).unwrap_or(serde_json::Value::Array(vec![]));
        let insert_result = sqlx::query(
            r#"INSERT INTO query_records
                (proxy_query_id, backend_query_id, cluster_group, cluster_name, engine_type,
                 frontend_protocol, source_dialect, target_dialect, was_rewritten, was_translated,
                 username, catalog, db_name, sql_preview, rewritten_sql, translated_sql, status,
                 routing_trace, queue_duration_ms, execution_duration_ms, rows_returned,
                 error_message, created_at, engine_elapsed_time_ms, cpu_time_ms, processed_rows,
                 processed_bytes, physical_input_bytes, peak_memory_bytes, spilled_bytes,
                 total_splits, cluster_group_id, cluster_id, query_tags,
                 query_hash, query_parameterized_hash, translated_query_hash,
                 agent_id, conversation_id, step_index, tool_call_id, query_intent,
                 guard_actions, was_guard_blocked, cache_hit, translation, worker)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,
                       -- Records from workers (#255) carry names, not this database's
                       -- ids, and may arrive after a cluster was deleted: resolve by name
                       -- for those, and store a missing row as NULL rather than failing
                       -- the foreign key.
                       COALESCE((SELECT id FROM cluster_group_configs WHERE id = $32),
                                (SELECT id FROM cluster_group_configs
                                  WHERE $47::text IS NOT NULL AND name = $3)),
                       COALESCE((SELECT id FROM cluster_configs WHERE id = $33),
                                (SELECT id FROM cluster_configs
                                  WHERE $47::text IS NOT NULL AND name = $4)),
                       $34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47)
               ON CONFLICT (proxy_query_id) DO NOTHING"#,
        )
        .bind(&r.proxy_query_id)
        .bind(&r.backend_query_id)
        .bind(&r.cluster_group.0)
        .bind(&r.cluster_name.0)
        .bind(format!("{:?}", r.engine_type))
        .bind(format!("{:?}", r.frontend_protocol))
        .bind(format!("{:?}", r.source_dialect))
        .bind(format!("{:?}", r.target_dialect))
        .bind(r.was_rewritten)
        .bind(r.was_translated)
        .bind(&r.user)
        .bind(&r.catalog)
        .bind(&r.database)
        .bind(&r.sql_preview)
        .bind(&r.rewritten_sql)
        .bind(&r.translated_sql)
        .bind(format!("{:?}", r.status))
        .bind(&r.routing_trace)
        .bind(r.queue_duration_ms as i64)
        .bind(r.execution_duration_ms as i64)
        .bind(r.rows_returned.map(|v| v as i64))
        .bind(&r.error_message)
        .bind(r.created_at)
        .bind(engine_elapsed_ms)
        .bind(cpu_ms)
        .bind(proc_rows)
        .bind(proc_bytes)
        .bind(phys_bytes)
        .bind(peak_mem)
        .bind(spilled)
        .bind(splits)
        .bind(r.cluster_group_config_id)
        .bind(r.cluster_config_id)
        .bind(query_tags_json)
        .bind(r.query_hash)
        .bind(r.query_parameterized_hash)
        .bind(r.translated_query_hash)
        .bind(&r.agent_id)
        .bind(&r.conversation_id)
        .bind(r.step_index)
        .bind(&r.tool_call_id)
        .bind(&r.query_intent)
        .bind(guard_actions_json)
        .bind(r.was_guard_blocked)
        .bind(r.cache_hit)
        .bind(r.translation.map(|outcome| {
            serde_json::to_value(outcome).expect("serializable translation outcome")
        }))
        .bind(&r.worker)
        .execute(&store.admin_pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert query_records: {e}")))?;
        if insert_result.rows_affected() == 0 {
            return Ok(());
        }

        // Upsert into query_digest_stats.
        if let Some(phash) = r.query_parameterized_hash {
            let rows = r.rows_returned.map(|v| v as i64).unwrap_or(0);
            let exec_ms = r.execution_duration_ms as i64;
            sqlx::query(
                r#"INSERT INTO query_digest_stats
                    (query_parameterized_hash, digest_text,
                     translated_query_hash, translated_digest_text,
                     first_seen, last_seen, call_count, sum_execution_ms, sum_rows_returned,
                     cluster_group)
                   VALUES ($1,$2,$3,$4,$5,$5,1,$6,$7,$8)
                   ON CONFLICT (query_parameterized_hash) DO UPDATE SET
                     last_seen = EXCLUDED.last_seen,
                     call_count = query_digest_stats.call_count + 1,
                     sum_execution_ms = query_digest_stats.sum_execution_ms + EXCLUDED.sum_execution_ms,
                     sum_rows_returned = query_digest_stats.sum_rows_returned + EXCLUDED.sum_rows_returned,
                     cluster_group = EXCLUDED.cluster_group,
                     digest_text = CASE WHEN query_digest_stats.digest_text = '' THEN EXCLUDED.digest_text ELSE query_digest_stats.digest_text END,
                     translated_digest_text = COALESCE(query_digest_stats.translated_digest_text, EXCLUDED.translated_digest_text)"#,
            )
            .bind(phash)
            .bind(r.digest_text.as_deref().unwrap_or(""))
            .bind(r.translated_query_hash)
            .bind(r.translated_digest_text.as_deref())
            .bind(r.created_at)
            .bind(exec_ms)
            .bind(rows)
            .bind(&r.cluster_group.0)
            .execute(&store.admin_pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Upsert query_digest_stats: {e}")))?;
        }

        Ok(())
    }
}
