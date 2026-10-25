//! SQL parsing and read/write classification shared across dispatch, guardrails,
//! and engine adapters. Parse results are cached per [`SqlParseCache`] instance so
//! multiple consumers on the same query only hit `polyglot_sql` once.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use polyglot_sql::expressions::Expression;
use polyglot_sql::DialectType;

use queryflux_core::query::SqlDialect;

#[derive(Debug)]
enum ParsedStatements {
    Ok(Vec<Expression>),
    Err,
}

/// The one place a [`SqlParseCache`] parses. polyglot-sql can overflow the default Tokio
/// (~2 MiB) stack; always parse on the dedicated large-stack pool (see polyglot_pool).
fn parse_on_pool(sql: String, dialect: &SqlDialect) -> ParsedStatements {
    let dialect = to_polyglot_dialect(dialect);
    crate::polyglot_pool::run(move || {
        polyglot_sql::parse(&sql, dialect)
            .map(ParsedStatements::Ok)
            .unwrap_or(ParsedStatements::Err)
    })
    .unwrap_or(ParsedStatements::Err)
}

/// Lazily parses SQL once and exposes shared classification helpers.
pub struct SqlParseCache {
    sql: String,
    dialect: SqlDialect,
    cache: OnceLock<ParsedStatements>,
    /// Times this cache ran polyglot-sql: at most one, or two if concurrent first callers
    /// raced in [`Self::ensure_parsed`].
    parses: AtomicU32,
    /// The sqlglot parse, shared by the catalog table references and access-control
    /// extraction and rewrite; `None` when sqlglot cannot parse the SQL. A `PyOnceLock`, not
    /// a `OnceLock`: the parse runs Python, which can release the GIL mid-initialization,
    /// and a `OnceLock` waiter holding the GIL would then deadlock with it.
    sqlglot: pyo3::sync::PyOnceLock<Option<pyo3::Py<pyo3::PyAny>>>,
    sqlglot_parses: AtomicU32,
}

impl std::fmt::Debug for SqlParseCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlParseCache")
            .field("sql", &self.sql)
            .field("dialect", &self.dialect)
            .field("cache", &self.cache)
            .field("parses", &self.parses)
            .field("sqlglot_parses", &self.sqlglot_parses)
            .finish_non_exhaustive()
    }
}

impl SqlParseCache {
    pub fn new(sql: impl Into<String>, dialect: SqlDialect) -> Self {
        Self {
            sql: sql.into(),
            dialect,
            cache: OnceLock::new(),
            parses: AtomicU32::new(0),
            sqlglot: pyo3::sync::PyOnceLock::new(),
            sqlglot_parses: AtomicU32::new(0),
        }
    }

    /// A cache for `sql` whose parse is already known: `statements` must be what parsing
    /// `sql` with `dialect` gives, as [`Self::interpolate`] guarantees.
    fn with_statements(sql: String, dialect: SqlDialect, statements: Vec<Expression>) -> Self {
        let cache = OnceLock::new();
        let _ = cache.set(ParsedStatements::Ok(statements));
        Self {
            sql,
            dialect,
            cache,
            parses: AtomicU32::new(0),
            sqlglot: pyo3::sync::PyOnceLock::new(),
            sqlglot_parses: AtomicU32::new(0),
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn dialect(&self) -> &SqlDialect {
        &self.dialect
    }

    fn parsed(&self) -> &ParsedStatements {
        self.cache.get_or_init(|| {
            self.parses.fetch_add(1, Ordering::Relaxed);
            parse_on_pool(self.sql.clone(), &self.dialect)
        })
    }

    /// How many times this cache has parsed its SQL (see the `parses` field).
    pub fn parse_count(&self) -> u32 {
        self.parses.load(Ordering::Relaxed)
    }

    /// The SQL parsed by sqlglot in this cache's dialect, once. Callers that change the tree
    /// copy it first (the Python side does). `None` when sqlglot cannot parse it: callers
    /// parse again themselves so they report sqlglot's error.
    pub fn sqlglot_tree<'py>(
        &self,
        py: pyo3::Python<'py>,
    ) -> Option<pyo3::Bound<'py, pyo3::PyAny>> {
        use pyo3::prelude::*;
        use pyo3::types::PyDict;
        self.sqlglot
            .get_or_init(py, || {
                self.sqlglot_parses.fetch_add(1, Ordering::Relaxed);
                let kwargs = PyDict::new(py);
                let dialect = self.dialect.sqlglot_write_name();
                kwargs
                    .set_item("dialect", (!dialect.is_empty()).then_some(dialect))
                    .ok()?;
                py.import("sqlglot")
                    .and_then(|m| m.call_method("parse_one", (self.sql.as_str(),), Some(&kwargs)))
                    .ok()
                    .map(Bound::unbind)
            })
            .as_ref()
            .map(|t| t.bind(py).clone())
    }

    /// How many times this cache has run sqlglot's parser: once, unless threads raced on the
    /// first call, in which case each parsed and one tree was kept.
    pub fn sqlglot_parse_count(&self) -> u32 {
        self.sqlglot_parses.load(Ordering::Relaxed)
    }

    /// Populate the cache without blocking the calling task's worker thread.
    ///
    /// `parsed()` (and everything built on it — `statements()`, `is_read_like()`)
    /// blocks synchronously on `polyglot_pool::run`: queueing the job and waiting
    /// for a pool worker. Calling that directly from an async fn parks a Tokio
    /// worker thread for the duration, which — now that dispatch and every guard
    /// check share one cache per query on the live request path, not just the
    /// original fingerprinting use — can starve unrelated requests under load if
    /// the pool's queue is full or a parse is slow. This runs the same work on a
    /// blocking-pool thread instead and awaits it.
    ///
    /// A no-op once the cache is warm — the common case, since dispatch and
    /// guardrails share one `SqlParseCache` per query and only the first caller
    /// actually parses.
    pub async fn ensure_parsed(&self) {
        if self.cache.get().is_some() {
            return;
        }
        self.parses.fetch_add(1, Ordering::Relaxed);
        let sql = self.sql.clone();
        let dialect = self.dialect.clone();
        let parsed = tokio::task::spawn_blocking(move || parse_on_pool(sql, &dialect))
            .await
            .unwrap_or(ParsedStatements::Err);
        // Ignore "already set": a concurrent caller raced us and won. Same
        // result either way — at worst we duplicated the parse once.
        let _ = self.cache.set(parsed);
    }

    /// Parsed statements when `polyglot_sql` succeeds.
    pub fn statements(&self) -> Option<&[Expression]> {
        match self.parsed() {
            ParsedStatements::Ok(stmts) => Some(stmts.as_slice()),
            ParsedStatements::Err => None,
        }
    }

    /// Async equivalent of [`Self::statements`] — never blocks the calling
    /// task's worker thread. Prefer this on any async request path.
    pub async fn statements_async(&self) -> Option<&[Expression]> {
        self.ensure_parsed().await;
        self.statements()
    }

    /// Whether the statement should use a result-set execution path (`execute`) vs
    /// an update path (`execute_update`).
    ///
    /// Deliberately stricter than [`is_read_stmt`]: `Expression::Command` (`BEGIN`,
    /// `COMMIT`, `ROLLBACK`, and any future catch-all the parser recognizes but
    /// doesn't model precisely) is treated as "read" by `is_read_stmt` so guardrails
    /// don't block it, but that's a permissive default for a different question
    /// ("is this safe to allow?"). Here we're deciding which ADBC API to call, and a
    /// `Command` produces no result set — routing it through `execute()` risks the
    /// same empty-stream misframing this cache exists to avoid (#97). Route it
    /// through `execute_update()` like DDL/DML instead.
    pub fn is_read_like(&self) -> bool {
        match self.statements() {
            Some(stmts) => {
                !stmts.is_empty()
                    && stmts
                        .iter()
                        .all(|s| is_read_stmt(s) && !matches!(s, Expression::Command(_)))
            }
            None => is_read_like_fallback(&self.sql),
        }
    }

    /// Async equivalent of [`Self::is_read_like`] — never blocks the calling
    /// task's worker thread. Prefer this on any async request path.
    pub async fn is_read_like_async(&self) -> bool {
        self.ensure_parsed().await;
        self.is_read_like()
    }

    /// [`crate::is_cacheable`] for this SQL. Uses this cache's parse when it parses with the
    /// dialect cache eligibility is defined in; otherwise (Athena, custom sqlglot dialects,
    /// which fingerprinting parses differently) checks it the way `is_cacheable` always has.
    pub async fn is_cacheable_async(&self) -> bool {
        let name = crate::fingerprint::polyglot_dialect(&self.dialect);
        if name.parse::<DialectType>().ok() == Some(to_polyglot_dialect(&self.dialect)) {
            return match self.statements_async().await {
                Some(stmts) => crate::eligibility::cacheable_statements(&self.sql, stmts),
                None => false,
            };
        }
        let sql = self.sql.clone();
        tokio::task::spawn_blocking(move || crate::is_cacheable(&sql, &name))
            .await
            .unwrap_or(false)
    }

    /// [`crate::params::interpolate_params`] from this cache's parse. The result's cache
    /// already holds the parse of the interpolated SQL, so it never parses.
    pub async fn interpolate(
        &self,
        params: &[queryflux_core::params::QueryParam],
    ) -> anyhow::Result<Self> {
        if params.is_empty() {
            return Ok(Self::new(self.sql.clone(), self.dialect.clone()));
        }
        let Some(stmts) = self.statements_async().await else {
            // Unparseable: report the parser's own error.
            return crate::params::interpolate_params(&self.sql, params, &self.dialect)
                .map(|sql| Self::new(sql, self.dialect.clone()));
        };
        let (sql, statements) =
            crate::params::interpolate_statements(stmts.to_vec(), params, &self.dialect)?;
        Ok(Self::with_statements(sql, self.dialect.clone(), statements))
    }
}

/// Optional hints from dispatch so adapters can skip re-parsing SQL.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutionHints {
    /// Precomputed read/update classification from a shared [`SqlParseCache`].
    pub is_read_like: Option<bool>,
}

/// Map a [`SqlDialect`] to the corresponding [`polyglot_sql::DialectType`].
pub fn to_polyglot_dialect(dialect: &SqlDialect) -> DialectType {
    match dialect {
        SqlDialect::Trino => DialectType::Trino,
        SqlDialect::Athena => DialectType::Athena,
        SqlDialect::DuckDb => DialectType::DuckDB,
        SqlDialect::StarRocks => DialectType::StarRocks,
        SqlDialect::ClickHouse => DialectType::ClickHouse,
        SqlDialect::MySql => DialectType::MySQL,
        SqlDialect::Postgres => DialectType::PostgreSQL,
        SqlDialect::Sqlite => DialectType::SQLite,
        SqlDialect::Snowflake => DialectType::Snowflake,
        SqlDialect::BigQuery => DialectType::BigQuery,
        SqlDialect::Databricks => DialectType::Databricks,
        SqlDialect::MsSql => DialectType::TSQL,
        SqlDialect::Redshift => DialectType::Redshift,
        SqlDialect::Exasol => DialectType::Exasol,
        SqlDialect::Generic | SqlDialect::Sqlglot(_) => DialectType::Generic,
    }
}

/// Returns true when an AST node represents a read-like statement.
pub fn is_read_stmt(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Subquery(_)
            | Expression::Describe(_)
            | Expression::Show(_)
            | Expression::Command(_)
    )
}

/// Classify SQL without retaining a parse cache. Prefer [`SqlParseCache`] on hot paths.
pub fn is_read_like_sql(sql: &str, dialect: &SqlDialect) -> bool {
    SqlParseCache::new(sql, dialect.clone()).is_read_like()
}

/// Fallback classifier for statements `polyglot_sql` cannot parse.
pub fn is_read_like_fallback(sql: &str) -> bool {
    let trimmed = strip_leading_sql_comments(sql).to_uppercase();
    trimmed.starts_with("SELECT")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN")
        || trimmed.starts_with("DESC ")
        || trimmed.starts_with("DESC\t")
}

/// Strip leading whitespace, `--` line comments, and `/* … */` block comments.
pub fn strip_leading_sql_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            s = match rest.find('\n') {
                Some(end) => &rest[end + 1..],
                None => "",
            };
        } else if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(end) => s = &rest[end + 2..],
                None => return s,
            }
        } else {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::params::QueryParam;
    use queryflux_core::query::SqlDialect;

    #[tokio::test]
    async fn one_parse_serves_every_check() {
        let sql = "SELECT * FROM orders WHERE id = ? AND region = 'eu'";
        let cache = SqlParseCache::new(sql, SqlDialect::Trino);
        assert!(cache.is_read_like_async().await);
        assert!(cache.is_cacheable_async().await);
        let bound = cache
            .interpolate(&[QueryParam::Numeric("7".into())])
            .await
            .unwrap();
        assert_eq!(cache.parse_count(), 1);

        // The interpolated SQL's cache holds its parse already.
        assert!(bound.sql().contains("id = 7"), "{}", bound.sql());
        assert!(bound.is_read_like_async().await);
        assert_eq!(bound.parse_count(), 0);
        assert_eq!(
            bound.sql(),
            crate::params::interpolate_params(
                sql,
                &[QueryParam::Numeric("7".into())],
                &SqlDialect::Trino
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn cacheable_matches_the_standalone_check() {
        for (sql, dialect) in [
            ("SELECT count(*) FROM orders", SqlDialect::Trino),
            ("SELECT now()", SqlDialect::Trino),
            ("INSERT INTO t SELECT 1", SqlDialect::Postgres),
            // Fingerprinting parses Athena as presto: checked the standalone way.
            ("SELECT * FROM logs WHERE year = '2026'", SqlDialect::Athena),
            ("SELECT rand()", SqlDialect::Athena),
        ] {
            let name = crate::fingerprint::polyglot_dialect(&dialect);
            let cache = SqlParseCache::new(sql, dialect);
            assert_eq!(
                cache.is_cacheable_async().await,
                crate::is_cacheable(sql, &name),
                "{sql}"
            );
        }
    }

    /// `is_read_like_async`/`statements_async` route the actual parse through
    /// `tokio::task::spawn_blocking` instead of blocking the calling task's
    /// worker thread directly — must still agree with the sync accessors.
    #[tokio::test]
    async fn async_accessors_match_sync_results() {
        for (sql, expect_read_like) in [
            ("SELECT 1", true),
            ("CREATE TABLE t (id INT)", false),
            ("INSERT INTO t VALUES (1)", false),
        ] {
            let cache = SqlParseCache::new(sql, SqlDialect::Generic);
            let async_result = cache.is_read_like_async().await;
            assert_eq!(async_result, cache.is_read_like(), "sql: {sql}");
            assert_eq!(async_result, expect_read_like, "sql: {sql}");
            assert_eq!(
                cache.statements_async().await.map(<[_]>::len),
                cache.statements().map(<[_]>::len),
                "sql: {sql}"
            );
        }
    }

    /// `ensure_parsed` populates a `OnceLock` from inside `spawn_blocking`; many
    /// concurrent callers on the same cache must not race into inconsistent
    /// state or panic — only one parse should "win", and every caller must
    /// still observe the correct, fully-parsed result.
    #[tokio::test]
    async fn ensure_parsed_is_race_safe_under_concurrent_callers() {
        let cache = std::sync::Arc::new(SqlParseCache::new(
            "CREATE TABLE t (id INT)",
            SqlDialect::Generic,
        ));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = std::sync::Arc::clone(&cache);
            handles.push(tokio::spawn(
                async move { cache.is_read_like_async().await },
            ));
        }
        for handle in handles {
            assert!(
                !handle.await.expect("task panicked"),
                "CREATE TABLE must not be read-like"
            );
        }
    }

    #[test]
    fn cache_parses_only_once() {
        let cache = SqlParseCache::new("SELECT 1", SqlDialect::Generic);
        assert!(cache.is_read_like());
        assert!(cache.statements().is_some());
        assert!(cache.is_read_like());
    }

    #[test]
    fn read_like_fallback_handles_line_comments() {
        assert!(is_read_like_fallback("-- comment\nSELECT 1"));
        assert!(!is_read_like_fallback(
            "-- comment\nINSERT INTO t VALUES (1)"
        ));
    }

    #[test]
    fn select_with_show_describe_explain_are_read() {
        assert!(is_read_like_sql("SELECT 1", &SqlDialect::Generic));
        assert!(is_read_like_sql(
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            &SqlDialect::Generic
        ));
        assert!(is_read_like_sql("SHOW TABLES", &SqlDialect::Generic));
        assert!(is_read_like_sql("DESCRIBE my_table", &SqlDialect::Generic));
        assert!(is_read_like_sql("EXPLAIN SELECT 1", &SqlDialect::Generic));
    }

    #[test]
    fn ddl_dml_are_not_read() {
        for sql in [
            "CREATE TABLE t (id INT)",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t WHERE id = 1",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN y INT",
        ] {
            assert!(
                !is_read_like_sql(sql, &SqlDialect::Generic),
                "expected non-read for: {sql}"
            );
        }
    }

    /// `Command` (e.g. bare `END` in a non-Postgres dialect) is "read" for
    /// `is_read_stmt` (guardrails' permissive default) but must NOT be read-like
    /// for ADBC routing — it produces no result set, so it belongs on the
    /// execute_update path like DDL/DML.
    #[test]
    fn command_statements_are_read_for_guardrails_but_not_for_execution_routing() {
        let sql = "END";
        let cache = SqlParseCache::new(sql, SqlDialect::Generic);
        let stmts = cache
            .statements()
            .unwrap_or_else(|| panic!("expected polyglot_sql to parse {sql:?} as a statement"));
        assert_eq!(stmts.len(), 1);
        assert!(
            matches!(stmts[0], Expression::Command(_)),
            "expected {sql:?} to parse as Expression::Command, got: {:?}",
            stmts[0]
        );
        assert!(
            is_read_stmt(&stmts[0]),
            "is_read_stmt should still treat Command as read (guardrails default): {sql}"
        );
        assert!(
            !cache.is_read_like(),
            "is_read_like must route Command through execute_update, not execute: {sql}"
        );
    }

    #[test]
    fn ast_classification_handles_comments() {
        assert!(is_read_like_sql(
            "-- comment\n/* block */ SELECT 1",
            &SqlDialect::Generic,
        ));
        assert!(!is_read_like_sql(
            "-- comment\nINSERT INTO t VALUES (1)",
            &SqlDialect::Generic,
        ));
    }

    #[test]
    fn dialect_specific_parse_still_classifies_select() {
        assert!(is_read_like_sql("SELECT 1", &SqlDialect::Trino));
        assert!(is_read_like_sql("SELECT 1", &SqlDialect::Snowflake));
        assert!(!is_read_like_sql(
            "CREATE TABLE t (id INT)",
            &SqlDialect::Trino
        ));
    }

    #[test]
    fn execution_hints_default_is_unset() {
        let hints = ExecutionHints::default();
        assert_eq!(hints.is_read_like, None);
    }

    /// Regression for snowflake_wire_tests::sql_api_multi_row_query stack overflow:
    /// classifying `unnest([...])` on a Tokio-sized (~2 MiB) stack must not abort —
    /// parse runs on the dedicated large-stack polyglot pool.
    #[test]
    fn unnest_array_classifies_without_stack_overflow() {
        let handle = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .name("classify-small-stack".into())
            .spawn(|| {
                let sql = "SELECT unnest([1, 2, 3]) AS n ORDER BY n";
                let cache = SqlParseCache::new(sql, SqlDialect::DuckDb);
                assert!(
                    cache.is_read_like(),
                    "unnest SELECT must classify as read-like"
                );
                assert!(cache.is_read_like());
            })
            .expect("spawn small-stack classify thread");
        handle
            .join()
            .expect("unnest classify panicked or overflowed on small stack");
    }
}
