//! SQL parsing and read/write classification shared across dispatch, guardrails,
//! and engine adapters. Parse results are cached per [`SqlParseCache`] instance so
//! multiple consumers on the same query only hit `polyglot_sql` once.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use polyglot_sql::expressions::{Expression, With};
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
    /// Same predicate as [`is_result_set_stmt`] (opaque `Command` statements
    /// produce no result set, #403), kept as a method because the unparseable-SQL
    /// fallback applies here. A `Command` routed through `execute()` risks the
    /// same empty-stream misframing this cache exists to avoid (#97). Route it
    /// through `execute_update()` like DDL/DML instead. Deliberately stricter
    /// than routing alone would demand is the guard's job ([`is_read_stmt`]) —
    /// this answers "which ADBC API to call", not "may this run under
    /// read_only".
    pub fn is_read_like(&self) -> bool {
        match self.statements() {
            Some(stmts) => !stmts.is_empty() && stmts.iter().all(is_result_set_stmt),
            None => is_read_like_fallback(&self.sql),
        }
    }

    /// Async equivalent of [`Self::is_read_like`] — never blocks the calling
    /// task's worker thread. Prefer this on any async request path.
    pub async fn is_read_like_async(&self) -> bool {
        self.ensure_parsed().await;
        self.is_read_like()
    }

    /// [`is_read_stmt`] over every parsed statement — the safety classification
    /// ("may this run under read_only") behind the read→write invariant checks
    /// in fixup scripts and access-control rewriting. Deliberately stricter than
    /// [`Self::is_read_like`]: a `SELECT` whose `WITH` clause carries a
    /// `DELETE ... RETURNING` still streams rows for routing, but comparing it
    /// as read-like would let a rewrite swap a read for exactly that shape
    /// without tripping the "must not turn a read into a write" check. The
    /// unparseable-SQL fallback matches [`Self::is_read_like`] so the
    /// invariants' parse-failure behavior stays what it always was.
    pub fn is_read_sql(&self) -> bool {
        match self.statements() {
            Some(stmts) => !stmts.is_empty() && stmts.iter().all(is_read_stmt),
            None => is_read_like_fallback(&self.sql),
        }
    }

    /// Async equivalent of [`Self::is_read_sql`] — never blocks the calling
    /// task's worker thread. Prefer this on any async request path.
    pub async fn is_read_sql_async(&self) -> bool {
        self.ensure_parsed().await;
        self.is_read_sql()
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
///
/// `Expression::Command` is the parser's catch-all for statements it leaves
/// opaque (`CALL ...`, procedural `BEGIN ...` blocks, `LOCK TABLES`, ...) and an
/// opaque statement can carry writes — on Trino, `CALL` runs connector procedures
/// like `system.rollback_to_snapshot` that mutate data. read_only must fail
/// closed, so a `Command` is not classified as a read (#403). Statements the
/// parser models precisely keep their classification — `EXPLAIN` parses as
/// `Describe` — and explicit transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`)
/// parses into its own precisely-modeled variants, none of which are reads.
///
/// A read-shaped statement can still hide a write in its `WITH` clause:
/// PostgreSQL executes data-modifying CTEs even when the top-level statement
/// is a SELECT (`WITH del AS (DELETE FROM orders RETURNING *) SELECT * FROM
/// del`), so every CTE body (and nested set-operation arm) must classify as a
/// read — or as an inert expression that cannot carry a write (see
/// [`with_is_read`]) — before the statement counts as one.
pub fn is_read_stmt(expr: &Expression) -> bool {
    match expr {
        Expression::Select(select) => with_is_read(select.with.as_ref()),
        Expression::Union(u) => {
            with_is_read(u.with.as_ref()) && is_read_stmt(&u.left) && is_read_stmt(&u.right)
        }
        Expression::Intersect(i) => {
            with_is_read(i.with.as_ref()) && is_read_stmt(&i.left) && is_read_stmt(&i.right)
        }
        Expression::Except(e) => {
            with_is_read(e.with.as_ref()) && is_read_stmt(&e.left) && is_read_stmt(&e.right)
        }
        Expression::Subquery(inner) => is_read_stmt(&inner.this),
        Expression::Describe(_) | Expression::Show(_) => true,
        _ => false,
    }
}

/// Whether executing this statement produces a result set — the routing
/// question behind [`SqlParseCache::is_read_like`] ("`execute()` vs
/// `execute_update()`"), deliberately not the read_only question
/// ([`is_read_stmt`]). A `Select` always streams rows even when its `WITH`
/// clause carries data-modifying CTEs — PostgreSQL runs the writes and still
/// returns the `SELECT`'s rows, so routing one through `execute_update()`
/// silently discards the results. CTE contents therefore don't matter here.
/// Opaque `Command` statements keep the #97/#403 behavior: no modeled result
/// set, route through `execute_update()` like DDL/DML.
pub fn is_result_set_stmt(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Values(_)
            | Expression::Subquery(_)
            | Expression::Describe(_)
            | Expression::Show(_)
    )
}

/// All CTE bodies must classify as reads — or as inert expressions that cannot
/// carry a statement of their own — for the surrounding statement to be one. A
/// CTE body can itself be a read-shaped statement with its own `WITH` clause,
/// so the classification recurses through [`is_read_stmt`]. Bare expressions
/// like ClickHouse's expression-first items (`WITH 1 AS x`, parsed as a
/// literal) and row constructors (`WITH t AS (VALUES (1))`) cannot hide a
/// write; treating them as non-reads denied legitimate reads.
fn with_is_read(with: Option<&With>) -> bool {
    with.map(|w| {
        w.ctes
            .iter()
            .all(|cte| is_read_stmt(&cte.this) || is_inert_cte_body(&cte.this))
    })
    .unwrap_or(true)
}

/// CTE bodies the parser models as bare expressions — a row constructor or a
/// scalar literal — carry no statement and cannot mutate. Anything else stays
/// with [`is_read_stmt`]'s fail-closed classification, so compound scalar forms
/// (`WITH a + 1 AS x`) keep being denied until their shape is modeled.
fn is_inert_cte_body(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Values(_)
            | Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Null(_)
    )
}

/// Classify SQL without retaining a parse cache. Prefer [`SqlParseCache`] on hot paths.
pub fn is_read_like_sql(sql: &str, dialect: &SqlDialect) -> bool {
    SqlParseCache::new(sql, dialect.clone()).is_read_like()
}

/// Safety-aware counterpart of [`is_read_like_sql`] for read→write invariant
/// checks — every statement must classify as a read under [`is_read_stmt`]
/// (modifying CTE bodies are writes, opaque `Command`s fail closed). Routing
/// keeps [`is_read_like_sql`]; this answers "did this text stop being safe to
/// treat as a read", where a `SELECT` wrapping a `DELETE ... RETURNING` CTE
/// counts as stopped even though both sides stream rows.
pub fn is_read_sql(sql: &str, dialect: &SqlDialect) -> bool {
    SqlParseCache::new(sql, dialect.clone()).is_read_sql()
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

    /// The read→write invariant classifiers must disagree exactly where a
    /// `SELECT` wraps a data-modifying CTE: routing (`is_read_like`) streams
    /// rows either way, safety (`is_read_sql`) denies the CTE carrier.
    #[test]
    fn is_read_sql_rejects_modifying_cte_carriers() {
        let plain = SqlParseCache::new("SELECT 1", SqlDialect::Postgres);
        assert!(plain.is_read_like());
        assert!(plain.is_read_sql());

        let carrier = SqlParseCache::new(
            "WITH del AS (DELETE FROM orders RETURNING *) SELECT 1",
            SqlDialect::Postgres,
        );
        assert!(carrier.is_read_like(), "routing still streams rows");
        assert!(!carrier.is_read_sql(), "safety sees the hidden write");

        let inert = SqlParseCache::new(
            "WITH t AS (VALUES (1)) SELECT * FROM t",
            SqlDialect::Postgres,
        );
        assert!(inert.is_read_sql(), "inert CTE bodies stay reads");

        // Unparseable SQL keeps the same string fallback as is_read_like, so
        // the invariants' parse-failure behavior is unchanged.
        let junk = SqlParseCache::new("SELECT {invalid", SqlDialect::Postgres);
        assert_eq!(junk.is_read_like(), junk.is_read_sql());
        assert_eq!(
            junk.is_read_sql(),
            is_read_like_fallback("SELECT {invalid"),
        );
        assert!(is_read_sql("WITH del AS (DELETE FROM orders RETURNING *) SELECT 1", &SqlDialect::Postgres) == false);
        assert!(is_read_sql("SELECT 1", &SqlDialect::Postgres));
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

    /// A data-modifying CTE under a read-shaped top level still executes its
    /// write (PostgreSQL runs `WITH del AS (DELETE ... RETURNING *) SELECT ...`
    /// as a write), so the read_only guard must look into `WITH` bodies — and
    /// the arms of set operations and subqueries — before calling a statement a
    /// read. Routing asks a different question: a `SELECT`-topped statement
    /// streams rows either way, so it must keep the `execute()` path or the
    /// results are silently discarded.
    #[test]
    fn data_modifying_ctes_are_not_reads() {
        for sql in [
            "WITH del AS (DELETE FROM orders RETURNING *) SELECT * FROM del",
            "WITH up AS (UPDATE orders SET total = 0 RETURNING *) SELECT * FROM up",
            "WITH ins AS (INSERT INTO audit VALUES (1) RETURNING *) SELECT * FROM ins",
            "WITH outer_cte AS (WITH inner_cte AS (DELETE FROM orders RETURNING *) SELECT * FROM inner_cte) SELECT * FROM outer_cte",
            "WITH del AS (DELETE FROM orders RETURNING *) SELECT 1 UNION SELECT 2",
        ] {
            let cache = SqlParseCache::new(sql, SqlDialect::Generic);
            let stmts = cache
                .statements()
                .unwrap_or_else(|| panic!("expected polyglot_sql to parse {sql:?}"));
            assert!(
                stmts.iter().all(|s| !is_read_stmt(s)),
                "read_only guard must deny write-carrying CTEs for: {sql}"
            );
            assert!(
                cache.is_read_like(),
                "SELECT-topped statement must keep the result-set path for: {sql}"
            );
        }
        // Plain reads through a CTE must stay reads.
        assert!(is_read_like_sql(
            "WITH recent AS (SELECT * FROM orders) SELECT * FROM recent",
            &SqlDialect::Generic
        ));
    }

    /// CTE bodies the parser models as bare expressions — row constructors and
    /// scalar literals (ClickHouse's expression-first `WITH 1 AS x`) — cannot
    /// hide a write; denying them locked legitimate reads out of read_only.
    #[test]
    fn inert_cte_bodies_stay_reads() {
        for (sql, dialect) in [
            (
                "WITH t(n) AS (VALUES (1)) SELECT * FROM t",
                SqlDialect::Generic,
            ),
            (
                "WITH recent AS (VALUES (1), (2)) SELECT * FROM recent",
                SqlDialect::Generic,
            ),
            ("WITH 1 AS x SELECT 1", SqlDialect::ClickHouse),
        ] {
            let cache = SqlParseCache::new(sql, dialect.clone());
            let stmts = cache
                .statements()
                .unwrap_or_else(|| panic!("expected polyglot_sql to parse {sql:?}"));
            assert!(
                stmts.iter().all(is_read_stmt),
                "guard must allow inert CTE body for: {sql}"
            );
            assert!(cache.is_read_like(), "expected result-set path for: {sql}");
        }
        // Compound scalar expressions have no modeled statement inside — the
        // fail-closed side keeps denying them until their shape is modeled.
        let strict = SqlParseCache::new("WITH a + 1 AS x SELECT 1", SqlDialect::ClickHouse);
        let stmts = strict
            .statements()
            .unwrap_or_else(|| panic!("expected polyglot_sql to parse WITH a + 1 AS x SELECT 1"));
        assert!(
            stmts.iter().all(|s| !is_read_stmt(s)),
            "compound scalar CTE body stays fail-closed"
        );
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

    /// `Command` is the parser's opaque catch-all (`CALL`, bare `END`, procedural
    /// `BEGIN` blocks, ...) and opaque statements can carry writes — read_only
    /// must fail closed (#403), so it is not a read. It still produces no result
    /// set for ADBC routing (#97), so it is not read-like either.
    #[test]
    fn command_statements_are_not_reads() {
        let sql = "CALL pragma_version()";
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
            !is_read_stmt(&stmts[0]),
            "is_read_stmt must fail closed for opaque Command statements: {sql}"
        );
        assert!(
            !cache.is_read_like(),
            "is_read_like must route Command through execute_update, not execute: {sql}"
        );
        let bare_end = SqlParseCache::new("END", SqlDialect::Generic);
        assert!(!bare_end.is_read_like(), "END is not read-like");
    }

    /// `BEGIN`/`COMMIT`/`ROLLBACK` parse into their own precisely-modeled
    /// variants (`Transaction`, `Commit`, ...), not the opaque `Command`
    /// catch-all — they were not reads before the #403 fail-closed change and
    /// must stay so; this pins the AST shape the Command reclassification
    /// relies on without coupling to one parser's exact variant names.
    #[test]
    fn transaction_statements_remain_non_reads() {
        for sql in ["BEGIN", "COMMIT", "ROLLBACK"] {
            let cache = SqlParseCache::new(sql, SqlDialect::Generic);
            let stmts = cache
                .statements()
                .unwrap_or_else(|| panic!("expected polyglot_sql to parse {sql:?}"));
            assert_eq!(stmts.len(), 1, "{sql}");
            assert!(
                !matches!(stmts[0], Expression::Command(_)),
                "{sql:?} must not parse as the opaque Command catch-all, got: {:?}",
                stmts[0]
            );
            assert!(!is_read_stmt(&stmts[0]), "{sql} is not a read");
        }
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
