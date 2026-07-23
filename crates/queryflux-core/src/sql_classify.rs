//! SQL parsing and read/write classification shared across dispatch, guardrails,
//! and engine adapters. Parse results are cached per [`SqlParseCache`] instance so
//! multiple consumers on the same query only hit `polyglot_sql` once.

use std::sync::OnceLock;

use polyglot_sql::expressions::Expression;
use polyglot_sql::DialectType;

use crate::query::SqlDialect;

#[derive(Debug)]
enum ParsedStatements {
    Ok(Vec<Expression>),
    Err,
}

/// Lazily parses SQL once and exposes shared classification helpers.
#[derive(Debug)]
pub struct SqlParseCache {
    sql: String,
    dialect: SqlDialect,
    cache: OnceLock<ParsedStatements>,
}

impl SqlParseCache {
    pub fn new(sql: impl Into<String>, dialect: SqlDialect) -> Self {
        Self {
            sql: sql.into(),
            dialect,
            cache: OnceLock::new(),
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
            // polyglot-sql can overflow the default Tokio (~2 MiB) stack; always
            // parse on the dedicated large-stack pool (see polyglot_pool).
            let sql = self.sql.clone();
            let dialect = to_polyglot_dialect(&self.dialect);
            crate::polyglot_pool::run(move || {
                polyglot_sql::parse(&sql, dialect)
                    .map(ParsedStatements::Ok)
                    .unwrap_or(ParsedStatements::Err)
            })
            .unwrap_or(ParsedStatements::Err)
        })
    }

    /// Parsed statements when `polyglot_sql` succeeds.
    pub fn statements(&self) -> Option<&[Expression]> {
        match self.parsed() {
            ParsedStatements::Ok(stmts) => Some(stmts.as_slice()),
            ParsedStatements::Err => None,
        }
    }

    /// Whether the statement should use a result-set execution path (`execute`) vs
    /// an update path (`execute_update`).
    pub fn is_read_like(&self) -> bool {
        match self.statements() {
            Some(stmts) => !stmts.is_empty() && stmts.iter().all(is_read_stmt),
            None => is_read_like_fallback(&self.sql),
        }
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
    use crate::query::SqlDialect;

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
