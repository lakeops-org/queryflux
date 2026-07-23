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
            polyglot_sql::parse(&self.sql, to_polyglot_dialect(&self.dialect))
                .map(ParsedStatements::Ok)
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
    fn select_is_read() {
        assert!(is_read_like_sql("SELECT 1", &SqlDialect::Generic));
    }

    #[test]
    fn create_is_not_read() {
        assert!(!is_read_like_sql(
            "CREATE TABLE t (id INT)",
            &SqlDialect::Generic
        ));
    }

    #[test]
    fn ast_classification_handles_comments() {
        assert!(is_read_like_sql(
            "-- comment\n/* block */ SELECT 1",
            &SqlDialect::Generic,
        ));
    }
}
