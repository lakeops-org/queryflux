use async_trait::async_trait;
use polyglot_sql::{
    expressions::{Expression, Literal},
    DialectType,
};
use queryflux_core::query::EngineType;
use queryflux_core::sql_classify::{is_read_like_fallback, is_read_stmt, to_polyglot_dialect};

use crate::context::{GuardContext, GuardLayer, GuardResult};

fn engine_dialect(engine: &EngineType) -> DialectType {
    to_polyglot_dialect(&engine.dialect())
}

/// Async so parsing (and any wait for a `polyglot_pool` worker) never blocks the
/// calling task's Tokio worker thread — `check` is on the live request path for
/// every guarded query, not just an occasional background call.
async fn guard_statements<'a>(
    ctx: &'a GuardContext<'a>,
) -> Result<std::borrow::Cow<'a, [Expression]>, ()> {
    if let Some(cache) = ctx.sql_parse {
        cache.ensure_parsed().await;
        return cache.statements().map(std::borrow::Cow::Borrowed).ok_or(());
    }

    let sql = ctx.translated_sql.to_string();
    let dialect = engine_dialect(ctx.engine_type);
    tokio::task::spawn_blocking(move || {
        queryflux_core::polyglot_pool::run(move || polyglot_sql::parse(&sql, dialect))
    })
    .await
    .ok()
    .flatten()
    .and_then(|r| r.ok())
    .map(std::borrow::Cow::Owned)
    .ok_or(())
}

/// The extension trait every guard implements.
#[async_trait]
pub trait Guard: Send + Sync {
    fn name(&self) -> &'static str;
    fn layer(&self) -> GuardLayer;
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult;
}

// ---------------------------------------------------------------------------
// read_only — block any query that is not a SELECT / SHOW / DESCRIBE / EXPLAIN
// ---------------------------------------------------------------------------

pub struct ReadOnlyGuard;

#[async_trait]
impl Guard for ReadOnlyGuard {
    fn name(&self) -> &'static str {
        "read_only"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        match guard_statements(ctx).await {
            Ok(stmts) => {
                for stmt in stmts.iter() {
                    if !is_read_stmt(stmt) {
                        return GuardResult::deny(
                            "write operations are not permitted",
                            "READ_ONLY_VIOLATION",
                        );
                    }
                }
                GuardResult::allow()
            }
            Err(_) => {
                if is_read_like_fallback(ctx.translated_sql) {
                    GuardResult::allow()
                } else {
                    GuardResult::deny("write operations are not permitted", "READ_ONLY_VIOLATION")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// row_limit — require LIMIT clause; optionally enforce a maximum
// ---------------------------------------------------------------------------

pub struct RowLimitGuard {
    pub max_rows: Option<u64>,
}

#[async_trait]
impl Guard for RowLimitGuard {
    fn name(&self) -> &'static str {
        "row_limit"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        match guard_statements(ctx).await {
            Ok(stmts) => {
                for stmt in stmts.iter() {
                    if !is_read_stmt(stmt) {
                        continue;
                    }
                    match top_level_limit(stmt) {
                        None => {
                            return GuardResult::warn(
                                "query has no LIMIT clause; result set may be large",
                            );
                        }
                        Some(limit_val) => {
                            if let Some(max) = self.max_rows {
                                if limit_val > max {
                                    return GuardResult::deny(
                                        format!("LIMIT {limit_val} exceeds maximum allowed {max}"),
                                        "ROW_LIMIT_EXCEEDED",
                                    );
                                }
                            }
                        }
                    }
                }
                GuardResult::allow()
            }
            Err(_) => {
                // Fall back to string heuristic.
                let upper = ctx.translated_sql.to_uppercase();
                if !upper.contains(" LIMIT ") {
                    return GuardResult::warn("query has no LIMIT clause; result set may be large");
                }
                if let Some(max) = self.max_rows {
                    if let Some(limit_val) = extract_limit_value_str(&upper) {
                        if limit_val > max {
                            return GuardResult::deny(
                                format!("LIMIT {limit_val} exceeds maximum allowed {max}"),
                                "ROW_LIMIT_EXCEEDED",
                            );
                        }
                    }
                }
                GuardResult::allow()
            }
        }
    }
}

/// Extract the LIMIT value from the outermost query node only, ignoring subqueries.
fn top_level_limit(expr: &Expression) -> Option<u64> {
    match expr {
        Expression::Select(s) => s.limit.as_ref().and_then(|l| literal_to_u64(&l.this)),
        Expression::Union(u) => u.limit.as_deref().and_then(literal_to_u64),
        Expression::Intersect(i) => i.limit.as_deref().and_then(literal_to_u64),
        Expression::Except(e) => e.limit.as_deref().and_then(literal_to_u64),
        _ => None,
    }
}

fn literal_to_u64(expr: &Expression) -> Option<u64> {
    if let Expression::Literal(lit) = expr {
        if let Literal::Number(s) = lit.as_ref() {
            return s.parse::<u64>().ok();
        }
    }
    None
}

fn extract_limit_value_str(upper_sql: &str) -> Option<u64> {
    let pos = upper_sql.find(" LIMIT ")?;
    let after = upper_sql[pos + 7..].trim_start();
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse::<u64>().ok()
}

// ---------------------------------------------------------------------------
// require_predicate — reject queries with no WHERE clause on fact tables
// ---------------------------------------------------------------------------

pub struct RequirePredicateGuard {
    /// Glob-style patterns matched against FROM table names. Empty = all tables.
    pub applies_to: Vec<String>,
}

#[async_trait]
impl Guard for RequirePredicateGuard {
    fn name(&self) -> &'static str {
        "require_predicate"
    }
    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }
    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        match guard_statements(ctx).await {
            Ok(stmts) => {
                for stmt in stmts.iter() {
                    if select_lacks_where(stmt, &self.applies_to) {
                        return GuardResult::deny(
                            "query must include a WHERE predicate",
                            "MISSING_PREDICATE",
                        );
                    }
                }
                GuardResult::allow()
            }
            Err(_) => {
                // Fall back to string heuristic.
                let upper = ctx.translated_sql.to_uppercase();
                if !upper.trim_start().starts_with("SELECT") {
                    return GuardResult::allow();
                }
                if (self.applies_to.is_empty()
                    || table_name_matches_any_str(&upper, &self.applies_to))
                    && !upper.contains(" WHERE ")
                {
                    return GuardResult::deny(
                        "query must include a WHERE predicate",
                        "MISSING_PREDICATE",
                    );
                }
                GuardResult::allow()
            }
        }
    }
}

/// Returns true when a SELECT (or any UNION/INTERSECT/EXCEPT arm) references a
/// matching table without a WHERE clause.
fn select_lacks_where(expr: &Expression, patterns: &[String]) -> bool {
    match expr {
        Expression::Select(s) => {
            if s.where_clause.is_some() {
                return false;
            }
            if patterns.is_empty() {
                return true;
            }
            s.from.as_ref().is_some_and(|from| {
                from.expressions
                    .iter()
                    .any(|e| from_expr_matches(e, patterns))
            })
        }
        Expression::Union(u) => {
            select_lacks_where(&u.left, patterns) || select_lacks_where(&u.right, patterns)
        }
        Expression::Intersect(i) => {
            select_lacks_where(&i.left, patterns) || select_lacks_where(&i.right, patterns)
        }
        Expression::Except(e) => {
            select_lacks_where(&e.left, patterns) || select_lacks_where(&e.right, patterns)
        }
        _ => false,
    }
}

/// Check whether a FROM-clause expression references a table matching any pattern.
fn from_expr_matches(expr: &Expression, patterns: &[String]) -> bool {
    match expr {
        Expression::Table(t) => {
            let full_name = match &t.schema {
                Some(s) => format!("{}.{}", s.name, t.name.name),
                None => t.name.name.clone(),
            };
            patterns
                .iter()
                .any(|p| simple_glob_match(&full_name.to_uppercase(), &p.to_uppercase()))
        }
        Expression::Alias(a) => from_expr_matches(&a.this, patterns),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn table_name_matches_any_str(upper_sql: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|p| simple_glob_match(upper_sql, &p.to_uppercase()))
}

/// Simple glob: `*` matches any sequence of characters.
fn simple_glob_match(haystack: &str, pattern: &str) -> bool {
    if !pattern.contains('*') {
        return haystack.contains(pattern);
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !haystack.starts_with(*part) {
                return false;
            }
            pos = part.len();
        } else {
            match haystack[pos..].find(*part) {
                Some(found) => pos += found + part.len(),
                None => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::{
        query::{ClusterGroupName, EngineType},
        tags::QueryTags,
    };

    struct TestCtx {
        sql: String,
        translated_sql: String,
        engine_type: EngineType,
        cluster_group: ClusterGroupName,
        query_tags: QueryTags,
    }

    impl TestCtx {
        fn new(sql: &str, translated: &str) -> Self {
            Self {
                sql: sql.to_string(),
                translated_sql: translated.to_string(),
                engine_type: EngineType::DuckDb,
                cluster_group: ClusterGroupName("default".to_string()),
                query_tags: QueryTags::new(),
            }
        }

        fn with_engine(mut self, engine: EngineType) -> Self {
            self.engine_type = engine;
            self
        }

        fn ctx(&self) -> GuardContext<'_> {
            GuardContext {
                sql: &self.sql,
                translated_sql: &self.translated_sql,
                engine_type: &self.engine_type,
                cluster_group: &self.cluster_group,
                user: None,
                agent_context: None,
                query_tags: &self.query_tags,
                sql_parse: None,
            }
        }
    }

    #[tokio::test]
    async fn read_only_allows_select() {
        let tc = TestCtx::new("SELECT 1", "SELECT 1");
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_cte() {
        let sql = "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_insert() {
        let sql = "INSERT INTO t VALUES (1)";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_delete() {
        let sql = "DELETE FROM t WHERE id = 1";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_update() {
        let sql = "UPDATE t SET x = 1 WHERE id = 2";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_show() {
        let sql = "SHOW TABLES";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_explain() {
        let sql = "EXPLAIN SELECT 1";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_describe() {
        let sql = "DESCRIBE orders";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_union_all() {
        let sql = "SELECT 1 AS n UNION ALL SELECT 2 AS n";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_truncate() {
        let sql = "TRUNCATE TABLE t";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_second_statement_insert() {
        let sql = "SELECT 1; INSERT INTO t VALUES (42)";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_multiple_read_statements_semicolon() {
        let sql = "SELECT 1 AS n; SELECT 2 AS n; WITH c AS (SELECT 3 AS n) SELECT n FROM c";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_when_batch_mixes_update_and_select() {
        let sql = "UPDATE t SET x = 1 WHERE id = 0; SELECT 1";
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    /// Multiple named CTEs, join, filtering inside CTEs — still read-only.
    #[tokio::test]
    async fn read_only_nested_ctes_with_join_allow() {
        let sql = r#"
WITH
  regional_sales AS (
    SELECT region, SUM(amount) AS total
    FROM sales
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY region
  ),
  top_regions AS (
    SELECT region
    FROM regional_sales
    WHERE total > 10000
  )
SELECT r.region, r.total
FROM regional_sales AS r
INNER JOIN top_regions AS t ON t.region = r.region
"#;
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_cte_then_union_all_allow() {
        let sql = r#"
WITH base AS (SELECT 1 AS n UNION ALL SELECT 2 AS n)
SELECT n FROM base
UNION ALL
SELECT 3 AS n
"#;
        let tc = TestCtx::new(sql, sql);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_allows_leading_whitespace_select() {
        let tc = TestCtx::new("", "\n\t  SELECT 1");
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    /// Parser-unfriendly SQL still goes through prefix heuristic (`SELECT`).
    #[tokio::test]
    async fn read_only_heuristic_allows_odd_select_when_parse_fails() {
        let sql = "SELECT {invalid but starts with SELECT";
        let tc = TestCtx::new(sql, sql).with_engine(EngineType::Adbc);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn read_only_blocks_merge_snowflake() {
        let sql =
            "MERGE INTO tgt USING src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET x = src.x";
        let tc = TestCtx::new(sql, sql).with_engine(EngineType::Snowflake);
        let r = ReadOnlyGuard.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_warns_on_missing_limit() {
        let tc = TestCtx::new("SELECT * FROM t", "SELECT * FROM t");
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_allows_within_max() {
        let sql = "SELECT * FROM t LIMIT 100";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_denies_over_max() {
        let sql = "SELECT * FROM t LIMIT 5000";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_not_fooled_by_subquery_limit() {
        // Outer LIMIT 10 is within max; inner LIMIT 9999 should not trigger denial.
        let sql = "SELECT * FROM (SELECT * FROM t LIMIT 9999) sub LIMIT 10";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_warns_when_max_rows_disabled_and_no_limit() {
        let tc = TestCtx::new("SELECT * FROM t", "SELECT * FROM t");
        let g = RowLimitGuard { max_rows: None };
        let r = g.check(&tc.ctx()).await;
        assert!(matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_allows_exactly_at_max() {
        let sql = "SELECT * FROM t LIMIT 1000";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_union_all_with_trailing_limit() {
        let sql = "SELECT * FROM a UNION ALL SELECT * FROM b LIMIT 100";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(500),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_skips_check_on_write_statement() {
        // INSERT has no meaningful LIMIT rule; guard ignores non-read statements.
        let sql = "INSERT INTO t SELECT * FROM u";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(100),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
        assert!(!matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_first_select_wins_in_batch() {
        let sql = "SELECT * FROM a; SELECT * FROM b LIMIT 10";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_multi_statement_each_select_has_limit() {
        let sql = "SELECT * FROM a LIMIT 5; SELECT * FROM b LIMIT 5";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(100),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
        assert!(!matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_multi_statement_second_select_lacks_limit_warns() {
        let sql = "SELECT * FROM t LIMIT 10; SELECT * FROM u";
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(1000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(matches!(r, GuardResult::Warn { .. }));
    }

    /// Outer LIMIT applies to the full CTE + join tree.
    #[tokio::test]
    async fn row_limit_nested_ctes_outer_limit_satisfies_guard() {
        let sql = r#"
WITH
  a AS (SELECT * FROM t1),
  b AS (SELECT * FROM t2 WHERE id > 0)
SELECT *
FROM a
INNER JOIN b ON a.id = b.id
LIMIT 50
"#;
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(100),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_nested_ctes_no_outer_limit_warns() {
        let sql = r#"
WITH
  x AS (SELECT * FROM foo),
  y AS (SELECT * FROM bar LIMIT 9999)
SELECT * FROM x CROSS JOIN y
"#;
        let tc = TestCtx::new(sql, sql);
        let g = RowLimitGuard {
            max_rows: Some(10000),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(matches!(r, GuardResult::Warn { .. }));
    }

    #[tokio::test]
    async fn row_limit_string_fallback_finds_limit() {
        let sql = "SELECT * FROM t LIMIT 77";
        let tc = TestCtx::new(sql, sql).with_engine(EngineType::Adbc);
        let g = RowLimitGuard {
            max_rows: Some(100),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn row_limit_string_fallback_denies_over_cap() {
        let sql = "SELECT * FROM t LIMIT 500";
        let tc = TestCtx::new(sql, sql).with_engine(EngineType::Adbc);
        let g = RowLimitGuard {
            max_rows: Some(100),
        };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_blocks_full_scan() {
        let tc = TestCtx::new("SELECT * FROM orders", "SELECT * FROM orders");
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_allows_with_where() {
        let sql = "SELECT * FROM orders WHERE id = 1";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_skips_non_matching_table() {
        // "users" does not match the fct_* pattern, so no WHERE is required.
        let tc = TestCtx::new("SELECT * FROM users", "SELECT * FROM users");
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_matches_glob() {
        // "fct_events" matches fct_* and has no WHERE → must be denied.
        let tc = TestCtx::new("SELECT * FROM fct_events", "SELECT * FROM fct_events");
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_union_denies_if_one_arm_lacks_where() {
        let sql = "SELECT * FROM fct_a WHERE id = 1 UNION ALL SELECT * FROM fct_b";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_union_allows_both_predicate() {
        let sql = "SELECT * FROM fct_a WHERE TRUE UNION ALL SELECT * FROM fct_b WHERE TRUE";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_schema_qualified_matches_dw_prefix_glob() {
        let sql = "SELECT * FROM dw.fct_events";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["dw.fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_schema_qualified_allows_with_where() {
        let sql = "SELECT * FROM dw.fct_events WHERE ds = '2024-01-01'";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["dw.fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_allows_insert_even_without_where() {
        let sql = "INSERT INTO fct_events SELECT * FROM staging";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_heuristic_non_select_allows() {
        let sql = "DELETE FROM fct_events WHERE id = 1";
        let tc = TestCtx::new(sql, sql).with_engine(EngineType::Adbc);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    /// `applies_to` empty: every bare SELECT must have a WHERE; CTE body does not satisfy the outer.
    #[tokio::test]
    async fn require_predicate_cte_outer_select_still_needs_where() {
        let sql = r#"
WITH prep AS (SELECT * FROM orders WHERE status = 'open')
SELECT * FROM prep
"#;
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_cte_outer_where_allows() {
        let sql = r#"
WITH prep AS (SELECT * FROM orders WHERE status = 'open')
SELECT * FROM prep WHERE id = 1
"#;
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    /// Outer FROM only references CTE aliases; fact table appears inside CTE — shallow FROM check does not flag it.
    #[tokio::test]
    async fn require_predicate_fact_inside_cte_not_in_outer_from_with_glob() {
        let sql = r#"
WITH filtered AS (SELECT * FROM fct_orders WHERE ds = '2024-01-01')
SELECT order_id, amount FROM filtered
"#;
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard {
            applies_to: vec!["fct_*".to_string()],
        };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_multi_statement_second_select_lacks_where() {
        let sql = "SELECT * FROM orders WHERE id = 1; SELECT * FROM orders";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(r.is_deny());
    }

    #[tokio::test]
    async fn require_predicate_multi_statement_both_with_where_allow() {
        let sql = "SELECT * FROM a WHERE x = 1; SELECT * FROM b WHERE y = 2";
        let tc = TestCtx::new(sql, sql);
        let g = RequirePredicateGuard { applies_to: vec![] };
        let r = g.check(&tc.ctx()).await;
        assert!(!r.is_deny());
    }
}
