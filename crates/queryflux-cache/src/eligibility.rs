//! Cache eligibility is stricter than fingerprint determinism or execution hints.
use polyglot_sql::{expressions::Expression, traversal::DfsIter};

/// Only positively classified read-only, deterministic SQL may be replayed.
/// Parse failures and unsupported syntax bypass caching, not normal execution.
/// Callers must apply this before both lookup and writer creation, even for hints.
pub fn is_cacheable(sql: &str, dialect: &str) -> bool {
    let owned_sql = sql.to_owned();
    let owned_dialect = dialect.to_owned();
    let read_only = queryflux_core::polyglot_pool::run(move || {
        polyglot_sql::parse_by_name(&owned_sql, &owned_dialect)
            .map(|statements| {
                !statements.is_empty() && statements.iter().all(|s| is_query(s) && safe_tree(s))
            })
            .unwrap_or(false)
    })
    .unwrap_or(false);
    read_only && crate::is_deterministic(sql, dialect)
}

/// Recognize supported query roots; nested read-only safety is checked separately.
fn is_query(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Subquery(_)
            | Expression::Values(_)
    )
}

/// Reject side effects, nondeterministic clauses, and unsupported nodes anywhere in the AST.
fn safe_tree(expr: &Expression) -> bool {
    // DfsIter visits CTE bodies and set operands, so checking only the root is
    // insufficient. Audited against polyglot-sql 0.9.2: DfsIter::next delegates
    // to ast_children::for_each_child and the generated AstNode visitors, which
    // include LIKE/ILIKE escape, CAST format/default, and aggregate filters.
    // The explicit recursive checks below are retained as defensive rechecks,
    // not workarounds for traversal gaps in 0.9.2. The allowlist and clause
    // restrictions conservatively keep unsupported syntax from being cached.
    DfsIter::new(expr).all(|node| match node {
        Expression::Select(s) => {
            s.into.is_none()
                && s.locks.is_empty()
                && s.sample.is_none()
                && s.settings.is_none()
                && s.format.is_none()
                && s.windows.is_none()
                && s.hint.is_none()
                && s.for_json.is_empty()
                && s.option.is_none()
                && s.operation_modifiers.is_empty()
                && s.distinct_on.iter().flatten().all(safe_tree)
                && s.exclude.iter().flatten().all(safe_tree)
        }
        Expression::Union(u) => is_query(&u.left) && is_query(&u.right),
        Expression::Intersect(u) => is_query(&u.left) && is_query(&u.right),
        Expression::Except(u) => is_query(&u.left) && is_query(&u.right),
        Expression::Subquery(s) => {
            is_query(&s.this)
                && s.order_by.is_none()
                && s.limit.is_none()
                && s.offset.is_none()
                && s.distribute_by.is_none()
                && s.sort_by.is_none()
                && s.cluster_by.is_none()
        }
        Expression::Values(v) => v
            .expressions
            .iter()
            .all(|row| row.expressions.iter().all(safe_tree)),
        Expression::Table(t) => {
            t.when.is_none()
                && t.table_sample.is_none()
                && t.hints.is_empty()
                && t.system_time.is_none()
                && t.identifier_func.is_none()
                && t.changes.is_none()
                && t.version.is_none()
        }
        Expression::Star(s) => s.replace.iter().flatten().all(|a| safe_tree(&a.this)),
        Expression::Cast(c) => {
            c.format.iter().all(|e| safe_tree(e)) && c.default.iter().all(|e| safe_tree(e))
        }
        // Defensively recheck ESCAPE, which DfsIter also visits in 0.9.2.
        Expression::Like(op) | Expression::ILike(op) => op.escape.iter().all(safe_tree),
        Expression::Count(c) => c.this.iter().chain(c.filter.iter()).all(safe_tree),
        Expression::Sum(a) | Expression::Avg(a) | Expression::Min(a) | Expression::Max(a) => {
            safe_tree(&a.this)
                && a.filter.iter().all(safe_tree)
                && a.order_by.iter().all(|o| safe_tree(&o.this))
                && a.having_max.iter().all(|(e, _)| safe_tree(e))
                && a.limit.iter().all(|e| safe_tree(e))
        }
        Expression::Case(c) => {
            c.operand.iter().chain(c.else_.iter()).all(safe_tree)
                && c.whens
                    .iter()
                    .all(|(when, then)| safe_tree(when) && safe_tree(then))
        }
        Expression::Annotated(a) => safe_tree(&a.this),
        // Leaves and containers whose expression children DfsIter visits fully.
        Expression::Literal(_)
        | Expression::Boolean(_)
        | Expression::Null(_)
        | Expression::Identifier(_)
        | Expression::Column(_)
        | Expression::Parameter(_)
        | Expression::Placeholder(_)
        | Expression::Alias(_)
        | Expression::Paren(_)
        | Expression::Not(_)
        | Expression::Neg(_)
        | Expression::BitwiseNot(_)
        | Expression::IsNull(_)
        | Expression::Exists(_)
        | Expression::And(_)
        | Expression::Or(_)
        | Expression::Add(_)
        | Expression::Sub(_)
        | Expression::Mul(_)
        | Expression::Div(_)
        | Expression::Mod(_)
        | Expression::Eq(_)
        | Expression::Neq(_)
        | Expression::Lt(_)
        | Expression::Lte(_)
        | Expression::Gt(_)
        | Expression::Gte(_)
        | Expression::BitwiseAnd(_)
        | Expression::BitwiseOr(_)
        | Expression::BitwiseXor(_)
        | Expression::Concat(_)
        | Expression::Between(_)
        | Expression::In(_)
        | Expression::Array(_)
        | Expression::Tuple(_)
        | Expression::Coalesce(_)
        | Expression::Greatest(_)
        | Expression::Least(_) => true,
        // Includes DML, DDL, commands, raw SQL, locks, and unknown functions
        // (which can have side effects). RETURNING does not make a write safe.
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keep supported reads cacheable across comments, CTEs, set operations, and batches.
    #[test]
    fn deterministic_reads() {
        for sql in [
            "SELECT 1",
            "-- a comment\nSELECT * FROM t WHERE id = 1",
            "/* queryflux:cache */ SELECT COUNT(*) FROM t",
            "SELECT SUM(id), MIN(id), MAX(id), AVG(id) FROM t",
            "WITH r AS (SELECT 1 AS id) SELECT * FROM r",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 INTERSECT SELECT 1",
            "SELECT 1 EXCEPT SELECT 2",
            "SELECT * FROM (SELECT 1) r",
            "VALUES (1), (2)",
            "SELECT 1; SELECT 2",
            "SELECT CASE WHEN 1 = 1 THEN 2 ELSE 3 END",
        ] {
            assert!(is_cacheable(sql, "postgresql"), "{sql}");
        }
    }

    /// Fail closed for writes, nested mutations, opaque commands, and invalid SQL.
    #[test]
    fn writes_and_ambiguous_sql_are_uncacheable() {
        for sql in [
            "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (1) RETURNING id",
            "/* queryflux:cache:ttl=300 */ INSERT INTO t VALUES (1) RETURNING id",
            "-- SELECT 1\nUPDATE t SET id = 2 RETURNING id", "DELETE FROM t RETURNING id",
            "CREATE TABLE t (id INTEGER)", "DROP TABLE t", "TRUNCATE t",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "SELECT 1; INSERT INTO t VALUES (1) RETURNING id", "INSERT INTO t VALUES (1); SELECT 1",
            "WITH changed AS (DELETE FROM t RETURNING id) SELECT * FROM changed",
            "WITH changed AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM changed",
            "WITH changed AS (UPDATE t SET id = 2 RETURNING id) SELECT * FROM changed",
            "WITH changed AS (DELETE FROM t RETURNING id) SELECT id FROM changed UNION ALL SELECT 1",
            "SELECT (WITH changed AS (DELETE FROM t RETURNING id) SELECT id FROM changed)",
            "SELECT CASE WHEN true THEN (WITH changed AS (DELETE FROM t RETURNING id) SELECT id FROM changed) ELSE 0 END",
            "SELECT COUNT((WITH changed AS (DELETE FROM t RETURNING id) SELECT id FROM changed))",
            "SELECT * INTO new_t FROM t", "SELECT * FROM t FOR UPDATE",
            "SELECT * FROM t FOR SHARE", "EXPLAIN ANALYZE DELETE FROM t", "CALL p()",
            "COPY t TO '/tmp/cache-test.csv'", "SET x = 1", "BEGIN", "COMMIT",
            "SHOW TABLES", "DESCRIBE t", "PRAGMA version", "SELECT unknown_udf()",
            "SELECT RANDOM()", "SELECT CURRENT_TIMESTAMP", "SELECT NOW()",
            "", "-- only a comment", "SELECT (", "SELECT 1; not valid sql !!!",
        ] { assert!(!is_cacheable(sql, "postgresql"), "{sql}"); }
    }

    /// Prove that a deterministic write remains ineligible for result caching.
    #[test]
    fn determinism_is_not_read_only_classification() {
        let sql = "INSERT INTO t VALUES (1) RETURNING id";
        assert!(crate::is_deterministic(sql, "duckdb"));
        assert!(!is_cacheable(sql, "duckdb"));
    }

    /// Exclude random sampling even when the SQL contains no volatile function call.
    #[test]
    fn random_sampling_is_not_cacheable() {
        assert!(!is_cacheable(
            "SELECT * FROM t USING SAMPLE 50 PERCENT",
            "duckdb"
        ));
    }

    /// Allow deterministic pattern filters while rejecting unsafe ESCAPE expressions.
    #[test]
    fn ordinary_pattern_filters_are_cacheable() {
        assert!(is_cacheable(
            "SELECT * FROM t WHERE name LIKE 'a%'",
            "postgresql"
        ));
        assert!(is_cacheable(
            "SELECT * FROM t WHERE name ILIKE 'a%'",
            "postgresql"
        ));
        assert!(is_cacheable(
            "SELECT * FROM t WHERE name LIKE 'a!_%' ESCAPE '!'",
            "postgresql"
        ));
        assert!(!is_cacheable(
            "SELECT 'a' LIKE 'a' ESCAPE unknown_udf()",
            "postgresql"
        ));
    }

    /// Verify the audited traversal fields and cache policy after successful parsing.
    #[test]
    fn traversal_covers_optional_expressions() {
        for (sql, dialect, safe_value) in [
            (
                "SELECT 'a' LIKE 'a' ESCAPE unknown_udf()",
                "postgresql",
                "'!'",
            ),
            (
                "SELECT 'a' ILIKE 'a' ESCAPE unknown_udf()",
                "postgresql",
                "'!'",
            ),
            (
                "SELECT CAST(1 AS STRING FORMAT unknown_udf())",
                "bigquery",
                "'999'",
            ),
            (
                "SELECT CAST('1' AS INT DEFAULT unknown_udf() ON CONVERSION ERROR)",
                "oracle",
                "0",
            ),
            (
                "SELECT COUNT(*) FILTER (WHERE unknown_udf()) FROM t",
                "postgresql",
                "TRUE",
            ),
            (
                "SELECT SUM(id) FILTER (WHERE unknown_udf()) FROM t",
                "postgresql",
                "TRUE",
            ),
        ] {
            queryflux_core::polyglot_pool::run(move || {
                let statements = polyglot_sql::parse_by_name(sql, dialect).unwrap();
                assert_eq!(statements.len(), 1, "{sql}");
                assert!(is_query(&statements[0]), "read root: {sql}");
                assert!(
                    DfsIter::new(&statements[0])
                        .any(|node| matches!(node, Expression::Function(_))),
                    "DfsIter must reach unknown_udf in the optional field: {sql}"
                );
                assert!(!safe_tree(&statements[0]), "unsafe optional field: {sql}");
            })
            .expect("AST check on the parser's large-stack pool");
            assert!(!is_cacheable(sql, dialect), "{sql}");
            let safe_sql = sql.replace("unknown_udf()", safe_value);
            assert!(is_cacheable(&safe_sql, dialect), "{safe_sql}");
        }
    }

    /// Verify AST traversal rejects nested writes without relying on parser failure.
    #[test]
    fn nested_writes_are_rejected_after_successful_parse() {
        // Prove the tree check catches these, rather than relying on parse failure.
        for sql in [
            "WITH w AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM w",
            "WITH w AS (UPDATE t SET id = 2 RETURNING id) SELECT * FROM w",
            "WITH w AS (DELETE FROM t RETURNING id) SELECT * FROM w UNION ALL SELECT 1",
            "SELECT CASE WHEN true THEN (WITH w AS (DELETE FROM t RETURNING id) SELECT id FROM w) ELSE 0 END",
            "SELECT COUNT((WITH w AS (DELETE FROM t RETURNING id) SELECT id FROM w))",
        ] {
            queryflux_core::polyglot_pool::run(move || {
                let statements = polyglot_sql::parse_by_name(sql, "postgresql").unwrap();
                assert_eq!(statements.len(), 1);
                assert!(is_query(&statements[0]), "read root: {sql}");
                assert!(!safe_tree(&statements[0]), "nested write: {sql}");
            })
            .expect("AST check on the parser's large-stack pool");
        }
    }
}
