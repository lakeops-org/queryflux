//! Bind parameters into SQL text for engines without native parameter support.

use queryflux_core::params::QueryParam;
use queryflux_core::query::SqlDialect;

/// Substitute `?` placeholders in `sql` with their typed literal representations.
///
/// Uses [`polyglot_sql`] to parse the SQL into an AST before substituting, so `?` inside
/// string literals, comments, and `$$`-quoted blocks is never incorrectly replaced.
///
/// The `dialect` is used for both parsing (so dialect-specific syntax is handled) and
/// generation (so literals are rendered in the correct dialect form).
///
/// Used as a fallback by dispatch when the backend adapter does not support native
/// prepared statements (`adapter.supports_native_params() == false`).
///
/// # Dialect coverage note
/// Correctness of literal quoting for edge cases — e.g. MySQL's backtick identifier
/// delimiter vs Postgres double-quote, or Trino's `E'...'` string escape prefix — depends
/// on polyglot_sql's per-dialect code-generation coverage. All current adapters that reach
/// this path have been verified against the dialects they use, but when adding a new
/// adapter that sets `supports_native_params() == false`, confirm that polyglot_sql
/// generates valid literals for its target dialect before shipping.
pub fn interpolate_params(
    sql: &str,
    params: &[QueryParam],
    dialect: &SqlDialect,
) -> anyhow::Result<String> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }

    let pg_dialect = crate::classify::to_polyglot_dialect(dialect);

    // polyglot-sql can overflow the default Tokio stack — parse off-worker.
    let sql_owned = sql.to_string();
    let statements = crate::polyglot_pool::run(move || polyglot_sql::parse(&sql_owned, pg_dialect))
        .ok_or_else(|| anyhow::anyhow!("SQL parse worker failed"))?
        .map_err(|e| anyhow::anyhow!("SQL parse error: {e}"))?;
    interpolate_statements(statements, params, dialect).map(|(sql, _)| sql)
}

/// [`interpolate_params`] on statements already parsed with `dialect`. Also returns the
/// rewritten statements, which are the parse of the returned SQL, so a
/// [`crate::classify::SqlParseCache`] for it needs no parse of its own.
pub fn interpolate_statements(
    statements: Vec<polyglot_sql::expressions::Expression>,
    params: &[QueryParam],
    dialect: &SqlDialect,
) -> anyhow::Result<(String, Vec<polyglot_sql::expressions::Expression>)> {
    use polyglot_sql::expressions::{BooleanLiteral, Expression, Literal, Null, ParameterStyle};
    use polyglot_sql::generate;
    use polyglot_sql::traversal::transform;
    use std::cell::Cell;

    let pg_dialect = crate::classify::to_polyglot_dialect(dialect);

    // Cell<usize> gives us interior mutability so the Fn closure can advance the index.
    let param_idx = Cell::new(0usize);

    let mut parts = Vec::with_capacity(statements.len());
    let mut rewritten_all = Vec::with_capacity(statements.len());

    for stmt in statements {
        let rewritten = transform(stmt, &|e| {
            let is_question = matches!(&e, Expression::Placeholder(_))
                || matches!(
                    &e,
                    Expression::Parameter(p) if p.style == ParameterStyle::Question
                );

            if is_question {
                let idx = param_idx.get();
                if let Some(param) = params.get(idx) {
                    param_idx.set(idx + 1);
                    let literal = match param {
                        QueryParam::Text(s) => {
                            Expression::Literal(Box::new(Literal::String(s.clone())))
                        }
                        QueryParam::Numeric(s) => {
                            Expression::Literal(Box::new(Literal::Number(s.clone())))
                        }
                        QueryParam::Boolean(b) => Expression::Boolean(BooleanLiteral { value: *b }),
                        QueryParam::Date(s) => {
                            Expression::Literal(Box::new(Literal::Date(s.clone())))
                        }
                        QueryParam::Timestamp(s) => {
                            Expression::Literal(Box::new(Literal::Timestamp(s.clone())))
                        }
                        QueryParam::Time(s) => {
                            Expression::Literal(Box::new(Literal::Time(s.clone())))
                        }
                        QueryParam::Null => Expression::Null(Null),
                    };
                    return Ok(Some(literal));
                }
                // More placeholders than params — leave unchanged; backend will error.
            }

            Ok(Some(e))
        })
        .map_err(|e| anyhow::anyhow!("AST transform error: {e}"))?;

        let sql_part = generate(&rewritten, pg_dialect)
            .map_err(|e| anyhow::anyhow!("SQL generate error: {e}"))?;
        parts.push(sql_part);
        rewritten_all.push(rewritten);
    }

    if param_idx.get() != params.len() {
        return Err(anyhow::anyhow!(
            "Parameter count mismatch: SQL has {} placeholder(s) but {} parameter(s) were provided",
            param_idx.get(),
            params.len()
        ));
    }

    Ok((parts.join(";\n"), rewritten_all))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn interp(sql: &str, params: Vec<QueryParam>) -> String {
        interpolate_params(sql, &params, &SqlDialect::Trino).expect("interpolate_params failed")
    }

    #[test]
    fn empty_params_returns_original() {
        assert_eq!(
            interpolate_params("SELECT 1", &[], &SqlDialect::Trino).unwrap(),
            "SELECT 1"
        );
    }

    #[test]
    fn text_param_is_quoted() {
        let params = vec![QueryParam::Text("alice".to_string())];
        assert!(interp("SELECT * FROM t WHERE name = ?", params).contains("'alice'"));
    }

    #[test]
    fn text_param_escapes_quotes() {
        let params = vec![QueryParam::Text("o'brien".to_string())];
        assert!(interp("SELECT * FROM t WHERE n = ?", params).contains("o''brien"));
    }

    #[test]
    fn numeric_param_is_unquoted() {
        let result = interp(
            "SELECT * FROM t WHERE id = ?",
            vec![QueryParam::Numeric("42".to_string())],
        );
        assert!(result.contains("42"));
        assert!(!result.contains("'42'"));
    }

    #[test]
    fn boolean_true() {
        let result = interp(
            "SELECT * FROM t WHERE flag = ?",
            vec![QueryParam::Boolean(true)],
        );
        assert!(result.to_uppercase().contains("TRUE"));
    }

    #[test]
    fn boolean_false() {
        let result = interp(
            "SELECT * FROM t WHERE flag = ?",
            vec![QueryParam::Boolean(false)],
        );
        assert!(result.to_uppercase().contains("FALSE"));
    }

    #[test]
    fn date_param() {
        let result = interp(
            "SELECT * FROM t WHERE dt = ?",
            vec![QueryParam::Date("2025-01-15".to_string())],
        );
        assert!(result.contains("2025-01-15"));
    }

    #[test]
    fn timestamp_param() {
        let result = interp(
            "SELECT * FROM t WHERE ts = ?",
            vec![QueryParam::Timestamp("2025-01-15 12:00:00".to_string())],
        );
        assert!(result.contains("2025-01-15 12:00:00"));
    }

    #[test]
    fn time_param() {
        let result = interp(
            "SELECT * FROM t WHERE t = ?",
            vec![QueryParam::Time("12:00:00".to_string())],
        );
        assert!(result.contains("12:00:00"));
    }

    #[test]
    fn null_param() {
        let result = interp("SELECT * FROM t WHERE x = ?", vec![QueryParam::Null]);
        assert!(result.to_uppercase().contains("NULL"));
    }

    #[test]
    fn multiple_params() {
        let params = vec![
            QueryParam::Numeric("1".to_string()),
            QueryParam::Text("hi".to_string()),
            QueryParam::Boolean(true),
        ];
        let result = interp("SELECT ?, ?, ?", params);
        assert!(result.contains('1'));
        assert!(result.contains("'hi'"));
        assert!(result.to_uppercase().contains("TRUE"));
    }

    #[test]
    fn placeholder_inside_string_not_replaced() {
        // The '?' inside the string literal must NOT consume a param.
        let params = vec![QueryParam::Numeric("99".to_string())];
        let result = interp("SELECT * FROM t WHERE name = '?' AND id = ?", params);
        assert!(result.contains("99"));
        // The literal '?' string should remain unchanged in the output.
        assert!(result.contains("'?'"));
    }
}
