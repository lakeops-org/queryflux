//! Integration tests for YAML/config `translation.pythonScripts`: Python `transform(ast, src, dst)`.
//!
//! Requires a working PyO3 interpreter with `sqlglot` on `PYTHONPATH` (same as CI: venv + `pip install -r requirements.txt`).
//! Run: `cargo test -p queryflux-translation`

use queryflux_core::{error::QueryFluxError, query::SqlDialect};
use queryflux_translation::{
    SchemaContext, SqlglotTranslator, TranslationService, TranslatorTrait,
};

fn require_sqlglot() {
    SqlglotTranslator::check_available().expect(
        "sqlglot not importable — set PYO3_PYTHON to a venv with `pip install -r requirements.txt`",
    );
}

async fn translate_trino(sql: &str, scripts: Vec<String>) -> queryflux_core::error::Result<String> {
    let t = SqlglotTranslator::new(SqlDialect::Trino, SqlDialect::Trino, scripts);
    t.translate(sql, &SchemaContext::default()).await
}

#[tokio::test]
async fn transform_script_rewrites_literal() {
    require_sqlglot();
    let script = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    for lit in ast.find_all(exp.Literal):
        try:
            if int(lit.this) == 1:
                lit.replace(exp.Literal.number(99))
                break
        except (TypeError, ValueError):
            pass
"#;
    let out = translate_trino("SELECT 1", vec![script.to_string()])
        .await
        .expect("translate");
    assert!(
        out.contains("99"),
        "expected literal rewritten to 99, got: {out}"
    );
}

#[tokio::test]
async fn transform_script_noop_when_dst_filter_not_met() {
    require_sqlglot();
    let script = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    # Only rewrite when hypothetically targeting mysql — trino->trino should skip
    if dst != "mysql":
        return
    for lit in ast.find_all(exp.Literal):
        try:
            if int(lit.this) == 1:
                lit.replace(exp.Literal.number(77))
                break
        except (TypeError, ValueError):
            pass
"#;
    let out = translate_trino("SELECT 1", vec![script.to_string()])
        .await
        .expect("translate");
    assert!(
        out.contains('1') && !out.contains("77"),
        "expected unchanged SELECT 1, got: {out}"
    );
}

#[tokio::test]
async fn transform_scripts_run_in_order() {
    require_sqlglot();
    let s1 = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    for t in ast.find_all(exp.Table):
        if t.name == "a":
            t.set("this", exp.to_identifier("b"))
"#;
    let s2 = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    for t in ast.find_all(exp.Table):
        if t.name == "b":
            t.set("this", exp.to_identifier("c"))
"#;
    let out = translate_trino("SELECT * FROM a", vec![s1.to_string(), s2.to_string()])
        .await
        .expect("translate");
    let lower = out.to_lowercase();
    assert!(
        lower.contains("c") && !lower.contains(" from a") && !lower.contains("from a"),
        "expected a->b->c, got: {out}"
    );
}

#[tokio::test]
async fn translation_service_appends_group_fixups_after_global() {
    require_sqlglot();
    let global = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    for t in ast.find_all(exp.Table):
        if t.name == "x":
            t.set("this", exp.to_identifier("y"))
"#;
    let group = r#"
import sqlglot.expressions as exp

def transform(ast, src, dst):
    for t in ast.find_all(exp.Table):
        if t.name == "y":
            t.set("this", exp.to_identifier("z"))
"#;
    let svc = TranslationService::new_sqlglot(vec![global.to_string()]).expect("service");
    let out = svc
        .maybe_translate(
            "SELECT * FROM x",
            &SqlDialect::Trino,
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[group.to_string()],
        )
        .await
        .expect("maybe_translate");
    let lower = out.to_lowercase();
    assert!(
        lower.contains("z") && !lower.contains(" x") && !lower.contains(" y"),
        "expected global then group fixups (x->y->z), got: {out}"
    );
}

#[tokio::test]
async fn script_without_transform_errors() {
    require_sqlglot();
    let script = "x = 1\n";
    let err = translate_trino("SELECT 1", vec![script.to_string()])
        .await
        .expect_err("expected missing transform");
    match err {
        QueryFluxError::Translation(msg) => {
            assert!(msg.contains("transform"), "unexpected message: {msg}");
        }
        other => panic!("expected Translation error, got {other:?}"),
    }
}

#[tokio::test]
async fn script_syntax_error_surfaces() {
    require_sqlglot();
    let script = "def broken(\n";
    let err = translate_trino("SELECT 1", vec![script.to_string()])
        .await
        .expect_err("expected script error");
    assert!(
        matches!(err, QueryFluxError::Translation(_)),
        "expected Translation error, got {err:?}"
    );
}

#[tokio::test]
async fn transform_raises_python_exception() {
    require_sqlglot();
    let script = r#"
def transform(ast, src, dst):
    raise RuntimeError("boom")
"#;
    let err = translate_trino("SELECT 1", vec![script.to_string()])
        .await
        .expect_err("expected transform failure");
    let QueryFluxError::Translation(msg) = err else {
        panic!("expected Translation error");
    };
    assert!(
        msg.contains("transform() call failed") || msg.contains("boom"),
        "unexpected message: {msg}"
    );
}

/// Real cross-dialect proof for the MCP `dialect` parameter's whole reason to exist:
/// MySQL quotes identifiers with backticks; DuckDB expects ANSI double quotes. This is
/// the exact rewrite `dispatch::should_attempt_translation` gates on for MCP — this test
/// exercises the actual sqlglot call directly (the e2e MCP suite can't: its test harness
/// uses `TranslationService::disabled()`, same as every other e2e harness, so it never
/// invokes real translation).
#[tokio::test]
async fn mysql_backtick_identifiers_translate_to_duckdb_double_quotes() {
    require_sqlglot();
    let t = SqlglotTranslator::new(SqlDialect::MySql, SqlDialect::DuckDb, vec![]);
    let out = t
        .translate("SELECT 1 AS `n`", &SchemaContext::default())
        .await
        .expect("translate");
    assert!(
        !out.contains('`'),
        "expected backticks rewritten for DuckDB, got: {out}"
    );
    assert!(
        out.contains("\"n\"") || out.contains(" n"),
        "expected the alias to survive translation, got: {out}"
    );
}
