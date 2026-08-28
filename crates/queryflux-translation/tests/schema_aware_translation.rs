//! Proves `SchemaContext` actually engages sqlglot's schema-aware optimizer branch
//! (not just the dialect-only transpile path), and exercises
//! `TranslationService::resolve_schema_context` end to end against a fake
//! `CatalogProvider` — without depending on `queryflux-catalog` (translation only
//! needs the `CatalogProvider` trait, not any concrete implementation).
//!
//! Requires a working PyO3 interpreter with `sqlglot` on `PYTHONPATH` (same as CI:
//! venv + `pip install -r requirements.txt`). Run: `cargo test -p queryflux-translation`

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::error::Result;
use queryflux_core::query::SqlDialect;
use queryflux_translation::{
    SchemaContext, SqlglotTranslator, TranslationService, TranslatorTrait,
};

fn require_sqlglot() {
    SqlglotTranslator::check_available().expect(
        "sqlglot not importable — set PYO3_PYTHON to a venv with `pip install -r requirements.txt`",
    );
}

/// Serves exactly one table, `t(x INT)` — enough to prove schema-aware behavior
/// without pulling in `queryflux-catalog`.
struct OneTableCatalog;

#[async_trait]
impl CatalogProvider for OneTableCatalog {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
        Ok(vec!["t".to_string()])
    }
    async fn get_table_schema(
        &self,
        _catalog: &str,
        _database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        if table != "t" {
            return Ok(None);
        }
        Ok(Some(TableSchema {
            catalog: String::new(),
            database: String::new(),
            table: "t".to_string(),
            columns: vec![ColumnDef {
                name: "x".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
            }],
        }))
    }
}

#[tokio::test]
async fn schema_aware_translation_qualifies_columns_dialect_only_does_not() {
    require_sqlglot();
    // Trino -> DuckDb (not Trino -> Trino): `translate_with_gil` treats matching
    // src/tgt as an unconditional no-op regardless of schema_context, so same-dialect
    // would never reach either branch and couldn't prove anything here.
    let translator = SqlglotTranslator::new(SqlDialect::Trino, SqlDialect::DuckDb, vec![]);

    let mut tables = HashMap::new();
    tables.insert(
        "t".to_string(),
        HashMap::from([("x".to_string(), "INT".to_string())]),
    );
    let schema = SchemaContext {
        catalog: None,
        database: None,
        tables,
    };
    let with_schema = translator
        .translate("SELECT x FROM t", &schema)
        .await
        .unwrap();
    assert!(
        with_schema.contains("\"t\".\"x\""),
        "schema-aware optimizer should qualify the unqualified column: {with_schema}"
    );

    let dialect_only = translator
        .translate("SELECT x FROM t", &SchemaContext::default())
        .await
        .unwrap();
    assert!(
        !dialect_only.contains("\"t\".\"x\""),
        "dialect-only translation must not qualify columns: {dialect_only}"
    );
}

#[tokio::test]
async fn resolve_schema_context_feeds_maybe_translate_end_to_end() {
    require_sqlglot();
    let service = TranslationService::new_sqlglot(vec![]).unwrap();
    let catalog: Arc<dyn CatalogProvider> = Arc::new(OneTableCatalog);

    let schema_context = service
        .resolve_schema_context("SELECT x FROM t", &SqlDialect::Trino, &catalog, None, None)
        .await;
    assert!(!schema_context.is_empty());
    assert_eq!(
        schema_context.tables.get("t").and_then(|c| c.get("x")),
        Some(&"INT".to_string())
    );

    // Trino -> DuckDb is not dialect-compatible, so maybe_translate can't take its
    // same-dialect fast path (lib.rs:116) — it must actually invoke sqlglot, and the
    // resolved schema_context should make that invocation the schema-aware branch.
    let translated = service
        .maybe_translate(
            "SELECT x FROM t",
            &SqlDialect::Trino,
            &SqlDialect::DuckDb,
            &schema_context,
            &[],
        )
        .await
        .unwrap();
    assert!(
        translated.contains("\"t\".\"x\""),
        "resolve_schema_context's output should have driven the schema-aware branch: {translated}"
    );
}

#[tokio::test]
async fn resolve_schema_context_is_empty_when_catalog_is_null() {
    require_sqlglot();
    let service = TranslationService::new_sqlglot(vec![]).unwrap();
    let catalog: Arc<dyn CatalogProvider> = Arc::new(queryflux_core::catalog::NullCatalogProvider);

    let schema_context = service
        .resolve_schema_context("SELECT x FROM t", &SqlDialect::Trino, &catalog, None, None)
        .await;
    assert!(schema_context.is_empty());
}
