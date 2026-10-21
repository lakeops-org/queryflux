//! `resolve_schema_context` for queries that reference tables in more than one schema, and
//! what the resolved context feeds: the access-control masking rewrite.
//!
//! Requires a working PyO3 interpreter with `sqlglot` on `PYTHONPATH` (same as CI:
//! venv + `pip install -r requirements.txt`). Run: `cargo test -p queryflux-translation`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::access_model::Columns;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::error::Result;
use queryflux_core::query::SqlDialect;
use queryflux_translation::{
    extract_resources, rewrite_table_scans, SchemaContext, SqlglotTranslator, TableKey,
    TablePolicy, TranslationService, TranslatorTrait,
};

fn require_sqlglot() {
    SqlglotTranslator::check_available().expect(
        "sqlglot not importable — set PYO3_PYTHON to a venv with `pip install -r requirements.txt`",
    );
}

/// `(database, table, columns)`. Answers only an exact `database` match, like a real catalog.
/// A lookup in `slow_db` takes a minute.
struct MapCatalog(
    Vec<(&'static str, &'static str, &'static [&'static str])>,
    Option<&'static str>,
);

#[async_trait]
impl CatalogProvider for MapCatalog {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        if self.1 == Some(database) {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        Ok(self
            .0
            .iter()
            .find(|(db, t, _)| *db == database && *t == table)
            .map(|(db, t, cols)| TableSchema {
                catalog: catalog.to_string(),
                database: db.to_string(),
                table: t.to_string(),
                columns: cols
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.to_string(),
                        data_type: "VARCHAR".to_string(),
                        nullable: true,
                    })
                    .collect(),
            }))
    }
}

fn catalog() -> Arc<dyn CatalogProvider> {
    Arc::new(MapCatalog(
        vec![
            ("sales", "customers", &["id", "name"]),
            ("sales", "orders", &["id", "amount"]),
            ("crm", "orders", &["id", "customer_id", "email", "ssn"]),
        ],
        None,
    ))
}

async fn resolve(sql: &str, default_db: Option<&str>) -> SchemaContext {
    TranslationService::new_sqlglot(vec![])
        .unwrap()
        .resolve_schema_context(sql, &SqlDialect::Trino, &catalog(), None, default_db)
        .await
}

fn columns(schema: &SchemaContext, name: &str) -> Vec<String> {
    let key: TableKey = name.into();
    schema
        .tables
        .get(&key)
        .unwrap_or_else(|| panic!("{name} not resolved: {schema:?}"))
        .keys()
        .cloned()
        .collect()
}

#[tokio::test]
async fn each_table_is_looked_up_in_its_own_schema() {
    require_sqlglot();
    let schema = resolve(
        "SELECT * FROM sales.customers c JOIN crm.orders o ON c.id = o.customer_id \
         JOIN sales.orders s ON s.id = o.id",
        None,
    )
    .await;
    assert_eq!(columns(&schema, "sales.customers"), ["id", "name"]);
    assert_eq!(columns(&schema, "sales.orders"), ["id", "amount"]);
    assert_eq!(
        columns(&schema, "crm.orders"),
        ["id", "customer_id", "email", "ssn"]
    );
}

#[tokio::test]
async fn unqualified_tables_resolve_to_the_session_default_not_the_first_table() {
    require_sqlglot();
    let schema = resolve(
        "SELECT * FROM sales.customers c JOIN orders o ON c.id = o.id",
        Some("crm"),
    )
    .await;
    assert_eq!(
        columns(&schema, "crm.orders"),
        ["id", "customer_id", "email", "ssn"]
    );
    assert!(
        !schema.tables.contains_key(&"sales.orders".into()),
        "{schema:?}"
    );
}

#[tokio::test]
async fn a_slow_schema_only_loses_its_own_tables() {
    require_sqlglot();
    let catalog: Arc<dyn CatalogProvider> = Arc::new(MapCatalog(
        vec![
            ("sales", "orders", &["id", "amount"]),
            ("crm", "orders", &["id"]),
        ],
        Some("crm"),
    ));
    let schema = TranslationService::new_sqlglot(vec![])
        .unwrap()
        .with_schema_resolution_timeout(Duration::from_millis(200))
        .resolve_schema_context(
            "SELECT * FROM sales.orders s JOIN crm.orders c ON s.id = c.id",
            &SqlDialect::Trino,
            &catalog,
            None,
            None,
        )
        .await;
    assert_eq!(columns(&schema, "sales.orders"), ["id", "amount"]);
    assert!(
        !schema.tables.contains_key(&"crm.orders".into()),
        "{schema:?}"
    );
}

/// The session default resolves the unqualified table for the optimizer, but the SQL sent to
/// the engine keeps every table name as the client wrote it.
#[tokio::test]
async fn translation_expands_the_right_table_and_keeps_names_as_written() {
    require_sqlglot();
    let sql = "SELECT o.*, s.amount FROM orders o JOIN sales.orders s ON o.id = s.id";
    let schema = resolve(sql, Some("crm")).await;
    let out = SqlglotTranslator::new(SqlDialect::Trino, SqlDialect::DuckDb, vec![])
        .translate(sql, &schema)
        .await
        .unwrap();
    assert!(
        out.contains("\"o\".\"email\""),
        "o.* not expanded from crm.orders: {out}"
    );
    assert!(
        out.contains("FROM \"orders\" AS \"o\""),
        "default schema leaked into output: {out}"
    );
    assert!(out.contains("\"sales\".\"orders\" AS \"s\""), "{out}");
    assert!(!out.contains("crm"), "{out}");
}

#[tokio::test]
async fn extraction_attributes_columns_to_the_right_table_and_reports_names_as_written() {
    require_sqlglot();
    let sql = "SELECT o.email, s.amount FROM orders o JOIN sales.orders s ON o.id = s.id";
    let schema = resolve(sql, Some("crm")).await;
    let st = extract_resources(sql, &SqlDialect::Trino, &schema).unwrap();
    let mut got: Vec<_> = st
        .resources
        .iter()
        .map(|r| {
            let mut cols = match &r.columns {
                Columns::Named(c) => c.clone(),
                Columns::All => vec!["*".into()],
            };
            cols.sort();
            (r.schema.clone(), r.table.clone(), cols)
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        [
            (
                None,
                "orders".to_string(),
                vec!["email".to_string(), "id".to_string()]
            ),
            (
                Some("sales".to_string()),
                "orders".to_string(),
                vec!["amount".to_string(), "id".to_string()]
            ),
        ]
    );
}

/// `crm.orders.ssn` is masked. The query's first table is in `sales`, and `sales` also has
/// an `orders` table. The masked scan must project `crm.orders`' own columns.
#[tokio::test]
async fn masked_star_projection_uses_the_masked_tables_own_columns() {
    require_sqlglot();
    let sql = "SELECT o.* FROM sales.customers c JOIN crm.orders o ON c.id = o.customer_id";
    let schema = resolve(sql, None).await;
    let policies = [TablePolicy {
        table: "crm.orders".to_string(),
        row_filters: vec![],
        masked_columns: vec![("ssn".to_string(), "'***'".to_string())],
    }];
    let rewritten = rewrite_table_scans(sql, &SqlDialect::Trino, &schema, &policies).unwrap();

    for col in ["customer_id", "email"] {
        assert!(
            rewritten.contains(col),
            "{col} missing from masked projection: {rewritten}"
        );
    }
    assert!(
        !rewritten.contains("amount"),
        "projection uses sales.orders' columns: {rewritten}"
    );
    assert!(rewritten.contains("'***'"), "ssn not masked: {rewritten}");
}
