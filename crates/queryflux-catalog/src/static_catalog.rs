//! In-memory catalog backed by config — for local demos and tests without
//! Glue/HMS/Iceberg. Table schemas are keyed by the config's own map key (bare or
//! qualified). An entry's declared `catalog`/`database` restricts which requests it answers;
//! when left empty the entry is un-namespaced and answers any (same as the e2e `MapCatalog`).

use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
#[cfg(test)]
use queryflux_core::config::StaticTableEntry;
use queryflux_core::config::StaticTableMap;
use queryflux_core::error::Result;

#[derive(Debug)]
pub struct StaticCatalogProvider {
    tables: HashMap<String, TableSchema>,
}

impl StaticCatalogProvider {
    pub fn new(tables: StaticTableMap) -> Self {
        let tables = tables
            .into_iter()
            .map(|(name, entry)| {
                let schema = TableSchema {
                    catalog: entry.catalog,
                    database: entry.database,
                    table: name.clone(),
                    columns: entry
                        .columns
                        .into_iter()
                        .map(|c| ColumnDef {
                            name: c.name,
                            data_type: c.data_type,
                            nullable: c.nullable,
                        })
                        .collect(),
                };
                (name, schema)
            })
            .collect();
        Self { tables }
    }
}

/// An entry's declared `catalog`/`database` only filters when it is non-empty: empty is the
/// demo default and means "not namespaced", so it matches any request. An empty *request*
/// namespace (an unqualified reference) likewise doesn't rule anything out. Two non-empty
/// names must agree.
fn namespace_matches(declared: &str, requested: &str) -> bool {
    declared.is_empty() || requested.is_empty() || declared.eq_ignore_ascii_case(requested)
}

impl StaticCatalogProvider {
    /// Entries visible under `catalog` (any when empty) and `database` (any when empty), by key.
    fn in_namespace<'a>(
        &'a self,
        catalog: &'a str,
        database: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a TableSchema)> + 'a {
        self.tables.iter().filter(move |(_, schema)| {
            namespace_matches(&schema.catalog, catalog)
                && namespace_matches(&schema.database, database)
        })
    }
}

#[async_trait]
impl CatalogProvider for StaticCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let mut catalogs: Vec<String> = self
            .tables
            .values()
            .filter(|schema| !schema.catalog.is_empty())
            .map(|schema| schema.catalog.clone())
            .collect();
        catalogs.sort();
        catalogs.dedup();
        Ok(catalogs)
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let mut databases: Vec<String> = self
            .in_namespace(catalog, "")
            .map(|(_, schema)| schema.database.clone())
            .collect();
        databases.sort();
        databases.dedup();
        Ok(databases)
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let mut names: Vec<String> = self
            .in_namespace(catalog, database)
            .map(|(key, _)| key.clone())
            .collect();
        names.sort();
        Ok(names)
    }

    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let bare = table.rsplit('.').next().unwrap_or(table);
        // Among same-named entries in a compatible namespace: one whose declared namespace the
        // request actually named beats an un-namespaced one, then an exact key match, and the
        // key breaks any remaining tie so the answer doesn't depend on hash order.
        let specificity = |schema: &TableSchema, key: &str| {
            let named = |declared: &str, requested: &str| {
                !declared.is_empty() && declared.eq_ignore_ascii_case(requested)
            };
            (
                named(&schema.catalog, catalog) as u8 + named(&schema.database, database) as u8,
                key == table,
            )
        };
        Ok(self
            .in_namespace(catalog, database)
            .filter(|(key, _)| key.as_str() == table || key.rsplit('.').next() == Some(bare))
            .max_by(|(ka, a), (kb, b)| {
                specificity(a, ka)
                    .cmp(&specificity(b, kb))
                    .then_with(|| kb.cmp(ka))
            })
            .map(|(_, schema)| schema.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::config::StaticColumnConfig;

    fn demo_tables() -> StaticTableMap {
        let mut tables = HashMap::new();
        tables.insert(
            "customers".to_string(),
            StaticTableEntry {
                catalog: String::new(),
                database: String::new(),
                columns: vec![
                    StaticColumnConfig {
                        name: "id".into(),
                        data_type: "INTEGER".into(),
                        nullable: true,
                    },
                    StaticColumnConfig {
                        name: "ssn".into(),
                        data_type: "VARCHAR".into(),
                        nullable: true,
                    },
                ],
            },
        );
        tables
    }

    #[tokio::test]
    async fn get_table_schema_resolves_bare_name() {
        let provider = StaticCatalogProvider::new(demo_tables());
        let schema = provider
            .get_table_schema("", "", "customers")
            .await
            .unwrap()
            .expect("customers");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
    }

    #[tokio::test]
    async fn list_tables_returns_configured_names() {
        let provider = StaticCatalogProvider::new(demo_tables());
        let names = provider.list_tables("", "").await.unwrap();
        assert_eq!(names, vec!["customers"]);
    }

    fn entry(catalog: &str, database: &str, cols: &[&str]) -> StaticTableEntry {
        StaticTableEntry {
            catalog: catalog.to_string(),
            database: database.to_string(),
            columns: cols
                .iter()
                .map(|c| StaticColumnConfig {
                    name: c.to_string(),
                    data_type: "VARCHAR".into(),
                    nullable: true,
                })
                .collect(),
        }
    }

    fn namespaced() -> StaticCatalogProvider {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), entry("sales", "eu", &["id", "total"]));
        tables.insert("users".to_string(), entry("crm", "core", &["id"]));
        tables.insert("bare".to_string(), entry("", "", &["x"]));
        StaticCatalogProvider::new(tables)
    }

    /// A request naming a different catalog/database must not receive a namespaced entry's
    /// schema just because the bare table name is the same.
    #[tokio::test]
    async fn get_table_schema_rejects_mismatched_namespace() {
        let p = namespaced();
        for (cat, db) in [
            ("other_catalog", "other_db"),
            ("sales", "us"),
            ("crm", "eu"),
        ] {
            assert!(
                p.get_table_schema(cat, db, "orders")
                    .await
                    .unwrap()
                    .is_none(),
                "{cat}.{db}.orders must not resolve to sales.eu.orders"
            );
        }
        let ok = p.get_table_schema("sales", "eu", "orders").await.unwrap();
        assert_eq!(ok.expect("sales.eu.orders").columns.len(), 2);
        // Unqualified request and qualified table name both still find it.
        assert!(p
            .get_table_schema("", "", "orders")
            .await
            .unwrap()
            .is_some());
        assert!(p
            .get_table_schema("sales", "eu", "sales.eu.orders")
            .await
            .unwrap()
            .is_some());
        // Un-namespaced entries stay usable from any session namespace (demo default).
        assert!(p
            .get_table_schema("any", "any", "bare")
            .await
            .unwrap()
            .is_some());
    }

    /// The same bare name under two catalogs resolves to the one the request names.
    #[tokio::test]
    async fn get_table_schema_disambiguates_same_bare_name_by_catalog() {
        let mut tables = HashMap::new();
        tables.insert("customers".to_string(), entry("", "", &["id", "ssn"]));
        tables.insert(
            "staging.customers".to_string(),
            entry("staging", "", &["id"]),
        );
        let p = StaticCatalogProvider::new(tables);

        let staging = p
            .get_table_schema("staging", "", "customers")
            .await
            .unwrap()
            .expect("staging.customers");
        assert_eq!(staging.catalog, "staging");
        let other = p
            .get_table_schema("prod", "", "customers")
            .await
            .unwrap()
            .expect("un-namespaced customers");
        assert_eq!(other.catalog, "");
        assert_eq!(other.columns.len(), 2);
    }

    #[tokio::test]
    async fn discovery_reflects_declared_namespaces() {
        let p = namespaced();
        assert_eq!(p.list_catalogs().await.unwrap(), vec!["crm", "sales"]);
        assert_eq!(p.list_databases("sales").await.unwrap(), vec!["", "eu"]);
        assert_eq!(p.list_databases("crm").await.unwrap(), vec!["", "core"]);
        assert_eq!(
            p.list_tables("sales", "eu").await.unwrap(),
            vec!["bare", "orders"]
        );
        assert_eq!(p.list_tables("sales", "us").await.unwrap(), vec!["bare"]);
        assert_eq!(p.list_tables("nope", "").await.unwrap(), vec!["bare"]);
    }
}
