//! In-memory catalog backed by config — for local demos and tests without
//! Glue/HMS/Iceberg. Table schemas are keyed by the config's own map key (bare or
//! qualified); lookup disambiguates same-named tables across catalogs/databases using
//! each entry's declared `catalog`/`database` fields when the request specifies them.

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

#[async_trait]
impl CatalogProvider for StaticCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        Ok(vec!["".to_string()])
    }

    async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let bare = table.rsplit('.').next().unwrap_or(table);
        let name_matches =
            |key: &str| -> bool { key == table || key.rsplit('.').next().unwrap_or(key) == bare };
        let catalog_matches = |schema: &TableSchema| -> bool {
            schema.catalog == catalog && schema.database == database
        };

        // Prefer an entry whose declared catalog/database agree with the request — a
        // same-named table filed under a different catalog must not win just because
        // it shares a bare key with the one the request actually asked for.
        if let Some(schema) = self
            .tables
            .iter()
            .find(|(key, schema)| name_matches(key) && catalog_matches(schema))
            .map(|(_, schema)| schema)
        {
            return Ok(Some(schema.clone()));
        }

        // No catalog/database-qualified match — fall back to any name match, preserving
        // prior lenient behavior for configs that never set catalog/database at all.
        Ok(self
            .tables
            .iter()
            .find(|(key, _)| name_matches(key))
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

    fn entry(catalog: &str, cols: &[&str]) -> StaticTableEntry {
        StaticTableEntry {
            catalog: catalog.to_string(),
            database: String::new(),
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

    /// Regression: a same-named table filed under two different catalogs must not
    /// collide — a request for `staging.customers` must never silently return the
    /// (unqualified) `customers` entry's schema just because it shares a bare name.
    #[tokio::test]
    async fn get_table_schema_disambiguates_same_bare_name_by_catalog() {
        let mut tables = HashMap::new();
        tables.insert("customers".to_string(), entry("", &["id", "ssn"]));
        tables.insert("staging.customers".to_string(), entry("staging", &["id"]));
        let provider = StaticCatalogProvider::new(tables);

        let staging = provider
            .get_table_schema("staging", "", "customers")
            .await
            .unwrap()
            .expect("staging.customers");
        assert_eq!(staging.catalog, "staging");
        assert_eq!(staging.columns.len(), 1);

        let default = provider
            .get_table_schema("", "", "customers")
            .await
            .unwrap()
            .expect("customers");
        assert_eq!(default.catalog, "");
        assert_eq!(default.columns.len(), 2);
    }

    #[tokio::test]
    async fn list_tables_returns_configured_names() {
        let provider = StaticCatalogProvider::new(demo_tables());
        let names = provider.list_tables("", "").await.unwrap();
        assert_eq!(names, vec!["customers"]);
    }
}
