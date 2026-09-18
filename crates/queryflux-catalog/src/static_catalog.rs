//! In-memory catalog backed by config — for local demos and tests without
//! Glue/HMS/Iceberg. Table schemas are keyed by bare table name (same as the
//! e2e `MapCatalog` helper).

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
        _catalog: &str,
        _database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let bare = table.rsplit('.').next().unwrap_or(table);
        Ok(self
            .tables
            .get(table)
            .or_else(|| self.tables.get(bare))
            .cloned())
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
}
