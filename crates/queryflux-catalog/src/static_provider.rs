//! Zero-I/O catalog provider backed by a literal, config-declared schema.

use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::config::StaticTableSchema;
use queryflux_core::error::Result;

/// Serves table/column metadata from a fixed list declared in config
/// (`catalogProvider: { type: static, schemas: [...] }`). No network calls, no
/// errors beyond lookup misses — the simplest possible `CatalogProvider`, and the
/// vehicle for testing schema-aware translation end to end without any external
/// dependency to stand up.
pub struct StaticCatalogProvider {
    schemas: HashMap<(String, String, String), TableSchema>,
}

impl StaticCatalogProvider {
    pub fn new(schemas: Vec<StaticTableSchema>) -> Self {
        let schemas = schemas
            .into_iter()
            .map(|s| {
                let key = (s.catalog.clone(), s.database.clone(), s.table.clone());
                let columns = s
                    .columns
                    .into_iter()
                    .map(|c| ColumnDef {
                        name: c.name,
                        data_type: c.data_type,
                        nullable: c.nullable,
                    })
                    .collect();
                let value = TableSchema {
                    catalog: s.catalog,
                    database: s.database,
                    table: s.table,
                    columns,
                };
                (key, value)
            })
            .collect();
        Self { schemas }
    }
}

#[async_trait]
impl CatalogProvider for StaticCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let mut catalogs: Vec<String> = self.schemas.keys().map(|(c, _, _)| c.clone()).collect();
        catalogs.sort();
        catalogs.dedup();
        Ok(catalogs)
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let mut databases: Vec<String> = self
            .schemas
            .keys()
            .filter(|(c, _, _)| c == catalog)
            .map(|(_, d, _)| d.clone())
            .collect();
        databases.sort();
        databases.dedup();
        Ok(databases)
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let mut tables: Vec<String> = self
            .schemas
            .keys()
            .filter(|(c, d, _)| c == catalog && d == database)
            .map(|(_, _, t)| t.clone())
            .collect();
        tables.sort();
        Ok(tables)
    }

    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let key = (catalog.to_string(), database.to_string(), table.to_string());
        Ok(self.schemas.get(&key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::config::StaticColumnDef;

    fn sample() -> Vec<StaticTableSchema> {
        vec![
            StaticTableSchema {
                catalog: "hive".into(),
                database: "analytics".into(),
                table: "orders".into(),
                columns: vec![
                    StaticColumnDef {
                        name: "order_id".into(),
                        data_type: "BIGINT".into(),
                        nullable: false,
                    },
                    StaticColumnDef {
                        name: "total".into(),
                        data_type: "DECIMAL(10,2)".into(),
                        nullable: true,
                    },
                ],
            },
            StaticTableSchema {
                catalog: "hive".into(),
                database: "analytics".into(),
                table: "customers".into(),
                columns: vec![StaticColumnDef {
                    name: "customer_id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                }],
            },
        ]
    }

    #[tokio::test]
    async fn round_trips_configured_schemas() {
        let provider = StaticCatalogProvider::new(sample());

        assert_eq!(provider.list_catalogs().await.unwrap(), vec!["hive"]);
        assert_eq!(
            provider.list_databases("hive").await.unwrap(),
            vec!["analytics"]
        );
        let mut tables = provider.list_tables("hive", "analytics").await.unwrap();
        tables.sort();
        assert_eq!(tables, vec!["customers", "orders"]);

        let schema = provider
            .get_table_schema("hive", "analytics", "orders")
            .await
            .unwrap()
            .expect("orders should be found");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "order_id");
    }

    #[tokio::test]
    async fn unknown_lookup_returns_none_not_error() {
        let provider = StaticCatalogProvider::new(sample());
        assert!(provider
            .get_table_schema("hive", "analytics", "does_not_exist")
            .await
            .unwrap()
            .is_none());
        assert!(provider
            .list_tables("hive", "does_not_exist")
            .await
            .unwrap()
            .is_empty());
    }
}
