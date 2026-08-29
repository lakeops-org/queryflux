use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    /// Convert to the nested map format sqlglot's MappingSchema expects:
    /// `{ catalog: { db: { table: { col: "TYPE" } } } }`
    #[allow(clippy::type_complexity)]
    pub fn to_sqlglot_schema(
        &self,
    ) -> HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>> {
        let columns: HashMap<String, String> = self
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect();
        let mut table_map = HashMap::new();
        table_map.insert(self.table.clone(), columns);
        let mut db_map = HashMap::new();
        db_map.insert(self.database.clone(), table_map);
        let mut catalog_map = HashMap::new();
        catalog_map.insert(self.catalog.clone(), db_map);
        catalog_map
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// Normalized SQL type string (e.g. "BIGINT", "VARCHAR", "TIMESTAMP").
    pub data_type: String,
    pub nullable: bool,
}

/// Provides catalog/schema metadata for SQL translation and routing.
/// Implementations are pluggable and composable (caching, fallback, static, live).
#[async_trait]
pub trait CatalogProvider: Send + Sync {
    async fn list_catalogs(&self) -> Result<Vec<String>>;
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;
    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>>;

    /// Bulk-fetch schemas for all tables referenced in a query.
    /// Default impl calls `get_table_schema` in parallel.
    async fn get_schemas_for_query(
        &self,
        catalog: Option<&str>,
        database: Option<&str>,
        tables: &[&str],
    ) -> Result<Vec<TableSchema>> {
        let catalog = catalog.unwrap_or("");
        let database = database.unwrap_or("");
        let mut schemas = Vec::new();
        for table in tables {
            if let Some(schema) = self.get_table_schema(catalog, database, table).await? {
                schemas.push(schema);
            }
        }
        Ok(schemas)
    }

    /// True for a provider that is known to always return empty results (i.e.
    /// `NullCatalogProvider`). Lets callers on a hot path (schema-aware translation)
    /// skip catalog lookup — and any SQL parsing needed just to build the lookup
    /// request — entirely when no real catalog is configured, rather than paying
    /// that cost only to get an empty answer back. Composite providers (caching,
    /// fallback) are not required to propagate this from their delegate(s): it's a
    /// fast-path hint, not a correctness requirement — worst case, a query pays for
    /// a lookup that returns nothing, exactly as it would without this method.
    fn is_null(&self) -> bool {
        false
    }
}

/// No-op catalog provider — returns empty results, sqlglot does best-effort translation.
pub struct NullCatalogProvider;

#[async_trait]
impl CatalogProvider for NullCatalogProvider {
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
        _catalog: &str,
        _database: &str,
        _table: &str,
    ) -> Result<Option<TableSchema>> {
        Ok(None)
    }
    fn is_null(&self) -> bool {
        true
    }
}
