//! Primary/secondary catalog provider composition.

use std::sync::Arc;

use async_trait::async_trait;
use queryflux_core::catalog::{CatalogProvider, TableSchema};
use queryflux_core::error::Result;

/// Tries `primary` first, matching the `catalogProvider: { type: fallback, primary,
/// secondary }` config shape. Falls through to `secondary` on:
/// - any `Err` from `primary`, for every method;
/// - `get_table_schema` specifically returning `Ok(None)` — "not found in primary"
///   is exactly the fallback use case (e.g. an engine-delegate primary plus a
///   static secondary for tables the engine doesn't know about yet).
///
/// `list_tables`/`list_databases`/`list_catalogs` do **not** fall through on an
/// empty `Ok` result — an empty catalog/database is a legitimate answer there,
/// not a "try harder" signal, unlike a missing single table.
pub struct FallbackCatalogProvider {
    primary: Arc<dyn CatalogProvider>,
    secondary: Arc<dyn CatalogProvider>,
}

impl FallbackCatalogProvider {
    pub fn new(primary: Arc<dyn CatalogProvider>, secondary: Arc<dyn CatalogProvider>) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait]
impl CatalogProvider for FallbackCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        match self.primary.list_catalogs().await {
            Ok(v) => Ok(v),
            Err(_) => self.secondary.list_catalogs().await,
        }
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        match self.primary.list_databases(catalog).await {
            Ok(v) => Ok(v),
            Err(_) => self.secondary.list_databases(catalog).await,
        }
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        match self.primary.list_tables(catalog, database).await {
            Ok(v) => Ok(v),
            Err(_) => self.secondary.list_tables(catalog, database).await,
        }
    }

    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        match self
            .primary
            .get_table_schema(catalog, database, table)
            .await
        {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => {
                self.secondary
                    .get_table_schema(catalog, database, table)
                    .await
            }
            Err(_) => {
                self.secondary
                    .get_table_schema(catalog, database, table)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::catalog::ColumnDef;
    use queryflux_core::error::QueryFluxError;

    struct AlwaysErrors;
    #[async_trait]
    impl CatalogProvider for AlwaysErrors {
        async fn list_catalogs(&self) -> Result<Vec<String>> {
            Err(QueryFluxError::Catalog("down".into()))
        }
        async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
            Err(QueryFluxError::Catalog("down".into()))
        }
        async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
            Err(QueryFluxError::Catalog("down".into()))
        }
        async fn get_table_schema(
            &self,
            _catalog: &str,
            _database: &str,
            _table: &str,
        ) -> Result<Option<TableSchema>> {
            Err(QueryFluxError::Catalog("down".into()))
        }
    }

    struct AlwaysEmpty;
    #[async_trait]
    impl CatalogProvider for AlwaysEmpty {
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
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            catalog: "hive".into(),
            database: "analytics".into(),
            table: "orders".into(),
            columns: vec![ColumnDef {
                name: "order_id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
        }
    }

    struct HasOrders;
    #[async_trait]
    impl CatalogProvider for HasOrders {
        async fn list_catalogs(&self) -> Result<Vec<String>> {
            Ok(vec!["hive".into()])
        }
        async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
            Ok(vec!["analytics".into()])
        }
        async fn list_tables(&self, _catalog: &str, _database: &str) -> Result<Vec<String>> {
            Ok(vec!["orders".into()])
        }
        async fn get_table_schema(
            &self,
            _catalog: &str,
            _database: &str,
            table: &str,
        ) -> Result<Option<TableSchema>> {
            if table == "orders" {
                Ok(Some(sample_schema()))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn falls_through_on_primary_error() {
        let fb = FallbackCatalogProvider::new(Arc::new(AlwaysErrors), Arc::new(HasOrders));
        assert_eq!(fb.list_catalogs().await.unwrap(), vec!["hive"]);
    }

    #[tokio::test]
    async fn falls_through_on_missing_table_schema() {
        let fb = FallbackCatalogProvider::new(Arc::new(AlwaysEmpty), Arc::new(HasOrders));
        let schema = fb
            .get_table_schema("hive", "analytics", "orders")
            .await
            .unwrap();
        assert!(schema.is_some());
    }

    #[tokio::test]
    async fn does_not_fall_through_on_empty_list_tables() {
        let fb = FallbackCatalogProvider::new(Arc::new(AlwaysEmpty), Arc::new(HasOrders));
        // AlwaysEmpty legitimately has no tables — must not be overridden by secondary.
        assert!(fb
            .list_tables("hive", "analytics")
            .await
            .unwrap()
            .is_empty());
    }
}
