//! Iceberg REST Catalog integration.
//!
//! Unlike Glue/Hive Metastore, the REST Catalog protocol genuinely *is*
//! Iceberg-specific — a REST catalog endpoint only ever serves Iceberg
//! tables — so this is the one integration built on the upstream
//! `iceberg`/`iceberg-catalog-rest` crates rather than a hand-rolled client.
//! Serves any implementation of the protocol: Polaris, Tabular, Unity's REST
//! endpoint, Snowflake's Horizon endpoint for Snowflake-managed Iceberg tables.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::spec::{PrimitiveType, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::config::IcebergRestAuthConfig;
use queryflux_core::error::{QueryFluxError, Result};

pub struct IcebergRestCatalogProvider {
    catalog: RestCatalog,
    /// Synthetic single-entry `list_catalogs()` result — the REST protocol
    /// has no "list catalogs" endpoint (an endpoint *is* one catalog), so this
    /// is just the configured name echoed back, same convention as Glue's
    /// synthetic `AwsDataCatalog`-style single catalog.
    catalog_name: String,
}

impl IcebergRestCatalogProvider {
    pub async fn new(
        catalog_name: &str,
        uri: &str,
        warehouse: Option<&str>,
        auth: Option<&IcebergRestAuthConfig>,
    ) -> Result<Self> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), uri.to_string());
        if let Some(warehouse) = warehouse {
            props.insert("warehouse".to_string(), warehouse.to_string());
        }
        match auth {
            Some(IcebergRestAuthConfig::OAuth2ClientCredentials {
                client_id,
                client_secret,
            }) => {
                props.insert(
                    "credential".to_string(),
                    format!("{client_id}:{client_secret}"),
                );
            }
            Some(IcebergRestAuthConfig::BearerToken { token }) => {
                props.insert("token".to_string(), token.clone());
            }
            None => {}
        }

        let catalog = RestCatalogBuilder::default()
            .load(catalog_name, props)
            .await
            .map_err(|e| {
                QueryFluxError::Catalog(format!(
                    "Iceberg REST catalog {catalog_name:?} at {uri:?}: {e}"
                ))
            })?;

        Ok(Self {
            catalog,
            catalog_name: catalog_name.to_string(),
        })
    }
}

/// A `database` in QueryFlux's flat catalog/database/table model maps onto a
/// (possibly multi-level) Iceberg namespace. Dotted components round-trip
/// through this, e.g. `"a.b"` <-> `NamespaceIdent::from_strs(["a", "b"])`.
fn namespace_ident(database: &str) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(database.split('.')).map_err(|e| {
        QueryFluxError::Catalog(format!(
            "Iceberg REST catalog: invalid namespace {database:?}: {e}"
        ))
    })
}

fn iceberg_err(context: &str, e: iceberg::Error) -> QueryFluxError {
    QueryFluxError::Catalog(format!("Iceberg REST catalog {context}: {e}"))
}

#[async_trait]
impl CatalogProvider for IcebergRestCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![self.catalog_name.clone()])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        let namespaces = self
            .catalog
            .list_namespaces(None)
            .await
            .map_err(|e| iceberg_err("list_namespaces", e))?;
        Ok(namespaces
            .into_iter()
            .map(|ns| ns.inner().join("."))
            .collect())
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let namespace = namespace_ident(database)?;
        let tables = self
            .catalog
            .list_tables(&namespace)
            .await
            .map_err(|e| iceberg_err("list_tables", e))?;
        Ok(tables.into_iter().map(|t| t.name).collect())
    }

    async fn get_table_schema(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let namespace = namespace_ident(database)?;
        let table_ident = TableIdent::new(namespace, table.to_string());

        let loaded = match self.catalog.load_table(&table_ident).await {
            Ok(t) => t,
            Err(e) if e.kind() == iceberg::ErrorKind::TableNotFound => return Ok(None),
            Err(e) => return Err(iceberg_err("load_table", e)),
        };

        let schema = loaded.metadata().current_schema();
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| ColumnDef {
                name: f.name.clone(),
                data_type: iceberg_type_to_sql(&f.field_type),
                nullable: !f.required,
            })
            .collect();

        Ok(Some(TableSchema {
            catalog: self.catalog_name.clone(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

/// Maps a typed `iceberg::spec::Type` to a SQL type-name string. Nested types
/// (struct/list/map) fall back to their Iceberg `Display` rendering rather
/// than a panic — schema-aware translation degrades gracefully for a column
/// it doesn't fully understand rather than failing the whole lookup.
fn iceberg_type_to_sql(ty: &Type) -> String {
    let primitive = match ty {
        Type::Primitive(p) => p,
        other => return other.to_string().to_uppercase(),
    };
    match primitive {
        PrimitiveType::Boolean => "BOOLEAN".to_string(),
        PrimitiveType::Int => "INT".to_string(),
        PrimitiveType::Long => "BIGINT".to_string(),
        PrimitiveType::Float => "FLOAT".to_string(),
        PrimitiveType::Double => "DOUBLE".to_string(),
        PrimitiveType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        PrimitiveType::Date => "DATE".to_string(),
        PrimitiveType::Time => "TIME".to_string(),
        PrimitiveType::Timestamp | PrimitiveType::TimestampNs => "TIMESTAMP".to_string(),
        PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
            "TIMESTAMP WITH TIME ZONE".to_string()
        }
        PrimitiveType::String => "VARCHAR".to_string(),
        PrimitiveType::Uuid => "UUID".to_string(),
        PrimitiveType::Fixed(len) => format!("BINARY({len})"),
        PrimitiveType::Binary => "VARBINARY".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_types_map_to_expected_sql_names() {
        assert_eq!(
            iceberg_type_to_sql(&Type::Primitive(PrimitiveType::Long)),
            "BIGINT"
        );
        assert_eq!(
            iceberg_type_to_sql(&Type::Primitive(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            })),
            "DECIMAL(10,2)"
        );
        assert_eq!(
            iceberg_type_to_sql(&Type::Primitive(PrimitiveType::Timestamptz)),
            "TIMESTAMP WITH TIME ZONE"
        );
        assert_eq!(
            iceberg_type_to_sql(&Type::Primitive(PrimitiveType::String)),
            "VARCHAR"
        );
    }

    #[test]
    fn dotted_database_name_round_trips_through_namespace_ident() {
        let ns = namespace_ident("a.b").unwrap();
        assert_eq!(ns.inner(), vec!["a".to_string(), "b".to_string()]);
    }
}
