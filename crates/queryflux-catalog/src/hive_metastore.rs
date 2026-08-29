//! Hive Metastore (Thrift) integration.
//!
//! Talks the *raw* HMS Thrift protocol via the `hive_metastore` crate's
//! generated client — deliberately not `iceberg-catalog-hms`, which only
//! understands Iceberg-format tables registered in HMS. HMS's own Table/
//! StorageDescriptor/FieldSchema model predates Iceberg entirely and is
//! format-agnostic, so this sees plain Hive/Parquet tables too — same
//! reasoning as `glue.rs` avoiding `iceberg-catalog-glue`.

use async_trait::async_trait;
use hive_metastore::{
    ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
    ThriftHiveMetastoreGetTableException,
};
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::error::{QueryFluxError, Result};
use volo_thrift::MaybeException;

/// HMS has no catalog concept of its own — every database/table lives under
/// one metastore. Mirrors Glue's single-synthetic-catalog convention.
const DEFAULT_CATALOG_NAME: &str = "hive_metastore";

pub struct HiveMetastoreCatalogProvider {
    client: ThriftHiveMetastoreClient,
}

impl HiveMetastoreCatalogProvider {
    /// `uri` is `thrift://host:port` (the `thrift://` scheme is accepted and
    /// stripped, since that's how operators are used to writing it, but the
    /// underlying connection is a plain TCP/Thrift socket, not HTTP).
    pub async fn new(uri: &str) -> Result<Self> {
        let hostport = uri.strip_prefix("thrift://").unwrap_or(uri);
        let address = std::net::ToSocketAddrs::to_socket_addrs(hostport)
            .map_err(|e| {
                QueryFluxError::Config(format!(
                    "catalogProvider hiveMetastore: invalid uri {uri:?}: {e}"
                ))
            })?
            .next()
            .ok_or_else(|| {
                QueryFluxError::Config(format!(
                    "catalogProvider hiveMetastore: could not resolve {uri:?}"
                ))
            })?;

        // Construction is synchronous (lazy connection — the socket only opens
        // on the first real RPC), same as `iceberg-catalog-hms`'s identical
        // setup; kept as an `async fn` regardless, to match the calling
        // convention every other provider's constructor uses.
        let client = ThriftHiveMetastoreClientBuilder::new("queryflux-catalog")
            .address(address)
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
            .build();

        Ok(Self { client })
    }
}

fn thrift_err(context: &str, e: impl std::fmt::Debug) -> QueryFluxError {
    QueryFluxError::Catalog(format!("Hive Metastore {context}: {e:?}"))
}

#[async_trait]
impl CatalogProvider for HiveMetastoreCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![DEFAULT_CATALOG_NAME.to_string()])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        let resp = self
            .client
            .get_all_databases()
            .await
            .map_err(|e| thrift_err("get_all_databases", e))?;
        match resp {
            MaybeException::Ok(names) => Ok(names.into_iter().map(|s| s.to_string()).collect()),
            MaybeException::Exception(e) => Err(thrift_err("get_all_databases", e)),
        }
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let resp = self
            .client
            .get_all_tables(database.to_string().into())
            .await
            .map_err(|e| thrift_err("get_all_tables", e))?;
        match resp {
            MaybeException::Ok(names) => Ok(names.into_iter().map(|s| s.to_string()).collect()),
            MaybeException::Exception(e) => Err(thrift_err("get_all_tables", e)),
        }
    }

    async fn get_table_schema(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let resp = self
            .client
            .get_table(database.to_string().into(), table.to_string().into())
            .await
            .map_err(|e| thrift_err("get_table", e))?;
        let hive_table = match resp {
            MaybeException::Ok(t) => t,
            // O2 = NoSuchObjectException — the table genuinely doesn't exist,
            // not a real error (mirrors Glue's EntityNotFoundException handling).
            MaybeException::Exception(ThriftHiveMetastoreGetTableException::O2(_)) => {
                return Ok(None);
            }
            MaybeException::Exception(e) => return Err(thrift_err("get_table", e)),
        };

        let mut columns: Vec<ColumnDef> = Vec::new();
        if let Some(sd) = &hive_table.sd {
            if let Some(cols) = &sd.cols {
                columns.extend(cols.iter().map(field_schema_to_column_def));
            }
        }
        // Partition keys are real, queryable columns — same reasoning as
        // Glue's partition-key handling.
        if let Some(partition_keys) = &hive_table.partition_keys {
            columns.extend(partition_keys.iter().map(field_schema_to_column_def));
        }

        Ok(Some(TableSchema {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

fn field_schema_to_column_def(f: &hive_metastore::FieldSchema) -> ColumnDef {
    ColumnDef {
        name: f.name.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        // Hive type strings (e.g. "bigint", "struct<a:int>") aren't normalized
        // to standard SQL — same approach as Glue, whose type strings are the
        // same Hive-derived vocabulary.
        data_type: f
            .r#type
            .as_ref()
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "STRING".to_string()),
        // FieldSchema carries no nullability — defaults to true, same as Glue.
        nullable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_mapping_uppercases_type_and_defaults_nullable_true() {
        let f = hive_metastore::FieldSchema {
            name: Some("event_id".into()),
            r#type: Some("bigint".into()),
            comment: None,
        };
        let col = field_schema_to_column_def(&f);
        assert_eq!(col.name, "event_id");
        assert_eq!(col.data_type, "BIGINT");
        assert!(col.nullable);
    }

    #[test]
    fn column_mapping_defaults_missing_type_to_string() {
        let f = hive_metastore::FieldSchema {
            name: Some("mystery_col".into()),
            r#type: None,
            comment: None,
        };
        let col = field_schema_to_column_def(&f);
        assert_eq!(col.data_type, "STRING");
    }

    #[tokio::test]
    async fn invalid_uri_is_rejected_before_any_network_call() {
        let result = HiveMetastoreCatalogProvider::new("not a valid host!!").await;
        assert!(result.is_err());
    }
}
