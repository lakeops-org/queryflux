//! AWS Glue Data Catalog integration.
//!
//! Talks directly to the Glue API (`aws-sdk-glue`) rather than through Iceberg's
//! own Glue catalog client — deliberately, so it sees tables of any format Glue
//! tracks (Iceberg, Hive/Parquet, CSV/JSON, ...), not just Iceberg ones. Mirrors
//! the Athena adapter's AWS credential / AssumeRole setup
//! (`crates/queryflux-engine-adapters/src/athena/mod.rs`) so the two behave
//! consistently for operators already familiar with one.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::config::ClusterAuth;
use queryflux_core::error::{QueryFluxError, Result};

/// Glue has no catalog concept of its own — every database/table lives under
/// the caller's AWS account. This is the single synthetic catalog name
/// `list_catalogs()` reports, matching the convention the Athena adapter
/// already uses for its own default-catalog handling.
const DEFAULT_CATALOG_NAME: &str = "AwsDataCatalog";

#[derive(Debug)]
pub struct GlueCatalogProvider {
    client: aws_sdk_glue::Client,
}

impl GlueCatalogProvider {
    /// Builds the Glue client. `auth` reuses the same `ClusterAuth` shape as
    /// engine cluster config; only `AccessKey`/`RoleArn`/absent (default AWS
    /// credential chain) are meaningful here — `Basic`/`Bearer`/`KeyPair` are
    /// rejected since they don't apply to AWS.
    pub async fn new(region: Option<String>, auth: Option<ClusterAuth>) -> Result<Self> {
        let aws_region = region.map(Region::new);

        let sdk_config = match auth {
            Some(ClusterAuth::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                let creds = Credentials::new(
                    access_key_id,
                    secret_access_key,
                    session_token,
                    None,
                    "queryflux-static",
                );
                let mut builder =
                    aws_config::defaults(BehaviorVersion::latest()).credentials_provider(creds);
                if let Some(r) = aws_region {
                    builder = builder.region(r);
                }
                builder.load().await
            }
            Some(ClusterAuth::RoleArn {
                role_arn,
                external_id,
            }) => {
                // Refreshing AssumeRole provider (not a one-shot AssumeRole call) so
                // temporary STS credentials renew before they expire — same reasoning
                // as the Athena adapter's identical setup.
                let mut base_builder = aws_config::defaults(BehaviorVersion::latest());
                if let Some(r) = aws_region.clone() {
                    base_builder = base_builder.region(r);
                }
                let base_config = base_builder.load().await;
                let mut role_builder = aws_config::sts::AssumeRoleProvider::builder(&role_arn)
                    .session_name("queryflux-catalog")
                    .configure(&base_config);
                if let Some(eid) = external_id {
                    role_builder = role_builder.external_id(eid);
                }
                let provider = role_builder.build().await;
                let mut builder =
                    aws_config::defaults(BehaviorVersion::latest()).credentials_provider(provider);
                if let Some(r) = aws_region {
                    builder = builder.region(r);
                }
                builder.load().await
            }
            Some(other) => {
                return Err(QueryFluxError::Config(format!(
                    "catalogProvider glue: unsupported auth type {other:?} — use \
                     accessKey or roleArn, or omit auth for the default AWS \
                     credential chain"
                )));
            }
            None => {
                let mut builder = aws_config::defaults(BehaviorVersion::latest());
                if let Some(r) = aws_region {
                    builder = builder.region(r);
                }
                builder.load().await
            }
        };

        let mut glue_builder = aws_sdk_glue::config::Builder::from(&sdk_config);
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
            if !endpoint.is_empty() {
                glue_builder = glue_builder.endpoint_url(endpoint);
            }
        }
        let client = aws_sdk_glue::Client::from_conf(glue_builder.build());

        Ok(Self { client })
    }
}

#[async_trait]
impl CatalogProvider for GlueCatalogProvider {
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        Ok(vec![DEFAULT_CATALOG_NAME.to_string()])
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self.client.get_databases();
            if let Some(t) = next_token {
                req = req.next_token(t);
            }
            let resp = req.send().await.map_err(|e| {
                QueryFluxError::Catalog(format!("Glue GetDatabases: {}", aws_err(&e)))
            })?;
            names.extend(resp.database_list().iter().map(|d| d.name().to_string()));
            next_token = resp.next_token().map(|t| t.to_string());
            if next_token.is_none() {
                break;
            }
        }
        Ok(names)
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self.client.get_tables().database_name(database);
            if let Some(t) = next_token {
                req = req.next_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| QueryFluxError::Catalog(format!("Glue GetTables: {}", aws_err(&e))))?;
            names.extend(resp.table_list().iter().map(|t| t.name().to_string()));
            next_token = resp.next_token().map(|t| t.to_string());
            if next_token.is_none() {
                break;
            }
        }
        Ok(names)
    }

    async fn get_table_schema(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let resp = match self
            .client
            .get_table()
            .database_name(database)
            .name(table)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|se| se.is_entity_not_found_exception())
                {
                    return Ok(None);
                }
                return Err(QueryFluxError::Catalog(format!(
                    "Glue GetTable: {}",
                    aws_err(&e)
                )));
            }
        };
        let Some(tbl) = resp.table() else {
            return Ok(None);
        };

        let mut columns: Vec<ColumnDef> = Vec::new();
        if let Some(sd) = tbl.storage_descriptor() {
            columns.extend(sd.columns().iter().map(glue_column_to_column_def));
        }
        // Partition keys are real, queryable columns on a Hive-style partitioned
        // table — the Athena adapter's own TableMetadata mapping folds them in
        // the same way, for the same reason (they're valid in WHERE/SELECT).
        columns.extend(tbl.partition_keys().iter().map(glue_column_to_column_def));

        Ok(Some(TableSchema {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

fn glue_column_to_column_def(c: &aws_sdk_glue::types::Column) -> ColumnDef {
    ColumnDef {
        name: c.name().to_string(),
        // Glue/Hive type strings (e.g. "bigint", "struct<a:int>") aren't
        // normalized to standard SQL here — sqlglot's MappingSchema accepts
        // most of them as-is. A dedicated type-mapping pass is future work if
        // this proves insufficient for the optimizer in practice.
        data_type: c.r#type().unwrap_or("string").to_uppercase(),
        // Glue's Column type carries no nullability — defaults to true, same
        // reasoning (and same default) as the Athena adapter's own mapping.
        nullable: true,
    }
}

/// Same error-chain formatter the Athena adapter uses, so Glue errors read the
/// same way in logs/admin-API responses as Athena's already do.
fn aws_err(e: &impl std::error::Error) -> String {
    let mut parts = vec![e.to_string()];
    let mut src = e.source();
    while let Some(s) = src {
        let s_str = s.to_string();
        if !parts.contains(&s_str) {
            parts.push(s_str);
        }
        src = s.source();
    }
    parts.join(": ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_mapping_uppercases_type_and_defaults_nullable_true() {
        let col = aws_sdk_glue::types::Column::builder()
            .name("order_id")
            .r#type("bigint")
            .build()
            .unwrap();
        let mapped = glue_column_to_column_def(&col);
        assert_eq!(mapped.name, "order_id");
        assert_eq!(mapped.data_type, "BIGINT");
        assert!(mapped.nullable);
    }

    #[test]
    fn column_mapping_defaults_missing_type_to_string() {
        let col = aws_sdk_glue::types::Column::builder()
            .name("mystery")
            .build()
            .unwrap();
        let mapped = glue_column_to_column_def(&col);
        assert_eq!(mapped.data_type, "STRING");
    }

    #[tokio::test]
    async fn unsupported_auth_type_is_rejected_before_any_network_call() {
        let err = GlueCatalogProvider::new(
            None,
            Some(ClusterAuth::Basic {
                username: "x".to_string(),
                password: "y".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("unsupported auth type"), "{err}");
    }
}
