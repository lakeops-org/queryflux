//! Catalog discovery for QueryFlux — pluggable, format-agnostic integrations
//! (Glue, Iceberg REST, Snowflake, ...) behind one `CatalogProvider` trait
//! (`queryflux_core::catalog`), feeding schema-aware SQL translation.
//!
//! Implemented today: `Static`/`Caching`/`Fallback` (no external dependency) and
//! `Glue` (direct AWS Glue Data Catalog access — format-agnostic, unlike going
//! through Iceberg's own Glue catalog client). `EngineDelegate`/`HiveMetastore`
//! remain unimplemented — see `plans/` for the full design. Any unimplemented
//! variant, or a real integration that fails to build (bad credentials,
//! unreachable endpoint), degrades to a no-op `NullCatalogProvider` with a
//! startup warning rather than refusing to boot.

pub mod caching;
pub mod fallback;
pub mod glue;
pub mod static_provider;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use queryflux_core::catalog::{CatalogProvider, NullCatalogProvider};
use queryflux_core::config::CatalogProviderConfig;

pub use caching::CachingCatalogProvider;
pub use fallback::FallbackCatalogProvider;
pub use glue::GlueCatalogProvider;
pub use static_provider::StaticCatalogProvider;

/// Builds a live `CatalogProvider` tree from config. Recursive (`Caching`/`Fallback`
/// wrap other configs), hence the boxed-future return type rather than plain `async
/// fn` — Rust can't size a directly-recursive `async fn`'s state.
///
/// Never fails: an unimplemented or misconfigured integration logs a warning and
/// substitutes `NullCatalogProvider` for that leaf, mirroring how the rest of
/// QueryFlux's startup degrades rather than refuses to boot on a bad integration
/// (see `TranslationService::new_sqlglot`'s fallback in `crates/queryflux/src/main.rs`).
pub fn build_catalog_provider(
    cfg: &CatalogProviderConfig,
) -> Pin<Box<dyn Future<Output = Arc<dyn CatalogProvider>> + Send + '_>> {
    Box::pin(async move {
        match cfg {
            CatalogProviderConfig::Null => {
                Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Static { schemas } => {
                Arc::new(StaticCatalogProvider::new(schemas.clone())) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::EngineDelegate { cluster_group } => {
                tracing::warn!(
                    cluster_group = %cluster_group,
                    "catalogProvider: type 'engineDelegate' is not implemented yet — \
                     using a no-op catalog provider (schema-aware translation will \
                     fall back to dialect-only)"
                );
                Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Glue { region, auth } => {
                match GlueCatalogProvider::new(region.clone(), auth.clone()).await {
                    Ok(provider) => Arc::new(provider) as Arc<dyn CatalogProvider>,
                    Err(e) => {
                        tracing::warn!(
                            "catalogProvider: failed to build 'glue' provider ({e}) — \
                             using a no-op catalog provider (schema-aware translation \
                             will fall back to dialect-only)"
                        );
                        Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
                    }
                }
            }

            CatalogProviderConfig::HiveMetastore { .. } => {
                tracing::warn!(
                    "catalogProvider: type 'hiveMetastore' is not implemented yet — \
                     using a no-op catalog provider (schema-aware translation will \
                     fall back to dialect-only)"
                );
                Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Caching {
                ttl_seconds,
                max_entries,
                delegate,
            } => {
                let inner = build_catalog_provider(delegate).await;
                Arc::new(CachingCatalogProvider::new(
                    inner,
                    *ttl_seconds,
                    *max_entries,
                )) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Fallback { primary, secondary } => {
                let primary = build_catalog_provider(primary).await;
                let secondary = build_catalog_provider(secondary).await;
                Arc::new(FallbackCatalogProvider::new(primary, secondary))
                    as Arc<dyn CatalogProvider>
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn null_config_yields_null_provider() {
        let provider = build_catalog_provider(&CatalogProviderConfig::Null).await;
        assert!(provider.is_null());
    }

    #[tokio::test]
    async fn unimplemented_variants_degrade_to_null_rather_than_panic() {
        let provider = build_catalog_provider(&CatalogProviderConfig::EngineDelegate {
            cluster_group: "trino-default".to_string(),
        })
        .await;
        assert!(provider.list_catalogs().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn caching_and_fallback_compose_recursively() {
        let cfg = CatalogProviderConfig::Caching {
            ttl_seconds: 60,
            max_entries: 100,
            delegate: Box::new(CatalogProviderConfig::Fallback {
                primary: Box::new(CatalogProviderConfig::Static { schemas: vec![] }),
                secondary: Box::new(CatalogProviderConfig::Null),
            }),
        };
        let provider = build_catalog_provider(&cfg).await;
        // Just proving the tree builds and is callable end to end.
        assert!(provider.list_catalogs().await.unwrap().is_empty());
    }
}
