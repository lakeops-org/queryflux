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
    build_catalog_provider_inner(cfg, false)
}

/// Integrations that make a real network call per lookup — worth flagging when
/// they're configured without a `Caching` ancestor, since every schema-aware
/// translation attempt on a table QueryFlux hasn't seen recently will otherwise
/// pay that round-trip. Grows as more real integrations (Iceberg REST, ...) land.
fn is_network_calling(cfg: &CatalogProviderConfig) -> bool {
    matches!(cfg, CatalogProviderConfig::Glue { .. })
}

/// `cached`: true when this call is already inside a `Caching` ancestor —
/// threaded down (not part of the public signature, which callers shouldn't
/// need to know about) so a network-calling leaf can warn when it isn't.
fn build_catalog_provider_inner(
    cfg: &CatalogProviderConfig,
    cached: bool,
) -> Pin<Box<dyn Future<Output = Arc<dyn CatalogProvider>> + Send + '_>> {
    Box::pin(async move {
        if is_network_calling(cfg) && !cached {
            tracing::warn!(
                "catalogProvider: this provider makes a real network call on every \
                 uncached lookup — consider wrapping it in `type: caching` \
                 (schema rarely changes, so a TTL of several minutes or more is \
                 usually safe) to avoid adding that latency to every query"
            );
        }
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
                let inner = build_catalog_provider_inner(delegate, true).await;
                Arc::new(CachingCatalogProvider::new(
                    inner,
                    *ttl_seconds,
                    *max_entries,
                )) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Fallback { primary, secondary } => {
                // `cached` propagates unchanged: wrapping the whole Fallback in
                // Caching covers both sides, but a bare Fallback over two
                // network-calling providers should warn for each independently.
                let primary = build_catalog_provider_inner(primary, cached).await;
                let secondary = build_catalog_provider_inner(secondary, cached).await;
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

    #[test]
    fn is_network_calling_flags_glue_only() {
        assert!(is_network_calling(&CatalogProviderConfig::Glue {
            region: None,
            auth: None,
        }));
        assert!(!is_network_calling(&CatalogProviderConfig::Null));
        assert!(!is_network_calling(&CatalogProviderConfig::Static {
            schemas: vec![]
        }));
        // Caching/Fallback/EngineDelegate/HiveMetastore are checked at their own
        // leaves during recursion, not flagged as "network-calling" themselves.
        assert!(!is_network_calling(
            &CatalogProviderConfig::EngineDelegate {
                cluster_group: "g".to_string()
            }
        ));
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
