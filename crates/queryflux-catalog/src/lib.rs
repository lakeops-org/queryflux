//! Catalog discovery for QueryFlux — pluggable, format-agnostic integrations
//! (Glue, Iceberg REST, Hive Metastore, ...) behind one `CatalogProvider` trait
//! (`queryflux_core::catalog`), feeding schema-aware SQL translation.
//!
//! Implemented: `Glue` (direct AWS Glue Data Catalog access), `IcebergRest`
//! (Iceberg REST Catalog protocol — Polaris, Tabular, etc.), `HiveMetastore`
//! (raw Thrift protocol), and `Fallback` (composes two providers,
//! primary-then-secondary). A real integration that fails to build (bad
//! credentials, unreachable endpoint) degrades to a no-op `NullCatalogProvider`
//! with a startup warning rather than refusing to boot.
//!
//! Caching is a `cache: Option<CatalogCacheConfig>` field on each real provider's
//! own config, not a separate wrapper type an operator has to remember to nest —
//! see `maybe_cached`.

pub mod caching;
pub mod fallback;
pub mod glue;
pub mod hive_metastore;
pub mod iceberg_rest;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use queryflux_core::catalog::{CatalogProvider, NullCatalogProvider};
use queryflux_core::config::{CatalogCacheConfig, CatalogProviderConfig};
use queryflux_core::error::{QueryFluxError, Result};

pub use caching::CachingCatalogProvider;
pub use fallback::FallbackCatalogProvider;
pub use glue::GlueCatalogProvider;
pub use hive_metastore::HiveMetastoreCatalogProvider;
pub use iceberg_rest::IcebergRestCatalogProvider;

/// A boxed, pinned future for the (necessarily recursive, thanks to `Fallback`)
/// provider-building functions below — factored out so clippy's
/// `type_complexity` lint doesn't fire on the inline spelling at each use site.
type BoxCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Wraps `provider` in a `CachingCatalogProvider` when `cache` is configured;
/// otherwise warns that every lookup will hit the backing service directly. Only
/// called for integrations that make a real network call per lookup — nothing
/// to cache (or warn about) for `Null`/`Fallback`.
fn maybe_cached(
    provider: Arc<dyn CatalogProvider>,
    cache: &Option<CatalogCacheConfig>,
) -> Arc<dyn CatalogProvider> {
    match cache {
        Some(c) => Arc::new(CachingCatalogProvider::new(
            provider,
            c.ttl_seconds,
            c.max_entries,
        )) as Arc<dyn CatalogProvider>,
        None => {
            tracing::warn!(
                "catalogProvider: this provider makes a real network call on every \
                 uncached lookup — consider setting its `cache` field (schema \
                 rarely changes, so a TTL of several minutes or more is usually \
                 safe) to avoid adding that latency to every query"
            );
            provider
        }
    }
}

/// Builds one leaf config into a live provider, surfacing the real construction
/// error instead of degrading to `NullCatalogProvider` — the counterpart to
/// `build_catalog_provider` used where the *reason* a config doesn't work
/// matters (the admin `/test` endpoint), rather than startup, where any error
/// must always degrade instead of blocking boot. Doesn't apply `cache` — a
/// connectivity check doesn't need the wrapper, only the underlying client.
///
/// `Fallback` builds a real `FallbackCatalogProvider` when both sides build, so
/// a subsequent connectivity check against the result exercises the same
/// fall-through-on-error semantics it would have at runtime, rather than only
/// ever testing whichever side happened to build first. It succeeds if
/// *either* side builds (that's the guarantee `fallback` makes at runtime),
/// only reporting failure — with both underlying errors — when neither does.
pub fn try_build_catalog_provider(
    cfg: &CatalogProviderConfig,
) -> BoxCatalogFuture<'_, Result<Arc<dyn CatalogProvider>>> {
    Box::pin(async move {
        match cfg {
            CatalogProviderConfig::Null => {
                Ok(Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>)
            }

            CatalogProviderConfig::Glue { region, auth, .. } => Ok(Arc::new(
                GlueCatalogProvider::new(region.clone(), auth.clone()).await?,
            )
                as Arc<dyn CatalogProvider>),

            CatalogProviderConfig::HiveMetastore { uri, .. } => {
                Ok(Arc::new(HiveMetastoreCatalogProvider::new(uri).await?)
                    as Arc<dyn CatalogProvider>)
            }

            CatalogProviderConfig::IcebergRest {
                uri,
                warehouse,
                catalog_name,
                auth,
                ..
            } => Ok(Arc::new(
                IcebergRestCatalogProvider::new(
                    catalog_name,
                    uri,
                    warehouse.as_deref(),
                    auth.as_ref(),
                )
                .await?,
            ) as Arc<dyn CatalogProvider>),

            CatalogProviderConfig::Fallback { primary, secondary } => {
                let primary_result = try_build_catalog_provider(primary).await;
                let secondary_result = try_build_catalog_provider(secondary).await;
                match (primary_result, secondary_result) {
                    (Ok(p), Ok(s)) => {
                        Ok(Arc::new(FallbackCatalogProvider::new(p, s)) as Arc<dyn CatalogProvider>)
                    }
                    // Only one side built — that's still a usable provider on
                    // its own, and matches how `build_catalog_provider` would
                    // degrade the other side to `NullCatalogProvider` anyway.
                    (Ok(p), Err(_)) => Ok(p),
                    (Err(_), Ok(s)) => Ok(s),
                    (Err(primary_err), Err(secondary_err)) => Err(QueryFluxError::Catalog(format!(
                        "both sides of 'fallback' failed to build — primary: {primary_err}; secondary: {secondary_err}"
                    ))),
                }
            }
        }
    })
}

/// Builds a live `CatalogProvider` tree from config. Recursive (`Fallback` wraps
/// two more configs), hence the boxed-future return type rather than plain `async
/// fn` — Rust can't size a directly-recursive `async fn`'s state.
///
/// Never fails: a misconfigured integration logs a warning and substitutes
/// `NullCatalogProvider` for that leaf (via `try_build_catalog_provider`),
/// mirroring how the rest of QueryFlux's startup degrades rather than refuses
/// to boot on a bad integration (see `TranslationService::new_sqlglot`'s
/// fallback in `crates/queryflux/src/main.rs`).
pub fn build_catalog_provider(
    cfg: &CatalogProviderConfig,
) -> BoxCatalogFuture<'_, Arc<dyn CatalogProvider>> {
    Box::pin(async move {
        match cfg {
            CatalogProviderConfig::Null => {
                Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
            }

            CatalogProviderConfig::Glue { cache, .. }
            | CatalogProviderConfig::HiveMetastore { cache, .. }
            | CatalogProviderConfig::IcebergRest { cache, .. } => {
                match try_build_catalog_provider(cfg).await {
                    Ok(provider) => maybe_cached(provider, cache),
                    Err(e) => {
                        tracing::warn!(
                            "catalogProvider: failed to build provider ({e}) — using a \
                             no-op catalog provider (schema-aware translation will fall \
                             back to dialect-only)"
                        );
                        Arc::new(NullCatalogProvider) as Arc<dyn CatalogProvider>
                    }
                }
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
    async fn unbuildable_provider_degrades_to_null_rather_than_panic() {
        // Unresolvable host — fails at construction (DNS resolution), no network
        // I/O needed to prove the degrade-not-panic contract.
        let provider = build_catalog_provider(&CatalogProviderConfig::HiveMetastore {
            uri: "not a valid host!!".to_string(),
            cache: None,
        })
        .await;
        assert!(provider.list_catalogs().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fallback_composes_recursively() {
        let cfg = CatalogProviderConfig::Fallback {
            primary: Box::new(CatalogProviderConfig::HiveMetastore {
                uri: "not a valid host!!".to_string(),
                cache: None,
            }),
            secondary: Box::new(CatalogProviderConfig::Null),
        };
        let provider = build_catalog_provider(&cfg).await;
        // Just proving the tree builds and is callable end to end.
        assert!(provider.list_catalogs().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn try_build_surfaces_the_real_construction_error() {
        // The whole reason this function exists over `build_catalog_provider`:
        // the admin `/test` endpoint needs the actual reason, not a silent
        // degrade to a no-op.
        let result = try_build_catalog_provider(&CatalogProviderConfig::HiveMetastore {
            uri: "not a valid host!!".to_string(),
            cache: None,
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn try_build_fallback_succeeds_if_either_side_builds() {
        let cfg = CatalogProviderConfig::Fallback {
            primary: Box::new(CatalogProviderConfig::HiveMetastore {
                uri: "not a valid host!!".to_string(),
                cache: None,
            }),
            secondary: Box::new(CatalogProviderConfig::Null),
        };
        // Primary fails to build, but secondary (`Null`) always succeeds — this
        // must not report an overall failure, mirroring `FallbackCatalogProvider`'s
        // own runtime resilience.
        assert!(try_build_catalog_provider(&cfg).await.is_ok());
    }

    #[tokio::test]
    async fn try_build_fallback_fails_only_when_both_sides_fail() {
        let cfg = CatalogProviderConfig::Fallback {
            primary: Box::new(CatalogProviderConfig::HiveMetastore {
                uri: "not a valid host!!".to_string(),
                cache: None,
            }),
            secondary: Box::new(CatalogProviderConfig::HiveMetastore {
                uri: "also not a valid host!!".to_string(),
                cache: None,
            }),
        };
        // `Arc<dyn CatalogProvider>` isn't `Debug`, so match instead of `unwrap_err`.
        let err = match try_build_catalog_provider(&cfg).await {
            Err(e) => e,
            Ok(_) => panic!("expected both sides to fail to build"),
        };
        // The combined message should mention both sides so a caller doesn't
        // have to guess which one failed.
        assert!(err.to_string().contains("primary"));
        assert!(err.to_string().contains("secondary"));
    }
}
