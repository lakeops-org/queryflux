//! Everything QueryFlux knows about a SQL text: parsing and classification (polyglot-sql),
//! fingerprints, table and column extraction for access control, and translation (sqlglot).
//! See `plans/queryflux-sql-plan.md`.

pub mod access;
pub mod classify;
mod eligibility;
pub mod fingerprint;
pub mod params;
pub mod polyglot_pool;
pub mod sqlglot;

pub use eligibility::is_cacheable;

use queryflux_core::bounded_cache::{BoundedCache, ContentHasher, MAX_CACHED_SQL_BYTES};

pub use access::{
    apply_write_filters, extract_resources, extract_resources_cached, extraction_cache,
    render_mask, rewrite_table_scans, rewrite_table_scans_parsed, ExtractedResource,
    ExtractedStatement, MaskRenderError, TablePolicy,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use classify::SqlParseCache;

use async_trait::async_trait;
pub use queryflux_core::schema_context::{ColumnMap, SchemaContext};
use queryflux_core::{
    catalog::CatalogProvider,
    config::{SqlCacheConfig, TranslationMode},
    error::{QueryFluxError, Result},
    query::{SqlDialect, TranslationOutcome, TranslationReason, TranslationStatus},
};
pub use sqlglot::{extract_table_refs_async, SqlglotTranslator, TableRef};

/// Translates SQL from one dialect to another.
///
/// The primary implementation (`SqlglotTranslator`) uses the sqlglot Python library
/// via PyO3. Additional implementations can provide custom fixups or passthrough.
#[async_trait]
pub trait TranslatorTrait: Send + Sync {
    fn source_dialect(&self) -> &SqlDialect;
    fn target_dialect(&self) -> &SqlDialect;

    /// Translate `sql` from `source_dialect` to `target_dialect`.
    /// `schema_context` is optional — when provided, sqlglot uses schema-aware
    /// optimization for more accurate type handling.
    async fn translate(&self, sql: &str, schema_context: &SchemaContext) -> Result<String>;
}

/// Passthrough translator — returns the SQL unchanged.
/// Used when source and target dialects are the same.
pub struct PassthroughTranslator {
    dialect: SqlDialect,
}

impl PassthroughTranslator {
    pub fn new(dialect: SqlDialect) -> Self {
        Self { dialect }
    }
}

#[async_trait]
impl TranslatorTrait for PassthroughTranslator {
    fn source_dialect(&self) -> &SqlDialect {
        &self.dialect
    }
    fn target_dialect(&self) -> &SqlDialect {
        &self.dialect
    }
    async fn translate(&self, sql: &str, _schema_context: &SchemaContext) -> Result<String> {
        Ok(sql.to_string())
    }
}

/// Central translation service.
///
/// Call `maybe_translate` before submitting SQL to a backend engine.
/// Returns the original SQL unchanged when dialects are compatible and no fixups run.
///
/// User-defined Python scripts run after every sqlglot translation. Each script
/// must define `def transform(sql: str, src: str, dst: str) -> str:`. Top-level
/// imports and helper functions are fully supported. Scripts receive SQL text and
/// return SQL text — a script that needs AST-level control can `import sqlglot`
/// (or any other parser) itself and return `.sql(dialect=dst)`.
/// Default catalog-lookup timeout for `resolve_schema_context` when the caller
/// doesn't override it via `with_schema_resolution_timeout` — matches
/// `TranslationConfig`'s own default.
const DEFAULT_SCHEMA_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(1500);

pub struct TranslationService {
    enabled: bool,
    /// `mode == Strict`. Atomic so a worker can apply the policy its control plane sends.
    strict_mode: std::sync::atomic::AtomicBool,
    error_on_unsupported: std::sync::atomic::AtomicBool,
    python_scripts: Vec<String>,
    schema_resolution_timeout: Duration,
    /// sqlglot results for repeated queries. See [`TranslationKey`].
    translations: TranslationCache,
    /// Table references `resolve_schema_context` looks up, by (SQL, dialect).
    table_refs: TableRefsCache,
}

/// Everything the sqlglot call in `maybe_translate_report` depends on. The SQL is kept
/// verbatim so two statements can never share an entry; scripts and schema, which can be
/// large, are content hashes. The policy is part of the key because strict mode changes
/// what sqlglot itself returns (`unsupported_level=RAISE`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TranslationKey {
    sql: String,
    /// sqlglot dialect names: exactly what the Python side receives.
    src: String,
    tgt: String,
    /// Global then group fixup scripts, in order.
    scripts: u128,
    schema: u128,
    strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableRefsKey {
    sql: String,
    dialect: String,
}

/// A cached sqlglot result. Only deterministic failures are stored (see
/// `SqlglotFailure::cacheable`); the report is rebuilt from it on every hit, so the
/// best-effort/strict mapping is still applied per query.
#[derive(Debug)]
enum CachedTranslation {
    Ok(String, Option<TranslationReason>),
    Err {
        message: String,
        reason: TranslationReason,
        reject_passthrough: bool,
    },
}

impl CachedTranslation {
    fn to_result(
        &self,
    ) -> std::result::Result<(String, Option<TranslationReason>), sqlglot::SqlglotFailure> {
        match self {
            CachedTranslation::Ok(sql, fallback) => Ok((sql.clone(), *fallback)),
            CachedTranslation::Err {
                message,
                reason,
                reject_passthrough,
            } => Err(sqlglot::SqlglotFailure {
                error: QueryFluxError::Translation(message.clone()),
                reason: *reason,
                reject_passthrough: *reject_passthrough,
                cacheable: true,
            }),
        }
    }

    fn from_result(
        result: &std::result::Result<(String, Option<TranslationReason>), sqlglot::SqlglotFailure>,
    ) -> Option<Self> {
        match result {
            Ok((sql, fallback)) => Some(CachedTranslation::Ok(sql.clone(), *fallback)),
            Err(failure) if failure.cacheable => match &failure.error {
                QueryFluxError::Translation(message) => Some(CachedTranslation::Err {
                    message: message.clone(),
                    reason: failure.reason,
                    reject_passthrough: failure.reject_passthrough,
                }),
                _ => None,
            },
            Err(_) => None,
        }
    }
}

fn scripts_hash(scripts: &[String]) -> u128 {
    let mut h = ContentHasher::new();
    h.write_u64(scripts.len() as u64);
    for script in scripts {
        h.write_str(script);
    }
    h.finish()
}

type TranslationCache = BoundedCache<TranslationKey, Arc<CachedTranslation>>;
type TableRefsCache = BoundedCache<TableRefsKey, Arc<Vec<TableRef>>>;

fn new_caches(config: &SqlCacheConfig) -> (TranslationCache, TableRefsCache) {
    (
        BoundedCache::new("translation", config.translations),
        BoundedCache::new("schema_table_refs", config.table_extraction),
    )
}

impl TranslationService {
    /// Create a service backed by sqlglot with optional user fixup scripts.
    /// Verifies sqlglot is importable at startup.
    pub fn new_sqlglot(python_scripts: Vec<String>) -> Result<Self> {
        SqlglotTranslator::check_available()?;
        // YAML is operator-authored and read once at startup, so — unlike the DB-persisted
        // per-group scripts in `queryflux::validate_group_translation_scripts`, which are
        // filtered rather than fatal — a script that fails the current contract aborts
        // startup here, consistent with this codebase's fail-startup-on-bad-explicit-config
        // convention elsewhere (e.g. access-control YAML validation).
        for script in &python_scripts {
            sqlglot::validate_fixup_script(script)?;
        }
        let (translations, table_refs) = new_caches(&SqlCacheConfig::default());
        Ok(Self {
            enabled: true,
            strict_mode: std::sync::atomic::AtomicBool::new(false),
            error_on_unsupported: std::sync::atomic::AtomicBool::new(false),
            python_scripts,
            schema_resolution_timeout: DEFAULT_SCHEMA_RESOLUTION_TIMEOUT,
            translations,
            table_refs,
        })
    }

    /// Create an unavailable service with the default best-effort policy.
    pub fn disabled() -> Self {
        let (translations, table_refs) = new_caches(&SqlCacheConfig::default());
        Self {
            enabled: false,
            strict_mode: std::sync::atomic::AtomicBool::new(false),
            error_on_unsupported: std::sync::atomic::AtomicBool::new(false),
            python_scripts: Vec::new(),
            schema_resolution_timeout: DEFAULT_SCHEMA_RESOLUTION_TIMEOUT,
            translations,
            table_refs,
        }
    }

    /// Size this service's caches (`translation.cache`): `translations` for sqlglot results,
    /// `tableExtraction` for the table references schema resolution looks up. 0 disables.
    pub fn with_cache_config(self, config: &SqlCacheConfig) -> Self {
        self.translations.set_capacity(config.translations);
        self.table_refs.set_capacity(config.table_extraction);
        self
    }

    /// Hit/miss/eviction counters of the translation cache.
    pub fn translation_cache_stats(&self) -> queryflux_core::bounded_cache::CacheStatsSnapshot {
        self.translations.stats()
    }

    /// Hit/miss/eviction counters of the schema table-reference cache.
    pub fn table_refs_cache_stats(&self) -> queryflux_core::bounded_cache::CacheStatsSnapshot {
        self.table_refs.stats()
    }

    /// Keep configured fixups even if sqlglot is unavailable at startup.
    pub fn unavailable(python_scripts: Vec<String>) -> Self {
        Self {
            python_scripts,
            ..Self::disabled()
        }
    }

    pub fn with_policy(self, mode: TranslationMode, error_on_unsupported: bool) -> Self {
        self.set_policy(mode, error_on_unsupported);
        self
    }

    /// The current failure policy: `(mode, error_on_unsupported)`.
    pub fn policy(&self) -> (TranslationMode, bool) {
        use std::sync::atomic::Ordering;
        let mode = if self.strict_mode.load(Ordering::Relaxed) {
            TranslationMode::Strict
        } else {
            TranslationMode::BestEffort
        };
        (mode, self.error_on_unsupported.load(Ordering::Relaxed))
    }

    /// Replace the failure policy at runtime (a worker applying its control plane's).
    pub fn set_policy(&self, mode: TranslationMode, error_on_unsupported: bool) {
        use std::sync::atomic::Ordering;
        self.strict_mode
            .store(mode == TranslationMode::Strict, Ordering::Relaxed);
        self.error_on_unsupported
            .store(error_on_unsupported, Ordering::Relaxed);
    }

    /// Global scripts can change SQL semantics after result-cache eligibility checks.
    pub fn has_global_fixups(&self) -> bool {
        !self.python_scripts.is_empty()
    }

    /// Overrides the default catalog-lookup timeout `resolve_schema_context` uses
    /// (config: `translation.schemaResolutionTimeoutMs`).
    pub fn with_schema_resolution_timeout(mut self, timeout: Duration) -> Self {
        self.schema_resolution_timeout = timeout;
        self
    }

    /// Translate `sql` from `src` to `tgt` if they differ.
    /// Compatible dialects without fixups bypass translation. Required translation
    /// failures follow the configured policy (best-effort passthrough or strict rejection).
    /// Fixup statement-kind violations always reject, regardless of policy.
    ///
    /// `group_fixups` are appended after global YAML `translation.pythonScripts` (same contract).
    pub async fn maybe_translate(
        &self,
        sql: &str,
        src: &SqlDialect,
        tgt: &SqlDialect,
        schema: &SchemaContext,
        group_fixups: &[String],
    ) -> Result<String> {
        self.maybe_translate_report(sql, src, tgt, schema, group_fixups)
            .await
            .result
    }

    /// Returns an outcome even on strict rejection, so callers can count and record it.
    pub async fn maybe_translate_report(
        &self,
        sql: &str,
        src: &SqlDialect,
        tgt: &SqlDialect,
        schema: &SchemaContext,
        group_fixups: &[String],
    ) -> TranslationReport {
        let mut combined = self.python_scripts.clone();
        combined.extend_from_slice(group_fixups);
        if src.is_compatible_with(tgt) && combined.is_empty() {
            return TranslationReport {
                result: Ok(sql.to_string()),
                outcome: TranslationOutcome::not_needed(),
            };
        }
        let strict = self.strict_mode.load(std::sync::atomic::Ordering::Relaxed)
            || self
                .error_on_unsupported
                .load(std::sync::atomic::Ordering::Relaxed);
        let result = if self.enabled {
            self.translate_cached(sql, src, tgt, schema, combined, strict)
                .await
        } else {
            Err(sqlglot::SqlglotFailure {
                error: QueryFluxError::Translation("required translation unavailable: sqlglot is not installed or could not be imported".into()),
                reason: TranslationReason::SqlglotUnavailable,
                reject_passthrough: false,
                cacheable: false,
            })
        };
        match result {
            Ok((translated, fallback)) => TranslationReport {
                result: Ok(translated),
                outcome: TranslationOutcome {
                    status: if fallback.is_some() {
                        TranslationStatus::Fallback
                    } else {
                        TranslationStatus::Yes
                    },
                    reason: fallback,
                },
            },
            Err(failure) => TranslationReport {
                result: if strict || failure.reject_passthrough {
                    Err(failure.error)
                } else {
                    Ok(sql.to_string())
                },
                outcome: TranslationOutcome {
                    status: TranslationStatus::No,
                    reason: Some(failure.reason),
                },
            },
        }
    }

    /// Run sqlglot, or reuse its result for an identical earlier call.
    async fn translate_cached(
        &self,
        sql: &str,
        src: &SqlDialect,
        tgt: &SqlDialect,
        schema: &SchemaContext,
        scripts: Vec<String>,
        strict: bool,
    ) -> std::result::Result<(String, Option<TranslationReason>), sqlglot::SqlglotFailure> {
        let key =
            (self.translations.is_enabled() && sql.len() <= MAX_CACHED_SQL_BYTES).then(|| {
                TranslationKey {
                    sql: sql.to_string(),
                    src: src.sqlglot_write_name(),
                    tgt: tgt.sqlglot_write_name(),
                    scripts: scripts_hash(&scripts),
                    schema: schema.content_hash(),
                    strict,
                }
            });
        if let Some(cached) = key.as_ref().and_then(|k| self.translations.get(k)) {
            return cached.to_result();
        }
        let result = SqlglotTranslator::new(src.clone(), tgt.clone(), scripts)
            .with_error_on_unsupported(strict)
            .translate_detailed(sql, schema)
            .await;
        if let (Some(key), Some(entry)) = (key, CachedTranslation::from_result(&result)) {
            self.translations.insert(key, Arc::new(entry));
        }
        result
    }

    /// Table references in `sql`, cached by (SQL, dialect). Parse failures yield an empty
    /// list (deterministic, cached); import errors, panics and timeouts are not cached.
    async fn table_refs(&self, parse: &Arc<SqlParseCache>) -> Option<Arc<Vec<TableRef>>> {
        let sql = parse.sql();
        let key = (self.table_refs.is_enabled() && sql.len() <= MAX_CACHED_SQL_BYTES).then(|| {
            TableRefsKey {
                sql: sql.to_string(),
                dialect: parse.dialect().sqlglot_write_name().to_string(),
            }
        });
        if let Some(refs) = key.as_ref().and_then(|k| self.table_refs.get(k)) {
            return Some(refs);
        }
        let refs = match tokio::time::timeout(
            self.schema_resolution_timeout,
            extract_table_refs_async(parse.clone()),
        )
        .await
        {
            Ok(Ok(refs)) => Arc::new(refs),
            Ok(Err(e)) => {
                tracing::debug!("resolve_schema_context: table-ref extraction failed: {e}");
                return None;
            }
            Err(_) => {
                tracing::debug!("resolve_schema_context: table-ref extraction timed out");
                return None;
            }
        };
        if let Some(key) = key {
            self.table_refs.insert(key, refs.clone());
        }
        Some(refs)
    }

    /// Best-effort schema resolution: extract the tables `sql` references, look them
    /// up via `catalog`, and flatten the result into a `SchemaContext` for
    /// `maybe_translate`. **Never fails** — a parse error, catalog error, or timeout
    /// all degrade to `SchemaContext::default()` (today's behavior without any
    /// catalog configured), so calling this can only ever improve translation
    /// accuracy, never block or break a query. Skips SQL parsing entirely when
    /// `catalog.is_null()`, so a query pays nothing extra when no catalog is configured.
    /// Uses `self`'s configured timeout (default 1500ms, see
    /// `with_schema_resolution_timeout`) for both the extraction step and the
    /// catalog-lookup step, budgeted separately.
    pub async fn resolve_schema_context(
        &self,
        sql: &str,
        src_dialect: &SqlDialect,
        catalog: &Arc<dyn CatalogProvider>,
        default_catalog: Option<&str>,
        default_database: Option<&str>,
    ) -> SchemaContext {
        let parse = Arc::new(SqlParseCache::new(sql, src_dialect.clone()));
        self.resolve_schema_context_parsed(&parse, catalog, default_catalog, default_database)
            .await
    }

    /// [`Self::resolve_schema_context`] reading the table references from `parse`'s
    /// shared sqlglot tree.
    pub async fn resolve_schema_context_parsed(
        &self,
        parse: &Arc<SqlParseCache>,
        catalog: &Arc<dyn CatalogProvider>,
        default_catalog: Option<&str>,
        default_database: Option<&str>,
    ) -> SchemaContext {
        if catalog.is_null() {
            return SchemaContext::default();
        }
        let timeout = self.schema_resolution_timeout;

        let refs = match self.table_refs(parse).await {
            Some(refs) if !refs.is_empty() => refs,
            _ => return SchemaContext::default(),
        };

        let tables: Vec<&str> = refs.iter().map(|r| r.table.as_str()).collect();
        let resolved_catalog = refs
            .first()
            .and_then(|r| r.catalog.as_deref())
            .or(default_catalog);
        let resolved_database = refs
            .first()
            .and_then(|r| r.database.as_deref())
            .or(default_database);

        let schemas = match tokio::time::timeout(
            timeout,
            catalog.get_schemas_for_query(resolved_catalog, resolved_database, &tables),
        )
        .await
        {
            Ok(Ok(schemas)) => schemas,
            Ok(Err(e)) => {
                tracing::debug!("resolve_schema_context: catalog lookup failed: {e}");
                return SchemaContext::default();
            }
            Err(_) => {
                tracing::debug!("resolve_schema_context: catalog lookup timed out");
                return SchemaContext::default();
            }
        };

        let mut tables_map = HashMap::new();
        for schema in schemas {
            let cols: ColumnMap = schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            tables_map.insert(schema.table.clone(), cols);
        }

        SchemaContext {
            catalog: resolved_catalog.map(String::from),
            database: resolved_database.map(String::from),
            tables: tables_map,
        }
    }
}

/// Translation result together with metadata retained on success and rejection.
pub struct TranslationReport {
    pub result: Result<String>,
    pub outcome: TranslationOutcome,
}
