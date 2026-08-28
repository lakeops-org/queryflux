pub mod sqlglot;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use queryflux_core::{catalog::CatalogProvider, error::Result, query::SqlDialect};
pub use sqlglot::{extract_table_refs_async, SqlglotTranslator, TableRef};

/// Schema context passed to the translator so sqlglot can produce accurate output.
/// Maps table name → { column name → SQL type string }.
#[derive(Debug, Default, Clone)]
pub struct SchemaContext {
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// table_name → { col_name → type_string }
    pub tables: HashMap<String, HashMap<String, String>>,
}

impl SchemaContext {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

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
/// Returns the original SQL unchanged when dialects match (zero overhead).
///
/// User-defined Python scripts run after every sqlglot translation. Each script
/// must define `def transform(ast, src: str, dst: str) -> None:`. Top-level
/// imports and helper functions are fully supported. Scripts mutate `ast`
/// in-place.
/// Default catalog-lookup timeout for `resolve_schema_context` when the caller
/// doesn't override it via `with_schema_resolution_timeout` — matches
/// `TranslationConfig`'s own default.
const DEFAULT_SCHEMA_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(1500);

pub struct TranslationService {
    enabled: bool,
    python_scripts: Vec<String>,
    schema_resolution_timeout: Duration,
}

impl TranslationService {
    /// Create a service backed by sqlglot with optional user fixup scripts.
    /// Verifies sqlglot is importable at startup.
    pub fn new_sqlglot(python_scripts: Vec<String>) -> Result<Self> {
        SqlglotTranslator::check_available()?;
        Ok(Self {
            enabled: true,
            python_scripts,
            schema_resolution_timeout: DEFAULT_SCHEMA_RESOLUTION_TIMEOUT,
        })
    }

    /// Create a no-op service (translation disabled).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            python_scripts: Vec::new(),
            schema_resolution_timeout: DEFAULT_SCHEMA_RESOLUTION_TIMEOUT,
        }
    }

    /// Overrides the default catalog-lookup timeout `resolve_schema_context` uses
    /// (config: `translation.schemaResolutionTimeoutMs`).
    pub fn with_schema_resolution_timeout(mut self, timeout: Duration) -> Self {
        self.schema_resolution_timeout = timeout;
        self
    }

    /// Translate `sql` from `src` to `tgt` if they differ.
    /// Returns the original SQL unchanged when dialects match or translation is disabled.
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
        if !self.enabled {
            return Ok(sql.to_string());
        }
        let mut combined = self.python_scripts.clone();
        combined.extend_from_slice(group_fixups);
        // Skip sqlglot when dialects are compatible AND no fixup scripts need to run.
        if src.is_compatible_with(tgt) && combined.is_empty() {
            return Ok(sql.to_string());
        }
        let translator = SqlglotTranslator::new(src.clone(), tgt.clone(), combined);
        translator.translate(sql, schema).await
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
        if catalog.is_null() {
            return SchemaContext::default();
        }
        let timeout = self.schema_resolution_timeout;

        let dialect = src_dialect.sqlglot_write_name().to_string();
        let refs =
            match tokio::time::timeout(timeout, extract_table_refs_async(sql.to_string(), dialect))
                .await
            {
                Ok(Ok(refs)) if !refs.is_empty() => refs,
                Ok(Ok(_)) => return SchemaContext::default(),
                Ok(Err(e)) => {
                    tracing::debug!("resolve_schema_context: table-ref extraction failed: {e}");
                    return SchemaContext::default();
                }
                Err(_) => {
                    tracing::debug!("resolve_schema_context: table-ref extraction timed out");
                    return SchemaContext::default();
                }
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
            let cols = schema
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
