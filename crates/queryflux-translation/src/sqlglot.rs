use std::collections::HashSet;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::SqlDialect,
};
use tracing::debug;

use crate::{SchemaContext, TranslatorTrait};

/// A table reference as written in the SQL, before catalog/database defaulting.
/// Distinct from (and unrelated to) the `TableRef` used elsewhere in this org for
/// query-history intelligence — this one is fed straight into catalog lookup for
/// schema-aware translation and never persisted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub database: Option<String>,
    pub table: String,
}

/// Parse `sql` under `dialect` and return every distinct table it references (via
/// sqlglot's `exp.Table` nodes), excluding names that resolve to a CTE defined in
/// the same query rather than a real table. Best-effort: a parse failure yields
/// `Ok(vec![])`, not an error — callers should treat that identically to "no
/// schema info available" and fall back to dialect-only translation.
pub fn extract_table_refs(sql: &str, dialect: &str) -> Result<Vec<TableRef>> {
    Python::attach(|py| extract_table_refs_with_gil(py, sql, dialect))
}

/// Async wrapper around `extract_table_refs`, off the async executor (same
/// `spawn_blocking` pattern `SqlglotTranslator::translate` already uses for GIL work).
pub async fn extract_table_refs_async(sql: String, dialect: String) -> Result<Vec<TableRef>> {
    tokio::task::spawn_blocking(move || extract_table_refs(&sql, &dialect))
        .await
        .map_err(|e| QueryFluxError::Translation(format!("spawn_blocking error: {e}")))?
}

fn extract_table_refs_with_gil(py: Python<'_>, sql: &str, dialect: &str) -> Result<Vec<TableRef>> {
    let sqlglot = PyModule::import(py, "sqlglot")
        .map_err(|e| QueryFluxError::Translation(format!("Failed to import sqlglot: {e}")))?;
    let exp = PyModule::import(py, "sqlglot.expressions").map_err(|e| {
        QueryFluxError::Translation(format!("Failed to import sqlglot.expressions: {e}"))
    })?;

    let parse_kwargs = PyDict::new(py);
    parse_kwargs.set_item("dialect", dialect).ok();
    let tree = match sqlglot.call_method("parse_one", (sql,), Some(&parse_kwargs)) {
        Ok(t) => t,
        Err(e) => {
            debug!("extract_table_refs: parse_one failed, treating as no refs: {e}");
            return Ok(vec![]);
        }
    };

    // CTE aliases shadow same-named real tables within this query — an unqualified
    // reference to one is never a catalog lookup target.
    let cte_cls = exp
        .getattr("CTE")
        .map_err(|e| QueryFluxError::Translation(format!("no sqlglot.expressions.CTE: {e}")))?;
    let ctes = tree
        .call_method1("find_all", (cte_cls,))
        .map_err(|e| QueryFluxError::Translation(format!("find_all(CTE) failed: {e}")))?;
    let mut cte_aliases: HashSet<String> = HashSet::new();
    for cte in ctes
        .try_iter()
        .map_err(|e| QueryFluxError::Translation(format!("iterate CTEs failed: {e}")))?
    {
        let cte = cte.map_err(|e| QueryFluxError::Translation(format!("CTE item failed: {e}")))?;
        // `alias_or_name` is a sqlglot property, not a method — plain attribute access.
        if let Ok(alias) = cte
            .getattr("alias_or_name")
            .and_then(|v| v.extract::<String>())
        {
            if !alias.is_empty() {
                cte_aliases.insert(alias);
            }
        }
    }

    let table_cls = exp
        .getattr("Table")
        .map_err(|e| QueryFluxError::Translation(format!("no sqlglot.expressions.Table: {e}")))?;
    let tables = tree
        .call_method1("find_all", (table_cls,))
        .map_err(|e| QueryFluxError::Translation(format!("find_all(Table) failed: {e}")))?;

    let mut refs = Vec::new();
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    for table in tables
        .try_iter()
        .map_err(|e| QueryFluxError::Translation(format!("iterate tables failed: {e}")))?
    {
        let table =
            table.map_err(|e| QueryFluxError::Translation(format!("table item failed: {e}")))?;
        let name: String = table
            .getattr("name")
            .and_then(|v| v.extract())
            .unwrap_or_default();
        if name.is_empty() {
            continue; // e.g. a derived/subquery table with no literal name
        }
        let catalog: String = table
            .getattr("catalog")
            .and_then(|v| v.extract())
            .unwrap_or_default();
        let database: String = table
            .getattr("db")
            .and_then(|v| v.extract())
            .unwrap_or_default();

        if catalog.is_empty() && database.is_empty() && cte_aliases.contains(&name) {
            continue;
        }

        let key = (catalog.clone(), database.clone(), name.clone());
        if !seen.insert(key) {
            continue;
        }
        refs.push(TableRef {
            catalog: (!catalog.is_empty()).then_some(catalog),
            database: (!database.is_empty()).then_some(database),
            table: name,
        });
    }
    Ok(refs)
}

/// SQL translator backed by the sqlglot Python library (via PyO3).
pub struct SqlglotTranslator {
    source: SqlDialect,
    target: SqlDialect,
    /// User-defined Python scripts executed in order after sqlglot translation.
    /// Each script must define `def transform(ast, src: str, dst: str) -> None`.
    python_scripts: Vec<String>,
}

impl SqlglotTranslator {
    pub fn new(source: SqlDialect, target: SqlDialect, python_scripts: Vec<String>) -> Self {
        Self {
            source,
            target,
            python_scripts,
        }
    }

    /// Verify that sqlglot is importable. Call once at startup.
    pub fn check_available() -> Result<()> {
        Python::attach(|py| {
            PyModule::import(py, "sqlglot").map_err(|e| {
                QueryFluxError::Translation(format!(
                    "sqlglot not found — run `pip install sqlglot`: {e}"
                ))
            })?;
            Ok(())
        })
    }
}

#[async_trait]
impl TranslatorTrait for SqlglotTranslator {
    fn source_dialect(&self) -> &SqlDialect {
        &self.source
    }

    fn target_dialect(&self) -> &SqlDialect {
        &self.target
    }

    async fn translate(&self, sql: &str, schema_context: &SchemaContext) -> Result<String> {
        let sql = sql.to_string();
        let src = self.source.sqlglot_write_name();
        let tgt = self.target.sqlglot_write_name();
        let schema_context = schema_context.clone();
        let python_scripts = self.python_scripts.clone();

        tokio::task::spawn_blocking(move || {
            translate_with_gil(&sql, &src, &tgt, &schema_context, &python_scripts)
        })
        .await
        .map_err(|e| QueryFluxError::Translation(format!("spawn_blocking error: {e}")))?
    }
}

fn translate_with_gil(
    sql: &str,
    src: &str,
    tgt: &str,
    schema_context: &SchemaContext,
    python_scripts: &[String],
) -> Result<String> {
    Python::attach(|py| {
        let sqlglot = PyModule::import(py, "sqlglot")
            .map_err(|e| QueryFluxError::Translation(format!("Failed to import sqlglot: {e}")))?;

        // 1. Dialect translation (skipped when src == tgt; fixup scripts may still run).
        let translated = if src == tgt {
            sql.to_string()
        } else if schema_context.is_empty() {
            debug!(src, tgt, "sqlglot dialect-only translation");
            translate_dialect_only(py, &sqlglot, sql, src, tgt)?
        } else {
            debug!(src, tgt, "sqlglot schema-aware translation");
            translate_with_schema(py, &sqlglot, sql, src, tgt, schema_context)?
        };

        // 2. Run user fixup scripts in order. Each may mutate the AST in-place.
        if python_scripts.is_empty() {
            return Ok(translated);
        }
        run_fixup_scripts(py, &sqlglot, &translated, src, tgt, python_scripts)
    })
}

fn translate_dialect_only(
    py: Python<'_>,
    sqlglot: &Bound<'_, PyModule>,
    sql: &str,
    src: &str,
    tgt: &str,
) -> Result<String> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("read", src).ok();
    kwargs.set_item("write", tgt).ok();

    let result = sqlglot
        .call_method("transpile", (sql,), Some(&kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("sqlglot.transpile failed: {e}")))?;

    let list: Vec<String> = result.extract().map_err(|e| {
        QueryFluxError::Translation(format!("Failed to extract transpile result: {e}"))
    })?;

    Ok(list.into_iter().next().unwrap_or_default())
}

fn translate_with_schema(
    py: Python<'_>,
    sqlglot: &Bound<'_, PyModule>,
    sql: &str,
    src: &str,
    tgt: &str,
    schema_context: &SchemaContext,
) -> Result<String> {
    let schema_dict = PyDict::new(py);
    for (table, cols) in &schema_context.tables {
        let col_dict = PyDict::new(py);
        for (col, ty) in cols {
            col_dict.set_item(col, ty).ok();
        }
        schema_dict.set_item(table, col_dict).ok();
    }

    let parse_kwargs = PyDict::new(py);
    parse_kwargs.set_item("dialect", src).ok();
    let tree = sqlglot
        .call_method("parse_one", (sql,), Some(&parse_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("sqlglot.parse_one failed: {e}")))?;

    let optimizer = PyModule::import(py, "sqlglot.optimizer").map_err(|e| {
        QueryFluxError::Translation(format!("Failed to import sqlglot.optimizer: {e}"))
    })?;
    let schema_mod = PyModule::import(py, "sqlglot.schema").map_err(|e| {
        QueryFluxError::Translation(format!("Failed to import sqlglot.schema: {e}"))
    })?;

    let mapping_schema_kwargs = PyDict::new(py);
    mapping_schema_kwargs.set_item("schema", schema_dict).ok();
    let schema_obj = schema_mod
        .call_method("MappingSchema", (), Some(&mapping_schema_kwargs))
        .map_err(|e| {
            QueryFluxError::Translation(format!("MappingSchema construction failed: {e}"))
        })?;

    let opt_kwargs = PyDict::new(py);
    opt_kwargs.set_item("schema", schema_obj).ok();
    opt_kwargs.set_item("dialect", src).ok();
    let optimized = optimizer
        .call_method("optimize", (&tree,), Some(&opt_kwargs))
        .unwrap_or_else(|e| {
            tracing::warn!("sqlglot optimizer failed ({e}), falling back to dialect-only");
            tree
        });

    let sql_kwargs = PyDict::new(py);
    sql_kwargs.set_item("dialect", tgt).ok();
    let translated: String = optimized
        .call_method("sql", (), Some(&sql_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("AST.sql() failed: {e}")))?
        .extract()
        .map_err(|e| QueryFluxError::Translation(format!("Failed to extract sql result: {e}")))?;

    Ok(translated)
}

/// Execute user-defined Python fixup scripts against the translated SQL.
///
/// Each script must define a function with this signature:
/// ```python
/// def transform(ast, src: str, dst: str) -> None:
///     # ast: sqlglot AST (already translated to the dst dialect) — mutate in-place
///     # src: source dialect name (e.g. "trino")
///     # dst: target dialect name (e.g. "athena")
/// ```
///
/// Imports and helper functions may appear at module level. Example:
/// ```python
/// import sqlglot.expressions as exp
///
/// def transform(ast, src: str, dst: str) -> None:
///     if dst == "athena":
///         for table in ast.find_all(exp.Table):
///             table.set("catalog", None)
/// ```
///
/// The AST is re-serialized once after all scripts run.
fn run_fixup_scripts(
    py: Python<'_>,
    sqlglot: &Bound<'_, PyModule>,
    sql: &str,
    src: &str,
    tgt: &str,
    scripts: &[String],
) -> Result<String> {
    // Parse the already-translated SQL in the target dialect.
    let parse_kwargs = PyDict::new(py);
    parse_kwargs.set_item("dialect", tgt).ok();
    let ast = sqlglot
        .call_method("parse_one", (sql,), Some(&parse_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("transform: parse_one failed: {e}")))?;

    for (i, script) in scripts.iter().enumerate() {
        // Execute the script in its own globals dict so that top-level imports
        // and helper functions work correctly (same approach as PythonScriptRouter).
        let globals = PyDict::new(py);
        let script_cstr = std::ffi::CString::new(script.as_str()).map_err(|e| {
            QueryFluxError::Translation(format!("translation script {i} contains null byte: {e}"))
        })?;
        py.run(script_cstr.as_c_str(), Some(&globals), None)
            .map_err(|e| {
                QueryFluxError::Translation(format!("translation script {i} error: {e}"))
            })?;

        let transform_fn = globals
            .get_item("transform")
            .map_err(|e| {
                QueryFluxError::Translation(format!(
                    "translation script {i} has no 'transform' function: {e}"
                ))
            })?
            .ok_or_else(|| {
                QueryFluxError::Translation(format!(
                    "translation script {i} has no 'transform' function"
                ))
            })?;

        transform_fn.call1((&ast, src, tgt)).map_err(|e| {
            QueryFluxError::Translation(format!(
                "translation script {i} transform() call failed: {e}"
            ))
        })?;
    }

    // Re-serialize once after all scripts have run.
    let sql_kwargs = PyDict::new(py);
    sql_kwargs.set_item("dialect", tgt).ok();
    let result: String = ast
        .call_method("sql", (), Some(&sql_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("transform: ast.sql() failed: {e}")))?
        .extract()
        .map_err(|e| QueryFluxError::Translation(format!("transform: extract failed: {e}")))?;

    Ok(result)
}

#[cfg(test)]
mod extract_table_refs_tests {
    use super::*;

    #[test]
    fn extracts_qualified_and_unqualified_tables() {
        let refs = extract_table_refs("SELECT * FROM hive.analytics.orders", "trino").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].catalog.as_deref(), Some("hive"));
        assert_eq!(refs[0].database.as_deref(), Some("analytics"));
        assert_eq!(refs[0].table, "orders");

        let refs = extract_table_refs("SELECT x FROM t", "trino").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].catalog, None);
        assert_eq!(refs[0].database, None);
        assert_eq!(refs[0].table, "t");
    }

    #[test]
    fn extracts_all_tables_in_a_join_without_duplicates() {
        let refs = extract_table_refs(
            "SELECT * FROM orders o JOIN orders x ON o.id = x.id JOIN customers c ON o.customer_id = c.id",
            "trino",
        )
        .unwrap();
        let names: std::collections::HashSet<_> = refs.iter().map(|r| r.table.as_str()).collect();
        // `orders` is joined against itself under two aliases — must appear once.
        assert_eq!(names, ["orders", "customers"].into_iter().collect());
    }

    #[test]
    fn cte_aliases_are_excluded_but_real_tables_survive() {
        let refs = extract_table_refs(
            "WITH recent AS (SELECT a FROM real_table) SELECT * FROM recent",
            "trino",
        )
        .unwrap();
        let names: Vec<&str> = refs.iter().map(|r| r.table.as_str()).collect();
        assert_eq!(names, vec!["real_table"]);
    }

    #[test]
    fn malformed_sql_yields_empty_not_error() {
        let refs = extract_table_refs("SELECT FROM WHERE", "trino").unwrap();
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn async_wrapper_matches_sync_result() {
        let refs = extract_table_refs_async("SELECT * FROM t".to_string(), "trino".to_string())
            .await
            .unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].table, "t");
    }
}
