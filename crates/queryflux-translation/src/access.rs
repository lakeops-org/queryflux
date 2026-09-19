//! Access-control SQL support: resource extraction (which tables/columns a query touches)
//! and the scan-site view-substitution that applies row filters + column masks.
//!
//! All sqlglot work runs on the **source** SQL, before `maybe_translate`. The rewrite
//! output is still source-dialect; the normal translation pass then carries the spliced
//! filter/mask expressions to the target engine.

use pyo3::prelude::*;
use queryflux_core::access_model::{ColumnMask, Columns, MaskType};
use queryflux_core::error::{QueryFluxError, Result};
use queryflux_core::query::SqlDialect;
use queryflux_core::schema_context::SchemaContext;

/// A table's referenced columns, as extracted from a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedResource {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub columns: Columns,
}

/// Per-table policy for [`rewrite_table_scans`].
#[derive(Debug, Clone)]
pub struct TablePolicy {
    /// `schema.table` (or bare `table`) — matched against scan sites.
    pub table: String,
    /// Source-dialect boolean expressions, AND-combined into the scan's `WHERE`.
    pub row_filters: Vec<String>,
    /// `(column, source-dialect mask expression)` pairs.
    pub masked_columns: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum MaskRenderError {
    /// `CUSTOM` mask with no `expression`.
    MissingCustomExpression(String),
    /// `CONSTANT` mask with no `value`.
    MissingConstantValue(String),
}

impl std::fmt::Display for MaskRenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaskRenderError::MissingCustomExpression(c) => {
                write!(f, "CUSTOM mask for column {c:?} has no expression")
            }
            MaskRenderError::MissingConstantValue(c) => {
                write!(f, "CONSTANT mask for column {c:?} has no value")
            }
        }
    }
}
impl std::error::Error for MaskRenderError {}

/// Render a named mask type to a **source-dialect** SQL expression that masks `column_ref`.
///
/// Named types are `CASE` / `substr` / `concat` / `regexp_replace` / `date_trunc` — the
/// query's own translation carries them to the target. `HASH` is best-effort (recommend
/// `CUSTOM` + `context.engine` for a heterogeneous fleet). `CUSTOM` returns the raw
/// author-supplied expression verbatim.
pub fn render_mask(
    mask: &ColumnMask,
    column_ref: &str,
    src: &SqlDialect,
) -> std::result::Result<String, MaskRenderError> {
    let c = column_ref;
    Ok(match mask.mask_type {
        MaskType::Null => "NULL".to_string(),
        MaskType::Constant => {
            let v = mask
                .value
                .as_deref()
                .ok_or_else(|| MaskRenderError::MissingConstantValue(mask.column.clone()))?;
            format!("'{}'", v.replace('\'', "''"))
        }
        MaskType::Redact => format!("regexp_replace({c}, '[A-Za-z0-9]', 'x')"),
        MaskType::ShowLast4 => {
            format!("CASE WHEN {c} IS NULL THEN NULL ELSE '****' || substr({c}, -4) END")
        }
        MaskType::ShowFirst4 => {
            format!("CASE WHEN {c} IS NULL THEN NULL ELSE substr({c}, 1, 4) || '****' END")
        }
        MaskType::DateShowYear => format!("date_trunc('year', {c})"),
        MaskType::Hash => match src {
            SqlDialect::Trino | SqlDialect::Athena => {
                format!("to_hex(sha256(to_utf8(cast({c} as varchar))))")
            }
            SqlDialect::DuckDb => format!("sha256(cast({c} as varchar))"),
            SqlDialect::Snowflake => format!("sha2(cast({c} as varchar), 256)"),
            SqlDialect::ClickHouse | SqlDialect::StarRocks => {
                format!("hex(sha256(cast({c} as varchar)))")
            }
            _ => format!("sha256(cast({c} as varchar))"),
        },
        MaskType::Custom => mask
            .expression
            .clone()
            .ok_or_else(|| MaskRenderError::MissingCustomExpression(mask.column.clone()))?,
    })
}

fn dialect_kwarg(dialect: &SqlDialect) -> String {
    dialect.sqlglot_write_name()
}

/// Extract every base table (and, when `schema` is populated, the columns attributed to
/// each) that `sql` references. CTE-defined names are excluded. Best-effort: a parse
/// failure yields `Ok(vec![])`.
pub fn extract_resources(
    sql: &str,
    src_dialect: &SqlDialect,
    schema: &SchemaContext,
) -> Result<Vec<ExtractedResource>> {
    let dialect = dialect_kwarg(src_dialect);
    let schema_json = schema_to_json(schema);
    Python::attach(|py| extract_resources_gil(py, sql, &dialect, &schema_json))
}

/// Apply row filters and (pre-rendered, source-dialect) column-mask expressions at every
/// scan site of each policied table. Output is source-dialect SQL.
pub fn rewrite_table_scans(
    sql: &str,
    src_dialect: &SqlDialect,
    schema: &SchemaContext,
    policies: &[TablePolicy],
) -> Result<String> {
    if policies.is_empty() {
        return Ok(sql.to_string());
    }
    let dialect = dialect_kwarg(src_dialect);
    let schema_json = schema_to_json(schema);
    let policies_json = policies_to_json(policies);
    Python::attach(|py| rewrite_table_scans_gil(py, sql, &dialect, &schema_json, &policies_json))
}

fn schema_to_json(schema: &SchemaContext) -> String {
    // { "table_name": ["col1", "col2", ...] }
    let map: std::collections::BTreeMap<&String, Vec<&String>> = schema
        .tables
        .iter()
        .map(|(t, cols)| (t, cols.keys().collect()))
        .collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

fn policies_to_json(policies: &[TablePolicy]) -> String {
    let v: Vec<serde_json::Value> = policies
        .iter()
        .map(|p| {
            serde_json::json!({
                "table": p.table,
                "row_filters": p.row_filters,
                "masked_columns": p.masked_columns
                    .iter()
                    .map(|(c, e)| serde_json::json!({"column": c, "expr": e}))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::to_string(&v).unwrap_or_else(|_| "[]".to_string())
}

/// Fixed Python — no string interpolation of SQL/expressions; SQL, dialect, schema and
/// policies all arrive as data arguments.
const ACCESS_PY: &str = r#"
import json
import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify


def _cte_names(tree):
    names = set()
    for cte in tree.find_all(exp.CTE):
        a = cte.alias_or_name
        if a:
            names.add(a.lower())
    return names


def _table_key(t):
    cat = t.catalog or ""
    db = t.db or ""
    name = t.name or ""
    return (cat, db, name)


def _qualified(t):
    db = t.db
    return (db + "." + t.name) if db else t.name


def extract_resources(sql, dialect, schema_json):
    schema = json.loads(schema_json) if schema_json else {}
    # A parse failure must surface as an error, never as "no tables": the backend engine may
    # accept SQL that sqlglot rejects, so an empty result here would let it through unchecked.
    tree = sqlglot.parse_one(sql, dialect=dialect or None)

    ctes = _cte_names(tree)

    # Best-effort column attribution: qualify when we have a schema.
    qualified = None
    if schema:
        try:
            qualified = qualify(tree.copy(), schema=schema, dialect=dialect or None)
        except Exception:
            qualified = None

    per_table = {}
    order = []
    for t in tree.find_all(exp.Table):
        if not t.name:
            continue
        if not t.catalog and not t.db and t.name.lower() in ctes:
            continue
        key = _table_key(t)
        if key not in per_table:
            per_table[key] = {
                "catalog": t.catalog or None,
                "schema": t.db or None,
                "table": t.name,
                "columns": set(),
                "all": False,
            }
            order.append(key)

    src = qualified if qualified is not None else tree
    # star -> all columns
    for star in src.find_all(exp.Star):
        for e in per_table.values():
            e["all"] = True
    # attribute columns to their table
    if qualified is not None:
        alias_to_table = {}
        for t in qualified.find_all(exp.Table):
            a = (t.alias or t.name)
            if a:
                alias_to_table[a] = _table_key(t)
        for col in qualified.find_all(exp.Column):
            tbl = col.table
            if tbl and tbl in alias_to_table:
                k = alias_to_table[tbl]
                if k in per_table:
                    per_table[k]["columns"].add(col.name)
    else:
        # no schema: we cannot safely attribute bare columns -> all
        for e in per_table.values():
            e["all"] = True

    out = []
    for key in order:
        e = per_table[key]
        cols = None if e["all"] else sorted(e["columns"])
        out.append({
            "catalog": e["catalog"],
            "schema": e["schema"],
            "table": e["table"],
            "columns": cols,
        })
    return json.dumps(out)


def rewrite_table_scans(sql, dialect, schema_json, policies_json):
    schema = json.loads(schema_json) if schema_json else {}
    policies = json.loads(policies_json)
    tree = sqlglot.parse_one(sql, dialect=dialect or None)
    ctes = _cte_names(tree)

    # Index policies (lowercased). A qualified policy is only reachable by an unqualified scan
    # through `qualified_by_bare`; an explicitly qualified scan never resolves to another
    # schema's policy.
    exact = {}
    qualified_by_bare = {}
    for p in policies:
        key = p["table"].lower()
        exact[key] = p
        if "." in key:
            qualified_by_bare.setdefault(key.rsplit(".", 1)[-1], []).append(p)

    def find_policy(t):
        name = t.name.lower()
        if t.db:
            # explicit schema: exact `schema.table`, else a policy written for the bare name
            return exact.get(_qualified(t).lower()) or exact.get(name)
        if name in exact:
            return exact[name]
        cands = qualified_by_bare.get(name, [])
        if len(cands) > 1:
            # cannot tell which schema the scan resolves to -> fail closed
            raise ValueError(
                "ambiguous policy for unqualified table %r: %s"
                % (t.name, ", ".join(sorted(c["table"] for c in cands)))
            )
        return cands[0] if cands else None

    def col_list_for(policy_table, node):
        # explicit schema columns win; else derive from the scan is impossible -> error
        cand = [policy_table.lower(), policy_table.rsplit(".", 1)[-1].lower()]
        for tname, cols in schema.items():
            if tname.lower() in cand or tname.rsplit(".", 1)[-1].lower() in cand:
                return list(cols)
        return None

    replaced = 0
    for t in list(tree.find_all(exp.Table)):
        if not t.name:
            continue
        if not t.catalog and not t.db and t.name.lower() in ctes:
            continue
        policy = find_policy(t)
        if policy is None:
            continue

        alias = t.alias or t.name

        masks = {m["column"].lower(): m["expr"] for m in policy["masked_columns"]}
        filters = [f for f in policy["row_filters"] if f and f.strip()]

        # projection
        if masks:
            cols = col_list_for(policy["table"], t)
            if not cols:
                raise ValueError("cannot enumerate columns for masked table %r" % policy["table"])
            selects = []
            for c in cols:
                if c.lower() in masks:
                    expr = sqlglot.parse_one(masks[c.lower()], dialect=dialect or None)
                    selects.append(exp.alias_(expr, c))
                else:
                    selects.append(exp.column(c))
        else:
            selects = [exp.Star()]

        inner = exp.select(*selects).from_(exp.table_(t.name, db=t.db, catalog=t.catalog))
        if filters:
            cond = None
            for f in filters:
                fe = sqlglot.parse_one(f, dialect=dialect or None)
                cond = fe if cond is None else exp.and_(cond, fe)
            inner = inner.where(cond)

        sub = exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias)))
        t.replace(sub)
        replaced += 1

    return tree.sql(dialect=dialect or None)
"#;

fn load_module<'py>(py: Python<'py>) -> Result<Bound<'py, PyModule>> {
    let code = std::ffi::CString::new(ACCESS_PY).unwrap();
    let file = std::ffi::CString::new("qflux_access.py").unwrap();
    let name = std::ffi::CString::new("qflux_access").unwrap();
    PyModule::from_code(py, code.as_c_str(), file.as_c_str(), name.as_c_str())
        .map_err(|e| QueryFluxError::Translation(format!("load access module: {e}")))
}

fn extract_resources_gil(
    py: Python<'_>,
    sql: &str,
    dialect: &str,
    schema_json: &str,
) -> Result<Vec<ExtractedResource>> {
    let module = load_module(py)?;
    let json: String = module
        .getattr("extract_resources")
        .and_then(|f| f.call1((sql, dialect, schema_json)))
        .and_then(|v| v.extract())
        .map_err(|e| QueryFluxError::Translation(format!("extract_resources: {e}")))?;
    let raw: Vec<RawResource> = serde_json::from_str(&json)
        .map_err(|e| QueryFluxError::Translation(format!("extract_resources decode: {e}")))?;
    Ok(raw
        .into_iter()
        .map(|r| ExtractedResource {
            catalog: r.catalog,
            schema: r.schema,
            table: r.table,
            columns: match r.columns {
                Some(c) => Columns::Named(c),
                None => Columns::All,
            },
        })
        .collect())
}

#[derive(serde::Deserialize)]
struct RawResource {
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
    columns: Option<Vec<String>>,
}

fn rewrite_table_scans_gil(
    py: Python<'_>,
    sql: &str,
    dialect: &str,
    schema_json: &str,
    policies_json: &str,
) -> Result<String> {
    let module = load_module(py)?;
    let out: String = module
        .getattr("rewrite_table_scans")
        .and_then(|f| f.call1((sql, dialect, schema_json, policies_json)))
        .and_then(|v| v.extract())
        .map_err(|e| QueryFluxError::Translation(format!("rewrite_table_scans: {e}")))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::schema_context::ColumnMap;
    use std::collections::HashMap;

    fn cm(col: &str, ty: MaskType) -> ColumnMask {
        ColumnMask {
            column: col.to_string(),
            mask_type: ty,
            value: None,
            expression: None,
        }
    }

    fn schema_with(table: &str, cols: &[&str]) -> SchemaContext {
        let mut tables = HashMap::new();
        tables.insert(
            table.to_string(),
            cols.iter()
                .map(|c| (c.to_string(), "varchar".to_string()))
                .collect::<ColumnMap>(),
        );
        SchemaContext {
            catalog: None,
            database: None,
            tables,
        }
    }

    #[test]
    fn render_mask_named_types() {
        let d = SqlDialect::Trino;
        assert_eq!(
            render_mask(&cm("s", MaskType::Null), "s", &d).unwrap(),
            "NULL"
        );
        let mut redact = cm("s", MaskType::Redact);
        assert!(render_mask(&redact, "s", &d)
            .unwrap()
            .contains("regexp_replace"));
        redact.mask_type = MaskType::ShowLast4;
        assert!(render_mask(&redact, "s", &d)
            .unwrap()
            .contains("substr(s, -4)"));
        let mut cst = cm("s", MaskType::Constant);
        cst.value = Some("X'Y".to_string());
        assert_eq!(render_mask(&cst, "s", &d).unwrap(), "'X''Y'");
        let cust = ColumnMask {
            column: "s".into(),
            mask_type: MaskType::Custom,
            value: None,
            expression: Some("upper(s)".into()),
        };
        assert_eq!(render_mask(&cust, "s", &d).unwrap(), "upper(s)");
        assert!(render_mask(&cm("s", MaskType::Custom), "s", &d).is_err());
    }

    #[test]
    fn extract_resources_basic_join() {
        let refs = extract_resources(
            "SELECT a.x FROM sales.orders a JOIN sales.customers b ON a.cid = b.id",
            &SqlDialect::Trino,
            &SchemaContext::default(),
        )
        .unwrap();
        let names: std::collections::HashSet<_> = refs.iter().map(|r| r.table.as_str()).collect();
        assert_eq!(names, ["orders", "customers"].into_iter().collect());
    }

    #[test]
    fn extract_resources_excludes_cte() {
        let refs = extract_resources(
            "WITH recent AS (SELECT x FROM base) SELECT * FROM recent",
            &SqlDialect::Trino,
            &SchemaContext::default(),
        )
        .unwrap();
        assert_eq!(
            refs.iter().map(|r| r.table.as_str()).collect::<Vec<_>>(),
            vec!["base"]
        );
    }

    #[test]
    fn extract_resources_errors_on_unparseable_sql() {
        // A parse failure must not look like "no tables" — that would be an allow.
        for sql in ["SELECT * FROM orders WHERE (((", "SELECT FROM FROM ((("] {
            assert!(
                extract_resources(sql, &SqlDialect::Trino, &SchemaContext::default()).is_err(),
                "expected an error for {sql:?}"
            );
        }
    }

    #[test]
    fn rewrite_row_filter_only_no_schema() {
        let out = rewrite_table_scans(
            "SELECT id FROM finance.transactions WHERE amount > 100",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec!["region = 'US'".into()],
                masked_columns: vec![],
            }],
        )
        .unwrap();
        assert!(out.contains("region = 'US'"), "got: {out}");
        assert!(
            out.contains("SELECT * FROM finance.transactions"),
            "got: {out}"
        );
    }

    #[test]
    fn rewrite_mask_in_cte_with_schema() {
        let schema = schema_with("transactions", &["id", "amount", "ssn", "region"]);
        let out = rewrite_table_scans(
            "WITH hv AS (SELECT t.id, t.ssn FROM finance.transactions t WHERE t.amount > 1000) SELECT id, ssn FROM hv",
            &SqlDialect::Trino,
            &schema,
            &[TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec!["region = 'US'".into()],
                masked_columns: vec![(
                    "ssn".into(),
                    render_mask(&cm("ssn", MaskType::ShowLast4), "ssn", &SqlDialect::Trino)
                        .unwrap(),
                )],
            }],
        )
        .unwrap();
        let lower = out.to_lowercase();
        assert!(lower.contains("region = 'us'"), "got: {out}");
        assert!(
            lower.contains("substring(ssn, -4)") || lower.contains("substr(ssn, -4)"),
            "mask expression must be present: {out}"
        );
        // masked column re-aliased back to `ssn` so outer references still resolve
        assert!(lower.contains("end as ssn"), "got: {out}");
    }

    #[test]
    fn rewrite_masked_table_without_schema_errors() {
        let err = rewrite_table_scans(
            "SELECT ssn FROM finance.transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec![],
                masked_columns: vec![("ssn".into(), "NULL".into())],
            }],
        );
        assert!(err.is_err());
    }

    #[test]
    fn rewrite_leaves_cte_named_like_table_alone() {
        let out = rewrite_table_scans(
            "WITH transactions AS (SELECT 1 AS id) SELECT id FROM transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[TablePolicy {
                table: "transactions".into(),
                row_filters: vec!["x = 1".into()],
                masked_columns: vec![],
            }],
        )
        .unwrap();
        assert!(!out.contains("x = 1"), "CTE must not be rewritten: {out}");
    }

    fn row_filter_policy(table: &str, filter: &str) -> TablePolicy {
        TablePolicy {
            table: table.into(),
            row_filters: vec![filter.into()],
            masked_columns: vec![],
        }
    }

    #[test]
    fn rewrite_qualified_policy_skips_same_named_table_in_other_schema() {
        let out = rewrite_table_scans(
            "SELECT id FROM hr.transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[row_filter_policy("finance.transactions", "region = 'US'")],
        )
        .unwrap();
        assert!(
            !out.contains("region"),
            "wrong schema must not match: {out}"
        );
    }

    #[test]
    fn rewrite_qualified_policy_applies_to_unqualified_scan() {
        let out = rewrite_table_scans(
            "SELECT id FROM transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[row_filter_policy("finance.transactions", "region = 'US'")],
        )
        .unwrap();
        assert!(out.contains("region = 'US'"), "got: {out}");
    }

    #[test]
    fn rewrite_bare_policy_applies_to_qualified_scan() {
        let out = rewrite_table_scans(
            "SELECT id FROM finance.transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[row_filter_policy("transactions", "region = 'US'")],
        )
        .unwrap();
        assert!(out.contains("region = 'US'"), "got: {out}");
    }

    #[test]
    fn rewrite_ambiguous_unqualified_scan_errors() {
        let err = rewrite_table_scans(
            "SELECT id FROM transactions",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[
                row_filter_policy("finance.transactions", "a = 1"),
                row_filter_policy("hr.transactions", "b = 2"),
            ],
        );
        assert!(err.is_err());
    }

    #[test]
    fn rewrite_masked_projection_keeps_schema_column_order() {
        let cols = ["zeta", "id", "ssn", "alpha", "region", "beta"];
        let schema = schema_with("transactions", &cols);
        let out = rewrite_table_scans(
            "SELECT * FROM finance.transactions",
            &SqlDialect::Trino,
            &schema,
            &[TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec![],
                masked_columns: vec![("ssn".into(), "NULL".into())],
            }],
        )
        .unwrap();
        assert!(
            out.contains("zeta, id, NULL AS ssn, alpha, region, beta"),
            "columns must follow schema order: {out}"
        );
    }
}
