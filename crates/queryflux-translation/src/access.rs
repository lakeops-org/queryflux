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
    /// The table the statement writes to (`INSERT INTO t`, `UPDATE t`, `CREATE TABLE t AS`,
    /// ...). Every other extracted table is read.
    pub is_write_target: bool,
}

/// The tables a statement touches, split into reads and the write target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedStatement {
    pub resources: Vec<ExtractedResource>,
    /// Whether the statement embeds reads of other tables — true for queries and for
    /// `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`CREATE ... AS <query>`, false for statements that
    /// merely name a table (`DESCRIBE`, `DROP`, `ALTER`, ...).
    pub embeds_reads: bool,
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
        MaskType::Redact => match src {
            // Trino/Athena's 3-arg `regexp_replace` already replaces every match; a 4th
            // positional argument there is a capture-group index, not a flag, so adding one
            // would either change behavior or be invalid.
            SqlDialect::Trino | SqlDialect::Athena => {
                format!("regexp_replace({c}, '[A-Za-z0-9]', 'x')")
            }
            // Postgres and DuckDB's 3-arg form replaces only the *first* match — silently
            // leaving most of a "redacted" value unmasked is exactly the data exposure this
            // mask exists to prevent, so the `g` (global) flag is required here.
            SqlDialect::Postgres | SqlDialect::DuckDb => {
                format!("regexp_replace({c}, '[A-Za-z0-9]', 'x', 'g')")
            }
            // Other dialects' regexp_replace global-vs-first-match semantics haven't been
            // verified; falls back to the (possibly first-match-only) form rather than
            // guessing at a flags syntax that could produce invalid SQL.
            _ => format!("regexp_replace({c}, '[A-Za-z0-9]', 'x')"),
        },
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
/// each) that `sql` references. CTE-defined names are excluded. A genuine parse failure
/// yields `Err` — callers must not treat it the same as an `Ok` with no resources (a
/// query that genuinely references no base tables), or a query the parser can't analyze silently
/// skips access control instead of hitting the caller's `onMissingSchema` fail path.
pub fn extract_resources(
    sql: &str,
    src_dialect: &SqlDialect,
    schema: &SchemaContext,
) -> Result<ExtractedStatement> {
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
    // { "table_name": { "col1": "type1", ... } } — sqlglot's `qualify(schema=...)`
    // requires this nesting (a dict of column -> type per table); handing it a
    // flat list of column names raises `SchemaError: ... must match the schema's
    // nesting level` internally, which `extract_resources` silently swallows and
    // treats as "no schema", so column attribution never actually runs.
    // `rewrite_table_scans`'s own `list(cols)` still works unchanged against this
    // shape — `list()` on a dict yields its keys, the column names it wants.
    serde_json::to_string(&schema.tables).unwrap_or_else(|_| "{}".to_string())
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


def _write_targets(tree):
    """([write-target Table nodes], whether the statement embeds reads).

    `INSERT INTO t ...`, `UPDATE t ...`, `DELETE FROM t ...`, `MERGE INTO t ...`,
    `TRUNCATE TABLE t, u` and `CREATE TABLE/VIEW t AS <query>` write to the named table(s);
    every *other* table they mention is read. Statements that only name a table without
    reading it (`DESCRIBE`, `DROP`, `ALTER`, ...) embed no reads.
    """
    if isinstance(tree, exp.TruncateTable):
        return [t for t in tree.expressions if isinstance(t, exp.Table)], False
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        node = tree.this
    elif isinstance(tree, exp.Create) and isinstance(tree.args.get("expression"), exp.Query):
        node = tree.this
    else:
        return [], isinstance(tree, exp.Query)
    if isinstance(node, exp.Schema):
        node = node.this
    return ([node] if isinstance(node, exp.Table) else []), True


def _set_assignment_columns(expressions):
    """Column names assigned by a list of `col = expr` nodes (an UPDATE's SET list, or an
    ON CONFLICT/ON DUPLICATE KEY DO UPDATE SET list)."""
    return {
        e.this.name
        for e in expressions or []
        if isinstance(e, exp.EQ) and isinstance(e.this, exp.Column)
    }


def _written_columns(tree):
    """Columns an INSERT column list / UPDATE ... SET names, sorted; None = the whole row
    (DELETE, MERGE, TRUNCATE, or an INSERT without a column list)."""
    cols = None
    if isinstance(tree, exp.Insert):
        if isinstance(tree.this, exp.Schema):
            cols = {c.name for c in tree.this.expressions if getattr(c, "name", None)}
        # `INSERT ... ON CONFLICT DO UPDATE SET ...` (or MySQL's `ON DUPLICATE KEY UPDATE`,
        # parsed into the same `conflict` arg) can write columns the insert column list
        # never named — those are also write-target columns a policy must see. A whole-row
        # insert (`cols` still None here) already covers them; only a named column list
        # needs extending.
        conflict = tree.args.get("conflict")
        if conflict is not None and cols is not None:
            cols |= _set_assignment_columns(conflict.expressions)
    elif isinstance(tree, exp.Update):
        cols = _set_assignment_columns(tree.expressions)
    return sorted(cols) if cols else None


def extract_resources(sql, dialect, schema_json):
    schema = json.loads(schema_json) if schema_json else {}
    # A parse failure must surface as an error, never as "no tables": the backend engine may
    # accept SQL that sqlglot rejects, so an empty result here would let it through unchecked.
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect or None)
    except Exception as e:
        return json.dumps({"error": str(e)})

    ctes = _cte_names(tree)

    # Best-effort column attribution: qualify when we have a schema.
    qualified = None
    if schema:
        try:
            qualified = qualify(tree.copy(), schema=schema, dialect=dialect or None)
        except Exception:
            qualified = None

    # Build the table listing from whichever tree we'll also read column
    # attribution from (`qualified` when we have one, else the raw `tree`).
    # qualify() renormalizes identifier casing per-dialect (e.g. `Orders` ->
    # `orders` on Trino, `orders` -> `ORDERS` on Snowflake) and can rewrite an
    # unaliased table's synthesized alias too — keying the table listing off a
    # *different* tree than the alias map risks the two keys never matching,
    # which would silently attribute zero columns to a real, matched table.
    src = qualified if qualified is not None else tree

    targets, embeds_reads = _write_targets(src)
    target_ids = {id(t) for t in targets}
    written = _written_columns(src)

    # A write target and a read of the same table (`INSERT INTO t SELECT ... FROM t`) are
    # tracked as separate entries: the read must still be policy-checked.
    per_table = {}
    order = []
    for t in src.find_all(exp.Table):
        if not t.name:
            continue
        if not t.catalog and not t.db and t.name.lower() in ctes:
            continue
        is_target = id(t) in target_ids
        key = _table_key(t) + (is_target,)
        if key not in per_table:
            per_table[key] = {
                "catalog": t.catalog or None,
                "schema": t.db or None,
                "table": t.name,
                "columns": set(),
                "all": False,
                "target": is_target,
            }
            order.append(key)

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
                k = alias_to_table[tbl] + (False,)
                if k in per_table:
                    per_table[k]["columns"].add(col.name)
    else:
        # no schema: we cannot safely attribute bare columns -> all
        for e in per_table.values():
            e["all"] = True

    out = []
    for key in order:
        e = per_table[key]
        # A write target reports what the statement writes, not what its WHERE reads.
        cols = written if e["target"] else (None if e["all"] else sorted(e["columns"]))
        out.append({
            "catalog": e["catalog"],
            "schema": e["schema"],
            "table": e["table"],
            "columns": cols,
            "target": e["target"],
        })
    return json.dumps({"resources": out, "embeds_reads": embeds_reads})


def rewrite_table_scans(sql, dialect, schema_json, policies_json):
    schema = json.loads(schema_json) if schema_json else {}
    policies = json.loads(policies_json)
    tree = sqlglot.parse_one(sql, dialect=dialect or None)
    ctes = _cte_names(tree)
    # The write target is not a scan site: swapping it for a filtered subquery would
    # produce invalid SQL (`UPDATE (SELECT ...) AS t ...`) and would not restrict anything.
    targets, _ = _write_targets(tree)
    target_ids = {id(t) for t in targets}

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

    def _star_selects_this_scan(table_node):
        p = table_node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                for e in p.expressions:
                    if isinstance(e, exp.Star):
                        return True
                    if isinstance(e, exp.Column) and e.name == "*":
                        # `a.*` expands only `a`; other scans in the same SELECT are not starred.
                        qualifier = (e.table or "").lower()
                        if not qualifier or qualifier in (
                            (table_node.alias or "").lower(),
                            (table_node.name or "").lower(),
                        ):
                            return True
                return False
            p = p.parent
        return False

    def _enclosing_select(table_node):
        p = table_node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                return p
            p = p.parent
        return None

    def _in_select_scope(node, select):
        p = node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                return p is select
            p = p.parent
        return False

    def col_list_for(policy_table, node, masks):
        # explicit schema columns win; else named references in the *enclosing*
        # SELECT (plus every masked column) are enough for a projection.
        # Unqualified names are taken only from that SELECT so a join's
        # `IN (SELECT id FROM t)` still projects `id` on the inner scan.
        # SELECT * still needs schema — we cannot invent the rest of the table.
        cand = [policy_table.lower(), policy_table.rsplit(".", 1)[-1].lower()]
        for tname, cols in schema.items():
            if tname.lower() in cand or tname.rsplit(".", 1)[-1].lower() in cand:
                return list(cols)
        if _star_selects_this_scan(node):
            return None
        alias = (node.alias or node.name or "").lower()
        names = []
        seen = set()

        def add(n):
            if not n:
                return
            k = n.lower()
            if k not in seen:
                seen.add(k)
                names.append(n)

        for mcol in masks:
            add(mcol)
        scope = _enclosing_select(node)
        src = scope if scope is not None else tree
        sole_in_scope = False
        if scope is not None:
            tables_here = [
                tbl
                for tbl in scope.find_all(exp.Table)
                if tbl.name
                and not (not tbl.catalog and not tbl.db and tbl.name.lower() in ctes)
                and _in_select_scope(tbl, scope)
            ]
            sole_in_scope = len(tables_here) == 1
        for col in src.find_all(exp.Column):
            if not col.name or col.name == "*":
                continue
            if scope is not None and not _in_select_scope(col, scope):
                continue
            tbl = (col.table or "").lower()
            if tbl:
                if tbl == alias or tbl in cand:
                    add(col.name)
            elif sole_in_scope:
                add(col.name)
        return names if names else None

    replaced = 0
    for t in list(tree.find_all(exp.Table)):
        if id(t) in target_ids or not t.name:
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
            cols = col_list_for(policy["table"], t, masks)
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
) -> Result<ExtractedStatement> {
    let module = load_module(py)?;
    let json: String = module
        .getattr("extract_resources")
        .and_then(|f| f.call1((sql, dialect, schema_json)))
        .and_then(|v| v.extract())
        .map_err(|e| QueryFluxError::Translation(format!("extract_resources: {e}")))?;
    let raw: RawOutput = serde_json::from_str(&json)
        .map_err(|e| QueryFluxError::Translation(format!("extract_resources decode: {e}")))?;
    match raw {
        RawOutput::Ok {
            resources,
            embeds_reads,
        } => Ok(ExtractedStatement {
            resources: resources
                .into_iter()
                .map(|r| ExtractedResource {
                    catalog: r.catalog,
                    schema: r.schema,
                    table: r.table,
                    columns: match r.columns {
                        Some(c) => Columns::Named(c),
                        None => Columns::All,
                    },
                    is_write_target: r.target,
                })
                .collect(),
            embeds_reads,
        }),
        RawOutput::Err { error } => Err(QueryFluxError::Translation(format!(
            "extract_resources: could not parse SQL: {error}"
        ))),
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum RawOutput {
    Ok {
        resources: Vec<RawResource>,
        embeds_reads: bool,
    },
    Err {
        error: String,
    },
}

#[derive(serde::Deserialize)]
struct RawResource {
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
    columns: Option<Vec<String>>,
    target: bool,
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
    /// REDACT must replace *every* alphanumeric — Postgres/DuckDB's 3-arg `regexp_replace`
    /// replaces only the first match, so those two dialects need the `g` flag; Trino's
    /// 3-arg form already replaces every match, and a 4th positional argument there is a
    /// capture-group index, not a flag, so it must be left alone.
    #[test]
    fn render_mask_redact_is_global_where_it_needs_to_be() {
        let redact = cm("s", MaskType::Redact);
        assert_eq!(
            render_mask(&redact, "s", &SqlDialect::Postgres).unwrap(),
            "regexp_replace(s, '[A-Za-z0-9]', 'x', 'g')"
        );
        assert_eq!(
            render_mask(&redact, "s", &SqlDialect::DuckDb).unwrap(),
            "regexp_replace(s, '[A-Za-z0-9]', 'x', 'g')"
        );
        assert_eq!(
            render_mask(&redact, "s", &SqlDialect::Trino).unwrap(),
            "regexp_replace(s, '[A-Za-z0-9]', 'x')"
        );
        assert_eq!(
            render_mask(&redact, "s", &SqlDialect::Athena).unwrap(),
            "regexp_replace(s, '[A-Za-z0-9]', 'x')"
        );
    }

    #[test]
    fn extract_resources_basic_join() {
        let refs = extract_resources(
            "SELECT a.x FROM sales.orders a JOIN sales.customers b ON a.cid = b.id",
            &SqlDialect::Trino,
            &SchemaContext::default(),
        )
        .unwrap()
        .resources;
        let names: std::collections::HashSet<_> = refs.iter().map(|r| r.table.as_str()).collect();
        assert_eq!(names, ["orders", "customers"].into_iter().collect());
    }

    /// Regression: a genuine parse failure must surface as `Err`, never `Ok(vec![])` — the
    /// caller (`OpaAccessGuard::check`) only takes its fail-closed `onMissingSchema: deny`
    /// path on `Err`; conflating a parse failure with "no tables referenced" let an
    /// unparseable query bypass access control entirely.
    #[test]
    fn extract_resources_parse_failure_is_err() {
        let err = extract_resources(
            "SELECT FROM FROM (((",
            &SqlDialect::Trino,
            &SchemaContext::default(),
        );
        assert!(
            err.is_err(),
            "a parse failure must be Err, not Ok(vec![]) — Ok(vec![]) must mean 'no tables'"
        );
    }

    #[test]
    fn extract_resources_excludes_cte() {
        let refs = extract_resources(
            "WITH recent AS (SELECT x FROM base) SELECT * FROM recent",
            &SqlDialect::Trino,
            &SchemaContext::default(),
        )
        .unwrap()
        .resources;
        assert_eq!(
            refs.iter().map(|r| r.table.as_str()).collect::<Vec<_>>(),
            vec!["base"]
        );
    }

    /// `(reads, write targets, embeds_reads)` for `sql`, table names sorted.
    fn split_statement(sql: &str) -> (Vec<String>, Vec<String>, bool) {
        let st = extract_resources(sql, &SqlDialect::Trino, &SchemaContext::default()).unwrap();
        let pick = |target: bool| {
            let mut names: Vec<String> = st
                .resources
                .iter()
                .filter(|r| r.is_write_target == target)
                .map(|r| r.table.clone())
                .collect();
            names.sort();
            names
        };
        (pick(false), pick(true), st.embeds_reads)
    }

    fn names(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    /// The reads inside a write statement must be reported as reads — otherwise a protected
    /// table can be copied out via `INSERT … SELECT` / `CREATE TABLE … AS`.
    #[test]
    fn extract_resources_separates_reads_from_the_write_target() {
        for (sql, reads, targets) in [
            (
                "INSERT INTO mine (id) SELECT id FROM secret",
                &["secret"][..],
                &["mine"][..],
            ),
            (
                "CREATE TABLE copy AS SELECT * FROM secret",
                &["secret"],
                &["copy"],
            ),
            ("CREATE VIEW v AS SELECT * FROM secret", &["secret"], &["v"]),
            (
                "UPDATE t SET x = 1 WHERE id IN (SELECT id FROM secret)",
                &["secret"],
                &["t"],
            ),
            ("DELETE FROM t WHERE x = 1", &[], &["t"]),
            ("INSERT INTO t VALUES (1)", &[], &["t"]),
            (
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x",
                &["s"],
                &["t"],
            ),
            // A read of the target table itself is still a read.
            ("INSERT INTO t SELECT * FROM t", &["t"], &["t"]),
            ("SELECT * FROM orders", &["orders"], &[]),
        ] {
            let (r, t, embeds) = split_statement(sql);
            assert_eq!((r, t), (names(reads), names(targets)), "{sql}");
            assert!(embeds, "{sql} should embed reads");
        }
    }

    /// `(table, columns)` of every write target in `sql`; `None` columns means the whole row.
    fn targets_with_columns(
        sql: &str,
        schema: &SchemaContext,
    ) -> Vec<(String, Option<Vec<String>>)> {
        let st = extract_resources(sql, &SqlDialect::Trino, schema).unwrap();
        let mut out: Vec<_> = st
            .resources
            .iter()
            .filter(|r| r.is_write_target)
            .map(|r| {
                let cols = match &r.columns {
                    Columns::Named(c) => Some(c.clone()),
                    Columns::All => None,
                };
                (r.table.clone(), cols)
            })
            .collect();
        out.sort();
        out
    }

    fn cols(xs: &[&str]) -> Option<Vec<String>> {
        Some(names(xs))
    }

    /// A write target carries the columns the statement writes — an `INSERT` column list or
    /// the `UPDATE … SET` columns — so a policy can restrict writes per column. Columns the
    /// `WHERE` merely reads are not written. Whole-row writes report all columns.
    #[test]
    fn extract_resources_reports_the_columns_a_write_targets() {
        let none = SchemaContext::default();
        let with_schema = schema_with("orders", &["id", "amount", "region"]);
        for schema in [&none, &with_schema] {
            for (sql, table, expected) in [
                ("INSERT INTO orders (region, id) VALUES ('EU', 1)", "orders", cols(&["id", "region"])),
                ("INSERT INTO orders VALUES (1, 2, 'EU')", "orders", None),
                (
                    "UPDATE orders SET region = 'EU', amount = 1 WHERE id = 3",
                    "orders",
                    cols(&["amount", "region"]),
                ),
                ("DELETE FROM orders WHERE id = 1", "orders", None),
                (
                    "MERGE INTO orders USING s ON orders.id = s.id WHEN MATCHED THEN UPDATE SET amount = s.amount",
                    "orders",
                    None,
                ),
            ] {
                assert_eq!(
                    targets_with_columns(sql, schema),
                    vec![(table.to_string(), expected)],
                    "{sql}"
                );
            }
        }
    }

    /// Regression: `INSERT ... ON CONFLICT DO UPDATE SET` can write columns the insert
    /// column list never named. A column policy must see those too, or it could allow the
    /// insert columns while the statement also silently modifies an unauthorized one on
    /// conflict. A whole-row insert already reports every column, so the conflict clause
    /// adds nothing there; `DO NOTHING` has no SET list to contribute at all.
    #[test]
    fn extract_resources_includes_on_conflict_update_columns() {
        let schema = SchemaContext::default();
        for (sql, expected) in [
            (
                "INSERT INTO orders (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET amount = 2",
                cols(&["amount", "id"]),
            ),
            (
                "INSERT INTO orders (id, region) VALUES (1, 'EU')                  ON CONFLICT (id) DO UPDATE SET amount = 2, region = 'US'",
                cols(&["amount", "id", "region"]),
            ),
            (
                "INSERT INTO orders VALUES (1, 2, 'EU') ON CONFLICT (id) DO UPDATE SET amount = 2",
                None,
            ),
            (
                "INSERT INTO orders (id) VALUES (1) ON CONFLICT (id) DO NOTHING",
                cols(&["id"]),
            ),
        ] {
            assert_eq!(
                targets_with_columns(sql, &schema),
                vec![("orders".to_string(), expected)],
                "{sql}"
            );
        }
    }

    /// Same coverage for MySQL's `ON DUPLICATE KEY UPDATE`, sqlglot's other spelling of the
    /// same `conflict` node.
    #[test]
    fn extract_resources_includes_on_duplicate_key_update_columns() {
        let schema = SchemaContext::default();
        let st = extract_resources(
            "INSERT INTO orders (id) VALUES (1) ON DUPLICATE KEY UPDATE amount = 2",
            &SqlDialect::MySql,
            &schema,
        )
        .unwrap();
        let target = st
            .resources
            .iter()
            .find(|r| r.is_write_target)
            .expect("write target");
        assert_eq!(
            match &target.columns {
                Columns::Named(c) => Some(c.clone()),
                Columns::All => None,
            },
            cols(&["amount", "id"])
        );
    }

    /// `TRUNCATE` is a write to every table it names and reads nothing.
    #[test]
    fn extract_resources_truncate_targets_every_named_table() {
        let sql = "TRUNCATE TABLE orders, customers";
        assert_eq!(
            targets_with_columns(sql, &SchemaContext::default()),
            vec![
                ("customers".to_string(), None),
                ("orders".to_string(), None)
            ]
        );
        let (reads, _, embeds) = split_statement(sql);
        assert!(reads.is_empty() && !embeds, "{reads:?} {embeds}");
    }

    /// Statements that merely name a table do not read it and must not be treated as reads.
    #[test]
    fn extract_resources_ddl_and_describe_embed_no_reads() {
        for sql in [
            "DESCRIBE orders",
            "DROP TABLE orders",
            "ALTER TABLE orders ADD COLUMN c INTEGER",
            "CREATE TABLE orders (id INTEGER)",
        ] {
            let (_, _, embeds) = split_statement(sql);
            assert!(!embeds, "{sql} must not embed reads");
        }
    }

    fn policy(table: &str) -> TablePolicy {
        TablePolicy {
            table: table.to_string(),
            row_filters: vec!["x = 1".to_string()],
            masked_columns: Vec::new(),
        }
    }

    /// The write target is not a scan site: replacing it with a filtered subquery would
    /// emit invalid SQL (`INSERT INTO (SELECT …)`), while the tables it reads still get the
    /// filter.
    #[test]
    fn rewrite_leaves_the_write_target_and_filters_the_reads() {
        let out = rewrite_table_scans(
            "INSERT INTO mine SELECT id FROM customers",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[policy("mine"), policy("customers")],
        )
        .unwrap()
        .to_lowercase();
        assert!(
            out.starts_with("insert into mine"),
            "target rewritten: {out}"
        );
        assert!(out.contains("from (select"), "read not filtered: {out}");
        assert!(out.contains("x = 1"), "filter missing: {out}");

        let out = rewrite_table_scans(
            "UPDATE t SET a = 1 WHERE id IN (SELECT id FROM secret)",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[policy("t"), policy("secret")],
        )
        .unwrap()
        .to_lowercase();
        assert!(out.starts_with("update t set"), "target rewritten: {out}");
        assert!(out.contains("x = 1"), "subquery read not filtered: {out}");
    }

    fn named_columns(columns: &Columns) -> Vec<String> {
        match columns {
            Columns::Named(cols) => {
                let mut cols = cols.clone();
                cols.sort();
                cols
            }
            Columns::All => panic!("expected an explicit column list, got Columns::All"),
        }
    }

    #[test]
    fn extract_resources_with_schema_attributes_columns() {
        let schema = schema_with("orders", &["id", "amount", "region"]);
        let refs = extract_resources(
            "SELECT o.id, o.amount FROM orders o",
            &SqlDialect::Trino,
            &schema,
        )
        .unwrap()
        .resources;
        assert_eq!(refs.len(), 1);
        assert_eq!(
            named_columns(&refs[0].columns),
            vec!["amount".to_string(), "id".to_string()]
        );
    }

    /// Regression: `qualify()` normalizes identifier casing per-dialect (e.g. `Orders`
    /// -> `orders` on Trino). Column attribution used to key the table listing off the
    /// pre-qualify tree and the alias map off the post-qualify tree, so any casing
    /// difference between them silently dropped every column for that table down to an
    /// empty list — even though a schema was provided and qualify() ran successfully.
    #[test]
    fn extract_resources_with_schema_attributes_columns_when_table_case_differs() {
        let schema = schema_with("orders", &["id", "amount"]);
        let refs = extract_resources(
            "SELECT o.id, o.amount FROM Orders o",
            &SqlDialect::Trino,
            &schema,
        )
        .unwrap()
        .resources;
        assert_eq!(refs.len(), 1);
        assert_eq!(
            named_columns(&refs[0].columns),
            vec!["amount".to_string(), "id".to_string()]
        );
    }

    /// Regression: Snowflake's default identifier folding uppercases both the table
    /// name and any unquoted alias during qualify() — for perfectly ordinary,
    /// all-lowercase source SQL, not just mixed-case edge cases. Column attribution
    /// must survive it instead of silently reporting zero columns for every Snowflake
    /// query that has a schema configured.
    #[test]
    fn extract_resources_with_schema_attributes_columns_on_snowflake() {
        let schema = schema_with("orders", &["id", "amount"]);
        let refs = extract_resources(
            "SELECT o.id, o.amount FROM orders o",
            &SqlDialect::Snowflake,
            &schema,
        )
        .unwrap()
        .resources;
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].table.to_lowercase(), "orders");
        // Snowflake folds unquoted identifiers to uppercase — columns included —
        // so compare case-insensitively; the point of this test is that the
        // column list isn't silently empty, not what case it comes back in.
        let cols: Vec<String> = named_columns(&refs[0].columns)
            .into_iter()
            .map(|c| c.to_lowercase())
            .collect();
        assert_eq!(cols, vec!["amount".to_string(), "id".to_string()]);
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
    fn rewrite_mask_named_columns_without_schema() {
        let out = rewrite_table_scans(
            "SELECT name, ssn FROM finance.transactions ORDER BY id",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec!["region = 'EU'".into()],
                masked_columns: vec![("ssn".into(), "NULL".into())],
            }],
        )
        .unwrap();
        let lower = out.to_lowercase();
        assert!(lower.contains("null"), "got: {out}");
        assert!(lower.contains("region = 'eu'"), "got: {out}");
        assert!(
            lower.contains("as ssn") || lower.contains("ssn"),
            "masked column must stay addressable: {out}"
        );
    }

    #[test]
    fn rewrite_mask_in_subquery_projects_unqualified_id() {
        let out = rewrite_table_scans(
            "SELECT a.id, b.id FROM customers a \
             JOIN customers b ON a.id < b.id \
             WHERE a.id IN (SELECT id FROM customers)",
            &SqlDialect::Trino,
            &SchemaContext::default(),
            &[TablePolicy {
                table: "customers".into(),
                row_filters: vec!["region = 'EU'".into()],
                masked_columns: vec![("ssn".into(), "NULL".into())],
            }],
        )
        .unwrap();
        let lower = out.to_lowercase();
        assert!(
            lower.contains("in (select id from"),
            "IN subquery must keep projecting id, got: {out}"
        );
        assert!(lower.contains("null"), "mask must still apply: {out}");
    }

    #[test]
    fn rewrite_masked_star_without_schema_errors() {
        let err = rewrite_table_scans(
            "SELECT * FROM finance.transactions",
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

    /// `a.*` expands only `a`. With no schema, a masked table that is joined but not starred
    /// can still be rewritten from its named references; one that *is* starred (by alias or
    /// by name) can't, and must be refused rather than leak the unmasked column.
    #[test]
    fn rewrite_qualified_star_only_applies_to_its_own_scan() {
        let policy = || {
            vec![TablePolicy {
                table: "finance.transactions".into(),
                row_filters: vec![],
                masked_columns: vec![("ssn".into(), "NULL".into())],
            }]
        };
        let run = |sql: &str| {
            rewrite_table_scans(
                sql,
                &SqlDialect::Trino,
                &SchemaContext::default(),
                &policy(),
            )
        };

        let out = run("SELECT a.* FROM other a JOIN finance.transactions b ON a.id = b.id")
            .expect("a.* does not star the masked table");
        assert!(
            out.to_lowercase().contains("null"),
            "mask must apply: {out}"
        );

        for starred in [
            "SELECT b.* FROM other a JOIN finance.transactions b ON a.id = b.id",
            "SELECT t.* FROM finance.transactions t",
            "SELECT transactions.* FROM finance.transactions",
            "SELECT a.*, b.* FROM other a JOIN finance.transactions b ON a.id = b.id",
            "SELECT * FROM other a JOIN finance.transactions b ON a.id = b.id",
        ] {
            assert!(run(starred).is_err(), "must fail closed: {starred}");
        }
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
