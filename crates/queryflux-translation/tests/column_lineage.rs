//! `column_lineage`: which input columns each written column comes from.
//!
//! Requires a working PyO3 interpreter with `sqlglot` on `PYTHONPATH` (same as CI:
//! venv + `pip install -r requirements.txt`). Run: `cargo test -p queryflux-translation`

use std::collections::BTreeMap;

use queryflux_core::lineage::{ColumnLineage, InputColumn};
use queryflux_core::query::SqlDialect;
use queryflux_translation::{column_lineage, ColumnMap, SchemaContext, SqlglotTranslator};

fn require_sqlglot() {
    SqlglotTranslator::check_available().expect(
        "sqlglot not importable — set PYO3_PYTHON to a venv with `pip install -r requirements.txt`",
    );
}

fn schema() -> SchemaContext {
    let table = |cols: &[&str]| -> ColumnMap {
        cols.iter()
            .map(|c| (c.to_string(), "VARCHAR".to_string()))
            .collect()
    };
    SchemaContext {
        catalog: None,
        database: None,
        tables: [
            (
                "sales.orders".into(),
                table(&["id", "customer_id", "amount", "status", "created_at"]),
            ),
            ("sales.customers".into(), table(&["id", "region"])),
            ("mart.daily".into(), table(&["day", "region", "revenue"])),
        ]
        .into_iter()
        .collect(),
    }
}

fn input(c: &InputColumn) -> String {
    let table = [c.schema.as_deref(), Some(c.table.as_str())]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(".");
    format!("{table}.{}:{}", c.column, c.subtype)
}

/// `written column → sorted "schema.table.column:SUBTYPE"`, and the sorted indirect inputs.
fn lineage(sql: &str, schema: &SchemaContext) -> (BTreeMap<String, Vec<String>>, Vec<String>) {
    require_sqlglot();
    let ColumnLineage { fields, indirect } = column_lineage(sql, &SqlDialect::Trino, schema)
        .unwrap()
        .unwrap_or_else(|| panic!("no lineage for {sql}"));
    let fields = fields
        .into_iter()
        .map(|(k, v)| {
            let mut v: Vec<_> = v.iter().map(input).collect();
            v.sort();
            (k, v)
        })
        .collect();
    let mut indirect: Vec<_> = indirect.iter().map(input).collect();
    indirect.sort();
    (fields, indirect)
}

fn map(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
        .collect()
}

#[test]
fn renames_expressions_and_aggregates_are_told_apart() {
    let (fields, indirect) = lineage(
        "INSERT INTO mart.daily (day, region, revenue) \
         SELECT date_trunc('day', o.created_at), c.region AS region, sum(o.amount) \
         FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id \
         WHERE o.status = 'paid' GROUP BY 1, 2",
        &schema(),
    );
    assert_eq!(
        fields,
        map(&[
            ("day", &["sales.orders.created_at:TRANSFORMATION"]),
            ("region", &["sales.customers.region:IDENTITY"]),
            ("revenue", &["sales.orders.amount:AGGREGATION"]),
        ])
    );
    for expected in [
        "sales.customers.id:JOIN",
        "sales.orders.customer_id:JOIN",
        "sales.orders.status:FILTER",
    ] {
        assert!(
            indirect.contains(&expected.to_string()),
            "{expected} not in {indirect:?}"
        );
    }
}

/// Without a column list, the target's columns (from the schema) name the outputs by
/// position; without a schema, the query's own output names do.
#[test]
fn insert_without_a_column_list() {
    let sql = "INSERT INTO mart.daily SELECT created_at, status, amount FROM sales.orders";
    let (fields, _) = lineage(sql, &schema());
    assert_eq!(
        fields.keys().collect::<Vec<_>>(),
        ["day", "region", "revenue"]
    );
    let (fields, _) = lineage(sql, &SchemaContext::default());
    assert_eq!(
        fields.keys().collect::<Vec<_>>(),
        ["amount", "created_at", "status"]
    );
}

#[test]
fn ctas_with_star_needs_the_schema() {
    let sql = "CREATE TABLE mart.copy AS SELECT * FROM sales.customers";
    let (fields, _) = lineage(sql, &schema());
    assert_eq!(
        fields,
        map(&[
            ("id", &["sales.customers.id:IDENTITY"]),
            ("region", &["sales.customers.region:IDENTITY"]),
        ])
    );
    let (fields, _) = lineage(sql, &SchemaContext::default());
    assert!(
        fields
            .values()
            .flatten()
            .all(|i| !i.contains(".id:") && !i.contains(".region:")),
        "a star with no schema must not be guessed: {fields:?}"
    );
}

#[test]
fn ctes_and_unions_trace_to_their_tables() {
    let (fields, _) = lineage(
        "INSERT INTO mart.daily (day, region, revenue) \
         WITH paid AS (SELECT created_at, amount FROM sales.orders WHERE status = 'paid'), \
              daily AS (SELECT created_at AS d, amount * 2 AS a FROM paid) \
         SELECT d, 'all', a FROM daily",
        &schema(),
    );
    assert_eq!(fields["day"], ["sales.orders.created_at:IDENTITY"]);
    assert_eq!(fields["revenue"], ["sales.orders.amount:TRANSFORMATION"]);
    assert!(!fields.contains_key("region"), "a literal has no inputs");

    let (fields, _) = lineage(
        "CREATE TABLE mart.ids AS SELECT id FROM sales.orders UNION ALL SELECT id FROM sales.customers",
        &schema(),
    );
    assert_eq!(
        fields["id"],
        ["sales.customers.id:IDENTITY", "sales.orders.id:IDENTITY"]
    );
}

#[test]
fn update_writes_its_set_columns() {
    let (fields, indirect) = lineage(
        "UPDATE sales.orders SET amount = amount * 1.2 WHERE status = 'open'",
        &schema(),
    );
    assert_eq!(
        fields,
        map(&[("amount", &["sales.orders.amount:TRANSFORMATION"])])
    );
    assert_eq!(indirect, ["sales.orders.status:FILTER"]);
}

#[test]
fn statements_that_write_nothing_from_a_query_have_none() {
    require_sqlglot();
    for sql in [
        "SELECT amount FROM sales.orders",
        "INSERT INTO mart.daily VALUES ('2026-10-21', 'eu', 1)",
        "DROP TABLE mart.daily",
    ] {
        assert_eq!(
            column_lineage(sql, &SqlDialect::Trino, &schema()).unwrap(),
            None,
            "{sql}"
        );
    }
    let huge = format!(
        "INSERT INTO mart.daily SELECT amount FROM sales.orders WHERE status IN ({})",
        vec!["'x'"; 70_000].join(",")
    );
    assert_eq!(
        column_lineage(&huge, &SqlDialect::Trino, &schema()).unwrap(),
        None
    );
}
