use adbc_core::{Connection, Statement};
use arrow::array::{
    Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;

use super::AdbcPool;

pub(crate) fn db_kwarg(db_kwargs: &[(String, String)], key: &str) -> Option<String> {
    db_kwargs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

pub(crate) fn uri_query_param(uri: &str, key: &str) -> Option<String> {
    let query = uri.split('?').nth(1)?;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return Some(v.to_string());
        }
    }
    None
}

pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn query_batches(pool: &AdbcPool, sql: &str) -> Option<Vec<RecordBatch>> {
    let mut conn = pool.get().ok()?;
    let mut stmt = conn.new_statement().ok()?;
    stmt.set_sql_query(sql).ok()?;
    let reader = stmt.execute().ok()?;
    super::collect_batches(reader).ok()
}

pub(crate) fn column_index(batch: &RecordBatch, name: &str) -> Option<usize> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(name))
}

pub(crate) fn cell_u64(batch: &RecordBatch, col: &str, row: usize) -> Option<u64> {
    if row >= batch.num_rows() {
        return None;
    }
    let idx = column_index(batch, col)?;
    let col = batch.column(idx);
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return (!a.is_null(row)).then(|| a.value(row).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return (!a.is_null(row)).then(|| a.value(row) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return (!a.is_null(row)).then(|| a.value(row).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        return (!a.is_null(row)).then(|| a.value(row).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
        return (!a.is_null(row)).then(|| a.value(row).max(0) as u64);
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return (!a.is_null(row))
            .then(|| a.value(row).parse().ok())
            .flatten();
    }
    None
}

pub(crate) fn cell_str(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    if row >= batch.num_rows() {
        return None;
    }
    let idx = column_index(batch, col)?;
    let col = batch.column(idx);
    col.as_any()
        .downcast_ref::<StringArray>()
        .and_then(|a| (!a.is_null(row)).then(|| a.value(row).to_string()))
}

pub(crate) fn first_cell_u64(batches: &[RecordBatch]) -> Option<u64> {
    batches
        .iter()
        .find(|b| b.num_rows() > 0 && b.num_columns() > 0)
        .and_then(|b| cell_u64(b, b.schema().field(0).name(), 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_query_param_parses_warehouse() {
        assert_eq!(
            uri_query_param("user@acct/db/schema?warehouse=WH&role=R", "warehouse"),
            Some("WH".to_string())
        );
    }

    #[test]
    fn escape_sql_literal_doubles_quotes() {
        assert_eq!(escape_sql_literal("ANALYTICS'WH"), "ANALYTICS''WH");
    }

    #[test]
    fn db_kwarg_finds_key() {
        let kwargs = vec![
            ("warehouse".into(), "WH1".into()),
            ("other".into(), "x".into()),
        ];
        assert_eq!(db_kwarg(&kwargs, "warehouse").as_deref(), Some("WH1"));
        assert_eq!(db_kwarg(&kwargs, "missing"), None);
    }

    #[test]
    fn uri_without_query_returns_none() {
        assert_eq!(uri_query_param("snowflake://acct/db", "warehouse"), None);
    }

    #[test]
    fn cell_u64_reads_int_and_string_columns() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("running", DataType::Int64, false),
            Field::new("running_str", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7_i64])),
                Arc::new(StringArray::from(vec!["12"])),
            ],
        )
        .unwrap();
        assert_eq!(cell_u64(&batch, "running", 0), Some(7));
        assert_eq!(cell_u64(&batch, "RUNNING", 0), Some(7));
        assert_eq!(cell_u64(&batch, "running_str", 0), Some(12));
    }

    #[test]
    fn cell_str_is_case_insensitive() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "STATE",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["suspended"]))])
                .unwrap();
        assert_eq!(cell_str(&batch, "state", 0).as_deref(), Some("suspended"));
    }

    #[test]
    fn first_cell_u64_skips_empty_batches() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "c",
            DataType::UInt64,
            false,
        )])));
        let full = super::super::test_fixtures::count_batch(42);
        assert_eq!(first_cell_u64(&[empty, full]), Some(42));
    }
}
