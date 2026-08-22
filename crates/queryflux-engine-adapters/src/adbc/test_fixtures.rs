//! Shared Arrow fixtures for introspection unit tests.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

pub fn snowflake_show_warehouses_batch(state: &str, running: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("running", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["ANALYTICS_WH"])),
            Arc::new(StringArray::from(vec![state])),
            Arc::new(Int64Array::from(vec![running])),
        ],
    )
    .expect("snowflake show warehouses batch")
}

pub fn count_batch(count: u64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![count]))])
        .expect("count batch")
}
