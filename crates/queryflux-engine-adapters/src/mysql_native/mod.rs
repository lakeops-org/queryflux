//! Shared MySQL-wire native execution helper.
//!
//! Used by any adapter that connects via `mysql_async` (StarRocks, ClickHouse MySQL iface).
//! Converts `mysql_async` rows directly into `NativeResultChunk`s, bypassing Arrow entirely.
//!
//! # Type precision vs. the Arrow path
//!
//! The Arrow path round-trips through `mysql_column_type_to_arrow` and `arrow_type_to_mysql_type`,
//! which loses precision for DECIMAL, DATETIME(6), and UNSIGNED integers. This module reads
//! column metadata directly from the mysql_async driver and encodes values from their
//! native `mysql_async::Value` representation, preserving full precision.

use bytes::Bytes;
use futures::stream;
use mysql_async::{consts::ColumnType, prelude::Queryable, Pool, Row, Value};
use queryflux_auth::QueryCredentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    native_result::{NativeColumn, NativeResultChunk, NativeRow, NativeTypeInfo, NativeTypeKind},
    params::{QueryParam, QueryParams},
    session::SessionContext,
    tags::{tags_to_json, QueryTags},
};

use crate::{BackendQueryIdSlot, NativeExecution};

/// Number of rows per `NativeResultChunk`. Balances channel overhead vs. memory.
const BATCH_SIZE: usize = 1_000;

/// Execute `sql` against `pool` and return a stream of `NativeResultChunk`s.
///
/// The connection is acquired, session is set up (USE database, @query_tag), the query
/// is executed, and rows are batched into `NativeResultChunk`s of up to `BATCH_SIZE` rows.
/// The first chunk carries column metadata; subsequent chunks carry rows only.
///
/// # Streaming note
/// This implementation collects all rows into memory before yielding chunks — the same
/// behaviour as the current Arrow path. The in-memory constraint is removed in a follow-up
/// by switching to `query_iter` + a spawned task, which requires working around
/// `mysql_async::QueryResult`'s borrow-based lifetime constraints.
pub async fn execute(
    pool: &Pool,
    sql: &str,
    session: &SessionContext,
    _credentials: &QueryCredentials,
    tags: &QueryTags,
    params: &QueryParams,
    id_slot: &BackendQueryIdSlot,
) -> Result<NativeExecution> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| QueryFluxError::Engine(format!("mysql_native: connection failed: {e}")))?;
    id_slot.publish(conn.id().to_string());

    if let Some(db) = session.database() {
        let use_sql = format!("USE `{}`", db.replace('`', "``"));
        conn.query_drop(&use_sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("mysql_native: USE failed: {e}")))?;
    }

    if !tags.is_empty() {
        let tag_json = tags_to_json(tags).to_string();
        let escaped = Value::from(tag_json).as_sql(false);
        let set_sql = format!("SET @query_tag = {escaped}");
        conn.query_drop(&set_sql).await.map_err(|e| {
            QueryFluxError::Engine(format!("mysql_native: SET @query_tag failed: {e}"))
        })?;
    }

    let rows: Vec<Row> = if params.is_empty() {
        conn.query::<Row, _>(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("mysql_native: query failed: {e}")))?
    } else {
        let mysql_params = mysql_async::Params::Positional(
            params.iter().map(query_param_to_mysql_value).collect(),
        );
        conn.exec::<Row, _, _>(sql, mysql_params)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("mysql_native: exec failed: {e}")))?
    };

    let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();

    if rows.is_empty() {
        let _ = stats_tx.send(None);
        return Ok(NativeExecution {
            stream: Box::pin(stream::empty()),
            stats: stats_rx,
        });
    }

    // Build column metadata from the first row's column descriptors.
    let columns: Vec<NativeColumn> = rows[0]
        .columns_ref()
        .iter()
        .map(|c| NativeColumn {
            name: c.name_str().to_string(),
            type_info: mysql_column_type_to_native(c.column_type()),
            nullable: true,
        })
        .collect();

    let num_cols = rows[0].len();

    // Convert all rows to NativeRows first, then batch them.
    let native_rows: Vec<NativeRow> = rows
        .into_iter()
        .map(|row| row_to_native(row, num_cols))
        .collect();

    // Produce NativeResultChunks: columns present only on the first chunk.
    let chunks: Vec<Result<NativeResultChunk>> = native_rows
        .chunks(BATCH_SIZE)
        .enumerate()
        .map(|(i, batch)| {
            Ok(NativeResultChunk {
                columns: if i == 0 { Some(columns.clone()) } else { None },
                rows: batch.to_vec(),
            })
        })
        .collect();

    let _ = stats_tx.send(None);
    Ok(NativeExecution {
        stream: Box::pin(stream::iter(chunks)),
        stats: stats_rx,
    })
}

// ---------------------------------------------------------------------------
// Row / value conversion
// ---------------------------------------------------------------------------

/// Convert a [`QueryParam`] to a `mysql_async` native value for prepared-statement binding.
fn query_param_to_mysql_value(p: &QueryParam) -> Value {
    match p {
        QueryParam::Text(s) => Value::Bytes(s.as_bytes().to_vec()),
        QueryParam::Numeric(s) => {
            if let Ok(n) = s.parse::<i64>() {
                Value::Int(n)
            } else if let Ok(f) = s.parse::<f64>() {
                Value::Double(f)
            } else {
                Value::Bytes(s.as_bytes().to_vec())
            }
        }
        QueryParam::Boolean(b) => Value::Int(*b as i64),
        QueryParam::Date(s) | QueryParam::Timestamp(s) | QueryParam::Time(s) => {
            Value::Bytes(s.as_bytes().to_vec())
        }
        QueryParam::Null => Value::NULL,
    }
}

fn row_to_native(mut row: Row, num_cols: usize) -> NativeRow {
    let values = (0..num_cols)
        .map(|i| match row.take::<Value, usize>(i) {
            Some(Value::NULL) | None => None,
            Some(v) => Some(value_to_bytes(v)),
        })
        .collect();
    NativeRow(values)
}

fn value_to_bytes(v: Value) -> Bytes {
    match v {
        Value::NULL => unreachable!("NULL handled before value_to_bytes"),
        Value::Bytes(b) => Bytes::from(b),
        Value::Int(i) => Bytes::from(i.to_string()),
        Value::UInt(u) => Bytes::from(u.to_string()),
        Value::Float(f) => Bytes::from(f.to_string()),
        Value::Double(d) => Bytes::from(d.to_string()),
        Value::Date(year, month, day, hour, min, sec, micros) => {
            if hour == 0 && min == 0 && sec == 0 && micros == 0 {
                Bytes::from(format!("{year:04}-{month:02}-{day:02}"))
            } else if micros == 0 {
                Bytes::from(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}"
                ))
            } else {
                Bytes::from(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}.{micros:06}"
                ))
            }
        }
        Value::Time(neg, days, hours, mins, secs, micros) => {
            let total_hours = days as i64 * 24 + hours as i64;
            let sign = if neg { "-" } else { "" };
            if micros == 0 {
                Bytes::from(format!("{sign}{total_hours:02}:{mins:02}:{secs:02}"))
            } else {
                Bytes::from(format!(
                    "{sign}{total_hours:02}:{mins:02}:{secs:02}.{micros:06}"
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Column type mapping: mysql_async ColumnType → NativeTypeKind
// ---------------------------------------------------------------------------

fn mysql_column_type_to_native(ct: ColumnType) -> NativeTypeInfo {
    let kind = match ct {
        ColumnType::MYSQL_TYPE_TINY => NativeTypeKind::TinyInt,
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => NativeTypeKind::SmallInt,
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => NativeTypeKind::Int,
        ColumnType::MYSQL_TYPE_LONGLONG => NativeTypeKind::BigInt,
        ColumnType::MYSQL_TYPE_FLOAT => NativeTypeKind::Float,
        ColumnType::MYSQL_TYPE_DOUBLE => NativeTypeKind::Double,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            NativeTypeKind::Decimal
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => NativeTypeKind::Date,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => NativeTypeKind::Time,
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
            NativeTypeKind::DateTime
        }
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            NativeTypeKind::Timestamp
        }
        ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB => NativeTypeKind::Text,
        ColumnType::MYSQL_TYPE_JSON => NativeTypeKind::Json,
        ColumnType::MYSQL_TYPE_BIT => NativeTypeKind::Binary,
        _ => NativeTypeKind::Varchar,
    };
    NativeTypeInfo {
        kind,
        precision: None,
        scale: None,
        unsigned: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::consts::ColumnType;
    use queryflux_core::params::QueryParam;

    // ── query_param_to_mysql_value ────────────────────────────────────────────

    #[test]
    fn text_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Text("alice".into())),
            Value::Bytes(b"alice".to_vec())
        );
    }

    #[test]
    fn integer_numeric_maps_to_int() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("42".into())),
            Value::Int(42)
        );
    }

    #[test]
    fn negative_integer_numeric_maps_to_int() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("-7".into())),
            Value::Int(-7)
        );
    }

    #[test]
    fn float_numeric_maps_to_double() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("2.5".into())),
            Value::Double(2.5)
        );
    }

    #[test]
    fn non_parseable_numeric_falls_back_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Numeric("bad".into())),
            Value::Bytes(b"bad".to_vec())
        );
    }

    #[test]
    fn boolean_true_maps_to_int_one() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Boolean(true)),
            Value::Int(1)
        );
    }

    #[test]
    fn boolean_false_maps_to_int_zero() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Boolean(false)),
            Value::Int(0)
        );
    }

    #[test]
    fn date_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Date("2025-01-15".into())),
            Value::Bytes(b"2025-01-15".to_vec())
        );
    }

    #[test]
    fn timestamp_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Timestamp("2025-01-15 12:00:00".into())),
            Value::Bytes(b"2025-01-15 12:00:00".to_vec())
        );
    }

    #[test]
    fn time_maps_to_bytes() {
        assert_eq!(
            query_param_to_mysql_value(&QueryParam::Time("08:30:00".into())),
            Value::Bytes(b"08:30:00".to_vec())
        );
    }

    #[test]
    fn null_maps_to_null() {
        assert_eq!(query_param_to_mysql_value(&QueryParam::Null), Value::NULL);
    }

    // ── value_to_bytes ────────────────────────────────────────────────────────

    #[test]
    fn bytes_value_roundtrips() {
        assert_eq!(
            value_to_bytes(Value::Bytes(b"hello".to_vec())),
            b"hello"[..]
        );
    }

    #[test]
    fn int_value_formats_as_decimal() {
        assert_eq!(value_to_bytes(Value::Int(-42)), "-42");
    }

    #[test]
    fn uint_value_formats_as_decimal() {
        assert_eq!(value_to_bytes(Value::UInt(u64::MAX)), u64::MAX.to_string());
    }

    #[test]
    fn float_value() {
        let b = value_to_bytes(Value::Float(1.5));
        assert_eq!(b, "1.5");
    }

    #[test]
    fn double_value() {
        let b = value_to_bytes(Value::Double(1.23));
        assert_eq!(b, "1.23");
    }

    // Date — pure date (no time component)
    #[test]
    fn date_only_formats_as_yyyy_mm_dd() {
        let b = value_to_bytes(Value::Date(2024, 3, 15, 0, 0, 0, 0));
        assert_eq!(b, "2024-03-15");
    }

    // Date — datetime without microseconds
    #[test]
    fn datetime_without_micros_formats_without_fractional() {
        let b = value_to_bytes(Value::Date(2024, 3, 15, 10, 30, 45, 0));
        assert_eq!(b, "2024-03-15 10:30:45");
    }

    // Date — datetime with microseconds
    #[test]
    fn datetime_with_micros_formats_six_fraction_digits() {
        let b = value_to_bytes(Value::Date(2024, 3, 15, 10, 30, 45, 123456));
        assert_eq!(b, "2024-03-15 10:30:45.123456");
    }

    // Date — zero-pad month and day
    #[test]
    fn date_zero_pads_month_and_day() {
        let b = value_to_bytes(Value::Date(2024, 1, 5, 0, 0, 0, 0));
        assert_eq!(b, "2024-01-05");
    }

    // Time — positive, no micros
    #[test]
    fn time_positive_no_micros() {
        let b = value_to_bytes(Value::Time(false, 0, 8, 30, 0, 0));
        assert_eq!(b, "08:30:00");
    }

    // Time — negative (MySQL TIME can be negative for intervals)
    #[test]
    fn time_negative_prefixes_minus() {
        let b = value_to_bytes(Value::Time(true, 0, 1, 0, 0, 0));
        assert_eq!(b, "-01:00:00");
    }

    // Time — multi-day spans fold into total hours
    #[test]
    fn time_multi_day_folds_into_hours() {
        // 2 days + 3 hours = 51 hours
        let b = value_to_bytes(Value::Time(false, 2, 3, 0, 0, 0));
        assert_eq!(b, "51:00:00");
    }

    // Time — with micros
    #[test]
    fn time_with_micros_formats_fractional() {
        let b = value_to_bytes(Value::Time(false, 0, 0, 0, 1, 500000));
        assert_eq!(b, "00:00:01.500000");
    }

    // ── mysql_column_type_to_native ───────────────────────────────────────────

    #[test]
    fn tiny_maps_to_tinyint() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_TINY).kind,
            NativeTypeKind::TinyInt
        );
    }

    #[test]
    fn longlong_maps_to_bigint() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_LONGLONG).kind,
            NativeTypeKind::BigInt
        );
    }

    #[test]
    fn newdecimal_maps_to_decimal() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_NEWDECIMAL).kind,
            NativeTypeKind::Decimal
        );
    }

    #[test]
    fn datetime2_maps_to_datetime() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_DATETIME2).kind,
            NativeTypeKind::DateTime
        );
    }

    #[test]
    fn json_maps_to_json() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_JSON).kind,
            NativeTypeKind::Json
        );
    }

    #[test]
    fn blob_maps_to_text() {
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_BLOB).kind,
            NativeTypeKind::Text
        );
    }

    #[test]
    fn unknown_type_maps_to_varchar() {
        // MYSQL_TYPE_STRING is a catch-all
        assert_eq!(
            mysql_column_type_to_native(ColumnType::MYSQL_TYPE_STRING).kind,
            NativeTypeKind::Varchar
        );
    }

    #[test]
    fn column_type_always_returns_no_precision_or_scale() {
        let info = mysql_column_type_to_native(ColumnType::MYSQL_TYPE_NEWDECIMAL);
        assert!(info.precision.is_none());
        assert!(info.scale.is_none());
        assert!(!info.unsigned);
    }
}
