---
title: Query Parameters
description: Typed positional ? placeholders from Snowflake and other frontends through dispatch to native backend binding.
image: img/queryflux-hero-banner.png
---
# Query parameters

Query parameters let clients send **typed positional values** separately from SQL text, using `?` as placeholders. QueryFlux reads them from the frontend wire protocol, carries them through the dispatch pipeline, and delivers them to each backend engine using that engine's native binding mechanism — or falls back to safe string interpolation for engines that do not have one.

---

## Why parameters matter

Without parameters, clients that want to filter on a value must embed it directly in SQL:

```sql
SELECT * FROM orders WHERE customer_id = 42 AND status = 'shipped'
```

With parameters the SQL template stays fixed and values are passed separately:

```sql
SELECT * FROM orders WHERE customer_id = ? AND status = ?
-- bindings: [42, "shipped"]
```

This matters for three reasons:

1. **Safety** — values never touch the SQL parser, so injection is structurally impossible rather than relying on escaping.
2. **Correctness** — the engine sees the value with its proper type (integer, boolean, timestamp) rather than a string that must be re-parsed.
3. **Plan reuse** — some engines (Athena, DuckDB) can plan the statement once and re-execute with different values.

---

## The `QueryParam` type

All parameters are represented as a `QueryParam` enum defined in `queryflux-core/src/params.rs`. The type is intentionally kept at the **logical level** — it carries enough information for any backend to bind correctly without being tied to any specific wire format.

| Variant | Rust type | Description |
|---------|-----------|-------------|
| `Text(String)` | `&str` | Arbitrary string value. Single-quoted in SQL interpolation. |
| `Numeric(String)` | pre-validated string | Integer or float as a string (e.g. `"42"`, `"3.14"`). Validated at parse time; stored as string to preserve representation (`"42"` not `"42.0"`). |
| `Boolean(bool)` | `bool` | `TRUE` / `FALSE`. |
| `Date(String)` | ISO-8601 string | `YYYY-MM-DD`. |
| `Timestamp(String)` | ISO-8601 string | `YYYY-MM-DD HH:MM:SS[.ffffff]`. |
| `Time(String)` | string | `HH:MM:SS[.ffffff]`. |
| `Null` | — | SQL `NULL`. |

`QueryParams` is a type alias for `Vec<QueryParam>`. Parameters are **positional** — index 0 maps to the first `?` in the SQL text.

---

## How parameters flow through the system

```
Client request (wire protocol)
         │
         ▼
   Frontend handler
   (e.g. Snowflake HTTP)
         │
         │  bindings_to_params() → Vec<QueryParam>
         ▼
   execute_to_sink() / dispatch_query()
         │
         │  passes sql + params together
         ▼
      Dispatch
         │
         │  maybe_translate(sql, src_dialect, tgt_dialect)
         │  (? placeholders are preserved through translation)
         ▼
   Translated SQL (target dialect)
         │
         ├── adapter.supports_native_params() == true?
         │       │ YES → pass translated sql + params to adapter
         │       │       adapter binds natively
         │       │
         │       └── NO  → interpolate_params(sql, params, tgt_dialect)
         │                  AST-safe substitution via polyglot-sql
         │                  pass final sql + empty params to adapter
         ▼
    Engine adapter
    (DuckDB, StarRocks, Athena, ADBC, …)
```

The dispatch layer in `queryflux-frontend/src/dispatch.rs` is the single decision point. Translation always happens first so `?` placeholders are carried through unchanged into the target dialect. Adapters that override `supports_native_params() -> bool` to return `true` receive params untouched; all others receive pre-interpolated SQL with an empty params slice.

---

## Dispatch interpolation fallback

When an adapter does not support native params, dispatch calls `interpolate_params` before the adapter sees the query. This function uses [`polyglot-sql`](https://github.com/tobilg/polyglot) to parse the SQL into an AST, replaces placeholder nodes with typed literal expressions, and regenerates SQL for the target dialect.

```
translated SQL (target dialect, ? intact)
    │
    │  polyglot::parse(sql, target_dialect)
    ▼
AST — Expression tree
    │
    │  transform(): replace Placeholder / Parameter{Question} nodes
    ▼
AST — literals substituted
    │
    │  polyglot::generate(ast, target_dialect)
    ▼
final SQL string (target dialect, literals embedded)
```

Because interpolation works on the AST rather than raw text, `?` inside comments, string literals, `$$`-quoted blocks, and other non-placeholder positions is never incorrectly consumed.

| `QueryParam` variant | AST node | Example output (Trino) |
|----------------------|----------|------------------------|
| `Text(s)` | `Literal::String` | `'alice'`, `'o''brien'` |
| `Numeric(s)` | `Literal::Number` | `42`, `3.14` |
| `Boolean(b)` | `BooleanLiteral` | `TRUE` / `FALSE` |
| `Date(s)` | `Literal::Date` | `DATE '2025-01-15'` |
| `Timestamp(s)` | `Literal::Timestamp` | `TIMESTAMP '2025-01-15 12:00:00'` |
| `Time(s)` | `Literal::Time` | `TIME '12:00:00'` |
| `Null` | `Null` | `NULL` |

The target dialect is passed to both `parse` and `generate` so that dialect-specific quoting, keyword casing, and literal syntax are handled correctly for the backend receiving the query.

The interpolation code and its unit tests live in `queryflux-core/src/params.rs`.

---

## Native binding per adapter

| Adapter | Native params | Binding mechanism |
|---------|--------------|-------------------|
| **DuckDB** | yes | `stmt.query_arrow(duckdb::params_from_iter(Vec<duckdb::types::Value>))` |
| **StarRocks** | yes | `conn.exec::<Row, _, _>(sql, mysql_async::Params::Positional(Vec<Value>))` |
| **Athena** | yes | `start_query_execution().set_execution_parameters(Some(Vec<String>))` in the AWS SDK |
| **ADBC** | yes | `stmt.bind(RecordBatch)` — one column per `?`, one row per execution |
| **DuckDB HTTP** | no — interpolation fallback | HTTP API has no parameter binding endpoint |
| **Trino** | no — interpolation fallback | `PREPARE` / `EXECUTE` requires two full HTTP round-trips; the fallback is functionally equivalent at lower cost |

### DuckDB

Numeric params are bound as `BigInt` when the string parses as `i64`, otherwise `Double`. Date, timestamp, and time values are bound as `Text` and let DuckDB's parser handle the conversion.

```rust
fn query_param_to_duckdb(p: &QueryParam) -> duckdb::types::Value {
    match p {
        QueryParam::Numeric(s) => {
            if let Ok(n) = s.parse::<i64>() { Value::BigInt(n) }
            else if let Ok(f) = s.parse::<f64>() { Value::Double(f) }
            else { Value::Text(s.clone()) }
        }
        // …
    }
}
```

### StarRocks

Uses `mysql_async` prepared statements. Booleans map to `Int(1)` / `Int(0)` (MySQL has no native boolean wire type). Date/timestamp/time values are sent as byte strings and parsed by StarRocks.

The shared helper `queryflux-engine-adapters/src/mysql_native/mod.rs` handles both the Arrow path (`execute_as_arrow`) and the native MySQL path (`execute_native`) — both accept `params: &QueryParams`.

### Athena

Athena's `execution_parameters` field takes `Vec<String>` — plain string representations of each value. Athena handles quoting and type coercion on its side. Booleans are sent as `"true"` / `"false"` (lowercase). `Null` is sent as `"NULL"`.

### ADBC

ADBC binds parameters as an Arrow `RecordBatch` via `stmt.bind(batch)`. The batch has one column per `?` placeholder, one row, and column names `p1`, `p2`, … (positional). Column Arrow types are chosen to preserve precision:

| `QueryParam` | Arrow column type |
|-------------|-------------------|
| `Text`, `Date`, `Timestamp`, `Time` | `Utf8` |
| `Numeric` — parses as `i64` | `Int64` |
| `Numeric` — parses as `f64` | `Float64` |
| `Numeric` — unparseable | `Utf8` |
| `Boolean` | `Boolean` |
| `Null` | `Null` |

---

## Frontend support

### Snowflake HTTP

The Snowflake connector sends parameters in the `parameterBindings` (wire protocol) or `bindings` (SQL API v2) field of the query request body:

```json
{
  "sqlText": "SELECT * FROM orders WHERE id = ? AND status = ?",
  "parameterBindings": {
    "1": { "type": "FIXED",  "value": "42"      },
    "2": { "type": "TEXT",   "value": "shipped"  }
  }
}
```

`bindings_to_params()` in `queryflux-frontend/src/snowflake/http/handlers/bindings.rs` converts this map to `QueryParams`. Keys are sorted numerically (`"1"`, `"2"`, …) regardless of JSON key order. The Snowflake type string maps to `QueryParam` as follows:

| Snowflake type | `QueryParam` variant |
|----------------|----------------------|
| `FIXED`, `REAL` | `Numeric` (pre-validated) |
| `BOOLEAN` | `Boolean` |
| `DATE` | `Date` |
| `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP` | `Timestamp` |
| `TIME` | `Time` |
| `TEXT`, `VARIANT`, others | `Text` |
| any with value `"NULL"` | `Null` |

Both the wire protocol handler (`/queries/v1/query-request`) and the SQL API v2 handler (`/api/v2/statements`) use the same conversion.

### Other frontends

| Frontend | Parameter support |
|----------|-----------------|
| **Trino HTTP** | No binding syntax in the Trino protocol; params arrive as literals embedded in SQL. |
| **Postgres wire** | Extended query protocol (`$1`, `$2` placeholders) — not yet wired to `QueryParams`. |
| **MySQL wire** | Prepared statement protocol — not yet wired to `QueryParams`. |
| **Flight SQL** | `CommandPreparedStatementQuery` — not yet wired to `QueryParams`. |

---

## Adding native param support to a new adapter

1. **Override `supports_native_params`** on your `SyncAdapter` or `AsyncAdapter` implementation:

   ```rust
   fn supports_native_params(&self) -> bool {
       true
   }
   ```

   This signals to dispatch to skip interpolation and pass `params` unchanged.

2. **Update `execute_as_arrow`** (and `execute_native` / `submit_query` if applicable) to accept and use `params`:

   ```rust
   async fn execute_as_arrow(&self, sql: &str, …, params: &QueryParams) -> Result<SyncExecution> {
       let native_params: Vec<YourType> = params.iter().map(query_param_to_your_type).collect();
       // bind and execute …
   }
   ```

3. **Write a `query_param_to_your_type` helper** that maps each `QueryParam` variant to your driver's native value type. The DuckDB and StarRocks helpers in their respective `mod.rs` files are the canonical examples.

4. **Add unit tests** covering every `QueryParam` variant, including the `Numeric` integer/float/fallback disambiguation. See `duckdb/mod.rs` and `mysql_native/mod.rs` for the test pattern.

---

## Testing

### Unit tests

Each adapter's `query_param_to_*` conversion function has its own `#[cfg(test)]` block:

| File | Tests | Covers |
|------|-------|--------|
| `queryflux-core/src/params.rs` | 15 | `interpolate_params`: all types, string literal protection, escaping, ordering |
| `queryflux-engine-adapters/src/duckdb/mod.rs` | 11 | `query_param_to_duckdb`: all variants, int/float disambiguation |
| `queryflux-engine-adapters/src/mysql_native/mod.rs` | 11 | `query_param_to_mysql_value`: all variants, boolean as int |
| `queryflux-engine-adapters/src/starrocks/mod.rs` | 11 | `query_param_to_mysql_value` (StarRocks copy): all variants |
| `queryflux-engine-adapters/src/athena/mod.rs` | 9 | `query_param_to_athena_string`: all variants, null as `"NULL"` |
| `queryflux-engine-adapters/src/adbc/mod.rs` | 9 | `params_to_record_batch`: Arrow type per variant, column names, one-row invariant |
| `queryflux-frontend/src/snowflake/http/handlers/bindings.rs` | 15 | `bindings_to_params` and `apply_parameter_bindings`: Snowflake type mapping, ordering, SQL injection safety |

Run all unit tests with:

```bash
cargo test --workspace --lib
```

### E2E tests

`crates/queryflux-e2e-tests/tests/query_params_tests.rs` exercises the full stack — Snowflake HTTP frontend → dispatch → DuckDB native binding — without any external dependencies:

```bash
cargo test -p queryflux-e2e-tests --test query_params_tests
```

The test harness spins up an in-process server with both the Trino HTTP and Snowflake HTTP frontends on the same port, backed by an in-memory DuckDB instance. A minimal `SnowflakeClient` (`src/snowflake_client.rs`) handles login, query submission, and `rowsetBase64` decoding (Arrow IPC).

Key scenarios covered:

| Test | What it verifies |
|------|-----------------|
| `text_param_is_bound_correctly` | Text param round-trips as the correct string |
| `text_param_with_single_quote_is_safe` | `o'brien` is not SQL-injectable |
| `integer_param_used_in_arithmetic` | `SELECT ? * 2` with `21` returns `42` |
| `integer_param_filters_rows_correctly` | `WHERE n > ?` with `1` returns exactly 2 rows from a 3-row set |
| `text_param_filters_rows_correctly` | `WHERE name = ?` with `"bob"` returns only bob |
| `null_text_param_produces_null_row` | `NULL` value produces a SQL `NULL` cell |
| `multiple_params_are_bound_in_order` | Three params bound to three columns in correct order |
| `params_applied_in_numeric_key_order_regardless_of_json_order` | JSON key order does not affect binding order |
| `boolean_true/false_param_is_bound_correctly` | Boolean params are truthy / falsy |
| `invalid_sql_returns_error_not_panic` | Error responses are structured, not panics |
