//! Query fingerprinting for QueryFlux.
//!
//! Two paths:
//! - [`fast_hash`] — zero-allocation hot-path hash (~200 ns). Used by the router.
//! - [`rich_fingerprint`] — AST-based normalization via polyglot-sql (~10-100 μs).
//!   Returns a [`QueryFingerprint`] with hashes and digest text. Called inside
//!   `tokio::spawn` after query completion, never on the routing hot path.

pub mod fallback;
pub mod fast;
pub mod polyglot_pool;
pub mod rich;

pub use fast::fast_hash;
pub use rich::{is_deterministic, rich_fingerprint, QueryFingerprint};

/// Map a `SqlDialect` to the dialect name string polyglot-sql expects.
/// Kept here — not in queryflux-core — because polyglot-sql is an implementation
/// detail of this crate only.
pub fn polyglot_dialect(dialect: &queryflux_core::query::SqlDialect) -> String {
    use queryflux_core::query::SqlDialect;
    match dialect {
        SqlDialect::Sqlglot(s) => s.clone(),
        SqlDialect::Trino => "trino".to_string(),
        SqlDialect::Athena => "presto".to_string(),
        SqlDialect::DuckDb => "duckdb".to_string(),
        SqlDialect::StarRocks => "starrocks".to_string(),
        SqlDialect::ClickHouse => "clickhouse".to_string(),
        SqlDialect::MySql => "mysql".to_string(),
        SqlDialect::Postgres => "postgresql".to_string(),
        SqlDialect::Sqlite => "sqlite".to_string(),
        SqlDialect::Snowflake => "snowflake".to_string(),
        SqlDialect::BigQuery => "bigquery".to_string(),
        SqlDialect::Databricks => "databricks".to_string(),
        SqlDialect::MsSql => "tsql".to_string(),
        SqlDialect::Redshift => "redshift".to_string(),
        SqlDialect::Exasol => "exasol".to_string(),
        SqlDialect::Generic => "generic".to_string(),
    }
}
