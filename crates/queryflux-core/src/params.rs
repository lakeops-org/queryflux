//! Typed query parameters for parameterized SQL execution.
//!
//! `QueryParams` flows from frontend (where the client sends `?` placeholders + typed values)
//! through the dispatch pipeline to backend adapters.  Adapters that support native prepared
//! statements receive the raw `QueryParams`; adapters that do not use
//! [`interpolate_params`] as a fallback, which substitutes each `?` with its typed literal.
//!
//! ## Protocol mapping
//! | Frontend protocol     | Wire representation                                              |
//! |-----------------------|------------------------------------------------------------------|
//! | Snowflake HTTP / API  | `parameterBindings` / `bindings` JSON map → `Vec<QueryParam>`   |
//! | PostgreSQL wire       | Extended-query `Bind` message typed params → `Vec<QueryParam>`  |
//! | MySQL wire            | `COM_STMT_EXECUTE` binary protocol params → `Vec<QueryParam>`   |
//! | Flight SQL            | Arrow IPC parameter batch → `Vec<QueryParam>`                   |

use serde::{Deserialize, Serialize};

/// A single typed query parameter corresponding to a `?` placeholder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryParam {
    /// Text / string value — rendered as `'value'` with internal `'` escaped to `''`.
    Text(String),
    /// Pre-validated numeric literal (integer or decimal) stored as its original string
    /// representation so that `"42"` stays `"42"` rather than becoming `"42.0"`.
    Numeric(String),
    /// Boolean — rendered as `TRUE` or `FALSE`.
    Boolean(bool),
    /// ISO-8601 date string (e.g. `"2025-01-15"`) — rendered as `DATE 'value'`.
    Date(String),
    /// Timestamp string (e.g. `"2025-01-15 12:00:00"`) — rendered as `TIMESTAMP 'value'`.
    Timestamp(String),
    /// Time string (e.g. `"12:00:00"`) — rendered as `TIME 'value'`.
    Time(String),
    /// SQL NULL.
    Null,
}

/// Ordered list of positional query parameters corresponding to `?` placeholders.
///
/// Empty means no parameters. Index 0 → first `?`, index 1 → second `?`, etc.
pub type QueryParams = Vec<QueryParam>;
