//! Resolved table/column schema for a query.
//!
//! Produced by the translation layer's catalog resolution and consumed by both
//! schema-aware translation and the access-control guard's scan-site rewrite
//! (column enumeration for masked tables). Lives in `queryflux-core` so crates
//! that need only the shape — e.g. `queryflux-guardrails` via `GuardContext` —
//! do not depend on `queryflux-translation`.

use std::collections::HashMap;

/// Schema context: table name → { column name → SQL type string }.
#[derive(Debug, Default, Clone)]
pub struct SchemaContext {
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// `table_name → { col_name → type_string }`
    pub tables: HashMap<String, HashMap<String, String>>,
}

impl SchemaContext {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Column names for `table`, if known. Matches on the bare table name and, as a
    /// fallback, the last dot-separated segment of a qualified key.
    pub fn columns_for(&self, table: &str) -> Option<Vec<String>> {
        if let Some(cols) = self.tables.get(table) {
            return Some(cols.keys().cloned().collect());
        }
        let bare = table.rsplit('.').next().unwrap_or(table);
        self.tables
            .iter()
            .find(|(k, _)| k.rsplit('.').next().unwrap_or(k.as_str()) == bare)
            .map(|(_, cols)| cols.keys().cloned().collect())
    }
}
