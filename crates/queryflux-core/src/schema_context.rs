//! Resolved table/column schema for a query.
//!
//! Produced by the translation layer's catalog resolution and consumed by both
//! schema-aware translation and the access-control guard's scan-site rewrite
//! (column enumeration for masked tables). Lives in `queryflux-core` so crates
//! that need only the shape — e.g. `queryflux-guardrails` via `GuardContext` —
//! do not depend on `queryflux-translation`.

use std::collections::HashMap;

use indexmap::IndexMap;

/// `column name → SQL type string`, in the table's declared column order.
pub type ColumnMap = IndexMap<String, String>;

/// Schema context: table name → { column name → SQL type string }.
///
/// Columns keep the catalog's declared order — the access-control rewrite builds a positional
/// projection from them, so `SELECT *` over a masked table must match the base table.
#[derive(Debug, Default, Clone)]
pub struct SchemaContext {
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// `table_name → { col_name → type_string }`
    pub tables: HashMap<String, ColumnMap>,
}

impl SchemaContext {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Stable 128-bit hash of everything in this context (catalog, database, tables in
    /// name order, columns in declared order with their types). Cache keys use it in place
    /// of the whole schema.
    pub fn content_hash(&self) -> u128 {
        let mut h = crate::bounded_cache::ContentHasher::new();
        h.write_str(self.catalog.as_deref().unwrap_or("\u{0}"));
        h.write_str(self.database.as_deref().unwrap_or("\u{0}"));
        let mut tables: Vec<(&String, &ColumnMap)> = self.tables.iter().collect();
        tables.sort_by(|a, b| a.0.cmp(b.0));
        h.write_u64(tables.len() as u64);
        for (table, cols) in tables {
            h.write_str(table);
            h.write_u64(cols.len() as u64);
            for (col, ty) in cols {
                h.write_str(col);
                h.write_str(ty);
            }
        }
        h.finish()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(cols: &[(&str, &str)]) -> SchemaContext {
        SchemaContext {
            catalog: Some("hive".into()),
            database: Some("sales".into()),
            tables: [(
                "orders".to_string(),
                cols.iter()
                    .map(|(c, t)| (c.to_string(), t.to_string()))
                    .collect(),
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn content_hash_tracks_every_part() {
        let base = ctx(&[("id", "BIGINT"), ("name", "VARCHAR")]);
        assert_eq!(base.content_hash(), base.clone().content_hash());
        let changed = [
            ctx(&[("id", "INT"), ("name", "VARCHAR")]),
            ctx(&[("name", "VARCHAR"), ("id", "BIGINT")]),
            ctx(&[("id", "BIGINT")]),
            SchemaContext {
                catalog: None,
                ..base.clone()
            },
            SchemaContext {
                database: Some("other".into()),
                ..base.clone()
            },
            SchemaContext::default(),
        ];
        for other in changed {
            assert_ne!(base.content_hash(), other.content_hash(), "{other:?}");
        }
    }
}
