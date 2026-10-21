//! Resolved table/column schema for a query.
//!
//! Produced by the translation layer's catalog resolution and consumed by both
//! schema-aware translation and the access-control guard's scan-site rewrite
//! (column enumeration for masked tables). Lives in `queryflux-core` so crates
//! that need only the shape — e.g. `queryflux-guardrails` via `GuardContext` —
//! do not depend on `queryflux-translation`.

use std::collections::{BTreeMap, HashMap};

use indexmap::IndexMap;

/// `column name → SQL type string`, in the table's declared column order.
pub type ColumnMap = IndexMap<String, String>;

/// A table as looked up in the catalog: the reference's own catalog and database, with the
/// session defaults filling in what it leaves out. `""` where neither gives one.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableKey {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

/// `"t"`, `"db.t"` or `"catalog.db.t"`; missing leading parts are `""`.
impl From<&str> for TableKey {
    fn from(name: &str) -> Self {
        let mut parts = name.rsplitn(3, '.');
        let table = parts.next().unwrap_or_default().to_string();
        let database = parts.next().unwrap_or_default().to_string();
        let catalog = parts.next().unwrap_or_default().to_string();
        Self {
            catalog,
            database,
            table,
        }
    }
}

impl From<String> for TableKey {
    fn from(name: String) -> Self {
        name.as_str().into()
    }
}

/// Schema context: each referenced table → { column name → SQL type string }.
///
/// Columns keep the catalog's declared order — the access-control rewrite builds a positional
/// projection from them, so `SELECT *` over a masked table must match the base table.
#[derive(Debug, Default, Clone)]
pub struct SchemaContext {
    /// The session's default catalog, applied to references that name a database but no catalog.
    pub catalog: Option<String>,
    /// The session's default database, applied to unqualified references.
    pub database: Option<String>,
    pub tables: HashMap<TableKey, ColumnMap>,
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
        let mut tables: Vec<(&TableKey, &ColumnMap)> = self.tables.iter().collect();
        tables.sort_by(|a, b| a.0.cmp(b.0));
        h.write_u64(tables.len() as u64);
        for (key, cols) in tables {
            h.write_str(&key.catalog);
            h.write_str(&key.database);
            h.write_str(&key.table);
            h.write_u64(cols.len() as u64);
            for (col, ty) in cols {
                h.write_str(col);
                h.write_str(ty);
            }
        }
        h.finish()
    }

    /// The JSON sqlglot's Python side reads: the session defaults, and the tables nested as
    /// `{catalog: {db: {table: {col: type}}}}` (sqlglot's `MappingSchema` needs one depth
    /// for every table, so a missing level is `""`).
    pub fn to_sqlglot_json(&self) -> String {
        if self.tables.is_empty() {
            return "{}".to_string();
        }
        // Typed maps, not `serde_json::Value`: `Value` objects sort their keys, which
        // would lose the declared column order.
        type Nested<'a> = BTreeMap<&'a str, BTreeMap<&'a str, BTreeMap<&'a str, &'a ColumnMap>>>;
        let mut tables: Nested = BTreeMap::new();
        for (key, cols) in &self.tables {
            tables
                .entry(&key.catalog)
                .or_default()
                .entry(&key.database)
                .or_default()
                .insert(&key.table, cols);
        }
        #[derive(serde::Serialize)]
        struct Out<'a> {
            catalog: Option<&'a str>,
            db: Option<&'a str>,
            tables: Nested<'a>,
        }
        serde_json::to_string(&Out {
            catalog: self.catalog.as_deref(),
            db: self.database.as_deref(),
            tables,
        })
        .unwrap_or_else(|_| "{}".to_string())
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
                "orders".into(),
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

    #[test]
    fn table_key_parses_dotted_names_right_aligned() {
        let k = |c: &str, d: &str, t: &str| TableKey {
            catalog: c.into(),
            database: d.into(),
            table: t.into(),
        };
        assert_eq!(TableKey::from("t"), k("", "", "t"));
        assert_eq!(TableKey::from("db.t"), k("", "db", "t"));
        assert_eq!(TableKey::from("c.db.t"), k("c", "db", "t"));
    }

    #[test]
    fn sqlglot_json_nests_tables_and_keeps_column_order() {
        let mut ctx = ctx(&[("zeta", "INT"), ("alpha", "INT")]);
        ctx.tables.insert(
            "crm.orders".into(),
            [("id".to_string(), "INT".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            ctx.to_sqlglot_json(),
            r#"{"catalog":"hive","db":"sales","tables":{"":{"":{"orders":{"zeta":"INT","alpha":"INT"}},"crm":{"orders":{"id":"INT"}}}}}"#
        );
        assert_eq!(SchemaContext::default().to_sqlglot_json(), "{}");
    }
}
