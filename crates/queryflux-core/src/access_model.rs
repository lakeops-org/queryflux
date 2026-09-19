//! Neutral data model for data-level access control (row filtering, column masking,
//! table/column allow-deny).
//!
//! Pure data — no I/O, no policy-engine wire types. `queryflux-access-control` maps this
//! to/from a provider's wire format (OPA today); `queryflux-translation` and the dispatch
//! stage consume it. Kept in `queryflux-core` so no crate needs the policy layer just to
//! speak the vocabulary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Verified caller identity handed to the policy engine. Never the backend connection
/// credential, never client-declared session parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Identity {
    pub user: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, Value>,
}

/// A namespaced operation, e.g. `table.select`, `table.insert`. Namespaced so
/// schema/catalog-level operations can be added later without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Operation(pub String);

impl Operation {
    /// Operations the guard classifies and can evaluate. Anything else in a connection's
    /// `operations` list is a typo that would silently switch enforcement off, so config
    /// validation rejects it.
    pub const SUPPORTED: &'static [&'static str] = &[
        "table.select",
        "table.insert",
        "table.update",
        "table.delete",
        "table.merge",
        "table.truncate",
        "table.create",
        "table.drop",
        "table.alter",
        "view.create",
        "view.drop",
        "view.alter",
        "schema.create",
        "schema.drop",
        "catalog.create",
        "catalog.drop",
    ];

    /// The operation a `CREATE OR REPLACE` also performs: replacing destroys the existing
    /// object, so it needs the matching `*.drop` too. `None` for anything that isn't a create.
    pub fn drop_counterpart(&self) -> Option<Self> {
        let (kind, verb) = self.0.split_once('.')?;
        (verb == "create").then(|| Self(format!("{kind}.drop")))
    }

    pub fn table_select() -> Self {
        Self("table.select".to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// True for read operations (masks only apply to reads).
    pub fn is_read(&self) -> bool {
        self.0 == "table.select"
    }
}

/// Which columns of a table the query touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Columns {
    /// `SELECT *` or schema unresolved — the caller sends "all columns".
    All,
    Named(Vec<String>),
}

/// What kind of catalog object a resource is. Reads always name tables; DDL can also target
/// views, schemas and catalogs (`CREATE DATABASE` is reported as a catalog).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceKind {
    #[default]
    Table,
    View,
    Schema,
    Catalog,
}

impl ResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceKind::Table => "table",
            ResourceKind::View => "view",
            ResourceKind::Schema => "schema",
            ResourceKind::Catalog => "catalog",
        }
    }
}

/// One object (a table with its referenced columns, or — for DDL — a view, schema or catalog)
/// a statement touches. For `Schema` the object is `catalog`.`schema` and `table` is empty; for
/// `Catalog` only `catalog` is set.
#[derive(Debug, Clone)]
pub struct AccessResource {
    pub kind: ResourceKind,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub columns: Columns,
}

impl AccessResource {
    /// The object's own name — the table/view name, the schema name, or the catalog name.
    /// This is the key a policy's per-resource decision is matched back on.
    pub fn name(&self) -> &str {
        match self.kind {
            ResourceKind::Table | ResourceKind::View => &self.table,
            ResourceKind::Schema => self.schema.as_deref().unwrap_or_default(),
            ResourceKind::Catalog => self.catalog.as_deref().unwrap_or_default(),
        }
    }

    /// `schema.table` (or bare `table`) for a table/view — the key policy decisions and the
    /// rewrite match on. `catalog.schema` (or bare `schema`) for a schema, and the bare
    /// catalog name for a catalog — what audit/error messages display for those DDL targets.
    pub fn qualified_name(&self) -> String {
        match self.kind {
            ResourceKind::Table | ResourceKind::View => match &self.schema {
                Some(s) => format!("{s}.{}", self.table),
                None => self.table.clone(),
            },
            ResourceKind::Schema => match &self.catalog {
                Some(c) => format!("{c}.{}", self.name()),
                None => self.name().to_string(),
            },
            ResourceKind::Catalog => self.name().to_string(),
        }
    }
}

/// Dynamic context for policy evaluation. `session_params` is an allowlisted subset of the
/// client's session context — usable to *build* a filter expression, never read as an
/// allow/deny input.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub cluster_group: String,
    pub engine: String,
    pub query_id: String,
    pub session_params: BTreeMap<String, String>,
}

/// The full request to the policy engine.
#[derive(Debug, Clone)]
pub struct AccessRequest {
    pub identity: Identity,
    pub operation: Operation,
    pub resources: Vec<AccessResource>,
    pub context: RequestContext,
}

/// How a column is masked. Named types render to portable SQL; `Custom` carries a raw
/// source-dialect expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaskType {
    /// `NULL`
    #[serde(rename = "NULL")]
    Null,
    /// Replace alphanumerics with `x`
    #[serde(rename = "REDACT")]
    Redact,
    /// Show only the last 4 characters
    #[serde(rename = "SHOW_LAST_4")]
    ShowLast4,
    /// Show only the first 4 characters
    #[serde(rename = "SHOW_FIRST_4")]
    ShowFirst4,
    /// Replace with a constant (`value`)
    #[serde(rename = "CONSTANT")]
    Constant,
    /// Truncate a date/timestamp to the year
    #[serde(rename = "DATE_SHOW_YEAR")]
    DateShowYear,
    /// SHA-256 hash — best-effort under transpilation
    #[serde(rename = "HASH")]
    Hash,
    /// Raw expression supplied in `expression`
    #[serde(rename = "CUSTOM")]
    Custom,
}

/// A row filter for a table. `expression` is a source-dialect boolean SQL string.
/// `ucast` is reserved for a future structured form and is rejected in v1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowFilter {
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub ucast: Option<Value>,
}

/// A column mask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMask {
    pub column: String,
    #[serde(rename = "type")]
    pub mask_type: MaskType,
    /// For `CONSTANT`.
    #[serde(default)]
    pub value: Option<String>,
    /// For `CUSTOM`.
    #[serde(default)]
    pub expression: Option<String>,
}

/// The policy engine's verdict for one table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDecision {
    pub table: String,
    pub allow: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub row_filters: Vec<RowFilter>,
    #[serde(default)]
    pub column_masks: Vec<ColumnMask>,
}

/// The full decision — one entry per resource in the request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccessDecision {
    #[serde(default)]
    pub resources: Vec<ResourceDecision>,
}

impl AccessDecision {
    /// Every resource allowed, no filters/masks.
    pub fn allow_all() -> Self {
        Self::default()
    }

    /// Deny with a single synthetic denied resource carrying `reason`.
    pub fn deny_all(reason: impl Into<String>) -> Self {
        Self {
            resources: vec![ResourceDecision {
                table: "*".to_string(),
                allow: false,
                reason: Some(reason.into()),
                row_filters: Vec::new(),
                column_masks: Vec::new(),
            }],
        }
    }

    /// True when every resource is allowed.
    pub fn is_allowed(&self) -> bool {
        self.resources.iter().all(|r| r.allow)
    }

    /// `(table, reason)` of the first denied resource, if any.
    pub fn first_denied(&self) -> Option<(&str, &str)> {
        self.resources.iter().find(|r| !r.allow).map(|r| {
            (
                r.table.as_str(),
                r.reason.as_deref().unwrap_or("access denied"),
            )
        })
    }

    /// `(table, expression)` for every row filter with an `expression`.
    pub fn row_filters(&self) -> impl Iterator<Item = (&str, &str)> {
        self.resources.iter().flat_map(|r| {
            r.row_filters
                .iter()
                .filter_map(move |f| f.expression.as_deref().map(|e| (r.table.as_str(), e)))
        })
    }

    /// `(table, &ColumnMask)` for every mask.
    pub fn column_masks(&self) -> impl Iterator<Item = (&str, &ColumnMask)> {
        self.resources
            .iter()
            .flat_map(|r| r.column_masks.iter().map(move |m| (r.table.as_str(), m)))
    }

    /// True when the query is allowed but a guard must still rewrite it.
    pub fn has_rewrite(&self) -> bool {
        self.is_allowed()
            && self
                .resources
                .iter()
                .any(|r| !r.row_filters.is_empty() || !r.column_masks.is_empty())
    }

    /// Any `RowFilter` carries a `ucast` value (unimplemented in v1).
    pub fn has_ucast_filter(&self) -> bool {
        self.resources
            .iter()
            .any(|r| r.row_filters.iter().any(|f| f.ucast.is_some()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rd(table: &str, allow: bool) -> ResourceDecision {
        ResourceDecision {
            table: table.to_string(),
            allow,
            reason: None,
            row_filters: Vec::new(),
            column_masks: Vec::new(),
        }
    }

    #[test]
    fn create_operations_have_a_drop_counterpart() {
        for (create, drop) in [
            ("table.create", "table.drop"),
            ("view.create", "view.drop"),
            ("schema.create", "schema.drop"),
            ("catalog.create", "catalog.drop"),
        ] {
            assert_eq!(
                Operation(create.to_string()).drop_counterpart(),
                Some(Operation(drop.to_string()))
            );
            assert!(Operation::SUPPORTED.contains(&create) && Operation::SUPPORTED.contains(&drop));
        }
        assert_eq!(Operation::table_select().drop_counterpart(), None);
        assert_eq!(Operation("table.drop".to_string()).drop_counterpart(), None);
    }

    #[test]
    fn resource_name_is_the_kinds_own_leaf() {
        let r = |kind, catalog: Option<&str>, schema: Option<&str>, table: &str| AccessResource {
            kind,
            catalog: catalog.map(String::from),
            schema: schema.map(String::from),
            table: table.to_string(),
            columns: Columns::All,
        };
        assert_eq!(
            r(ResourceKind::Table, Some("c"), Some("s"), "orders").name(),
            "orders"
        );
        assert_eq!(r(ResourceKind::View, None, Some("s"), "v").name(), "v");
        assert_eq!(
            r(ResourceKind::Schema, Some("c"), Some("analytics"), "").name(),
            "analytics"
        );
        assert_eq!(
            r(ResourceKind::Catalog, Some("prod"), None, "").name(),
            "prod"
        );
    }

    /// A `Schema`/`Catalog` resource's `table` is empty, so `qualified_name` must build its
    /// display name from `kind` instead of falling through to the table/view formatting
    /// (which would otherwise render as a bare trailing dot or an empty string).
    #[test]
    fn qualified_name_covers_every_kind() {
        let r = |kind, catalog: Option<&str>, schema: Option<&str>, table: &str| AccessResource {
            kind,
            catalog: catalog.map(String::from),
            schema: schema.map(String::from),
            table: table.to_string(),
            columns: Columns::All,
        };
        assert_eq!(
            r(ResourceKind::Table, None, Some("s"), "orders").qualified_name(),
            "s.orders"
        );
        assert_eq!(
            r(ResourceKind::Table, None, None, "orders").qualified_name(),
            "orders"
        );
        assert_eq!(
            r(ResourceKind::Schema, Some("prod"), Some("analytics"), "").qualified_name(),
            "prod.analytics"
        );
        assert_eq!(
            r(ResourceKind::Schema, None, Some("analytics"), "").qualified_name(),
            "analytics"
        );
        assert_eq!(
            r(ResourceKind::Catalog, Some("prod"), None, "").qualified_name(),
            "prod"
        );
    }

    #[test]
    fn is_allowed_and_first_denied() {
        let d = AccessDecision {
            resources: vec![rd("a", true), rd("b", false)],
        };
        assert!(!d.is_allowed());
        assert_eq!(d.first_denied().map(|(t, _)| t), Some("b"));

        assert!(AccessDecision::allow_all().is_allowed());
        assert!(!AccessDecision::deny_all("nope").is_allowed());
    }

    #[test]
    fn iters_over_filters_and_masks() {
        let mut a = rd("s.a", true);
        a.row_filters.push(RowFilter {
            expression: Some("x = 1".to_string()),
            ucast: None,
        });
        a.column_masks.push(ColumnMask {
            column: "ssn".to_string(),
            mask_type: MaskType::ShowLast4,
            value: None,
            expression: None,
        });
        let d = AccessDecision { resources: vec![a] };
        assert!(d.has_rewrite());
        assert_eq!(d.row_filters().collect::<Vec<_>>(), vec![("s.a", "x = 1")]);
        assert_eq!(d.column_masks().count(), 1);
    }

    #[test]
    fn column_mask_deserializes_type_field() {
        let m: ColumnMask =
            serde_json::from_str(r#"{"column":"ssn","type":"SHOW_LAST_4"}"#).unwrap();
        assert_eq!(m.mask_type, MaskType::ShowLast4);
    }
}
