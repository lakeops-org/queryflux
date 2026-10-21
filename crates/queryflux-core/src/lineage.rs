//! Column-level lineage of a statement that writes a table: which input columns each
//! written column is computed from. Extracted by `queryflux_translation::column_lineage`,
//! carried on query records, and reported as OpenLineage's `columnLineage` facet.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnLineage {
    /// Each written column → the input columns its value is computed from.
    pub fields: BTreeMap<String, Vec<InputColumn>>,
    /// Input columns that decide which rows are written, or how they group and order
    /// (`WHERE`, `JOIN … ON`, `GROUP BY`, `ORDER BY`, window partitions, `CASE` conditions).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indirect: Vec<InputColumn>,
}

/// An input column, its table named as the SQL wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputColumn {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub column: String,
    /// The OpenLineage transformation subtype. Direct: `IDENTITY`, `TRANSFORMATION`,
    /// `AGGREGATION`. Indirect: `FILTER`, `JOIN`, `GROUP_BY`, `SORT`, `WINDOW`, `CONDITIONAL`.
    pub subtype: String,
}
