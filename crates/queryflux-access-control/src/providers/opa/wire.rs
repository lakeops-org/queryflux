//! OPA wire format: `{"input": {...}}` request, `{"result": {...}}` response, and the
//! mapping to/from `queryflux_core::access_model`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use queryflux_core::access_model::{
    AccessDecision, AccessRequest, AccessResource, ColumnMask, Columns, ResourceDecision, RowFilter,
};

// ---- request ----

#[derive(Serialize)]
pub(super) struct OpaRequest<'a> {
    pub input: OpaInput<'a>,
}

#[derive(Serialize)]
pub(super) struct OpaInput<'a> {
    pub identity: WireIdentity<'a>,
    pub action: WireAction<'a>,
    pub context: WireContext<'a>,
}

#[derive(Serialize)]
pub(super) struct WireIdentity<'a> {
    pub user: &'a str,
    pub groups: &'a [String],
    pub roles: &'a [String],
    pub attributes: &'a BTreeMap<String, Value>,
}

#[derive(Serialize)]
pub(super) struct WireAction<'a> {
    pub operation: &'a str,
    pub resources: Vec<WireResource<'a>>,
}

#[derive(Serialize)]
pub(super) struct WireResource<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<&'a str>,
    pub table: &'a str,
    /// `null` = all columns (schema unresolved / `SELECT *`).
    pub columns: Option<&'a [String]>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireContext<'a> {
    pub cluster_group: &'a str,
    pub engine: &'a str,
    pub query_id: &'a str,
    pub session_params: &'a BTreeMap<String, String>,
}

pub(super) fn to_request(req: &AccessRequest) -> OpaRequest<'_> {
    OpaRequest {
        input: OpaInput {
            identity: WireIdentity {
                user: &req.identity.user,
                groups: &req.identity.groups,
                roles: &req.identity.roles,
                attributes: &req.identity.attributes,
            },
            action: WireAction {
                operation: req.operation.as_str(),
                resources: req
                    .resources
                    .iter()
                    .map(|r| WireResource {
                        catalog: r.catalog.as_deref(),
                        schema: r.schema.as_deref(),
                        table: &r.table,
                        columns: match &r.columns {
                            Columns::All => None,
                            Columns::Named(c) => Some(c.as_slice()),
                        },
                    })
                    .collect(),
            },
            context: WireContext {
                cluster_group: &req.context.cluster_group,
                engine: &req.context.engine,
                query_id: &req.context.query_id,
                session_params: &req.context.session_params,
            },
        },
    }
}

// ---- response ----

#[derive(Deserialize)]
pub(super) struct OpaResponse {
    #[serde(default)]
    pub result: Option<OpaResult>,
}

#[derive(Deserialize)]
pub(super) struct OpaResult {
    #[serde(default)]
    pub resources: Vec<WireResourceDecision>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WireResourceDecision {
    pub table: String,
    #[serde(default)]
    pub allow: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub row_filters: Vec<RowFilter>,
    #[serde(default)]
    pub column_masks: Vec<ColumnMask>,
}

/// Map an OPA `{"result": {...}}` body to a neutral [`AccessDecision`].
///
/// A missing `result` (OPA returns `{}` for "undefined") is treated as **deny-all** — the
/// policy must explicitly produce a per-resource verdict. A non-empty response that does not
/// cover every requested resource is completed with an explicit deny for each omitted one,
/// so a partial verdict can never read as "allowed" for a table the policy never judged.
pub(super) fn from_response(resp: OpaResponse, requested: &AccessRequest) -> AccessDecision {
    match resp.result {
        Some(result) if !result.resources.is_empty() => {
            let mut resources: Vec<ResourceDecision> = result
                .resources
                .into_iter()
                .map(|r| ResourceDecision {
                    table: r.table,
                    allow: r.allow,
                    reason: r.reason,
                    row_filters: r.row_filters,
                    column_masks: r.column_masks,
                })
                .collect();
            for res in &requested.resources {
                if !resources.iter().any(|d| decision_covers(&d.table, res)) {
                    let name = res.qualified_name();
                    resources.push(ResourceDecision {
                        reason: Some(format!("policy returned no decision for {name}")),
                        table: name,
                        allow: false,
                        row_filters: Vec::new(),
                        column_masks: Vec::new(),
                    });
                }
            }
            AccessDecision { resources }
        }
        _ => AccessDecision::deny_all(format!(
            "policy returned no decision for {} resource(s)",
            requested.resources.len()
        )),
    }
}

/// Whether a decision's `table` key refers to `res`. Policies echo the input's bare `table`
/// or build `schema.table` / `catalog.schema.table`; match case-insensitively, as the
/// rewrite does.
fn decision_covers(decision_table: &str, res: &AccessResource) -> bool {
    let d = decision_table.to_lowercase();
    let table = res.table.to_lowercase();
    if d == table {
        return true;
    }
    let Some(schema) = res.schema.as_deref().map(str::to_lowercase) else {
        return false;
    };
    let qualified = format!("{schema}.{table}");
    if d == qualified {
        return true;
    }
    res.catalog
        .as_deref()
        .is_some_and(|c| d == format!("{}.{qualified}", c.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::access_model::{AccessResource, Identity, Operation, RequestContext};

    fn request(tables: &[(&str, &str)]) -> AccessRequest {
        AccessRequest {
            identity: Identity::default(),
            operation: Operation::table_select(),
            resources: tables
                .iter()
                .map(|(s, t)| AccessResource {
                    catalog: None,
                    schema: Some((*s).into()),
                    table: (*t).into(),
                    columns: Columns::All,
                })
                .collect(),
            context: RequestContext::default(),
        }
    }

    fn response(entries: &[(&str, bool)]) -> OpaResponse {
        OpaResponse {
            result: Some(OpaResult {
                resources: entries
                    .iter()
                    .map(|(t, allow)| WireResourceDecision {
                        table: (*t).into(),
                        allow: *allow,
                        reason: None,
                        row_filters: Vec::new(),
                        column_masks: Vec::new(),
                    })
                    .collect(),
            }),
        }
    }

    #[test]
    fn complete_response_is_unchanged() {
        let d = from_response(
            response(&[("orders", true), ("s.customers", true)]),
            &request(&[("s", "orders"), ("s", "customers")]),
        );
        assert!(d.is_allowed());
        assert_eq!(d.resources.len(), 2);
    }

    #[test]
    fn omitted_resource_is_denied() {
        let d = from_response(
            response(&[("orders", true)]),
            &request(&[("s", "orders"), ("s", "payroll")]),
        );
        assert!(!d.is_allowed());
        assert_eq!(d.first_denied().map(|(t, _)| t), Some("s.payroll"));
    }

    #[test]
    fn coverage_is_case_insensitive() {
        let d = from_response(response(&[("ORDERS", true)]), &request(&[("s", "orders")]));
        assert!(d.is_allowed());
    }

    #[test]
    fn empty_or_missing_result_denies_all() {
        let req = request(&[("s", "orders")]);
        assert!(!from_response(OpaResponse { result: None }, &req).is_allowed());
        assert!(!from_response(response(&[]), &req).is_allowed());
    }
}
