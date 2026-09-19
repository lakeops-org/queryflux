//! OPA wire format: `{"input": {...}}` request, `{"result": {...}}` response, and the
//! mapping to/from `queryflux_core::access_model`.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use queryflux_core::access_model::{
    AccessDecision, AccessRequest, ColumnMask, Columns, ResourceDecision, RowFilter,
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
/// policy must explicitly produce a per-resource verdict. A `result` that omits a verdict
/// for one of the *requested* resources (e.g. a policy bug that only matches some of the
/// tables in a join) is treated as a denial for that resource specifically — `is_allowed()`
/// is a vacuous `all()` over whatever's present, so a silently-missing entry must not be
/// allowed to mean "allowed".
pub(super) fn from_response(resp: OpaResponse, requested: &AccessRequest) -> AccessDecision {
    let Some(result) = resp.result.filter(|r| !r.resources.is_empty()) else {
        return AccessDecision::deny_all(format!(
            "policy returned no decision for {} resource(s)",
            requested.resources.len()
        ));
    };

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

    let decided: HashSet<String> = resources.iter().map(|r| r.table.clone()).collect();
    for req in &requested.resources {
        if !decided.contains(&req.table) {
            resources.push(ResourceDecision {
                table: req.table.clone(),
                allow: false,
                reason: Some("policy returned no decision for this resource".to_string()),
                row_filters: Vec::new(),
                column_masks: Vec::new(),
            });
        }
    }

    AccessDecision { resources }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::access_model::{AccessResource, Identity, Operation, RequestContext};

    fn requested(tables: &[&str]) -> AccessRequest {
        AccessRequest {
            identity: Identity::default(),
            operation: Operation::table_select(),
            resources: tables
                .iter()
                .map(|t| AccessResource {
                    catalog: None,
                    schema: None,
                    table: t.to_string(),
                    columns: Columns::All,
                })
                .collect(),
            context: RequestContext::default(),
        }
    }

    /// Regression: a response that only decides some of the requested resources (e.g. a
    /// policy bug that matches one table in a join but not another) must not let the
    /// unmentioned table's vacuous absence read as "allowed" — `is_allowed()` is an
    /// `all()` over whatever's present, so a missing entry has to become an explicit deny.
    #[test]
    fn missing_resource_in_response_is_denied_not_vacuously_allowed() {
        let resp: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"table": "orders", "allow": true}]}}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders", "customers"]));
        assert!(!decision.is_allowed());
        assert_eq!(decision.first_denied().map(|(t, _)| t), Some("customers"));
    }

    #[test]
    fn fully_decided_response_is_allowed() {
        let resp: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"table": "orders", "allow": true}]}}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(decision.is_allowed());
    }

    #[test]
    fn empty_result_is_deny_all() {
        let resp: OpaResponse = serde_json::from_str(r#"{}"#).unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
    }
}
