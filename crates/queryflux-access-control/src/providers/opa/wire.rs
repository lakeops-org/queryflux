//! OPA wire format: `{"input": {...}}` request, `{"result": {...}}` response, and the
//! mapping to/from `queryflux_core::access_model`.

use std::collections::BTreeMap;

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
/// policy must explicitly produce a per-resource verdict.
pub(super) fn from_response(resp: OpaResponse, requested: &AccessRequest) -> AccessDecision {
    match resp.result {
        Some(result) if !result.resources.is_empty() => AccessDecision {
            resources: result
                .resources
                .into_iter()
                .map(|r| ResourceDecision {
                    table: r.table,
                    allow: r.allow,
                    reason: r.reason,
                    row_filters: r.row_filters,
                    column_masks: r.column_masks,
                })
                .collect(),
        },
        _ => AccessDecision::deny_all(format!(
            "policy returned no decision for {} resource(s)",
            requested.resources.len()
        )),
    }
}
