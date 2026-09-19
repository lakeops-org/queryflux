//! Cerbos `CheckResources` wire format: `POST {url}{checkResourcesPath}` with
//! `{"principal", "resources"}`, and the mapping to/from `queryflux_core::access_model`.
//!
//! Cerbos's `CheckResources` API has no native concept of a row filter or column mask —
//! only per-action `EFFECT_ALLOW`/`EFFECT_DENY` and a generic `outputs` array of
//! `{"src", "val"}` pairs collected from whichever policy rules activated. Row
//! filters/masks are carried through `val` using a convention **QueryFlux defines**, the
//! same way the OPA provider requires a specific rego decision-document shape: a policy
//! rule that wants to contribute one emits a self-describing object on activation —
//!
//! ```yaml
//! output:
//!   when:
//!     ruleActivated: |
//!       {"kind": "row_filter", "expression": "region = '" + P.attr.region + "'"}
//! ```
//! ```yaml
//! output:
//!   when:
//!     ruleActivated: |
//!       {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}
//! ```
//!
//! Every resource is sent under the fixed `kind: "table"` — one Cerbos resource policy
//! governs every table generically, matching on `R.attr.catalog` / `R.attr.schema` /
//! `R.attr.table` the way a single rego package already matches on `input.action.resources`
//! for OPA. `columns` is `null` for "all columns" (schema unresolved / `SELECT *`), mirroring
//! the OPA wire format exactly.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use queryflux_core::access_model::{
    AccessDecision, AccessRequest, ColumnMask, Columns, ResourceDecision, RowFilter,
};

/// Every table is sent as this fixed Cerbos resource `kind` — see the module doc.
const RESOURCE_KIND: &str = "table";

// ---- request ----

#[derive(Serialize)]
pub(super) struct CheckResourcesRequest<'a> {
    pub request_id: &'a str,
    pub principal: CerbosPrincipal<'a>,
    pub resources: Vec<ResourceEntry<'a>>,
}

#[derive(Serialize)]
pub(super) struct CerbosPrincipal<'a> {
    pub id: &'a str,
    pub roles: &'a [String],
    pub attr: PrincipalAttr<'a>,
}

#[derive(Serialize)]
pub(super) struct PrincipalAttr<'a> {
    pub groups: &'a [String],
    #[serde(flatten)]
    pub attributes: &'a BTreeMap<String, Value>,
}

#[derive(Serialize)]
pub(super) struct ResourceEntry<'a> {
    pub resource: CerbosResource<'a>,
    pub actions: [&'a str; 1],
}

#[derive(Serialize)]
pub(super) struct CerbosResource<'a> {
    pub id: &'a str,
    pub kind: &'a str,
    pub attr: ResourceAttr<'a>,
}

#[derive(Serialize)]
pub(super) struct ResourceAttr<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<&'a str>,
    pub table: &'a str,
    /// `null` = all columns (schema unresolved / `SELECT *`).
    pub columns: Option<&'a [String]>,
}

pub(super) fn to_request<'a>(
    request_id: &'a str,
    req: &'a AccessRequest,
) -> CheckResourcesRequest<'a> {
    let action = req.operation.as_str();
    CheckResourcesRequest {
        request_id,
        principal: CerbosPrincipal {
            id: &req.identity.user,
            roles: &req.identity.roles,
            attr: PrincipalAttr {
                groups: &req.identity.groups,
                attributes: &req.identity.attributes,
            },
        },
        resources: req
            .resources
            .iter()
            .map(|r| ResourceEntry {
                resource: CerbosResource {
                    id: &r.table,
                    kind: RESOURCE_KIND,
                    attr: ResourceAttr {
                        catalog: r.catalog.as_deref(),
                        schema: r.schema.as_deref(),
                        table: &r.table,
                        columns: match &r.columns {
                            Columns::All => None,
                            Columns::Named(c) => Some(c.as_slice()),
                        },
                    },
                },
                actions: [action],
            })
            .collect(),
    }
}

// ---- response ----

#[derive(Deserialize)]
pub(super) struct CheckResourcesResponse {
    #[serde(default)]
    pub results: Vec<CerbosResult>,
}

#[derive(Deserialize)]
pub(super) struct CerbosResult {
    #[serde(default)]
    pub actions: BTreeMap<String, String>,
    #[serde(default)]
    pub outputs: Vec<CerbosOutput>,
}

#[derive(Deserialize)]
pub(super) struct CerbosOutput {
    #[serde(default)]
    pub val: Value,
}

/// The `val` shape QueryFlux's own policies must emit to contribute a row filter or
/// column mask — see the module doc. An output whose `val` doesn't match either shape
/// (e.g. an unrelated audit-message output) is silently skipped, not an error.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PolicyOutput {
    RowFilter {
        expression: String,
    },
    ColumnMask {
        #[serde(flatten)]
        mask: ColumnMask,
    },
}

/// One activated rule contributes exactly one `outputs[]` entry, but its `val` may itself
/// be a *list* of `{"kind": ...}` objects — a single rule masking several columns (or
/// combining a row filter with column masks) in one CEL expression, rather than needing a
/// separate rule per output. Both a bare object and a list of objects are accepted.
#[derive(Deserialize)]
#[serde(untagged)]
enum PolicyOutputs {
    One(PolicyOutput),
    Many(Vec<PolicyOutput>),
}

impl PolicyOutputs {
    fn into_vec(self) -> Vec<PolicyOutput> {
        match self {
            PolicyOutputs::One(o) => vec![o],
            PolicyOutputs::Many(v) => v,
        }
    }
}

const EFFECT_ALLOW: &str = "EFFECT_ALLOW";

/// Map a Cerbos `CheckResources` response to a neutral [`AccessDecision`].
///
/// Cerbos's API contract guarantees one `results` entry per requested resource, in the
/// same order — so resources are correlated **by position**, not by echoed id (avoids any
/// ambiguity from duplicate/missing ids). A length mismatch is a provider-level anomaly and
/// is treated as **deny-all**, the same fail-closed stance as an OPA response that omits a
/// decision for one of several requested resources.
pub(super) fn from_response(
    resp: CheckResourcesResponse,
    requested: &AccessRequest,
) -> AccessDecision {
    if resp.results.len() != requested.resources.len() {
        return AccessDecision::deny_all(format!(
            "policy engine returned {} result(s) for {} requested resource(s)",
            resp.results.len(),
            requested.resources.len()
        ));
    }

    let action = requested.operation.as_str();
    let resources = resp
        .results
        .into_iter()
        .zip(requested.resources.iter())
        .map(|(result, req_resource)| {
            let allow = result.actions.get(action).map(String::as_str) == Some(EFFECT_ALLOW);

            let mut row_filters = Vec::new();
            let mut column_masks = Vec::new();
            for output in result.outputs {
                let Ok(parsed) = serde_json::from_value::<PolicyOutputs>(output.val) else {
                    // Not a row-filter/column-mask output (e.g. an unrelated audit
                    // message) — not this provider's concern.
                    continue;
                };
                for item in parsed.into_vec() {
                    match item {
                        PolicyOutput::RowFilter { expression } => {
                            row_filters.push(RowFilter {
                                expression: Some(expression),
                                ucast: None,
                            });
                        }
                        PolicyOutput::ColumnMask { mask } => column_masks.push(mask),
                    }
                }
            }

            ResourceDecision {
                table: req_resource.table.clone(),
                allow,
                reason: None,
                row_filters,
                column_masks,
            }
        })
        .collect();

    AccessDecision { resources }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::access_model::{
        AccessResource, Identity, MaskType, Operation, RequestContext,
    };

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

    #[test]
    fn allow_with_no_outputs() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(decision.is_allowed());
        assert!(!decision.has_rewrite());
    }

    #[test]
    fn deny_when_action_effect_is_deny() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_DENY"}, "outputs": []}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
    }

    #[test]
    fn missing_action_key_is_denied_not_vacuously_allowed() {
        let resp: CheckResourcesResponse =
            serde_json::from_str(r#"{"results": [{"actions": {}, "outputs": []}]}"#).unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
    }

    #[test]
    fn row_filter_output_is_parsed() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": {"kind": "row_filter", "expression": "region = 'US'"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(decision.is_allowed());
        assert_eq!(
            decision.row_filters().collect::<Vec<_>>(),
            vec![("orders", "region = 'US'")]
        );
    }

    #[test]
    fn column_mask_output_is_parsed() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        let masks: Vec<_> = decision.column_masks().collect();
        assert_eq!(masks.len(), 1);
        assert_eq!(masks[0].0, "orders");
        assert_eq!(masks[0].1.column, "ssn");
        assert_eq!(masks[0].1.mask_type, MaskType::ShowLast4);
    }

    #[test]
    fn unrecognized_output_shape_is_skipped_not_an_error() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": "create_allowed:john"}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(decision.is_allowed());
        assert!(!decision.has_rewrite());
    }

    #[test]
    fn result_count_mismatch_is_deny_all() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders", "customers"]));
        assert!(!decision.is_allowed());
    }

    #[test]
    fn multi_resource_correlated_by_position() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [
                {"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []},
                {"actions": {"table.select": "EFFECT_DENY"}, "outputs": []}
            ]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders", "customers"]));
        assert!(!decision.is_allowed());
        assert_eq!(decision.first_denied().map(|(t, _)| t), Some("customers"));
    }

    /// A single activated rule can emit a *list* of outputs — e.g. one rule combining a
    /// row filter with several column masks in one CEL expression — rather than needing a
    /// separate rule (and separate `outputs[]` entry) per contribution.
    #[test]
    fn one_output_entry_carrying_a_list_of_kinds_is_expanded() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#combined", "val": [
                    {"kind": "row_filter", "expression": "region = 'EU'"},
                    {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"},
                    {"kind": "column_mask", "column": "email", "type": "REDACT"}
                ]}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["customers"]));
        assert!(decision.is_allowed());
        assert_eq!(
            decision.row_filters().collect::<Vec<_>>(),
            vec![("customers", "region = 'EU'")]
        );
        let masks: Vec<_> = decision.column_masks().collect();
        assert_eq!(masks.len(), 2);
        assert_eq!(masks[0].1.column, "ssn");
        assert_eq!(masks[0].1.mask_type, MaskType::ShowLast4);
        assert_eq!(masks[1].1.column, "email");
        assert_eq!(masks[1].1.mask_type, MaskType::Redact);
    }

    /// Outputs from *multiple* activated rules accumulate — a row-filter rule and two
    /// independent per-column masking rules all firing for the same resource/action.
    #[test]
    fn outputs_from_multiple_activated_rules_accumulate() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#row_filter_rule", "val": {"kind": "row_filter", "expression": "region = 'EU'"}},
                {"src": "r#mask_ssn_rule", "val": {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}},
                {"src": "r#mask_clearance_rule", "val": {"kind": "column_mask", "column": "clearance", "type": "CONSTANT", "value": "[REDACTED]"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["customers"]));
        assert!(decision.is_allowed());
        assert_eq!(decision.row_filters().count(), 1);
        let masks: Vec<_> = decision.column_masks().collect();
        assert_eq!(masks.len(), 2);
        assert_eq!(masks[1].1.mask_type, MaskType::Constant);
        assert_eq!(masks[1].1.value.as_deref(), Some("[REDACTED]"));
    }

    /// A mix of well-formed and malformed/unrelated outputs on the same result: the good
    /// ones must still be picked up even though a sibling output doesn't parse.
    #[test]
    fn valid_outputs_survive_alongside_unrelated_ones() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#audit", "val": "someone_looked_at_this"},
                {"src": "r#mask", "val": {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}},
                {"src": "r#unknown_kind", "val": {"kind": "quarantine", "reason": "not ours"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["customers"]));
        assert!(decision.is_allowed());
        let masks: Vec<_> = decision.column_masks().collect();
        assert_eq!(masks.len(), 1);
        assert_eq!(masks[0].1.column, "ssn");
    }

    /// Only the exact string `"EFFECT_ALLOW"` counts as allowed — any other value
    /// (including a plausible-looking but wrong effect name, or Cerbos's own
    /// `EFFECT_NO_MATCH` when no rule matched at all) must deny, not vacuously allow.
    #[test]
    fn only_exact_effect_allow_string_is_treated_as_allowed() {
        for effect in [
            "EFFECT_DENY",
            "EFFECT_NO_MATCH",
            "effect_allow",
            "ALLOW",
            "",
        ] {
            let resp: CheckResourcesResponse = serde_json::from_str(&format!(
                r#"{{"results": [{{"actions": {{"table.select": "{effect}"}}, "outputs": []}}]}}"#
            ))
            .unwrap();
            let decision = from_response(resp, &requested(&["orders"]));
            assert!(
                !decision.is_allowed(),
                "effect {effect:?} must not be allowed"
            );
        }
    }

    /// A three-resource request (e.g. a join across customers/orders/payroll) where each
    /// resource gets an independent decision — a deny on one must not affect the others'
    /// row filters/masks, and `first_denied` must report the actual denied table.
    #[test]
    fn independent_decisions_across_a_three_way_join() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [
                {"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                    {"src": "r#c", "val": {"kind": "row_filter", "expression": "region = 'EU'"}}
                ]},
                {"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []},
                {"actions": {"table.select": "EFFECT_DENY"}, "outputs": []}
            ]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["customers", "orders", "payroll"]));
        assert!(!decision.is_allowed());
        assert_eq!(decision.first_denied().map(|(t, _)| t), Some("payroll"));
        assert_eq!(
            decision.row_filters().collect::<Vec<_>>(),
            vec![("customers", "region = 'EU'")]
        );
    }

    /// An empty request (no resources at all — e.g. `SELECT 1`, though in practice the
    /// guard never calls the provider for that case) round-trips to an empty, allowed
    /// decision rather than a spurious length-mismatch deny.
    #[test]
    fn empty_request_and_empty_results_is_allowed() {
        let resp: CheckResourcesResponse = serde_json::from_str(r#"{"results": []}"#).unwrap();
        let decision = from_response(resp, &requested(&[]));
        assert!(decision.is_allowed());
    }

    #[test]
    fn to_request_sends_fixed_kind_and_columns_null_for_all() {
        let req = requested(&["orders"]);
        let wire_req = to_request("req-1", &req);
        assert_eq!(wire_req.resources.len(), 1);
        assert_eq!(wire_req.resources[0].resource.kind, RESOURCE_KIND);
        assert_eq!(wire_req.resources[0].resource.id, "orders");
        assert!(wire_req.resources[0].resource.attr.columns.is_none());
        assert_eq!(wire_req.resources[0].actions, ["table.select"]);
        assert_eq!(wire_req.request_id, "req-1");
    }

    #[test]
    fn to_request_sends_named_columns_when_resolved() {
        let mut req = requested(&["orders"]);
        req.resources[0].columns = Columns::Named(vec!["id".to_string(), "amount".to_string()]);
        let wire_req = to_request("req-1", &req);
        assert_eq!(
            wire_req.resources[0].resource.attr.columns,
            Some(["id".to_string(), "amount".to_string()].as_slice())
        );
    }

    #[test]
    fn to_request_carries_identity_roles_and_groups_separately() {
        let mut req = requested(&["orders"]);
        req.identity.user = "bob".to_string();
        req.identity.roles = vec!["analyst".to_string()];
        req.identity.groups = vec!["analysts".to_string()];
        let wire_req = to_request("req-1", &req);
        assert_eq!(wire_req.principal.id, "bob");
        assert_eq!(wire_req.principal.roles, ["analyst".to_string()]);
        assert_eq!(wire_req.principal.attr.groups, ["analysts".to_string()]);
    }
}
