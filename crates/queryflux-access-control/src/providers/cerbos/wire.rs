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

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
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

/// `groups` is a dedicated field (Cerbos policies match `P.attr.groups` directly), but
/// `attributes` is an operator-configured, arbitrary-keyed map (`auth.oidc.attributeClaims`
/// can name any JWT claim, including one literally called `groups`) — flattening it
/// alongside the `groups` field with `#[serde(flatten)]` would then emit the `"groups"` key
/// twice, which Cerbos's protojson decoder rejects outright (every request from that
/// principal fails as a provider error, not a policy decision). Serialized by hand instead
/// so a same-named attribute is dropped rather than colliding.
pub(super) struct PrincipalAttr<'a> {
    pub groups: &'a [String],
    pub attributes: &'a BTreeMap<String, Value>,
}

impl<'a> Serialize for PrincipalAttr<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1 + self.attributes.len()))?;
        map.serialize_entry("groups", self.groups)?;
        for (key, value) in self.attributes {
            if key != "groups" {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
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
    /// The allowlisted subset of `SessionContext.extra` named by
    /// `accessControl.connections.<name>.sessionParamKeys` — policies read it as
    /// `R.attr.sessionParams.<key>` (see the delegation pattern in the module/website
    /// docs). Never trusted for allow/deny by QueryFlux itself; only usable to *build* a
    /// filter expression the policy returns.
    #[serde(rename = "sessionParams", skip_serializing_if = "BTreeMap::is_empty")]
    pub session_params: &'a BTreeMap<String, String>,
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
                        session_params: &req.context.session_params,
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
/// column mask — see the module doc. One activated rule contributes exactly one
/// `outputs[]` entry, but its `val` may itself be a *list* of `{"kind": ...}` objects — a
/// single rule masking several columns (or combining a row filter with column masks) in
/// one CEL expression, rather than needing a separate rule per output; both a bare object
/// and a list of objects are accepted (see [`parse_recognized_output`]). An output with no
/// recognized `kind` (e.g. an unrelated audit-message output) is skipped, not an error; one
/// *with* a recognized `kind` that fails to parse is.
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

/// Classifies one item from an `outputs[].val` — a bare object, or one element of a list —
/// as ours (`Some`/`Err`) or not (`None`). Recognized by `"kind"` *before* attempting to
/// deserialize the rest of the shape: an unrelated output legitimately has no `kind`, or a
/// `kind` this provider doesn't define, and must be skipped, not treated as a parse
/// failure. A `kind` of `row_filter`/`column_mask` that *does* fail to parse (missing
/// field, wrong type — e.g. a policy typo like `"type": "SHOW_LAST4"`) is a QueryFlux
/// output that's ours to apply, and reports as an error rather than silently vanishing.
fn parse_recognized_output(val: &Value) -> Result<Option<PolicyOutput>, String> {
    match val.get("kind").and_then(Value::as_str) {
        Some(kind @ ("row_filter" | "column_mask")) => serde_json::from_value(val.clone())
            .map(Some)
            .map_err(|e| format!("malformed {kind} output: {e}")),
        _ => Ok(None),
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
            let mut malformed: Option<String> = None;
            'outputs: for output in result.outputs {
                let items: Vec<Value> = match output.val {
                    Value::Array(items) => items,
                    other => vec![other],
                };
                for item in items {
                    match parse_recognized_output(&item) {
                        Ok(None) => {}
                        Ok(Some(PolicyOutput::RowFilter { expression })) => {
                            row_filters.push(RowFilter {
                                expression: Some(expression),
                                ucast: None,
                            });
                        }
                        Ok(Some(PolicyOutput::ColumnMask { mask })) => column_masks.push(mask),
                        Err(reason) => {
                            malformed = Some(reason);
                            break 'outputs;
                        }
                    }
                }
            }

            // A recognized-but-malformed row_filter/column_mask output means the policy
            // author asked for a restriction QueryFlux couldn't apply — the resource must
            // not read as allowed-and-unrestricted just because parsing failed.
            if let Some(reason) = malformed {
                return ResourceDecision {
                    table: req_resource.table.clone(),
                    allow: false,
                    reason: Some(format!(
                        "policy output for {} could not be applied: {reason}",
                        req_resource.table
                    )),
                    row_filters: Vec::new(),
                    column_masks: Vec::new(),
                };
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

    /// Regression: a `column_mask` output with a typo'd `type` (e.g. `SHOW_LAST4` instead
    /// of `SHOW_LAST_4`) is *ours* — its `kind` says so — and must deny the resource rather
    /// than silently running with the column unmasked.
    #[test]
    fn malformed_column_mask_denies_rather_than_dropping_the_mask() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST4"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
        let (table, reason) = decision.first_denied().unwrap();
        assert_eq!(table, "orders");
        assert!(reason.contains("malformed"), "got: {reason}");
    }

    /// Same regression for `row_filter`: a non-string `expression` (e.g. a CEL condition
    /// that evaluated to a number) must deny, not vanish along with the restriction.
    #[test]
    fn malformed_row_filter_denies_rather_than_dropping_the_filter() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": {"kind": "row_filter", "expression": 1}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
    }

    /// An output with no recognized `kind` at all (some other feature's output) is still
    /// skipped, not treated as a denial — only *our* kinds are held to this standard.
    #[test]
    fn output_with_unrecognized_kind_is_still_skipped() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": {"kind": "audit_note", "message": "checked"}}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(decision.is_allowed());
        assert!(!decision.has_rewrite());
    }

    /// A list `val` with one valid row_filter and one malformed column_mask must not let
    /// the valid entry's absence-of-error paper over the malformed one — the resource is
    /// still denied, even though the row filter alone parsed fine.
    #[test]
    fn one_malformed_item_in_a_list_denies_even_if_a_sibling_item_is_valid() {
        let resp: CheckResourcesResponse = serde_json::from_str(
            r#"{"results": [{"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": [
                {"src": "r#rule", "val": [
                    {"kind": "row_filter", "expression": "region = 'EU'"},
                    {"kind": "column_mask", "column": "ssn", "type": "NOT_A_REAL_TYPE"}
                ]}
            ]}]}"#,
        )
        .unwrap();
        let decision = from_response(resp, &requested(&["orders"]));
        assert!(!decision.is_allowed());
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

    /// Regression: the delegation pattern documented on the Cerbos provider page
    /// (`R.attr.sessionParams.customer_id`) requires the allowlisted session params to
    /// actually reach the resource `attr` on the wire — without this, `sessionParamKeys`
    /// silently does nothing for Cerbos even though the same config key works for OPA.
    #[test]
    fn to_request_forwards_session_params_into_resource_attr() {
        let mut req = requested(&["orders"]);
        req.context
            .session_params
            .insert("customer_id".to_string(), "cust-42".to_string());
        let wire_req = to_request("req-1", &req);
        let value = serde_json::to_value(&wire_req.resources[0].resource.attr).unwrap();
        assert_eq!(value["sessionParams"]["customer_id"], "cust-42");
    }

    /// No session params configured → the key is omitted entirely rather than sent as an
    /// empty object on every request.
    #[test]
    fn to_request_omits_session_params_when_empty() {
        let req = requested(&["orders"]);
        let wire_req = to_request("req-1", &req);
        let value = serde_json::to_value(&wire_req.resources[0].resource.attr).unwrap();
        assert!(value.get("sessionParams").is_none());
    }

    /// Regression: `auth.oidc.attributeClaims` is operator-configured and can name any JWT
    /// claim, including one literally called `groups`. Flattening `attributes` alongside
    /// the dedicated `groups` field would then serialize the `"groups"` key twice — valid
    /// JSON, but Cerbos's protojson decoder rejects duplicate object keys outright, turning
    /// every request from that principal into a provider error instead of a policy
    /// decision. The struct's own `groups` field must win; the colliding attribute is
    /// dropped rather than emitted a second time.
    #[test]
    fn principal_attr_does_not_duplicate_the_groups_key_when_an_attribute_is_named_groups() {
        let mut req = requested(&["orders"]);
        req.identity.groups = vec!["analysts".to_string()];
        req.identity
            .attributes
            .insert("groups".to_string(), serde_json::json!(["from-claim"]));
        req.identity
            .attributes
            .insert("region".to_string(), serde_json::json!("eu"));
        let wire_req = to_request("req-1", &req);

        let value = serde_json::to_value(&wire_req.principal.attr).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("groups"), Some(&serde_json::json!(["analysts"])));
        assert_eq!(obj.get("region"), Some(&serde_json::json!("eu")));

        // `serde_json::Value` collapses duplicate keys on the way in, so also check the
        // exact byte stream for a second `"groups":` occurrence, which is what Cerbos's
        // decoder actually receives and rejects.
        let raw = serde_json::to_string(&wire_req.principal.attr).unwrap();
        assert_eq!(
            raw.matches("\"groups\":").count(),
            1,
            "the wire payload must contain \"groups\" exactly once, got: {raw}"
        );
    }
}
