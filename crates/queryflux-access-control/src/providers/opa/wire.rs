//! OPA wire format: `{"input": {...}}` request, `{"result": {...}}` response, and the
//! mapping to/from `queryflux_core::access_model`.

use std::collections::{BTreeMap, HashMap};

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

fn is_empty_str(s: &&str) -> bool {
    s.is_empty()
}

#[derive(Serialize)]
pub(super) struct WireResource<'a> {
    /// `table`, `view`, `schema` or `catalog` — reads are always tables; DDL can target the rest.
    pub kind: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<&'a str>,
    /// Omitted for `schema`/`catalog` resources, which have no table.
    #[serde(skip_serializing_if = "is_empty_str")]
    pub table: &'a str,
    /// The object's own name (table/view, schema, or catalog) — echo it back in the response.
    pub name: &'a str,
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
                        kind: r.kind.as_str(),
                        catalog: r.catalog.as_deref(),
                        schema: r.schema.as_deref(),
                        table: &r.table,
                        name: r.name(),
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
    /// Echo of the resource's `table` (table resources) or `name` (any kind); `name` wins if
    /// both are present. Neither → the decision can't be matched and the resource is denied.
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
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
/// policy must explicitly produce a per-resource verdict. A response that does not cover
/// every requested resource gets an explicit deny for each omitted one, so a partial verdict
/// can never read as "allowed" for a table the policy never judged.
///
/// One [`ResourceDecision`] is produced per *requested* resource (not per response entry):
/// its `table` is always the request's own canonical [`AccessResource::qualified_name`],
/// never whatever string the policy echoed. Policies commonly reply with the bare `table`
/// from the input rather than round-tripping `schema`/`catalog`; comparing those bare
/// strings directly downstream (e.g. when matching a row filter to a scan site) would let a
/// decision meant for one schema's table apply to a same-named table in another schema. A
/// bare reply is accepted only when that bare name is unambiguous — the *only* requested
/// resource with that name — otherwise which schema it meant can't be known, and the
/// resource is treated as undecided (denied) rather than guessed at.
pub(super) fn from_response(resp: OpaResponse, requested: &AccessRequest) -> AccessDecision {
    let Some(result) = resp.result.filter(|r| !r.resources.is_empty()) else {
        return AccessDecision::deny_all(format!(
            "policy returned no decision for {} resource(s)",
            requested.resources.len()
        ));
    };

    let mut bare_counts: HashMap<String, usize> = HashMap::new();
    // A `name` echo is exact (no schema/catalog to reconstruct), but two *different*
    // requested resources can still share the same bare name — two `orders` tables in
    // different schemas, or two `analytics` schemas in different catalogs — so it needs the
    // same ambiguity guard `bare_counts` gives the `table` echo, keyed by (kind, name)
    // since a table and a schema can coincidentally share a leaf name too.
    let mut name_counts: HashMap<(&'static str, String), usize> = HashMap::new();
    for res in &requested.resources {
        // Schema/catalog resources have no `table` (it's `""`); an empty-string key would
        // let two unrelated schema/catalog resources in one request falsely look "unique"
        // (or ambiguous) to each other. They're matched by `name` instead (see
        // `decision_covers`), so they never need a `bare_counts` entry at all.
        if !res.table.is_empty() {
            *bare_counts.entry(res.table.to_lowercase()).or_insert(0) += 1;
        }
        *name_counts
            .entry((res.kind.as_str(), res.name().to_lowercase()))
            .or_insert(0) += 1;
    }

    let resources = requested
        .resources
        .iter()
        .map(|res| {
            match result
                .resources
                .iter()
                .find(|d| decision_covers(d, res, &bare_counts, &name_counts))
            {
                Some(d) => ResourceDecision {
                    table: res.qualified_name(),
                    allow: d.allow,
                    reason: d.reason.clone(),
                    row_filters: d.row_filters.clone(),
                    column_masks: d.column_masks.clone(),
                },
                None => {
                    let name = res.qualified_name();
                    ResourceDecision {
                        reason: Some(format!("policy returned no decision for {name}")),
                        table: name,
                        allow: false,
                        row_filters: Vec::new(),
                        column_masks: Vec::new(),
                    }
                }
            }
        })
        .collect();
    AccessDecision { resources }
}

/// Whether a decision refers to `res`. A `name` echo (any kind) matches case-insensitively
/// against [`AccessResource::name`] — but, just like the `table` echo below, only when that
/// (kind, name) is unambiguous in the request: two different tables (or two schemas, two
/// catalogs, …) can share a bare name, and a `name` echo doesn't carry enough to tell them
/// apart either. Falling back to a `table` echo (table/view resources only — schema/catalog
/// resources have no `table` to fall back to): policies commonly reply with the bare `table`
/// from the input rather than round-tripping `schema`/`catalog`; comparing those bare
/// strings directly downstream (e.g. when matching a row filter to a scan site) would let a
/// decision meant for one schema's table apply to a same-named table in another schema. A
/// bare reply is accepted only when that bare name is unambiguous — the *only* requested
/// resource with that name — otherwise which schema it meant can't be known, and the
/// resource is treated as undecided (denied) rather than guessed at. `bare_counts` and
/// `name_counts` map a lowercased bare table name / (kind, name) pair to how many resources
/// in the *request* share it (see [`from_response`]).
fn decision_covers(
    decision: &WireResourceDecision,
    res: &AccessResource,
    bare_counts: &HashMap<String, usize>,
    name_counts: &HashMap<(&'static str, String), usize>,
) -> bool {
    if let Some(name) = decision.name.as_deref() {
        if name.eq_ignore_ascii_case(res.name())
            && name_counts
                .get(&(res.kind.as_str(), res.name().to_lowercase()))
                .copied()
                == Some(1)
        {
            return true;
        }
    }
    if res.table.is_empty() {
        // A schema/catalog resource only ever matches by `name`, above.
        return false;
    }
    let Some(table_echo) = decision.table.as_deref() else {
        return false;
    };
    let d = table_echo.to_lowercase();
    let table = res.table.to_lowercase();
    let Some(schema) = res.schema.as_deref().map(str::to_lowercase) else {
        return d == table;
    };
    let qualified = format!("{schema}.{table}");
    if d == qualified {
        return true;
    }
    if res
        .catalog
        .as_deref()
        .is_some_and(|c| d == format!("{}.{qualified}", c.to_lowercase()))
    {
        return true;
    }
    d == table && bare_counts.get(&table).copied() == Some(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::access_model::{
        AccessResource, Identity, Operation, RequestContext, ResourceKind,
    };

    fn requested(tables: &[&str]) -> AccessRequest {
        AccessRequest {
            identity: Identity::default(),
            operation: Operation::table_select(),
            resources: tables
                .iter()
                .map(|t| AccessResource {
                    kind: ResourceKind::Table,
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

    fn schema_request(schema: &str) -> AccessRequest {
        let mut req = requested(&[]);
        req.operation = Operation("schema.drop".to_string());
        req.resources = vec![AccessResource {
            kind: ResourceKind::Schema,
            catalog: Some("prod".to_string()),
            schema: Some(schema.to_string()),
            table: String::new(),
            columns: Columns::All,
        }];
        req
    }

    /// DDL targets go out with their `kind` and `name`; a schema has no `table`.
    #[test]
    fn schema_resource_is_sent_with_kind_and_name_and_no_table() {
        let req = schema_request("analytics");
        let json = serde_json::to_value(to_request(&req)).unwrap();
        let r = &json["input"]["action"]["resources"][0];
        assert_eq!(r["kind"], "schema");
        assert_eq!(r["name"], "analytics");
        assert_eq!(r["schema"], "analytics");
        assert_eq!(r["catalog"], "prod");
        assert!(r.get("table").is_none(), "{r}");
        assert_eq!(json["input"]["action"]["operation"], "schema.drop");

        let table = serde_json::to_value(to_request(&requested(&["orders"]))).unwrap();
        let t = &table["input"]["action"]["resources"][0];
        assert_eq!(
            (t["kind"].as_str(), t["table"].as_str(), t["name"].as_str()),
            (Some("table"), Some("orders"), Some("orders"))
        );
    }

    /// A policy may echo `name` (any kind) or `table` (table resources); `name` wins.
    #[test]
    fn response_is_matched_on_name_or_table() {
        let by_name: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"name": "analytics", "allow": true}]}}"#,
        )
        .unwrap();
        assert!(from_response(by_name, &schema_request("analytics")).is_allowed());

        let by_table: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"table": "orders", "allow": true}]}}"#,
        )
        .unwrap();
        assert!(from_response(by_table, &requested(&["orders"])).is_allowed());

        let both: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"table": "x", "name": "analytics", "allow": true}]}}"#,
        )
        .unwrap();
        assert!(from_response(both, &schema_request("analytics")).is_allowed());

        // An echo that matches nothing is a missing decision, not an implicit allow.
        let wrong: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"name": "other", "allow": true}]}}"#,
        )
        .unwrap();
        assert!(!from_response(wrong, &schema_request("analytics")).is_allowed());
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

    fn request(tables: &[(&str, &str)]) -> AccessRequest {
        AccessRequest {
            identity: Identity::default(),
            operation: Operation::table_select(),
            resources: tables
                .iter()
                .map(|(s, t)| AccessResource {
                    kind: ResourceKind::Table,
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
                        table: Some((*t).into()),
                        name: None,
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

    /// A `table` in the response is always normalized to the request's own qualified name,
    /// never left as whatever the policy echoed — downstream code (grouping row filters and
    /// masks into per-table policies) keys off this string, and matching it must be exact.
    #[test]
    fn decision_table_is_normalized_to_the_requested_qualified_name() {
        let d = from_response(response(&[("orders", true)]), &request(&[("s", "orders")]));
        assert_eq!(d.resources[0].table, "s.orders");
    }

    /// Regression: a bare-name response entry must not resolve to *every* same-named table
    /// across different schemas — that would apply one schema's row filters/masks (or its
    /// allow/deny) to another schema's table entirely. A single bare `orders` decision must
    /// not cover both `sales.orders` and `hr.orders`; since it can't be known which one the
    /// policy meant, both are treated as undecided (denied), not both allowed.
    #[test]
    fn ambiguous_bare_decision_does_not_cover_either_same_named_table() {
        let d = from_response(
            response(&[("orders", true)]),
            &request(&[("sales", "orders"), ("hr", "orders")]),
        );
        assert!(!d.is_allowed());
        assert_eq!(d.resources.len(), 2);
        for r in &d.resources {
            assert!(
                !r.allow,
                "{r:?} must not be allowed by the ambiguous bare decision"
            );
        }
    }

    /// Same regression as `ambiguous_bare_decision_does_not_cover_either_same_named_table`,
    /// but for a `name` echo: two schema resources in different catalogs share the bare
    /// name `analytics`, so a single `{"name": "analytics", ...}` decision must not resolve
    /// to either of them.
    #[test]
    fn ambiguous_name_decision_does_not_cover_either_same_named_schema() {
        let mut req = requested(&[]);
        req.operation = Operation("schema.drop".to_string());
        req.resources = vec![
            AccessResource {
                kind: ResourceKind::Schema,
                catalog: Some("prod".to_string()),
                schema: Some("analytics".to_string()),
                table: String::new(),
                columns: Columns::All,
            },
            AccessResource {
                kind: ResourceKind::Schema,
                catalog: Some("staging".to_string()),
                schema: Some("analytics".to_string()),
                table: String::new(),
                columns: Columns::All,
            },
        ];
        let resp: OpaResponse = serde_json::from_str(
            r#"{"result": {"resources": [{"name": "analytics", "allow": true}]}}"#,
        )
        .unwrap();
        let d = from_response(resp, &req);
        assert!(!d.is_allowed());
        assert_eq!(d.resources.len(), 2);
        for r in &d.resources {
            assert!(
                !r.allow,
                "{r:?} must not be allowed by the ambiguous name decision"
            );
        }
    }

    /// A qualified response entry is unambiguous even when its bare name collides with
    /// another requested table's — only the ambiguous *bare* form needs uniqueness.
    #[test]
    fn qualified_decision_still_resolves_its_own_table_despite_a_same_named_sibling() {
        let d = from_response(
            response(&[("sales.orders", true), ("hr.orders", false)]),
            &request(&[("sales", "orders"), ("hr", "orders")]),
        );
        assert!(!d.is_allowed());
        let sales = d
            .resources
            .iter()
            .find(|r| r.table == "sales.orders")
            .unwrap();
        let hr = d.resources.iter().find(|r| r.table == "hr.orders").unwrap();
        assert!(sales.allow);
        assert!(!hr.allow);
    }
}
