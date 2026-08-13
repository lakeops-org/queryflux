//! Split [`RouterConfig`]-shaped JSON into DB rows with `target_group_id` columns (no group names
//! inside `definition`), and merge back on load.

use queryflux_core::error::{QueryFluxError, Result};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::routing_json::{field, map_field, PROTO_CAMEL_SNAKE};

/// One row from `routing_rules` (load path).
#[derive(Debug, Clone)]
pub struct RoutingRulePersistRow {
    pub sort_order: i32,
    pub router_logical_index: i32,
    pub slice_index: i32,
    pub target_group_id: Option<i64>,
    pub definition: Value,
}

fn lookup_group_id(name: &str, name_to_id: &HashMap<String, i64>) -> Result<i64> {
    name_to_id
        .get(name)
        .copied()
        .ok_or_else(|| QueryFluxError::Persistence(format!("unknown cluster group '{name}'")))
}

fn name_for_id(id: i64, id_to_name: &HashMap<i64, String>) -> Result<String> {
    id_to_name
        .get(&id)
        .cloned()
        .ok_or_else(|| QueryFluxError::Persistence(format!("unknown cluster group id {id}")))
}

/// Expand one admin/router JSON object into persisted slices: `(definition without group, target_group_id)`.
pub fn expand_router_for_persistence(
    router: &Value,
    name_to_id: &HashMap<String, i64>,
) -> Result<Vec<(Value, Option<i64>)>> {
    let Some(ty) = router.get("type").and_then(|t| t.as_str()) else {
        return Ok(vec![(router.clone(), None)]);
    };

    match ty {
        "protocolBased" => {
            let mut out = Vec::new();
            for (camel, snake) in PROTO_CAMEL_SNAKE {
                if let Some(x) = field(router, camel, snake) {
                    if let Some(s) = x.as_str() {
                        if s.is_empty() {
                            continue;
                        }
                        let id = lookup_group_id(s, name_to_id)?;
                        out.push((
                            json!({
                                "type": "_qfProtoLeg",
                                "protocol": camel,
                            }),
                            Some(id),
                        ));
                    }
                }
            }
            Ok(out)
        }
        "header" => {
            let hn = field(router, "headerName", "header_name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let mut out = Vec::new();
            if let Some(obj) = map_field(router, "headerValueToGroup", "header_value_to_group") {
                for (hv_key, gv) in obj {
                    let name = gv.as_str().ok_or_else(|| {
                        QueryFluxError::Persistence("header map value must be a string".into())
                    })?;
                    if name.is_empty() {
                        continue;
                    }
                    let id = lookup_group_id(name, name_to_id)?;
                    out.push((
                        json!({
                            "type": "_qfHeaderLeg",
                            "headerName": hn,
                            "headerValue": hv_key,
                        }),
                        Some(id),
                    ));
                }
            }
            Ok(out)
        }
        "userGroup" => {
            let mut out = Vec::new();
            if let Some(obj) = map_field(router, "userToGroup", "user_to_group") {
                for (user, gv) in obj {
                    let name = gv.as_str().ok_or_else(|| {
                        QueryFluxError::Persistence("userToGroup value must be a string".into())
                    })?;
                    if name.is_empty() {
                        continue;
                    }
                    let id = lookup_group_id(name, name_to_id)?;
                    out.push((
                        json!({
                            "type": "_qfUserLeg",
                            "username": user,
                        }),
                        Some(id),
                    ));
                }
            }
            Ok(out)
        }
        "clientTags" => {
            let mut out = Vec::new();
            if let Some(obj) = map_field(router, "tagToGroup", "tag_to_group") {
                for (tag, gv) in obj {
                    let name = gv.as_str().ok_or_else(|| {
                        QueryFluxError::Persistence("tagToGroup value must be a string".into())
                    })?;
                    if name.is_empty() {
                        continue;
                    }
                    let id = lookup_group_id(name, name_to_id)?;
                    out.push((
                        json!({
                            "type": "_qfTagLeg",
                            "tag": tag,
                        }),
                        Some(id),
                    ));
                }
            }
            Ok(out)
        }
        "queryRegex" => {
            let mut out = Vec::new();
            if let Some(arr) = router.get("rules").and_then(|x| x.as_array()) {
                for r in arr {
                    let regex = r
                        .get("regex")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let action = r.get("action").and_then(|x| x.as_str()).unwrap_or("route");
                    if action.eq_ignore_ascii_case("deny") {
                        let mut def = Map::new();
                        def.insert("type".into(), json!("_qfRegexLeg"));
                        def.insert("regex".into(), json!(regex));
                        def.insert("action".into(), json!("deny"));
                        if let Some(err) = r.get("error") {
                            if !err.is_null() {
                                def.insert("error".into(), err.clone());
                            }
                        }
                        out.push((Value::Object(def), None));
                        continue;
                    }
                    let tg = field(r, "targetGroup", "target_group")
                        .and_then(|x| x.as_str())
                        .unwrap_or("");
                    if tg.is_empty() {
                        continue;
                    }
                    let id = lookup_group_id(tg, name_to_id)?;
                    out.push((
                        json!({
                            "type": "_qfRegexLeg",
                            "regex": regex,
                        }),
                        Some(id),
                    ));
                }
            }
            Ok(out)
        }
        "tags" => {
            let mut out = Vec::new();
            let arr = router
                .get("rules")
                .or_else(|| router.get("tag_rules"))
                .and_then(|x| x.as_array());
            let Some(arr) = arr else {
                return Ok(out);
            };
            for r in arr {
                let tags = r.get("tags").cloned().unwrap_or(json!({}));
                let tg = field(r, "targetGroup", "target_group")
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if tg.is_empty() {
                    continue;
                }
                let id = lookup_group_id(tg, name_to_id)?;
                out.push((
                    json!({
                        "type": "_qfTagsRuleLeg",
                        "tags": tags,
                    }),
                    Some(id),
                ));
            }
            Ok(out)
        }
        "compound" => {
            let combine = router.get("combine").cloned().unwrap_or(json!("all"));
            let conditions = router.get("conditions").cloned().unwrap_or(json!([]));
            let tg = field(router, "targetGroup", "target_group")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            if tg.is_empty() {
                return Err(QueryFluxError::Persistence(
                    "compound router requires targetGroup".into(),
                ));
            }
            let id = lookup_group_id(tg, name_to_id)?;
            Ok(vec![(
                json!({
                    "type": "compound",
                    "combine": combine,
                    "conditions": conditions,
                }),
                Some(id),
            )])
        }
        "pythonScript" => Ok(vec![(router.clone(), None)]),
        _ => Ok(vec![(router.clone(), None)]),
    }
}

fn is_legacy_full_router(v: &Value, target_group_id: Option<i64>) -> bool {
    target_group_id.is_none()
        && v.get("type")
            .and_then(|t| t.as_str())
            .map(|s| !s.starts_with("_qf"))
            .unwrap_or(false)
}

/// Merge persisted rows into [`RouterConfig`]-compatible JSON values (with group **names**).
pub fn collapse_rows_to_routers(
    rows: &[RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Vec<Value>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut sorted: Vec<&RoutingRulePersistRow> = rows.iter().collect();
    sorted.sort_by(|a, b| {
        a.sort_order
            .cmp(&b.sort_order)
            .then_with(|| a.router_logical_index.cmp(&b.router_logical_index))
            .then_with(|| a.slice_index.cmp(&b.slice_index))
            .then_with(|| a.definition.to_string().cmp(&b.definition.to_string()))
    });

    let mut routers: Vec<Value> = Vec::new();
    let mut i = 0;
    while i < sorted.len() {
        let logical = sorted[i].router_logical_index;
        let mut chunk: Vec<&RoutingRulePersistRow> = Vec::new();
        while i < sorted.len() && sorted[i].router_logical_index == logical {
            chunk.push(sorted[i]);
            i += 1;
        }

        if chunk.len() == 1 && is_legacy_full_router(&chunk[0].definition, chunk[0].target_group_id)
        {
            routers.push(chunk[0].definition.clone());
            continue;
        }

        let first_ty = chunk[0].definition.get("type").and_then(|t| t.as_str());
        let merged = match first_ty {
            Some("_qfProtoLeg") => merge_proto_chunk(&chunk, id_to_name)?,
            Some("_qfHeaderLeg") => merge_header_chunk(&chunk, id_to_name)?,
            Some("_qfUserLeg") => merge_user_chunk(&chunk, id_to_name)?,
            Some("_qfTagLeg") => merge_tag_chunk(&chunk, id_to_name)?,
            Some("_qfRegexLeg") => merge_regex_chunk(&chunk, id_to_name)?,
            Some("_qfTagsRuleLeg") => merge_tags_rules_chunk(&chunk, id_to_name)?,
            Some("compound") => merge_compound_chunk(chunk[0], id_to_name)?,
            Some("pythonScript") => chunk[0].definition.clone(),
            _ => {
                if chunk.len() == 1 {
                    chunk[0].definition.clone()
                } else {
                    return Err(QueryFluxError::Persistence(
                        "cannot merge routing rule chunk".into(),
                    ));
                }
            }
        };
        routers.push(merged);
    }

    Ok(routers)
}

fn merge_proto_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let mut m = Map::new();
    m.insert("type".to_string(), json!("protocolBased"));
    for leg in chunk {
        let proto = leg
            .definition
            .get("protocol")
            .and_then(|x| x.as_str())
            .ok_or_else(|| QueryFluxError::Persistence("_qfProtoLeg missing protocol".into()))?;
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfProtoLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        m.insert(proto.to_string(), json!(name));
    }
    Ok(Value::Object(m))
}

fn merge_header_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let header_name = chunk[0]
        .definition
        .get("headerName")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let mut map = Map::new();
    for leg in chunk {
        let hv = leg
            .definition
            .get("headerValue")
            .and_then(|x| x.as_str())
            .ok_or_else(|| {
                QueryFluxError::Persistence("_qfHeaderLeg missing headerValue".into())
            })?;
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfHeaderLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        map.insert(hv.to_string(), json!(name));
    }
    Ok(json!({
        "type": "header",
        "headerName": header_name,
        "headerValueToGroup": Value::Object(map),
    }))
}

fn merge_user_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let mut map = Map::new();
    for leg in chunk {
        let user = leg
            .definition
            .get("username")
            .and_then(|x| x.as_str())
            .ok_or_else(|| QueryFluxError::Persistence("_qfUserLeg missing username".into()))?;
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfUserLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        map.insert(user.to_string(), json!(name));
    }
    Ok(json!({
        "type": "userGroup",
        "userToGroup": Value::Object(map),
    }))
}

fn merge_tag_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let mut map = Map::new();
    for leg in chunk {
        let tag = leg
            .definition
            .get("tag")
            .and_then(|x| x.as_str())
            .ok_or_else(|| QueryFluxError::Persistence("_qfTagLeg missing tag".into()))?;
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfTagLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        map.insert(tag.to_string(), json!(name));
    }
    Ok(json!({
        "type": "clientTags",
        "tagToGroup": Value::Object(map),
    }))
}

fn merge_regex_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let mut rules = Vec::new();
    for leg in chunk {
        let regex = leg
            .definition
            .get("regex")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let action = leg
            .definition
            .get("action")
            .and_then(|x| x.as_str())
            .unwrap_or("route");
        if action.eq_ignore_ascii_case("deny") {
            let mut rule = Map::new();
            rule.insert("regex".into(), json!(regex));
            rule.insert("action".into(), json!("deny"));
            if let Some(err) = leg.definition.get("error") {
                if !err.is_null() {
                    rule.insert("error".into(), err.clone());
                }
            }
            rules.push(Value::Object(rule));
            continue;
        }
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfRegexLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        rules.push(json!({
            "regex": regex,
            "targetGroup": name,
        }));
    }
    Ok(json!({
        "type": "queryRegex",
        "rules": Value::Array(rules),
    }))
}

fn merge_tags_rules_chunk(
    chunk: &[&RoutingRulePersistRow],
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let mut rules = Vec::new();
    for leg in chunk {
        let tags = leg.definition.get("tags").cloned().unwrap_or(json!({}));
        let gid = leg.target_group_id.ok_or_else(|| {
            QueryFluxError::Persistence("_qfTagsRuleLeg missing target_group_id".into())
        })?;
        let name = name_for_id(gid, id_to_name)?;
        rules.push(json!({
            "tags": tags,
            "targetGroup": name,
        }));
    }
    Ok(json!({
        "type": "tags",
        "rules": Value::Array(rules),
    }))
}

fn merge_compound_chunk(
    leg: &RoutingRulePersistRow,
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let gid = leg.target_group_id.ok_or_else(|| {
        QueryFluxError::Persistence("compound router row missing target_group_id".into())
    })?;
    let name = name_for_id(gid, id_to_name)?;
    let mut m =
        leg.definition.as_object().cloned().ok_or_else(|| {
            QueryFluxError::Persistence("compound definition must be object".into())
        })?;
    m.insert("targetGroup".to_string(), json!(name));
    Ok(Value::Object(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_compound() {
        let mut m = HashMap::new();
        m.insert("g1".to_string(), 1i64);
        let mut idn = HashMap::new();
        idn.insert(1i64, "g1".to_string());

        let r = json!({
            "type": "compound",
            "combine": "all",
            "conditions": [],
            "targetGroup": "g1",
        });
        let slices = expand_router_for_persistence(&r, &m).unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].0.get("targetGroup").is_none());
        assert_eq!(slices[0].1, Some(1));

        let row = RoutingRulePersistRow {
            sort_order: 0,
            router_logical_index: 0,
            slice_index: 0,
            target_group_id: slices[0].1,
            definition: slices[0].0.clone(),
        };
        let out = collapse_rows_to_routers(&[row], &idn).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["targetGroup"], json!("g1"));
    }

    /// Studio persists one logical router per chain row; distinct `router_logical_index` values must
    /// become separate `routers[]` entries in order (by `sort_order`).
    #[test]
    fn collapse_two_header_routers_preserves_evaluation_order() {
        let mut m = HashMap::new();
        m.insert("analytics".to_string(), 1i64);
        m.insert("batch".to_string(), 2i64);
        let mut idn = HashMap::new();
        idn.insert(1i64, "analytics".to_string());
        idn.insert(2i64, "batch".to_string());

        let rows = vec![
            RoutingRulePersistRow {
                sort_order: 0,
                router_logical_index: 0,
                slice_index: 0,
                target_group_id: Some(1),
                definition: json!({
                    "type": "_qfHeaderLeg",
                    "headerName": "X-Env",
                    "headerValue": "prod",
                }),
            },
            RoutingRulePersistRow {
                sort_order: 1,
                router_logical_index: 1,
                slice_index: 0,
                target_group_id: Some(2),
                definition: json!({
                    "type": "_qfHeaderLeg",
                    "headerName": "X-Team",
                    "headerValue": "etl",
                }),
            },
        ];
        let out = collapse_rows_to_routers(&rows, &idn).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["type"], json!("header"));
        assert_eq!(out[0]["headerName"], json!("X-Env"));
        assert_eq!(out[1]["type"], json!("header"));
        assert_eq!(out[1]["headerName"], json!("X-Team"));
    }

    /// Rows out of insertion order are sorted by `sort_order` before merge.
    #[test]
    fn collapse_sorts_by_sort_order_before_grouping() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), 1i64);
        m.insert("b".to_string(), 2i64);
        let mut idn = HashMap::new();
        idn.insert(1i64, "a".to_string());
        idn.insert(2i64, "b".to_string());

        let rows = vec![
            RoutingRulePersistRow {
                sort_order: 5,
                router_logical_index: 1,
                slice_index: 0,
                target_group_id: Some(2),
                definition: json!({
                    "type": "_qfHeaderLeg",
                    "headerName": "H2",
                    "headerValue": "v2",
                }),
            },
            RoutingRulePersistRow {
                sort_order: 1,
                router_logical_index: 0,
                slice_index: 0,
                target_group_id: Some(1),
                definition: json!({
                    "type": "_qfHeaderLeg",
                    "headerName": "H1",
                    "headerValue": "v1",
                }),
            },
        ];
        let out = collapse_rows_to_routers(&rows, &idn).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["headerName"], json!("H1"));
        assert_eq!(out[1]["headerName"], json!("H2"));
    }

    #[test]
    fn roundtrip_tags_router() {
        let mut m = HashMap::new();
        m.insert("g1".to_string(), 1i64);
        let mut idn = HashMap::new();
        idn.insert(1i64, "g1".to_string());

        let r = json!({
            "type": "tags",
            "rules": [
                {
                    "tags": { "premium": null, "team": "eng" },
                    "targetGroup": "g1",
                }
            ],
        });
        let slices = expand_router_for_persistence(&r, &m).unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0["type"], json!("_qfTagsRuleLeg"));
        assert_eq!(slices[0].1, Some(1));

        let row = RoutingRulePersistRow {
            sort_order: 0,
            router_logical_index: 0,
            slice_index: 0,
            target_group_id: slices[0].1,
            definition: slices[0].0.clone(),
        };
        let out = collapse_rows_to_routers(&[row], &idn).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], json!("tags"));
        assert_eq!(out[0]["rules"][0]["targetGroup"], json!("g1"));
        assert_eq!(out[0]["rules"][0]["tags"]["team"], json!("eng"));
    }

    #[test]
    fn expand_python_script_router_single_slice_no_group_column() {
        let router = json!({
            "type": "pythonScript",
            "script": "def route(q, c):\n    return None\n",
            "scriptFile": null,
        });
        let slices = expand_router_for_persistence(&router, &HashMap::new()).unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].1.is_none());
        assert_eq!(slices[0].0["type"], json!("pythonScript"));

        let row = RoutingRulePersistRow {
            sort_order: 0,
            router_logical_index: 0,
            slice_index: 0,
            target_group_id: None,
            definition: slices[0].0.clone(),
        };
        let out = collapse_rows_to_routers(&[row], &HashMap::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], json!("pythonScript"));
        assert_eq!(out[0]["script"], router["script"]);
    }

    #[test]
    fn roundtrip_query_regex_deny_without_group() {
        let router = json!({
            "type": "queryRegex",
            "rules": [{
                "regex": "(?i)^\\\\s*DROP",
                "action": "deny",
                "error": "DDL blocked"
            }]
        });
        let slices = expand_router_for_persistence(&router, &HashMap::new()).unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].1.is_none());
        assert_eq!(slices[0].0["type"], json!("_qfRegexLeg"));
        assert_eq!(slices[0].0["action"], json!("deny"));

        let row = RoutingRulePersistRow {
            sort_order: 0,
            router_logical_index: 0,
            slice_index: 0,
            target_group_id: None,
            definition: slices[0].0.clone(),
        };
        let out = collapse_rows_to_routers(&[row], &HashMap::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], json!("queryRegex"));
        assert_eq!(out[0]["rules"][0]["action"], json!("deny"));
        assert_eq!(out[0]["rules"][0]["error"], json!("DDL blocked"));
        assert!(out[0]["rules"][0].get("targetGroup").is_none());
    }
}
