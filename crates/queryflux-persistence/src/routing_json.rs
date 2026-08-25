//! Map routing JSON between the admin API (cluster group numeric ids) and persisted
//! [`queryflux_core::config::RouterConfig`] (group names, what the proxy loads at startup).
//!
//! Accepts both camelCase (serde-native) and snake_case (Studio `JSON.stringify`) keys on input.
//! For `type: "tags"`, also accepts legacy `tag_rules` and per-rule `targetGroupId`, and emits
//! canonical `{ "rules": [ { "tags", "targetGroup" } ] }` for storage.

use queryflux_core::error::{QueryFluxError, Result};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

pub const PROTO_CAMEL_SNAKE: &[(&str, &str)] = &[
    ("trinoHttp", "trino_http"),
    ("postgresWire", "postgres_wire"),
    ("mysqlWire", "mysql_wire"),
    ("clickhouseHttp", "clickhouse_http"),
    ("flightSql", "flight_sql"),
    ("snowflakeHttp", "snowflake_http"),
    ("snowflakeSqlApi", "snowflake_sql_api"),
    ("mcp", "mcp"),
];

pub fn field<'a>(v: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    v.get(camel).or_else(|| v.get(snake))
}

pub fn map_field<'a>(
    v: &'a Value,
    camel: &str,
    snake: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    field(v, camel, snake).and_then(|x| x.as_object())
}

/// Every distinct cluster group **name** referenced by a stored router JSON value.
pub fn collect_group_names_from_router_json(v: &Value) -> Vec<String> {
    let mut seen = std::collections::HashSet::<String>::new();
    collect_group_names_from_router_json_inner(v, &mut seen);
    let mut out: Vec<String> = seen.into_iter().collect();
    out.sort();
    out
}

fn push_str_group(seen: &mut std::collections::HashSet<String>, s: &str) {
    if !s.is_empty() {
        seen.insert(s.to_string());
    }
}

fn collect_group_names_from_router_json_inner(
    v: &Value,
    seen: &mut std::collections::HashSet<String>,
) {
    let Some(ty) = v.get("type").and_then(|x| x.as_str()) else {
        return;
    };
    match ty {
        "protocolBased" => {
            for (camel, snake) in PROTO_CAMEL_SNAKE {
                if let Some(x) = field(v, camel, snake) {
                    if let Some(s) = x.as_str() {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "header" => {
            if let Some(obj) = map_field(v, "headerValueToGroup", "header_value_to_group") {
                for val in obj.values() {
                    if let Some(s) = val.as_str() {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "userGroup" => {
            if let Some(obj) = map_field(v, "userToGroup", "user_to_group") {
                for val in obj.values() {
                    if let Some(s) = val.as_str() {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "queryRegex" => {
            if let Some(arr) = v.get("rules").and_then(|x| x.as_array()) {
                for r in arr {
                    if let Some(s) =
                        field(r, "targetGroup", "target_group").and_then(|x| x.as_str())
                    {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "tags" => {
            if let Some(arr) = v
                .get("rules")
                .or_else(|| v.get("tag_rules"))
                .and_then(|x| x.as_array())
            {
                for r in arr {
                    if let Some(s) =
                        field(r, "targetGroup", "target_group").and_then(|x| x.as_str())
                    {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "clientTags" => {
            if let Some(obj) = map_field(v, "tagToGroup", "tag_to_group") {
                for val in obj.values() {
                    if let Some(s) = val.as_str() {
                        push_str_group(seen, s);
                    }
                }
            }
        }
        "compound" => {
            if let Some(s) = field(v, "targetGroup", "target_group").and_then(|x| x.as_str()) {
                push_str_group(seen, s);
            }
        }
        _ => {}
    }
}

fn group_value_to_name(v: &Value, id_to_name: &HashMap<i64, String>) -> Result<String> {
    match v {
        Value::Number(n) => {
            let id = n
                .as_i64()
                .ok_or_else(|| QueryFluxError::Persistence("invalid group id".into()))?;
            id_to_name.get(&id).cloned().ok_or_else(|| {
                QueryFluxError::Persistence(format!("unknown cluster group id {id}"))
            })
        }
        Value::String(s) => Ok(s.clone()),
        Value::Null => Ok(String::new()),
        _ => Err(QueryFluxError::Persistence(
            "group reference must be a numeric id or string name".into(),
        )),
    }
}

fn optional_proto_out(
    v: &Value,
    camel: &str,
    snake: &str,
    id_to_name: &HashMap<i64, String>,
) -> Result<Value> {
    let Some(raw) = field(v, camel, snake) else {
        return Ok(Value::Null);
    };
    match raw {
        Value::Null => Ok(Value::Null),
        Value::String(s) if s.is_empty() => Ok(Value::Null),
        other => {
            let name = group_value_to_name(other, id_to_name)?;
            if name.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(json!(name))
            }
        }
    }
}

/// Convert admin PUT JSON (ids allowed) into JSON compatible with [`RouterConfig`] (names only, camelCase).
pub fn resolve_routers_for_storage(
    routers: &[Value],
    id_to_name: &HashMap<i64, String>,
) -> Result<Vec<Value>> {
    routers
        .iter()
        .map(|r| resolve_one_router_for_storage(r, id_to_name))
        .collect()
}

fn resolve_one_router_for_storage(v: &Value, id_to_name: &HashMap<i64, String>) -> Result<Value> {
    let Some(ty) = v.get("type").and_then(|x| x.as_str()) else {
        return Ok(v.clone());
    };

    match ty {
        "protocolBased" => {
            let mut out = Map::new();
            out.insert("type".to_string(), json!("protocolBased"));
            for (camel, snake) in PROTO_CAMEL_SNAKE {
                let val = optional_proto_out(v, camel, snake, id_to_name)?;
                if !val.is_null() {
                    out.insert((*camel).to_string(), val);
                }
            }
            Ok(Value::Object(out))
        }
        "header" => {
            let header_name = field(v, "headerName", "header_name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let mut m = Map::new();
            if let Some(obj) = map_field(v, "headerValueToGroup", "header_value_to_group") {
                for (hk, hv) in obj {
                    let name = group_value_to_name(hv, id_to_name)?;
                    if !name.is_empty() {
                        m.insert(hk.clone(), json!(name));
                    }
                }
            }
            Ok(json!({
                "type": "header",
                "headerName": header_name,
                "headerValueToGroup": Value::Object(m),
            }))
        }
        "userGroup" => {
            let mut m = Map::new();
            if let Some(obj) = map_field(v, "userToGroup", "user_to_group") {
                for (uk, uv) in obj {
                    let name = group_value_to_name(uv, id_to_name)?;
                    if !name.is_empty() {
                        m.insert(uk.clone(), json!(name));
                    }
                }
            }
            Ok(json!({
                "type": "userGroup",
                "userToGroup": Value::Object(m),
            }))
        }
        "queryRegex" => {
            let rules: Result<Vec<Value>> = v
                .get("rules")
                .and_then(|x| x.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|rule| {
                            let regex = field(rule, "regex", "regex").cloned().unwrap_or(json!(""));
                            let action = rule
                                .get("action")
                                .and_then(|a| a.as_str())
                                .unwrap_or("route");
                            if action.eq_ignore_ascii_case("deny") {
                                let mut out = Map::new();
                                out.insert("regex".into(), regex);
                                out.insert("action".into(), json!("deny"));
                                if let Some(err) = rule.get("error").and_then(|e| e.as_str()) {
                                    out.insert("error".into(), json!(err));
                                }
                                return Ok(Value::Object(out));
                            }
                            let tg = field(rule, "targetGroup", "target_group")
                                .or_else(|| rule.get("targetGroupId"));
                            let name = group_value_to_name(tg.unwrap_or(&Value::Null), id_to_name)?;
                            Ok(json!({
                                "regex": regex,
                                "targetGroup": name,
                            }))
                        })
                        .collect()
                })
                .unwrap_or(Ok(vec![]));
            Ok(json!({
                "type": "queryRegex",
                "rules": Value::Array(rules?),
            }))
        }
        "clientTags" => {
            let mut m = Map::new();
            if let Some(obj) = map_field(v, "tagToGroup", "tag_to_group") {
                for (tk, tv) in obj {
                    let name = group_value_to_name(tv, id_to_name)?;
                    if !name.is_empty() {
                        m.insert(tk.clone(), json!(name));
                    }
                }
            }
            Ok(json!({
                "type": "clientTags",
                "tagToGroup": Value::Object(m),
            }))
        }
        "compound" => {
            let combine = v.get("combine").cloned().unwrap_or(json!("all"));
            let conditions = v.get("conditions").cloned().unwrap_or(json!([]));
            let tg = field(v, "targetGroup", "target_group").or_else(|| v.get("targetGroupId"));
            let name = group_value_to_name(tg.unwrap_or(&Value::Null), id_to_name)?;
            Ok(json!({
                "type": "compound",
                "combine": combine,
                "conditions": conditions,
                "targetGroup": name,
            }))
        }
        "pythonScript" => {
            let script = field(v, "script", "script")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let script_file = field(v, "scriptFile", "script_file").and_then(|x| {
                if x.is_null() {
                    None
                } else {
                    x.as_str().map(|s| s.to_string())
                }
            });
            Ok(json!({
                "type": "pythonScript",
                "script": script,
                "scriptFile": script_file,
            }))
        }
        "tags" => {
            let rules_src = v
                .get("rules")
                .or_else(|| v.get("tag_rules"))
                .and_then(|x| x.as_array());
            let Some(arr) = rules_src else {
                return Ok(json!({ "type": "tags", "rules": [] }));
            };
            let rules: Result<Vec<Value>> = arr
                .iter()
                .map(|rule| {
                    let tags = rule.get("tags").cloned().unwrap_or(json!({}));
                    let tg = field(rule, "targetGroup", "target_group")
                        .or_else(|| rule.get("targetGroupId"));
                    let name = group_value_to_name(tg.unwrap_or(&Value::Null), id_to_name)?;
                    Ok(json!({
                        "tags": tags,
                        "targetGroup": name,
                    }))
                })
                .collect();
            Ok(json!({
                "type": "tags",
                "rules": Value::Array(rules?),
            }))
        }
        _ => Ok(v.clone()),
    }
}

/// Enrich stored [`RouterConfig`] JSON for the Studio (numeric ids alongside names where known).
pub fn enrich_routers_for_api(routers: &[Value], name_to_id: &HashMap<String, i64>) -> Vec<Value> {
    routers
        .iter()
        .map(|r| enrich_one_router_for_api(r, name_to_id))
        .collect()
}

fn enrich_one_router_for_api(v: &Value, name_to_id: &HashMap<String, i64>) -> Value {
    let Some(ty) = v.get("type").and_then(|x| x.as_str()) else {
        return v.clone();
    };
    let mut out = v.as_object().cloned().unwrap_or_default();

    match ty {
        "protocolBased" => {
            for (camel, snake) in PROTO_CAMEL_SNAKE {
                if let Some(Value::String(s)) = field(v, camel, snake) {
                    if !s.is_empty() {
                        if let Some(id) = name_to_id.get(s) {
                            out.insert(format!("{camel}GroupId"), json!(id));
                        }
                    }
                }
            }
        }
        "header" => {
            if let Some(obj) = map_field(v, "headerValueToGroup", "header_value_to_group") {
                let mut ids = Map::new();
                for (hk, hv) in obj {
                    if let Some(s) = hv.as_str() {
                        if let Some(id) = name_to_id.get(s) {
                            ids.insert(hk.clone(), json!(id));
                        }
                    }
                }
                if !ids.is_empty() {
                    out.insert("headerValueToGroupId".to_string(), Value::Object(ids));
                }
            }
        }
        "userGroup" => {
            if let Some(obj) = map_field(v, "userToGroup", "user_to_group") {
                let mut ids = Map::new();
                for (uk, hv) in obj {
                    if let Some(s) = hv.as_str() {
                        if let Some(id) = name_to_id.get(s) {
                            ids.insert(uk.clone(), json!(id));
                        }
                    }
                }
                if !ids.is_empty() {
                    out.insert("userToGroupId".to_string(), Value::Object(ids));
                }
            }
        }
        "queryRegex" => {
            if let Some(arr) = v.get("rules").and_then(|x| x.as_array()) {
                let new_rules: Vec<Value> = arr
                    .iter()
                    .map(|rule| {
                        let mut ro = rule.as_object().cloned().unwrap_or_default();
                        if let Some(Value::String(s)) = field(rule, "targetGroup", "target_group") {
                            if let Some(id) = name_to_id.get(s) {
                                ro.insert("targetGroupId".to_string(), json!(id));
                            }
                        }
                        Value::Object(ro)
                    })
                    .collect();
                out.insert("rules".to_string(), Value::Array(new_rules));
            }
        }
        "clientTags" => {
            if let Some(obj) = map_field(v, "tagToGroup", "tag_to_group") {
                let mut ids = Map::new();
                for (tk, hv) in obj {
                    if let Some(s) = hv.as_str() {
                        if let Some(id) = name_to_id.get(s) {
                            ids.insert(tk.clone(), json!(id));
                        }
                    }
                }
                if !ids.is_empty() {
                    out.insert("tagToGroupId".to_string(), Value::Object(ids));
                }
            }
        }
        "compound" => {
            if let Some(Value::String(s)) = field(v, "targetGroup", "target_group") {
                if let Some(id) = name_to_id.get(s) {
                    out.insert("targetGroupId".to_string(), json!(id));
                }
            }
        }
        "tags" => {
            if let Some(arr) = v.get("rules").and_then(|x| x.as_array()) {
                let new_rules: Vec<Value> = arr
                    .iter()
                    .map(|rule| {
                        let mut ro = rule.as_object().cloned().unwrap_or_default();
                        if let Some(Value::String(s)) = field(rule, "targetGroup", "target_group") {
                            if let Some(id) = name_to_id.get(s) {
                                ro.insert("targetGroupId".to_string(), json!(id));
                            }
                        }
                        Value::Object(ro)
                    })
                    .collect();
                out.insert("rules".to_string(), Value::Array(new_rules));
            }
        }
        _ => {}
    }

    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_compound() {
        let v = json!({
            "type": "compound",
            "combine": "all",
            "conditions": [],
            "targetGroup": "analytics"
        });
        assert_eq!(
            collect_group_names_from_router_json(&v),
            vec!["analytics".to_string()]
        );
    }

    #[test]
    fn resolve_compound_id() {
        let v = json!({
            "type": "compound",
            "combine": "all",
            "conditions": [],
            "targetGroupId": 7
        });
        let mut m = HashMap::new();
        m.insert(7, "analytics".to_string());
        let out = resolve_one_router_for_storage(&v, &m).unwrap();
        assert_eq!(out["targetGroup"], json!("analytics"));
    }

    /// `PROTO_CAMEL_SNAKE` is a hand-maintained mirror of the field names on
    /// [`queryflux_core::config::RouterConfig::ProtocolBased`]. A field added there without a
    /// matching entry here is silently dropped by every persistence write/collect/enrich path
    /// (see https://github.com/lakeops-org/queryflux/issues/86, fixed for Snowflake in #84).
    /// Serializing a fully-populated variant and diffing its keys against `PROTO_CAMEL_SNAKE`
    /// catches that drift without needing the struct to derive `EnumIter`/similar.
    #[test]
    fn proto_camel_snake_matches_protocol_based_fields() {
        use queryflux_core::config::RouterConfig;

        let all_set = RouterConfig::ProtocolBased {
            trino_http: Some("g".into()),
            postgres_wire: Some("g".into()),
            mysql_wire: Some("g".into()),
            clickhouse_http: Some("g".into()),
            flight_sql: Some("g".into()),
            snowflake_http: Some("g".into()),
            snowflake_sql_api: Some("g".into()),
            mcp: Some("g".into()),
        };
        let serialized = serde_json::to_value(&all_set).unwrap();
        let obj = serialized.as_object().unwrap();

        let struct_fields: std::collections::HashSet<&str> = obj
            .keys()
            .map(|k| k.as_str())
            .filter(|k| *k != "type")
            .collect();
        let const_fields: std::collections::HashSet<&str> =
            PROTO_CAMEL_SNAKE.iter().map(|(camel, _)| *camel).collect();

        assert_eq!(
            struct_fields, const_fields,
            "PROTO_CAMEL_SNAKE has drifted from RouterConfig::ProtocolBased's fields; a \
             mismatch here means protocol-based routing rules are silently dropped on write, \
             read, or Studio enrichment for the missing protocol"
        );

        // The camel-case check above only proves PROTO_CAMEL_SNAKE's *keys* line up with the
        // struct's fields. It says nothing about the paired snake_case value — a typo there
        // (e.g. "snowflake_sqlapi" instead of "snowflake_sql_api") would still pass it, yet
        // would make `field()` fail to recognize Studio's snake_case JSON.stringify input and
        // silently drop that protocol's routing rule. Pin the exact pairs against the known
        // Rust field names (mysql_wire, snowflake_sql_api, ...) to catch that too.
        let expected_mappings: std::collections::HashSet<(&str, &str)> = [
            ("trinoHttp", "trino_http"),
            ("postgresWire", "postgres_wire"),
            ("mysqlWire", "mysql_wire"),
            ("clickhouseHttp", "clickhouse_http"),
            ("flightSql", "flight_sql"),
            ("snowflakeHttp", "snowflake_http"),
            ("snowflakeSqlApi", "snowflake_sql_api"),
            ("mcp", "mcp"),
        ]
        .into_iter()
        .collect();
        let const_mappings: std::collections::HashSet<(&str, &str)> =
            PROTO_CAMEL_SNAKE.iter().copied().collect();
        assert_eq!(
            const_mappings, expected_mappings,
            "PROTO_CAMEL_SNAKE's snake_case half doesn't match RouterConfig::ProtocolBased's \
             actual field names; Studio's snake_case JSON.stringify input would silently fail \
             to resolve for the mismatched protocol"
        );
    }

    #[test]
    fn resolve_and_collect_protocol_based_snowflake() {
        let v = json!({
            "type": "protocolBased",
            "snowflakeHttp": 3,
            "snowflakeSqlApi": 4,
        });
        let mut m = HashMap::new();
        m.insert(3, "snowflake-group".to_string());
        m.insert(4, "snowflake-sql-api-group".to_string());

        let stored = resolve_one_router_for_storage(&v, &m).unwrap();
        assert_eq!(stored["snowflakeHttp"], json!("snowflake-group"));
        assert_eq!(stored["snowflakeSqlApi"], json!("snowflake-sql-api-group"));

        let mut names = collect_group_names_from_router_json(&stored);
        names.sort();
        assert_eq!(
            names,
            vec![
                "snowflake-group".to_string(),
                "snowflake-sql-api-group".to_string(),
            ]
        );

        let mut name_to_id = HashMap::new();
        name_to_id.insert("snowflake-group".to_string(), 3i64);
        name_to_id.insert("snowflake-sql-api-group".to_string(), 4i64);
        let enriched = enrich_one_router_for_api(&stored, &name_to_id);
        assert_eq!(enriched["snowflakeHttpGroupId"], json!(3));
        assert_eq!(enriched["snowflakeSqlApiGroupId"], json!(4));
    }

    #[test]
    fn resolve_tags_tag_rules_and_target_group_id() {
        let v = json!({
            "type": "tags",
            "tag_rules": [
                {
                    "tags": { "team": "eng", "premium": null },
                    "targetGroupId": 3
                }
            ]
        });
        let mut m = HashMap::new();
        m.insert(3, "analytics".to_string());
        let out = resolve_one_router_for_storage(&v, &m).unwrap();
        assert_eq!(out["type"], json!("tags"));
        let rules = out["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["targetGroup"], json!("analytics"));
        assert_eq!(rules[0]["tags"]["team"], json!("eng"));
        assert_eq!(rules[0]["tags"]["premium"], json!(null));
    }
}
