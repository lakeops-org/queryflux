use std::collections::HashMap;

use serde::Deserialize;

/// Normalized query tags: a map from tag key to an optional value.
///
/// - Key-only tag (Trino style):  `"batch"  => None`
/// - Key-value tag:               `"team"   => Some("eng")`
///
/// Tags are always optional. An empty map means no tags were provided.
pub type QueryTags = HashMap<String, Option<String>>;

/// Parse a raw tag string into [`QueryTags`].
///
/// Two wire formats are accepted:
/// - **JSON object** (tried first): `{"team":"eng","batch":null}`
/// - **k:v comma-separated** (fallback): `team:eng,batch,cost_center:701`
///
/// Validation rules (soft — invalid entries are silently dropped):
/// - Max 20 tags per query.
/// - Key and value each max 128 characters.
/// - Keys must match `[a-zA-Z0-9_-]+`.
///
/// Returns `(tags, warnings)`. Warnings are non-empty when entries were dropped.
pub fn parse_query_tags(raw: &str) -> (QueryTags, Vec<String>) {
    let raw = raw.trim();
    if raw.is_empty() {
        return (HashMap::new(), vec![]);
    }

    // Try JSON first.
    if raw.starts_with('{') {
        if let Ok(map) = serde_json::from_str::<serde_json::Value>(raw) {
            if let Some(obj) = map.as_object() {
                return validate_tags(obj.iter().map(|(k, v)| {
                    let val = match v {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    };
                    (k.clone(), val)
                }));
            }
        }
    }

    // Fall back to k:v comma-separated.
    validate_tags(raw.split(',').map(|part| {
        let part = part.trim();
        match part.split_once(':') {
            Some((k, v)) => (k.trim().to_string(), Some(v.trim().to_string())),
            None => (part.to_string(), None),
        }
    }))
}

/// Merge two tag maps: `base` values are overridden by `override_tags` on the same key.
/// Equivalent to `base ← override_tags`.
pub fn merge_tags(base: &QueryTags, override_tags: &QueryTags) -> QueryTags {
    let mut result = base.clone();
    result.extend(override_tags.iter().map(|(k, v)| (k.clone(), v.clone())));
    result
}

/// Convert `QueryTags` to a JSONB-compatible `serde_json::Value` for Postgres storage.
pub fn tags_to_json(tags: &QueryTags) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = tags
        .iter()
        .map(|(k, v)| {
            let val = match v {
                Some(s) => serde_json::Value::String(s.clone()),
                None => serde_json::Value::Null,
            };
            (k.clone(), val)
        })
        .collect();
    serde_json::Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Internal validation
// ---------------------------------------------------------------------------

const MAX_TAGS: usize = 20;
const MAX_KEY_LEN: usize = 128;
const MAX_VAL_LEN: usize = 128;

fn validate_tags(iter: impl Iterator<Item = (String, Option<String>)>) -> (QueryTags, Vec<String>) {
    let mut tags = HashMap::new();
    let mut warnings = Vec::new();

    for (key, val) in iter {
        if tags.len() >= MAX_TAGS {
            warnings.push(format!(
                "query_tags: max {MAX_TAGS} tags exceeded, remaining tags dropped"
            ));
            break;
        }
        if key.is_empty() {
            continue;
        }
        if key.len() > MAX_KEY_LEN {
            warnings.push(format!(
                "query_tags: key '{}...' exceeds {MAX_KEY_LEN} chars, dropped",
                &key[..MAX_KEY_LEN.min(key.len())]
            ));
            continue;
        }
        if !key
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            warnings.push(format!(
                "query_tags: key '{key}' contains invalid characters, dropped"
            ));
            continue;
        }
        if let Some(ref v) = val {
            if v.len() > MAX_VAL_LEN {
                warnings.push(format!(
                    "query_tags: value for key '{key}' exceeds {MAX_VAL_LEN} chars, dropped"
                ));
                continue;
            }
        }
        tags.insert(key, val);
    }

    (tags, warnings)
}

// ---------------------------------------------------------------------------
// Serde helpers for config (HashMap<String, Option<String>> in YAML)
// ---------------------------------------------------------------------------

/// Deserialize a flat `HashMap<String, String>` (from YAML config) into `QueryTags`.
/// Config uses plain string values; there is no `null` in YAML for this use case.
pub fn deserialize_config_tags<'de, D>(deserializer: D) -> Result<QueryTags, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map = HashMap::<String, String>::deserialize(deserializer)?;
    Ok(map.into_iter().map(|(k, v)| (k, Some(v))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kv_style() {
        let (tags, warnings) = parse_query_tags("team:eng,cost_center:701,batch");
        assert_eq!(tags.get("team"), Some(&Some("eng".to_string())));
        assert_eq!(tags.get("cost_center"), Some(&Some("701".to_string())));
        assert_eq!(tags.get("batch"), Some(&None));
        assert!(warnings.is_empty());
    }

    #[test]
    fn parse_json_style() {
        let (tags, warnings) =
            parse_query_tags(r#"{"team":"eng","cost_center":"701","batch":null}"#);
        assert_eq!(tags.get("team"), Some(&Some("eng".to_string())));
        assert_eq!(tags.get("batch"), Some(&None));
        assert!(warnings.is_empty());
    }

    #[test]
    fn parse_empty() {
        let (tags, warnings) = parse_query_tags("");
        assert!(tags.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn validation_drops_invalid_key() {
        let (tags, warnings) = parse_query_tags("valid-key:ok,inv@lid:bad");
        assert!(tags.contains_key("valid-key"));
        assert!(!tags.contains_key("inv@lid"));
        assert!(!warnings.is_empty());
    }

    #[test]
    fn validation_drops_over_limit() {
        let many: String = (0..25)
            .map(|i| format!("k{i}:v{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let (tags, warnings) = parse_query_tags(&many);
        assert_eq!(tags.len(), MAX_TAGS);
        assert!(!warnings.is_empty());
    }

    #[test]
    fn merge_override_wins() {
        let base: QueryTags = [("team".to_string(), Some("base".to_string()))].into();
        let over: QueryTags = [("team".to_string(), Some("override".to_string()))].into();
        let merged = merge_tags(&base, &over);
        assert_eq!(merged.get("team"), Some(&Some("override".to_string())));
    }

    // ---------------------------------------------------------------------------
    // Group default tags — merge semantics
    // ---------------------------------------------------------------------------

    fn tags(pairs: &[(&str, Option<&str>)]) -> QueryTags {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn group_tags_only_all_present() {
        let group = tags(&[("env", Some("prod")), ("batch", None)]);
        let session: QueryTags = HashMap::new();
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("env"), Some(&Some("prod".to_string())));
        assert_eq!(effective.get("batch"), Some(&None));
        assert_eq!(effective.len(), 2);
    }

    #[test]
    fn session_tags_only_no_group() {
        let group: QueryTags = HashMap::new();
        let session = tags(&[("team", Some("eng"))]);
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("team"), Some(&Some("eng".to_string())));
    }

    #[test]
    fn group_and_session_no_overlap_both_present() {
        let group = tags(&[("env", Some("prod"))]);
        let session = tags(&[("team", Some("eng"))]);
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("env"), Some(&Some("prod".to_string())));
        assert_eq!(effective.get("team"), Some(&Some("eng".to_string())));
        assert_eq!(effective.len(), 2);
    }

    #[test]
    fn session_wins_on_key_conflict() {
        let group = tags(&[("env", Some("prod")), ("cost_center", Some("701"))]);
        let session = tags(&[("env", Some("staging"))]);
        let effective = merge_tags(&group, &session);
        // session overrides group value
        assert_eq!(effective.get("env"), Some(&Some("staging".to_string())));
        // group-only tag passes through
        assert_eq!(effective.get("cost_center"), Some(&Some("701".to_string())));
    }

    #[test]
    fn session_key_only_overrides_group_value() {
        // Group set env=prod; session sets env as key-only (null) — session wins.
        let group = tags(&[("env", Some("prod"))]);
        let session = tags(&[("env", None)]);
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("env"), Some(&None));
    }

    #[test]
    fn group_key_only_passes_through_when_session_silent() {
        let group = tags(&[("batch", None)]);
        let session: QueryTags = HashMap::new();
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("batch"), Some(&None));
    }

    #[test]
    fn session_value_overrides_group_key_only() {
        // Group set batch as key-only; session provides a value for it.
        let group = tags(&[("batch", None)]);
        let session = tags(&[("batch", Some("nightly"))]);
        let effective = merge_tags(&group, &session);
        assert_eq!(effective.get("batch"), Some(&Some("nightly".to_string())));
    }

    #[test]
    fn both_empty_gives_empty() {
        let effective = merge_tags(&HashMap::new(), &HashMap::new());
        assert!(effective.is_empty());
    }
}
