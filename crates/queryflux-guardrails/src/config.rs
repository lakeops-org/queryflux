use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root guardrails config — global defaults + per-group overrides.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuardChainConfig {
    /// Runs for every query regardless of cluster group.
    #[serde(default)]
    pub global: GuardLayerConfig,
    /// Per-group additional guards. Group chain appends after global chain.
    #[serde(default)]
    pub groups: HashMap<String, GuardGroupConfig>,
}

impl GuardChainConfig {
    /// Validates all guard specs in the config. Returns the first error found.
    pub fn validate(&self) -> Result<(), String> {
        for spec in &self.global.plan {
            spec.validate()?;
        }
        for (group, cfg) in &self.groups {
            for spec in &cfg.plan {
                spec.validate()
                    .map_err(|e| format!("group \"{group}\": {e}"))?;
            }
        }
        Ok(())
    }
}

/// Guard config for one layer (currently only Plan / L2 for Phase 1B).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuardLayerConfig {
    #[serde(default)]
    pub plan: Vec<GuardSpec>,
}

/// Per-group override — adds guards on top of the global chain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuardGroupConfig {
    #[serde(default)]
    pub plan: Vec<GuardSpec>,
}

/// One guard entry in the config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardSpec {
    pub kind: GuardKind,
    /// For built-in guards: the guard name. For script/webhook: unused (name comes from kind).
    #[serde(default)]
    pub name: Option<String>,
    /// Guard-specific parameters (e.g. max_rows, applies_to patterns).
    #[serde(default, flatten)]
    pub params: GuardParams,
}

impl GuardSpec {
    /// Returns an error when required fields are absent.
    pub fn validate(&self) -> Result<(), String> {
        match &self.kind {
            GuardKind::BuiltIn => match self.name.as_deref() {
                Some("read_only" | "row_limit" | "require_predicate") => Ok(()),
                Some(other) => Err(format!("unsupported built_in guard name \"{other}\"")),
                None => Err("built_in guard is missing required field \"name\"".to_string()),
            },
            GuardKind::PythonScript {
                script_id, script, ..
            } => {
                let has_script = script.as_ref().is_some_and(|s| !s.trim().is_empty());
                let has_id = script_id.is_some();
                if has_script && has_id {
                    return Err(
                        "python_script guard must set either \"script\" or \"script_id\", not both"
                            .to_string(),
                    );
                }
                if !has_script && !has_id {
                    return Err(
                        "python_script guard requires either \"script\" or \"script_id\""
                            .to_string(),
                    );
                }
                Ok(())
            }
            GuardKind::HttpWebhook { url, .. } => {
                if url.trim().is_empty() {
                    return Err("http_webhook guard is missing required field \"url\"".to_string());
                }
                Ok(())
            }
        }
    }
}

/// Which kind of guard this is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardKind {
    BuiltIn,
    PythonScript {
        script_id: Option<i64>,
        script: Option<String>,
        timeout_ms: Option<u64>,
    },
    HttpWebhook {
        url: String,
        timeout_ms: Option<u64>,
        retry_count: Option<u32>,
        headers: Option<HashMap<String, String>>,
        fail_behavior: Option<FailBehavior>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FailBehavior {
    #[default]
    Deny,
    Allow,
}

/// Parameters that vary per guard type.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GuardParams {
    /// `row_limit`: maximum rows allowed (default: none).
    pub max_rows: Option<u64>,
    /// `require_predicate` / `partition_predicate_required`: table patterns this guard applies to.
    pub applies_to: Option<Vec<String>>,
    /// `partition_predicate_required`: map of table_pattern → partition column.
    pub tables: Option<HashMap<String, String>>,
    /// `time_range_limit`: default maximum lookback window (e.g. "90d", "1y").
    pub default_max_lookback: Option<String>,
    /// `cost_estimate`: max bytes scanned before blocking.
    pub max_scanned_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn guard_chain_config_roundtrip() {
        let mut groups = HashMap::new();
        groups.insert(
            "analytics".to_string(),
            GuardGroupConfig {
                plan: vec![GuardSpec {
                    kind: GuardKind::BuiltIn,
                    name: Some("row_limit".to_string()),
                    params: GuardParams {
                        max_rows: Some(10_000),
                        ..Default::default()
                    },
                }],
            },
        );
        let cfg = GuardChainConfig {
            global: GuardLayerConfig {
                plan: vec![
                    GuardSpec {
                        kind: GuardKind::BuiltIn,
                        name: Some("read_only".to_string()),
                        params: GuardParams::default(),
                    },
                    GuardSpec {
                        kind: GuardKind::HttpWebhook {
                            url: "https://hooks.example.com/guard".to_string(),
                            timeout_ms: Some(5000),
                            retry_count: Some(1),
                            headers: None,
                            fail_behavior: Some(FailBehavior::Allow),
                        },
                        name: None,
                        params: GuardParams::default(),
                    },
                ],
            },
            groups,
        };

        let v = serde_json::to_value(&cfg).expect("serialize");
        let parsed: GuardChainConfig = serde_json::from_value(v).expect("deserialize");
        assert_eq!(parsed.global.plan.len(), 2);
        assert!(matches!(parsed.global.plan[0].kind, GuardKind::BuiltIn));
        assert_eq!(parsed.global.plan[0].name.as_deref(), Some("read_only"));
        match &parsed.global.plan[1].kind {
            GuardKind::HttpWebhook {
                url,
                timeout_ms,
                retry_count,
                headers,
                fail_behavior,
            } => {
                assert_eq!(url, "https://hooks.example.com/guard");
                assert_eq!(*timeout_ms, Some(5000));
                assert_eq!(*retry_count, Some(1));
                assert!(headers.is_none());
                assert!(matches!(fail_behavior, Some(FailBehavior::Allow)));
            }
            _ => panic!("expected HttpWebhook"),
        }
        assert_eq!(
            parsed.groups["analytics"].plan[0].params.max_rows,
            Some(10_000)
        );
    }

    #[test]
    fn guard_chain_config_snake_case_json() {
        let raw = json!({
            "global": {
                "plan": [{
                    "kind": "built_in",
                    "name": "require_predicate",
                    "applies_to": ["fct_*"]
                }]
            }
        });
        let cfg: GuardChainConfig = serde_json::from_value(raw).expect("from_value");
        assert_eq!(cfg.global.plan.len(), 1);
        assert_eq!(
            cfg.global.plan[0].params.applies_to,
            Some(vec!["fct_*".to_string()])
        );
    }

    #[test]
    fn python_script_guard_kind_roundtrip() {
        let raw = json!({
            "global": {
                "plan": [{
                    "kind": { "python_script": { "script_id": 42, "timeout_ms": 2500 } },
                    "name": "ignored_for_script",
                }]
            }
        });
        let cfg: GuardChainConfig = serde_json::from_value(raw).unwrap();
        match &cfg.global.plan[0].kind {
            GuardKind::PythonScript {
                script_id,
                script,
                timeout_ms,
            } => {
                assert_eq!(*script_id, Some(42));
                assert!(script.is_none());
                assert_eq!(*timeout_ms, Some(2500));
            }
            _ => panic!("expected PythonScript"),
        }
    }

    #[test]
    fn validate_accepts_external_guard_kinds() {
        let script = GuardSpec {
            kind: GuardKind::PythonScript {
                script_id: Some(42),
                script: None,
                timeout_ms: Some(2500),
            },
            name: None,
            params: GuardParams::default(),
        };
        script.validate().expect("python_script should validate");

        let webhook = GuardSpec {
            kind: GuardKind::HttpWebhook {
                url: "https://hooks.example.com/guard".to_string(),
                timeout_ms: Some(500),
                retry_count: Some(1),
                headers: None,
                fail_behavior: Some(FailBehavior::Deny),
            },
            name: None,
            params: GuardParams::default(),
        };
        webhook.validate().expect("http_webhook should validate");
    }

    #[test]
    fn validate_rejects_missing_external_guard_fields() {
        let script = GuardSpec {
            kind: GuardKind::PythonScript {
                script_id: None,
                script: None,
                timeout_ms: None,
            },
            name: None,
            params: GuardParams::default(),
        };
        assert!(script.validate().unwrap_err().contains("script"));

        let webhook = GuardSpec {
            kind: GuardKind::HttpWebhook {
                url: String::new(),
                timeout_ms: None,
                retry_count: None,
                headers: None,
                fail_behavior: None,
            },
            name: None,
            params: GuardParams::default(),
        };
        assert!(webhook.validate().unwrap_err().contains("url"));
    }

    #[test]
    fn validate_rejects_blank_inline_script() {
        let blank = GuardSpec {
            kind: GuardKind::PythonScript {
                script_id: None,
                script: Some("   ".to_string()),
                timeout_ms: None,
            },
            name: None,
            params: GuardParams::default(),
        };
        assert!(blank.validate().unwrap_err().contains("script"));
    }

    #[test]
    fn validate_rejects_both_script_and_id() {
        let both = GuardSpec {
            kind: GuardKind::PythonScript {
                script_id: Some(1),
                script: Some("def check(ctx): pass".to_string()),
                timeout_ms: None,
            },
            name: None,
            params: GuardParams::default(),
        };
        assert!(both.validate().unwrap_err().contains("not both"));
    }
}
