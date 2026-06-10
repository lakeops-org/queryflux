pub mod built_in;
pub mod chain;
pub mod config;
pub mod context;
pub mod external;

pub use chain::GuardChain;
pub use config::{GuardChainConfig, GuardGroupConfig, GuardKind, GuardLayerConfig};
pub use context::{GuardContext, GuardLayer, GuardResult};

use queryflux_persistence::GuardAction;

/// Convert a `GuardResult` into a `GuardAction` for audit recording.
pub fn result_to_action(guard_name: &str, result: &GuardResult) -> GuardAction {
    match result {
        GuardResult::Allow { metadata } => GuardAction {
            guard: guard_name.to_string(),
            action: "allow".to_string(),
            reason: None,
            code: None,
            metadata: metadata.clone(),
        },
        GuardResult::Warn { reason } => GuardAction {
            guard: guard_name.to_string(),
            action: "warn".to_string(),
            reason: Some(reason.clone()),
            code: None,
            metadata: None,
        },
        GuardResult::Deny { reason, code } => GuardAction {
            guard: guard_name.to_string(),
            action: "deny".to_string(),
            reason: Some(reason.clone()),
            code: code.clone(),
            metadata: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn result_to_action_allow() {
        let a = result_to_action("g", &GuardResult::allow());
        assert_eq!(a.guard, "g");
        assert_eq!(a.action, "allow");
        assert!(a.reason.is_none());
        assert!(a.code.is_none());
    }

    #[test]
    fn result_to_action_allow_with_metadata() {
        let mut m = HashMap::new();
        m.insert("k".to_string(), "v".to_string());
        let a = result_to_action("g", &GuardResult::Allow { metadata: Some(m) });
        assert_eq!(a.action, "allow");
        assert!(a.reason.is_none());
    }

    #[test]
    fn result_to_action_warn() {
        let a = result_to_action("w", &GuardResult::warn("heads up"));
        assert_eq!(a.action, "warn");
        assert_eq!(a.reason.as_deref(), Some("heads up"));
        assert!(a.code.is_none());
    }

    #[test]
    fn result_to_action_deny() {
        let a = result_to_action("d", &GuardResult::deny("nope", "ERR_CODE"));
        assert_eq!(a.action, "deny");
        assert_eq!(a.reason.as_deref(), Some("nope"));
        assert_eq!(a.code.as_deref(), Some("ERR_CODE"));
    }
}
