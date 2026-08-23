use std::collections::HashMap;

use queryflux_core::{
    query::{ClusterGroupName, EngineType},
    session::AgentContext,
    sql_classify::SqlParseCache,
    tags::QueryTags,
};

/// Which pipeline stage a guard runs at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardLayer {
    /// L1 — runs on the NL question before any LLM call (Phase 4).
    Input,
    /// L2 — runs on translated SQL before engine submission.
    Plan,
    /// L3 — runs on returned rows / NL summary (Phase 4, MCP only).
    Output,
}

/// Everything a guard implementation can inspect.
pub struct GuardContext<'a> {
    pub sql: &'a str,
    pub translated_sql: &'a str,
    pub engine_type: &'a EngineType,
    pub cluster_group: &'a ClusterGroupName,
    pub user: Option<&'a str>,
    pub agent_context: Option<&'a AgentContext>,
    pub query_tags: &'a QueryTags,
    /// Shared parse cache from dispatch. When set, guards must not re-parse SQL.
    pub sql_parse: Option<&'a SqlParseCache>,
}

/// The verdict a guard returns after inspecting a query.
#[derive(Debug, Clone)]
pub enum GuardResult {
    /// Query is permitted. Optional metadata is stored in `guard_actions` for observability.
    Allow {
        metadata: Option<HashMap<String, String>>,
    },
    /// Query is permitted but a warning is logged and recorded.
    Warn { reason: String },
    /// Query is blocked. `code` is machine-readable so agents can react programmatically.
    Deny {
        reason: String,
        code: Option<String>,
    },
}

impl GuardResult {
    pub fn allow() -> Self {
        Self::Allow { metadata: None }
    }

    pub fn deny(reason: impl Into<String>, code: impl Into<String>) -> Self {
        Self::Deny {
            reason: reason.into(),
            code: Some(code.into()),
        }
    }

    pub fn warn(reason: impl Into<String>) -> Self {
        Self::Warn {
            reason: reason.into(),
        }
    }

    pub fn is_deny(&self) -> bool {
        matches!(self, Self::Deny { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_result_is_deny() {
        assert!(!GuardResult::allow().is_deny());
        assert!(!GuardResult::warn("x").is_deny());
        assert!(GuardResult::deny("x", "C").is_deny());
    }
}
