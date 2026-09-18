use std::collections::{BTreeMap, HashMap};

use queryflux_core::{
    query::{ClusterGroupName, EngineType, SqlDialect},
    schema_context::SchemaContext,
    session::AgentContext,
    sql_classify::SqlParseCache,
    tags::QueryTags,
};
use serde_json::Value;

/// Which pipeline stage a guard runs at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardLayer {
    /// L1 — runs on the NL question before any LLM call (Phase 4).
    Input,
    /// L2 — runs on the query before engine submission. The access-control guard (invoked
    /// directly via `run_access_control_stage`, not through `GuardChain`) runs at this
    /// layer on the **source** SQL, before dialect translation. `GuardChain`-based Plan
    /// guards (built-in, webhook, script) are unchanged: they still run after translation,
    /// on the final SQL that will actually reach the engine.
    Plan,
    /// L3 — runs on returned rows / NL summary (Phase 4, MCP only).
    Output,
}

/// Everything a guard implementation can inspect.
///
/// `sql` / `sql_parse` / `dialect` reflect whichever SQL representation the caller built
/// this context from — see [`GuardLayer::Plan`] for how that differs between the
/// access-control guard (source SQL) and `GuardChain`-based Plan guards (translated SQL).
/// `engine_type` is the eventual target engine (for guards that care which backend a query
/// lands on).
pub struct GuardContext<'a> {
    pub sql: &'a str,
    /// The dialect `sql` / `sql_parse` are parsed as (see [`GuardLayer::Plan`]: source
    /// dialect for the access-control guard, target dialect for `GuardChain`-based guards).
    pub dialect: &'a SqlDialect,
    pub engine_type: &'a EngineType,
    pub cluster_group: &'a ClusterGroupName,
    pub user: Option<&'a str>,
    /// Verified group memberships from `AuthContext`.
    pub groups: &'a [String],
    /// Verified roles from `AuthContext`.
    pub roles: &'a [String],
    /// Verified ABAC attributes from `AuthContext` (data-level policy input).
    pub attributes: &'a BTreeMap<String, Value>,
    pub agent_context: Option<&'a AgentContext>,
    pub query_tags: &'a QueryTags,
    /// Client-declared session context (`SessionContext.extra`), for guards that forward
    /// an allowlisted subset to an external policy engine. Never trusted for allow/deny.
    pub session_extra: &'a HashMap<String, String>,
    /// Resolved schema for the referenced tables, when a catalog is configured.
    pub schema: Option<&'a SchemaContext>,
    /// Shared parse cache from dispatch, built from `sql` + `dialect` above. When set,
    /// guards must not re-parse SQL.
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
    /// Query is permitted but the guard rewrote it. `sql` replaces the working SQL for the
    /// rest of the pipeline. It is still source-dialect (row filters + rendered column
    /// masks already spliced at the scan site); `maybe_translate` runs next.
    Rewrite {
        sql: String,
        metadata: Option<HashMap<String, String>>,
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

    pub fn rewrite(sql: impl Into<String>) -> Self {
        Self::Rewrite {
            sql: sql.into(),
            metadata: None,
        }
    }

    pub fn is_deny(&self) -> bool {
        matches!(self, Self::Deny { .. })
    }
}

/// Outcome of running a whole guard layer.
#[derive(Debug, Clone)]
pub enum GuardChainOutcome {
    /// A guard denied the query. `code` is machine-readable.
    Blocked {
        reason: String,
        code: Option<String>,
    },
    /// The query may proceed. `sql` is `Some` when a guard rewrote it (source dialect),
    /// `None` when it is unchanged.
    Proceed { sql: Option<String> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_result_is_deny() {
        assert!(!GuardResult::allow().is_deny());
        assert!(!GuardResult::warn("x").is_deny());
        assert!(GuardResult::deny("x", "C").is_deny());
        assert!(!GuardResult::rewrite("SELECT 1").is_deny());
    }
}
