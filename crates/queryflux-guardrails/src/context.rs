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
    /// L2 — runs on the query before engine submission. The dedicated access-control
    /// guard runs at this layer on the **source** SQL, before dialect translation; the
    /// generic guard chain (Python/webhook guards) runs at this layer too but *after*
    /// translation, on the final engine SQL — see [`GuardContext::sql`] /
    /// [`GuardContext::original_sql`] for how to tell which SQL a given `check()` call
    /// actually received.
    Plan,
    /// L3 — runs on returned rows / NL summary (Phase 4, MCP only).
    Output,
}

/// Everything a guard implementation can inspect.
///
/// `sql` / `sql_parse` / `dialect` are whatever SQL this particular guard call actually
/// evaluates — the pre-translation source SQL for the dedicated access-control guard,
/// or the final post-translation engine SQL for the generic `Plan`-layer guard chain.
/// `original_sql` carries the pre-translation client SQL when it differs from `sql`
/// (i.e. for the post-translation guard-chain call); it is `None` when `sql` already
/// *is* the original (no separate translated form exists yet at that call site).
/// `engine_type` is the eventual target engine (for guards that care which backend a
/// query lands on).
pub struct GuardContext<'a> {
    pub sql: &'a str,
    /// The pre-translation client SQL, when `sql` is a post-translation rendering of it.
    pub original_sql: Option<&'a str>,
    /// Dialect `sql` / `sql_parse` are parsed as.
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
    /// masks already spliced at the scan site); `maybe_translate` runs next. Only
    /// meaningful for a guard invoked directly (the access-control guard, via
    /// `run_access_control_stage`) — a `GuardChain` cannot carry this back to its caller
    /// and treats it as an error (see `GuardChain::run`).
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
    /// The query may proceed unchanged. A `GuardChain` has no mechanism to carry a
    /// `GuardResult::Rewrite` back to the caller — the one guard that ever returns
    /// `Rewrite` (the access-control guard) runs outside any chain, on pre-translation
    /// SQL, via its own dedicated call site (`access_control_guard::run_access_control_stage`).
    /// A guard placed *in* a chain that returns `Rewrite` is treated as a
    /// configuration/implementation error (see `GuardChain::run`), not silently ignored.
    Proceed,
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
