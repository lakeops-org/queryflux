use crate::{
    built_in::Guard,
    context::{GuardChainOutcome, GuardContext, GuardLayer, GuardResult},
    result_to_action,
};
use queryflux_persistence::GuardAction;

/// Ordered chain of guards for a single layer.
pub struct GuardChain {
    guards: Vec<Box<dyn Guard>>,
}

impl GuardChain {
    pub fn new(guards: Vec<Box<dyn Guard>>) -> Self {
        Self { guards }
    }

    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    /// Run all guards for the given layer in order.
    ///
    /// Returns `(actions, outcome)`. Stops at the first `Deny` (recording it first). A
    /// `Rewrite` replaces the working SQL: later guards check the rewritten SQL, and the
    /// outcome is `Rewritten` so the caller submits it.
    pub async fn run(
        &self,
        ctx: &GuardContext<'_>,
        layer: GuardLayer,
    ) -> (Vec<GuardAction>, GuardChainOutcome) {
        run_chains([self], ctx, layer).await
    }
}

/// Run several chains in order (e.g. global then per-group) as one chain, so a rewrite
/// by an earlier chain is what later chains check.
pub async fn run_chains<'c>(
    chains: impl IntoIterator<Item = &'c GuardChain>,
    ctx: &GuardContext<'_>,
    layer: GuardLayer,
) -> (Vec<GuardAction>, GuardChainOutcome) {
    let mut actions: Vec<GuardAction> = Vec::new();
    let mut rewritten: Option<String> = None;

    for guard in chains.into_iter().flat_map(|c| c.guards.iter()) {
        if guard.layer() != layer {
            continue;
        }

        let result = match &rewritten {
            // The shared parse cache describes the input SQL, not the rewrite.
            Some(sql) => {
                let ctx = GuardContext {
                    sql,
                    sql_parse: None,
                    ..*ctx
                };
                guard.check(&ctx).await
            }
            None => guard.check(ctx).await,
        };
        actions.push(result_to_action(guard.name(), &result));

        match result {
            GuardResult::Deny { reason, code } => {
                return (actions, GuardChainOutcome::Blocked { reason, code });
            }
            GuardResult::Rewrite { sql, .. } => rewritten = Some(sql),
            GuardResult::Allow { .. } | GuardResult::Warn { .. } => {}
        }
    }

    let outcome = match rewritten {
        Some(sql) => GuardChainOutcome::Rewritten { sql },
        None => GuardChainOutcome::Proceed,
    };
    (actions, outcome)
}

/// Client-facing message for a blocked query: guard, code and the guard's reason (which
/// says how to fix the query), so agents can correct and retry.
pub fn deny_message(actions: &[GuardAction]) -> String {
    match actions.iter().find(|a| a.action == "deny") {
        Some(a) => {
            let reason = a.reason.as_deref().unwrap_or("query blocked by guardrail");
            match &a.code {
                Some(code) => format!("blocked by guard {} [{code}]: {reason}", a.guard),
                None => format!("blocked by guard {}: {reason}", a.guard),
            }
        }
        None => "query blocked by guardrail".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use queryflux_core::{
        query::{ClusterGroupName, EngineType, SqlDialect},
        tags::QueryTags,
    };
    use std::collections::{BTreeMap, HashMap};

    use crate::context::GuardResult;

    fn is_blocked(o: &GuardChainOutcome) -> bool {
        matches!(o, GuardChainOutcome::Blocked { .. })
    }

    struct TestGuard {
        name: &'static str,
        layer: GuardLayer,
        result: GuardResult,
    }

    #[async_trait]
    impl Guard for TestGuard {
        fn name(&self) -> &'static str {
            self.name
        }

        fn layer(&self) -> GuardLayer {
            self.layer.clone()
        }

        async fn check(&self, _ctx: &GuardContext<'_>) -> GuardResult {
            self.result.clone()
        }
    }

    struct TestCtx {
        sql: String,
        dialect: SqlDialect,
        engine_type: EngineType,
        cluster_group: ClusterGroupName,
        query_tags: QueryTags,
        groups: Vec<String>,
        roles: Vec<String>,
        attributes: BTreeMap<String, serde_json::Value>,
        session_extra: HashMap<String, String>,
    }

    impl TestCtx {
        fn plan_select_fixture() -> Self {
            Self {
                sql: "SELECT 1".to_string(),
                dialect: EngineType::DuckDb.dialect(),
                engine_type: EngineType::DuckDb,
                cluster_group: ClusterGroupName("default".to_string()),
                query_tags: QueryTags::new(),
                groups: Vec::new(),
                roles: Vec::new(),
                attributes: BTreeMap::new(),
                session_extra: HashMap::new(),
            }
        }

        fn ctx(&self) -> GuardContext<'_> {
            GuardContext {
                sql: &self.sql,
                original_sql: None,
                dialect: &self.dialect,
                engine_type: &self.engine_type,
                cluster_group: &self.cluster_group,
                user: None,
                groups: &self.groups,
                roles: &self.roles,
                attributes: &self.attributes,
                agent_context: None,
                query_tags: &self.query_tags,
                session_extra: &self.session_extra,
                schema: None,
                sql_parse: None,
            }
        }
    }

    #[tokio::test]
    async fn run_empty_chain() {
        let chain = GuardChain::new(vec![]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, outcome) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert!(actions.is_empty());
        assert!(!is_blocked(&outcome));
    }

    #[tokio::test]
    async fn run_skips_wrong_layer() {
        let chain = GuardChain::new(vec![Box::new(TestGuard {
            name: "input_only",
            layer: GuardLayer::Input,
            result: GuardResult::deny("should not run", "X"),
        })]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, outcome) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert!(actions.is_empty());
        assert!(!is_blocked(&outcome));
    }

    #[tokio::test]
    async fn run_collects_multiple_allows() {
        let chain = GuardChain::new(vec![
            Box::new(TestGuard {
                name: "a",
                layer: GuardLayer::Plan,
                result: GuardResult::allow(),
            }),
            Box::new(TestGuard {
                name: "b",
                layer: GuardLayer::Plan,
                result: GuardResult::warn("careful"),
            }),
        ]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, outcome) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].guard, "a");
        assert_eq!(actions[0].action, "allow");
        assert_eq!(actions[1].guard, "b");
        assert_eq!(actions[1].action, "warn");
        assert!(!is_blocked(&outcome));
    }

    #[tokio::test]
    async fn run_stops_on_first_deny() {
        let chain = GuardChain::new(vec![
            Box::new(TestGuard {
                name: "first",
                layer: GuardLayer::Plan,
                result: GuardResult::allow(),
            }),
            Box::new(TestGuard {
                name: "blocker",
                layer: GuardLayer::Plan,
                result: GuardResult::deny("stop", "S"),
            }),
            Box::new(TestGuard {
                name: "never",
                layer: GuardLayer::Plan,
                result: GuardResult::allow(),
            }),
        ]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, outcome) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[1].guard, "blocker");
        assert_eq!(actions[1].action, "deny");
        assert!(is_blocked(&outcome));
    }

    #[tokio::test]
    async fn run_applies_rewrite_and_later_guards_see_it() {
        struct SeesSql;
        #[async_trait]
        impl Guard for SeesSql {
            fn name(&self) -> &'static str {
                "sees"
            }
            fn layer(&self) -> GuardLayer {
                GuardLayer::Plan
            }
            async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
                if ctx.sql == "SELECT 1 LIMIT 5" && ctx.sql_parse.is_none() {
                    GuardResult::allow()
                } else {
                    GuardResult::deny(format!("saw {}", ctx.sql), "X")
                }
            }
        }
        let first = GuardChain::new(vec![Box::new(TestGuard {
            name: "rewriter",
            layer: GuardLayer::Plan,
            result: GuardResult::rewrite("SELECT 1 LIMIT 5"),
        })]);
        let second = GuardChain::new(vec![Box::new(SeesSql)]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, outcome) = run_chains([&first, &second], &tc.ctx(), GuardLayer::Plan).await;
        assert_eq!(actions[0].action, "rewrite");
        assert_eq!(actions[1].action, "allow");
        match outcome {
            GuardChainOutcome::Rewritten { sql } => assert_eq!(sql, "SELECT 1 LIMIT 5"),
            other => panic!("expected Rewritten, got {other:?}"),
        }
    }

    #[test]
    fn deny_message_names_guard_and_code() {
        let a = crate::result_to_action(
            "row_limit",
            &GuardResult::deny("too big", "ROW_LIMIT_EXCEEDED"),
        );
        assert_eq!(
            deny_message(&[a]),
            "blocked by guard row_limit [ROW_LIMIT_EXCEEDED]: too big"
        );
    }
}
