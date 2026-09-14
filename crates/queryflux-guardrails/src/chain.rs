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
    /// Returns `(actions, outcome)`. Stops at the first `Deny` (recording it first).
    /// A `Rewrite` result is recorded and its SQL carried forward as
    /// `GuardChainOutcome::Proceed { sql: Some(..) }`. If more than one guard rewrites,
    /// the last wins — v1 places at most one rewriting guard (the access-control guard)
    /// and always last, so downstream guards never see stale SQL.
    pub async fn run(
        &self,
        ctx: &GuardContext<'_>,
        layer: GuardLayer,
    ) -> (Vec<GuardAction>, GuardChainOutcome) {
        let mut actions: Vec<GuardAction> = Vec::new();
        let mut rewritten_sql: Option<String> = None;

        for guard in &self.guards {
            if guard.layer() != layer {
                continue;
            }

            let result = guard.check(ctx).await;
            actions.push(result_to_action(guard.name(), &result));

            match &result {
                GuardResult::Deny { reason, code } => {
                    return (
                        actions,
                        GuardChainOutcome::Blocked {
                            reason: reason.clone(),
                            code: code.clone(),
                        },
                    );
                }
                GuardResult::Rewrite { sql, .. } => {
                    rewritten_sql = Some(sql.clone());
                }
                GuardResult::Allow { .. } | GuardResult::Warn { .. } => {}
            }
        }

        (actions, GuardChainOutcome::Proceed { sql: rewritten_sql })
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
}
