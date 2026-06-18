use crate::{
    built_in::Guard,
    context::{GuardContext, GuardLayer},
    result_to_action,
};
use queryflux_core::query::GuardAction;

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
    /// Returns `(actions, was_blocked)`. Stops at the first Deny but still
    /// records the deny action before returning.
    pub async fn run(&self, ctx: &GuardContext<'_>, layer: GuardLayer) -> (Vec<GuardAction>, bool) {
        let mut actions: Vec<GuardAction> = Vec::new();
        let mut was_blocked = false;

        for guard in &self.guards {
            if guard.layer() != layer {
                continue;
            }

            let result = guard.check(ctx).await;
            let is_deny = result.is_deny();
            actions.push(result_to_action(guard.name(), &result));

            if is_deny {
                was_blocked = true;
                break;
            }
        }

        (actions, was_blocked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use queryflux_core::{
        query::{ClusterGroupName, EngineType},
        tags::QueryTags,
    };

    use crate::context::GuardResult;

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
        translated_sql: String,
        engine_type: EngineType,
        cluster_group: ClusterGroupName,
        query_tags: QueryTags,
    }

    impl TestCtx {
        fn plan_select_fixture() -> Self {
            Self {
                sql: String::new(),
                translated_sql: "SELECT 1".to_string(),
                engine_type: EngineType::DuckDb,
                cluster_group: ClusterGroupName("default".to_string()),
                query_tags: QueryTags::new(),
            }
        }

        fn ctx(&self) -> GuardContext<'_> {
            GuardContext {
                sql: &self.sql,
                translated_sql: &self.translated_sql,
                engine_type: &self.engine_type,
                cluster_group: &self.cluster_group,
                user: None,
                agent_context: None,
                query_tags: &self.query_tags,
            }
        }
    }

    #[tokio::test]
    async fn run_empty_chain() {
        let chain = GuardChain::new(vec![]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, blocked) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert!(actions.is_empty());
        assert!(!blocked);
    }

    #[tokio::test]
    async fn run_skips_wrong_layer() {
        let chain = GuardChain::new(vec![Box::new(TestGuard {
            name: "input_only",
            layer: GuardLayer::Input,
            result: GuardResult::deny("should not run", "X"),
        })]);
        let tc = TestCtx::plan_select_fixture();
        let (actions, blocked) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert!(actions.is_empty());
        assert!(!blocked);
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
        let (actions, blocked) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].guard, "a");
        assert_eq!(actions[0].action, "allow");
        assert_eq!(actions[1].guard, "b");
        assert_eq!(actions[1].action, "warn");
        assert!(!blocked);
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
        let (actions, blocked) = chain.run(&tc.ctx(), GuardLayer::Plan).await;
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[1].guard, "blocker");
        assert_eq!(actions[1].action, "deny");
        assert!(blocked);
    }
}
