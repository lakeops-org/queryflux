//! Post-routing resolution: authorization-aware first-fit when the chain used fallback.

use queryflux_auth::{AuthContext, AuthorizationChecker};
use queryflux_core::{error::QueryFluxError, query::ClusterGroupName};
use queryflux_routing::chain::RoutingTrace;

/// When the router chain used its static fallback, pick the first cluster group in
/// `group_order` the caller may access. Explicit router matches are returned as-is
/// (dispatch enforces `allowUsers` / `allowGroups` separately).
///
/// Callers should snapshot `group_order` and `authorization` and drop any live-config
/// lock before awaiting this — `check` may perform remote I/O (e.g. OpenFGA).
pub async fn resolve_routed_group(
    group_order: &[String],
    authorization: &dyn AuthorizationChecker,
    routed: ClusterGroupName,
    trace: &mut RoutingTrace,
    auth_ctx: &AuthContext,
) -> Result<ClusterGroupName, QueryFluxError> {
    if !trace.used_fallback {
        return Ok(routed);
    }

    for name in group_order {
        if authorization.check(auth_ctx, name).await {
            if name != &trace.final_group {
                trace.final_group = name.clone();
            }
            return Ok(ClusterGroupName(name.clone()));
        }
    }

    Err(QueryFluxError::Unauthorized(format!(
        "user '{}' is not authorized for any cluster group",
        auth_ctx.user
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use queryflux_auth::{
        AllowAllAuthorization, AuthContext, AuthorizationChecker, SimpleAuthorizationPolicy,
    };
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
        strategy::RoundRobinStrategy,
    };
    use queryflux_core::{
        config::ClusterGroupAuthorizationConfig,
        query::{ClusterGroupName, ClusterName, EngineType},
    };
    use queryflux_routing::chain::{RouterChain, RoutingTrace};

    use super::*;
    use crate::state::LiveConfig;

    fn auth(user: &str, groups: &[&str]) -> AuthContext {
        AuthContext {
            user: user.into(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            roles: vec![],
            raw_token: None,
            ..Default::default()
        }
    }

    fn live_with_policies(
        policies: HashMap<String, ClusterGroupAuthorizationConfig>,
    ) -> LiveConfig {
        let analytics = ClusterGroupName("analytics".into());
        let default = ClusterGroupName("default".into());
        let cluster = ClusterName("c1".into());
        let state = Arc::new(ClusterState::new(
            cluster.clone(),
            analytics.clone(),
            None,
            None,
            EngineType::Trino,
            Some("http://trino.test:8080".into()),
            10,
            true,
        ));
        let mut groups = HashMap::new();
        groups.insert(
            analytics.clone(),
            (
                vec![state],
                Arc::new(RoundRobinStrategy::new())
                    as Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
            ),
        );
        LiveConfig {
            router_chain: RouterChain::new(vec![], default.clone()),
            guard_chain: None,
            group_guard_chains: HashMap::new(),
            cluster_manager: Arc::new(SimpleClusterGroupManager::new(groups)),
            adapters: HashMap::new(),
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members: HashMap::from([
                ("analytics".into(), vec!["c1".into()]),
                ("default".into(), vec!["c1".into()]),
            ]),
            group_order: vec!["analytics".into(), "default".into()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
            group_max_queued_queries: HashMap::new(),
            group_capacity_wait_timeout_secs: HashMap::new(),
            group_cache_settings: HashMap::new(),
            auth_provider: Arc::new(queryflux_auth::NoneAuthProvider::new(false)),
            authorization: Arc::new(SimpleAuthorizationPolicy::new(policies))
                as Arc<dyn AuthorizationChecker>,
        }
    }

    #[tokio::test]
    async fn explicit_route_skips_first_fit() {
        let live = live_with_policies(HashMap::from([
            (
                "analytics".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-a".into()],
                    allow_users: vec![],
                },
            ),
            (
                "default".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-b".into()],
                    allow_users: vec![],
                },
            ),
        ]));
        let mut trace = RoutingTrace {
            decisions: vec![],
            final_group: "default".into(),
            used_fallback: false,
            denied: None,
        };
        let routed = ClusterGroupName("default".into());
        let out = resolve_routed_group(
            &live.group_order,
            live.authorization.as_ref(),
            routed.clone(),
            &mut trace,
            &auth("alice", &[]),
        )
        .await
        .unwrap();
        assert_eq!(out, routed);
    }

    #[tokio::test]
    async fn fallback_picks_first_authorized_group() {
        let live = live_with_policies(HashMap::from([
            (
                "analytics".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-a".into()],
                    allow_users: vec![],
                },
            ),
            (
                "default".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-b".into()],
                    allow_users: vec![],
                },
            ),
        ]));
        let mut trace = RoutingTrace {
            decisions: vec![],
            final_group: "default".into(),
            used_fallback: true,
            denied: None,
        };
        let out = resolve_routed_group(
            &live.group_order,
            live.authorization.as_ref(),
            ClusterGroupName("default".into()),
            &mut trace,
            &auth("alice", &["team-a"]),
        )
        .await
        .unwrap();
        assert_eq!(out.0, "analytics");
        assert_eq!(trace.final_group, "analytics");
    }

    #[tokio::test]
    async fn fallback_none_authorized_returns_unauthorized() {
        let live = live_with_policies(HashMap::from([
            (
                "analytics".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-a".into()],
                    allow_users: vec![],
                },
            ),
            (
                "default".into(),
                ClusterGroupAuthorizationConfig {
                    allow_groups: vec!["team-b".into()],
                    allow_users: vec![],
                },
            ),
        ]));
        let mut trace = RoutingTrace {
            decisions: vec![],
            final_group: "default".into(),
            used_fallback: true,
            denied: None,
        };
        let err = resolve_routed_group(
            &live.group_order,
            live.authorization.as_ref(),
            ClusterGroupName("default".into()),
            &mut trace,
            &auth("bob", &[]),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryFluxError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn allow_all_policy_uses_first_group_on_fallback() {
        let live = live_with_policies(HashMap::new());
        let mut live = live;
        live.authorization = Arc::new(AllowAllAuthorization::default());
        let mut trace = RoutingTrace {
            decisions: vec![],
            final_group: "default".into(),
            used_fallback: true,
            denied: None,
        };
        let out = resolve_routed_group(
            &live.group_order,
            live.authorization.as_ref(),
            ClusterGroupName("default".into()),
            &mut trace,
            &auth("anyone", &[]),
        )
        .await
        .unwrap();
        assert_eq!(out.0, "analytics");
    }
}
