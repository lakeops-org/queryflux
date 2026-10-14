//! Deterministic reload interleavings; no sleeps or external services.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{body::Bytes, extract::State, http::HeaderMap, response::IntoResponse};
use queryflux_auth::AuthContext;
use queryflux_cluster_manager::{
    cluster_state::ClusterState,
    simple::SimpleClusterGroupManager,
    strategy::{ClusterCandidate, ClusterSelectionStrategy, RoundRobinStrategy},
};
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, ClusterName, EngineType, FrontendProtocol, ProxyQueryId},
    session::SessionContext,
};
use queryflux_engine_adapters::{
    trino::{TrinoAdapter, TrinoConfig},
    AdapterKind,
};
use queryflux_routing::{chain::RouterChain, RouterTrait, RoutingDecision};
use tokio::sync::Notify;

use super::{setup_sync_query, DispatchAdapter};
use crate::state::{test_fixtures::app_state, AppState, LiveConfig};

struct PausedRouter {
    entered: Arc<Notify>,
    resume: Arc<Notify>,
    explicit: bool,
}

#[async_trait]
impl RouterTrait for PausedRouter {
    fn type_name(&self) -> &'static str {
        "PausedRouter"
    }

    async fn route(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _protocol: &FrontendProtocol,
        _auth: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(if self.explicit {
            RoutingDecision::Route(ClusterGroupName("old".into()))
        } else {
            RoutingDecision::NoMatch
        })
    }
}

fn configure_generation(live: &mut LiveConfig, name: &str, running: u64, queued: u64) {
    let group = ClusterGroupName(name.into());
    let cluster = ClusterName(format!("{name}-cluster"));
    let cluster_state = Arc::new(ClusterState::new(
        cluster.clone(),
        group.clone(),
        None,
        None,
        EngineType::Trino,
        Some("http://unused.invalid".into()),
        running,
        true,
    ));
    // A mixed generation must also fail authorization, even if its group survives.
    let other = if name == "old" { "new" } else { "old" };
    live.authorization = Arc::new(queryflux_auth::SimpleAuthorizationPolicy::new(
        HashMap::from([(
            other.into(),
            queryflux_core::config::ClusterGroupAuthorizationConfig {
                allow_users: vec!["nobody".into()],
                allow_groups: vec![],
            },
        )]),
    ));
    live.cluster_configs.clear();
    live.router_chain = RouterChain::new(vec![], group.clone());
    live.group_order = vec![name.into()];
    live.group_members = HashMap::from([(name.into(), vec![cluster.0.clone()])]);
    live.group_max_queued_queries = HashMap::from([(name.into(), Some(queued))]);
    live.group_default_tags = HashMap::from([(
        name.into(),
        HashMap::from([("generation".into(), Some(name.into()))]),
    )]);
    live.group_capacity_wait_timeout_secs = HashMap::from([(name.into(), 5)]);
    live.cluster_manager = Arc::new(SimpleClusterGroupManager::new(HashMap::from([(
        group.clone(),
        (
            vec![cluster_state],
            Arc::new(RoundRobinStrategy::new()) as Arc<dyn ClusterSelectionStrategy>,
        ),
    )])));
    live.adapters = HashMap::from([(
        cluster.0.clone(),
        AdapterKind::Async(Arc::new(TrinoAdapter::new(
            cluster,
            group,
            TrinoConfig {
                endpoint: "http://unused.invalid".into(),
                tls_skip_verify: false,
                auth: None,
            },
        ))),
    )]);
}

async fn submit(state: Arc<AppState>) -> serde_json::Value {
    let response = crate::trino_http::handlers::post_statement(
        State(state),
        HeaderMap::new(),
        Bytes::from_static(b"SELECT 1"),
    )
    .await
    .into_response();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn routing_reload(explicit: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let state = app_state(false);
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        {
            let mut live = state.live.write().await;
            configure_generation(&mut live, "old", 0, 1);
            live.router_chain = RouterChain::new(
                vec![Box::new(PausedRouter {
                    entered: entered.clone(),
                    resume: resume.clone(),
                    explicit,
                })],
                ClusterGroupName("old".into()),
            );
        }
        let request = tokio::spawn(submit(state.clone()));
        entered.notified().await;

        // Publish while routing is suspended. A retained read lock would deadlock here.
        let mut next = state.snapshot().await;
        configure_generation(&mut next, "new", 0, 2);
        *state.live.write().await = next;
        resume.notify_one();

        let old = request.await.unwrap();
        assert_eq!(old["stats"]["state"], "QUEUED", "{old}");
        let queued = state
            .persistence
            .get_queued(&ProxyQueryId(old["id"].as_str().unwrap().into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queued.cluster_group.0, "old");

        // Requests begun after publication use the new fallback, membership, and queue limit.
        for _ in 0..2 {
            let new = submit(state.clone()).await;
            assert_eq!(new["stats"]["state"], "QUEUED", "{new}");
            let queued = state
                .persistence
                .get_queued(&ProxyQueryId(new["id"].as_str().unwrap().into()))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(queued.cluster_group.0, "new");
        }
        let full = submit(state).await;
        assert!(
            full["error"].is_object(),
            "new generation's queue limit must apply: {full}"
        );
    })
    .await
    .expect("reload and routing must finish without retaining a config lock");
}

#[tokio::test]
async fn explicit_route_keeps_one_generation_during_reload() {
    routing_reload(true).await;
}

#[tokio::test]
async fn fallback_route_keeps_one_generation_during_reload() {
    routing_reload(false).await;
}

struct PausedStrategy {
    entered: Arc<Notify>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ClusterSelectionStrategy for PausedStrategy {
    fn requires_blocking_dispatch(&self) -> bool {
        true
    }

    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        assert_eq!(candidates[0].max_running_queries, 1);
        self.entered.notify_one();
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        Some(0)
    }
}

#[tokio::test]
async fn sync_admission_keeps_adapter_and_tags_from_routing_generation() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let state = app_state(false);
        let entered = Arc::new(Notify::new());
        let (resume, receiver) = std::sync::mpsc::channel();
        {
            let mut live = state.live.write().await;
            configure_generation(&mut live, "old", 1, 1);
            live.cluster_configs.insert("old-cluster".into(), queryflux_core::config::ClusterConfig {
                engine: Some(queryflux_core::config::EngineConfig::Trino),
                query_auth: Some(queryflux_core::config::QueryAuthConfig::Impersonate),
                ..Default::default()
            });
            live.cluster_manager = Arc::new(SimpleClusterGroupManager::new(HashMap::from([(
                ClusterGroupName("old".into()),
                (
                    vec![Arc::new(ClusterState::new(
                        ClusterName("old-cluster".into()),
                        ClusterGroupName("old".into()),
                        Some(11),
                        Some(12),
                        EngineType::Trino,
                        None,
                        1,
                        true,
                    ))],
                    Arc::new(PausedStrategy {
                        entered: entered.clone(),
                        resume: std::sync::Mutex::new(receiver),
                    }) as Arc<dyn ClusterSelectionStrategy>,
                ),
            )])));
        }
        let live = state.snapshot().await;
        let expected_adapter = match live.adapters.get("old-cluster").unwrap() {
            AdapterKind::Async(adapter) => adapter.clone(),
            _ => unreachable!(),
        };
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            setup_sync_query(
                &request_state,
                &live,
                "SELECT 1".into(),
                vec![],
                SessionContext::default(),
                FrontendProtocol::Mcp,
                ClusterGroupName("old".into()),
                &AuthContext { user: "alice".into(), ..Default::default() },
                &[],
            )
            .await
        });
        entered.notified().await;
        let mut next = state.snapshot().await;
        configure_generation(&mut next, "new", 0, 2);
        *state.live.write().await = next;
        resume.send(()).unwrap();
        let mut setup = request.await.unwrap().unwrap();
        match &setup.adapter {
            DispatchAdapter::Async(adapter) => assert!(Arc::ptr_eq(adapter, &expected_adapter)),
            _ => panic!("expected original adapter"),
        }
        assert!(matches!(&setup.credentials, queryflux_auth::QueryCredentials::Impersonate { user } if user == "alice"));
        assert_eq!(setup.ctx.group.0, "old");
        assert_eq!(setup.ctx.cluster.0, "old-cluster");
        assert_eq!(setup.ctx.cluster_config_id, Some(11));
        assert_eq!(setup.ctx.cluster_group_config_id, Some(12));
        assert_eq!(
            setup.ctx.query_tags.get("generation").and_then(|tag| tag.as_deref()),
            Some("old")
        );
        setup.slot.release().await;
    })
    .await
    .expect("sync admission must finish across reload");
}

#[tokio::test]
async fn completed_submit_releases_the_acquiring_generation_after_reload() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // Both successful and failed terminal responses take the same release path.
        for terminal_state in ["FINISHED", "FAILED"] {
            let state = app_state(false);
            let entered = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let app = axum::Router::new().route("/v1/statement", axum::routing::post({
                let entered = entered.clone();
                let resume = resume.clone();
                move || {
                    let entered = entered.clone();
                    let resume = resume.clone();
                    async move {
                        entered.notify_one();
                        resume.notified().await;
                        axum::Json(serde_json::json!({
                            "id": "completed-on-submit",
                            "infoUri": "http://unused.invalid/query",
                            "stats": { "state": terminal_state, "queued": false, "scheduled": true }
                        }))
                    }
                }
            }));
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let _server_guard = crate::abort::AbortOnDrop::new(server);
            let group = ClusterGroupName("old".into());
            let cluster = ClusterName("old-cluster".into());
            {
                let mut live = state.live.write().await;
                configure_generation(&mut live, "old", 2, 1);
                live.adapters.insert(
                    cluster.0.clone(),
                    AdapterKind::Async(Arc::new(TrinoAdapter::new(
                        cluster.clone(),
                        group.clone(),
                        TrinoConfig {
                            endpoint,
                            tls_skip_verify: false,
                            auth: None,
                        },
                    ))),
                );
            }
            let original_manager = state.snapshot().await.cluster_manager;
            let request = tokio::spawn(submit(state.clone()));
            entered.notified().await;
            assert_eq!(
                original_manager
                    .cluster_state(&group, &cluster)
                    .await
                    .unwrap()
                    .unwrap()
                    .running_queries,
                1
            );

            // Publish a replacement with the same names and an independently occupied slot.
            let mut replacement = state.snapshot().await;
            configure_generation(&mut replacement, "old", 2, 1);
            let replacement_manager = replacement.cluster_manager.clone();
            assert_eq!(
                replacement_manager.acquire_cluster(&group).await.unwrap(),
                Some(cluster.clone())
            );
            *state.live.write().await = replacement;
            resume.notify_one();

            let response = request.await.unwrap();
            assert_eq!(response["stats"]["state"], terminal_state);
            assert_eq!(
                original_manager
                    .cluster_state(&group, &cluster)
                    .await
                    .unwrap()
                    .unwrap()
                    .running_queries,
                0,
                "terminal submit must release the manager that granted the slot"
            );
            assert_eq!(
                replacement_manager
                    .cluster_state(&group, &cluster)
                    .await
                    .unwrap()
                    .unwrap()
                    .running_queries,
                1,
                "terminal submit must not release another generation's slot"
            );
        }
    })
    .await
    .expect("terminal dispatch should finish across reload");
}
