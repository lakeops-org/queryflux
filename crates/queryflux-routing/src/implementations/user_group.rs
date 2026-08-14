use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::{RouterTrait, RoutingDecision};

/// Routes by authenticated username → cluster group.
///
/// Prefers `auth_ctx.user` (verified identity). Falls back to `session.user()`
/// only when auth context is unavailable (e.g. auth disabled).
pub struct UserGroupRouter {
    /// username → cluster group name
    mapping: HashMap<String, ClusterGroupName>,
}

impl UserGroupRouter {
    pub fn new(mapping: HashMap<String, ClusterGroupName>) -> Self {
        Self { mapping }
    }
}

#[async_trait]
impl RouterTrait for UserGroupRouter {
    fn type_name(&self) -> &'static str {
        "UserGroup"
    }

    async fn route(
        &self,
        _sql: &str,
        session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&queryflux_auth::AuthContext>,
    ) -> Result<RoutingDecision> {
        let user = auth_ctx
            .map(|ctx| ctx.user.as_str())
            .or_else(|| session.user());
        if let Some(user) = user {
            return Ok(self.mapping.get(user).cloned().into());
        }
        Ok(RoutingDecision::NoMatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_auth::AuthContext;
    use queryflux_core::query::FrontendProtocol;
    use queryflux_core::session::SessionContext;

    fn mapping() -> HashMap<String, ClusterGroupName> {
        HashMap::from([
            ("alice".into(), ClusterGroupName("prod".into())),
            ("bob".into(), ClusterGroupName("dev".into())),
        ])
    }

    #[tokio::test]
    async fn prefers_auth_context_user() {
        let router = UserGroupRouter::new(mapping());
        let session = SessionContext {
            user: Some("bob".into()),
            ..Default::default()
        };
        let auth = AuthContext {
            user: "alice".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
        };
        let group = router
            .route(
                "SELECT 1",
                &session,
                &FrontendProtocol::TrinoHttp,
                Some(&auth),
            )
            .await
            .unwrap();
        assert_eq!(group, Some(ClusterGroupName("prod".into())).into());
    }

    #[tokio::test]
    async fn falls_back_to_session_user() {
        let router = UserGroupRouter::new(mapping());
        let session = SessionContext {
            user: Some("bob".into()),
            ..Default::default()
        };
        let group = router
            .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
            .await
            .unwrap();
        assert_eq!(group, Some(ClusterGroupName("dev".into())).into());
    }

    #[tokio::test]
    async fn unmatched_user_returns_none() {
        let router = UserGroupRouter::new(mapping());
        let auth = AuthContext {
            user: "carol".into(),
            groups: vec![],
            roles: vec![],
            raw_token: None,
        };
        let group = router
            .route(
                "SELECT 1",
                &SessionContext::default(),
                &FrontendProtocol::TrinoHttp,
                Some(&auth),
            )
            .await
            .unwrap();
        assert_eq!(group, RoutingDecision::NoMatch);
    }
}
