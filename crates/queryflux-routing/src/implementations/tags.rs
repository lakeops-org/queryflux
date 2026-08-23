use async_trait::async_trait;
use queryflux_core::{
    config::TagRoutingRule,
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
    tags::QueryTags,
};

use crate::{RouterTrait, RoutingDecision};

/// Routes queries based on tag key+value matching.
///
/// Rules are evaluated in order; the first rule where **all** specified tags match wins.
///
/// Matching semantics per tag entry in a rule:
/// - Config value `None`  → key must be present in session tags; any value (or no value) accepted.
/// - Config value `Some(v)` → key must be present and session tag value must equal `v`.
///
/// Works across all frontend protocols — tags are read from the normalized
/// [`SessionContext::tags()`] method, not from protocol-specific headers.
pub struct TagsRouter {
    rules: Vec<TagRoutingRule>,
}

impl TagsRouter {
    pub fn new(rules: Vec<TagRoutingRule>) -> Self {
        Self { rules }
    }
}

#[async_trait]
impl RouterTrait for TagsRouter {
    fn type_name(&self) -> &'static str {
        "Tags"
    }

    async fn route(
        &self,
        _sql: &str,
        session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
        _auth_ctx: Option<&queryflux_auth::AuthContext>,
    ) -> Result<RoutingDecision> {
        let session_tags = session.tags();
        if session_tags.is_empty() {
            return Ok(RoutingDecision::NoMatch);
        }

        for rule in &self.rules {
            if rule_matches(&rule.tags, session_tags) {
                return Ok(RoutingDecision::Route(ClusterGroupName(
                    rule.target_group.clone(),
                )));
            }
        }

        Ok(RoutingDecision::NoMatch)
    }
}

/// Returns true when all config tags in `rule_tags` match `session_tags`.
fn rule_matches(
    rule_tags: &std::collections::HashMap<String, Option<String>>,
    session_tags: &QueryTags,
) -> bool {
    rule_tags.iter().all(|(key, expected_val)| {
        match session_tags.get(key) {
            None => false, // key not present — no match
            Some(actual_val) => match expected_val {
                None => true, // config says key-only — any value accepted
                Some(expected) => actual_val.as_deref() == Some(expected.as_str()),
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use queryflux_core::{
        config::TagRoutingRule, query::FrontendProtocol, session::SessionContext,
    };

    use super::TagsRouter;
    use crate::{RouterTrait, RoutingDecision};

    fn group(name: &str) -> queryflux_core::query::ClusterGroupName {
        queryflux_core::query::ClusterGroupName(name.to_string())
    }

    fn mysql_session_with_tags(raw: &str) -> SessionContext {
        let (tags, _) = queryflux_core::tags::parse_query_tags(raw);
        SessionContext {
            tags,
            ..Default::default()
        }
    }

    fn trino_session_with_tags(tags_header: &str) -> SessionContext {
        let tags = tags_header
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(|t| (t.to_string(), None))
            .collect();
        SessionContext {
            extra: HashMap::from([("x-trino-client-tags".to_string(), tags_header.to_string())]),
            tags,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn single_kv_match() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
            target_group: "engineering".to_string(),
        }]);
        let session = mysql_session_with_tags("team:eng,cost_center:701");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("engineering")).into());
    }

    #[tokio::test]
    async fn kv_no_match_wrong_value() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
            target_group: "engineering".to_string(),
        }]);
        let session = mysql_session_with_tags("team:analytics");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, RoutingDecision::NoMatch);
    }

    #[tokio::test]
    async fn key_only_matches_any_value() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("batch".to_string(), None)]),
            target_group: "batch-cluster".to_string(),
        }]);
        let session = mysql_session_with_tags("batch:nightly");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("batch-cluster")).into());
    }

    #[tokio::test]
    async fn key_only_matches_key_only_tag() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("batch".to_string(), None)]),
            target_group: "batch-cluster".to_string(),
        }]);
        let session = mysql_session_with_tags("batch,team:eng");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("batch-cluster")).into());
    }

    #[tokio::test]
    async fn and_logic_all_must_match() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([
                ("team".to_string(), Some("eng".to_string())),
                ("env".to_string(), Some("prod".to_string())),
            ]),
            target_group: "prod-eng".to_string(),
        }]);
        // Only one tag matches — should not route.
        let session = mysql_session_with_tags("team:eng,env:staging");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, RoutingDecision::NoMatch);
    }

    #[tokio::test]
    async fn first_rule_wins() {
        let router = TagsRouter::new(vec![
            TagRoutingRule {
                tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
                target_group: "first".to_string(),
            },
            TagRoutingRule {
                tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
                target_group: "second".to_string(),
            },
        ]);
        let session = mysql_session_with_tags("team:eng");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("first")).into());
    }

    #[tokio::test]
    async fn trino_client_tags_key_only_match() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("reporting".to_string(), None)]),
            target_group: "analytics".to_string(),
        }]);
        let session = trino_session_with_tags("reporting,batch");
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("analytics")).into());
    }

    #[tokio::test]
    async fn no_tags_returns_none() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
            target_group: "engineering".to_string(),
        }]);
        let session = SessionContext::default();
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, RoutingDecision::NoMatch);
    }

    #[tokio::test]
    async fn json_format_tags() {
        let router = TagsRouter::new(vec![TagRoutingRule {
            tags: HashMap::from([("team".to_string(), Some("eng".to_string()))]),
            target_group: "engineering".to_string(),
        }]);
        let session = mysql_session_with_tags(r#"{"team":"eng","env":"prod"}"#);
        let result = router
            .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
            .await
            .unwrap();
        assert_eq!(result, Some(group("engineering")).into());
    }
}
