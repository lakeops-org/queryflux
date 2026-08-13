use async_trait::async_trait;
use queryflux_core::{
    config::{QueryRegexRule, RegexRouteAction},
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use regex::Regex;

use crate::{RouterTrait, RoutingDecision};

enum CompiledAction {
    Route(ClusterGroupName),
    Deny(String),
}

/// Routes based on regex patterns matched against the SQL text.
/// Rules are evaluated in order — first match wins (route or deny).
pub struct QueryRegexRouter {
    rules: Vec<(Regex, CompiledAction)>,
}

impl QueryRegexRouter {
    /// Build from a list of (pattern, group) pairs. Skips rules with invalid regex
    /// and logs a warning so a bad pattern doesn't crash startup.
    ///
    /// Prefer [`Self::from_rules`] when deny actions are needed.
    pub fn new(rules: Vec<(String, String)>) -> Self {
        let compiled = rules
            .into_iter()
            .filter_map(|(pattern, group)| match Regex::new(&pattern) {
                Ok(re) => Some((re, CompiledAction::Route(ClusterGroupName(group)))),
                Err(e) => {
                    tracing::warn!(
                        "QueryRegexRouter: skipping invalid regex {:?}: {}",
                        pattern,
                        e
                    );
                    None
                }
            })
            .collect();
        Self { rules: compiled }
    }

    /// Build from full [`QueryRegexRule`] config (supports `action: deny`).
    pub fn from_rules(rules: Vec<QueryRegexRule>) -> Self {
        let compiled = rules
            .into_iter()
            .filter_map(|rule| {
                let re = match Regex::new(&rule.regex) {
                    Ok(re) => re,
                    Err(e) => {
                        tracing::warn!(
                            "QueryRegexRouter: skipping invalid regex {:?}: {}",
                            rule.regex,
                            e
                        );
                        return None;
                    }
                };
                let action = match rule.action {
                    RegexRouteAction::Deny => {
                        let message = rule
                            .error
                            .filter(|s| !s.trim().is_empty())
                            .unwrap_or_else(|| "Query denied by routing rule".to_string());
                        CompiledAction::Deny(message)
                    }
                    RegexRouteAction::Route => {
                        let Some(group) = rule.target_group.filter(|s| !s.is_empty()) else {
                            tracing::warn!(
                                "QueryRegexRouter: skipping route rule with empty targetGroup \
                                 (regex={:?})",
                                rule.regex
                            );
                            return None;
                        };
                        CompiledAction::Route(ClusterGroupName(group))
                    }
                };
                Some((re, action))
            })
            .collect();
        Self { rules: compiled }
    }
}

#[async_trait]
impl RouterTrait for QueryRegexRouter {
    fn type_name(&self) -> &'static str {
        "QueryRegex"
    }

    async fn route(
        &self,
        sql: &str,
        _session: &SessionContext,
        _frontend_protocol: &FrontendProtocol,
        _auth_ctx: Option<&queryflux_auth::AuthContext>,
    ) -> Result<RoutingDecision> {
        for (re, action) in &self.rules {
            if re.is_match(sql) {
                return Ok(match action {
                    CompiledAction::Route(group) => RoutingDecision::Route(group.clone()),
                    CompiledAction::Deny(message) => RoutingDecision::Deny {
                        message: message.clone(),
                    },
                });
            }
        }
        Ok(RoutingDecision::NoMatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::config::QueryRegexRule;

    #[tokio::test]
    async fn deny_action_rejects_matching_sql() {
        let router = QueryRegexRouter::from_rules(vec![QueryRegexRule {
            regex: r"(?i)^\s*(INSERT|UPDATE|DELETE|DROP)".into(),
            target_group: None,
            action: RegexRouteAction::Deny,
            error: Some("Writes are not permitted".into()),
        }]);
        let decision = router
            .route(
                "INSERT INTO t VALUES (1)",
                &Default::default(),
                &FrontendProtocol::TrinoHttp,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            decision,
            RoutingDecision::Deny {
                message: "Writes are not permitted".into()
            }
        );
    }

    #[tokio::test]
    async fn route_action_still_selects_group() {
        let router = QueryRegexRouter::from_rules(vec![QueryRegexRule {
            regex: r"(?i)from\s+prod\.".into(),
            target_group: Some("heavy".into()),
            action: RegexRouteAction::Route,
            error: None,
        }]);
        let decision = router
            .route(
                "SELECT * FROM prod.orders",
                &Default::default(),
                &FrontendProtocol::TrinoHttp,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            decision,
            RoutingDecision::Route(ClusterGroupName("heavy".into()))
        );
    }
}
