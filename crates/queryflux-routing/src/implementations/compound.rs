use async_trait::async_trait;
use queryflux_auth::AuthContext;
use queryflux_core::{
    config::{CompoundCombineMode, CompoundCondition},
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use regex::Regex;

use crate::{RouterTrait, RoutingDecision};

enum CompiledCondition {
    Protocol(FrontendProtocol),
    Header { name: String, value: String },
    User { username: String },
    ClientTag { tag: String },
    QueryRegex(Regex),
}

fn protocol_from_config(s: &str) -> Option<FrontendProtocol> {
    match s {
        "trinoHttp" => Some(FrontendProtocol::TrinoHttp),
        "postgresWire" => Some(FrontendProtocol::PostgresWire),
        "mysqlWire" => Some(FrontendProtocol::MySqlWire),
        "clickhouseHttp" => Some(FrontendProtocol::ClickHouseHttp),
        "flightSql" => Some(FrontendProtocol::FlightSql),
        "snowflakeHttp" => Some(FrontendProtocol::SnowflakeHttp),
        "snowflakeSqlApi" => Some(FrontendProtocol::SnowflakeSqlApi),
        _ => None,
    }
}

fn header_equals(session: &SessionContext, header_name: &str, expected_value: &str) -> bool {
    let key = header_name.to_lowercase();
    session.extra.get(&key).map(|s| s.as_str()) == Some(expected_value)
}

fn client_tags_contain(session: &SessionContext, tag: &str) -> bool {
    let Some(raw) = session.extra.get("x-trino-client-tags") else {
        return false;
    };
    raw.split(',').map(|t| t.trim()).any(|t| t == tag)
}

/// Routes when a set of conditions holds, combined with AND (`All`) or OR (`Any`).
pub struct CompoundRouter {
    combine: CompoundCombineMode,
    conditions: Vec<CompiledCondition>,
    target: ClusterGroupName,
}

impl CompoundRouter {
    pub fn new(
        combine: CompoundCombineMode,
        conditions: Vec<CompoundCondition>,
        target_group: String,
    ) -> Self {
        let compiled: Vec<CompiledCondition> = conditions
            .into_iter()
            .filter_map(|c| match c {
                CompoundCondition::Protocol { protocol } => {
                    protocol_from_config(&protocol).map(CompiledCondition::Protocol)
                }
                CompoundCondition::Header {
                    header_name,
                    header_value,
                } => Some(CompiledCondition::Header {
                    name: header_name,
                    value: header_value,
                }),
                CompoundCondition::User { username } => Some(CompiledCondition::User { username }),
                CompoundCondition::ClientTag { tag } => Some(CompiledCondition::ClientTag { tag }),
                CompoundCondition::QueryRegex { regex } => match Regex::new(&regex) {
                    Ok(re) => Some(CompiledCondition::QueryRegex(re)),
                    Err(e) => {
                        tracing::warn!("CompoundRouter: skipping invalid regex {:?}: {}", regex, e);
                        None
                    }
                },
            })
            .collect();

        Self {
            combine,
            conditions: compiled,
            target: ClusterGroupName(target_group),
        }
    }

    fn eval_one(
        cond: &CompiledCondition,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> bool {
        match cond {
            CompiledCondition::Protocol(expected) => frontend_protocol == expected,
            CompiledCondition::Header { name, value } => {
                header_equals(session, name, value.as_str())
            }
            CompiledCondition::User { username } => {
                let u = auth_ctx.map(|a| a.user.as_str()).or_else(|| session.user());
                u == Some(username.as_str())
            }
            CompiledCondition::ClientTag { tag } => client_tags_contain(session, tag),
            CompiledCondition::QueryRegex(re) => re.is_match(sql),
        }
    }

    fn matches(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> bool {
        if self.conditions.is_empty() {
            return false;
        }
        match self.combine {
            CompoundCombineMode::All => self
                .conditions
                .iter()
                .all(|c| Self::eval_one(c, sql, session, frontend_protocol, auth_ctx)),
            CompoundCombineMode::Any => self
                .conditions
                .iter()
                .any(|c| Self::eval_one(c, sql, session, frontend_protocol, auth_ctx)),
        }
    }
}

#[async_trait]
impl RouterTrait for CompoundRouter {
    fn type_name(&self) -> &'static str {
        "Compound"
    }

    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        if self.matches(sql, session, frontend_protocol, auth_ctx) {
            Ok(RoutingDecision::Route(self.target.clone()))
        } else {
            Ok(RoutingDecision::NoMatch)
        }
    }
}
