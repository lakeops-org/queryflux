use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::{RouterTrait, RoutingDecision};

/// Routes based on which frontend protocol the client used.
/// Useful for directing MySQL-wire clients (StarRocks) to a different group
/// than Trino HTTP clients, or Flight SQL clients to a dedicated group.
pub struct ProtocolBasedRouter {
    pub trino_http: Option<ClusterGroupName>,
    pub postgres_wire: Option<ClusterGroupName>,
    pub mysql_wire: Option<ClusterGroupName>,
    pub clickhouse_http: Option<ClusterGroupName>,
    pub flight_sql: Option<ClusterGroupName>,
    pub snowflake_http: Option<ClusterGroupName>,
    pub snowflake_sql_api: Option<ClusterGroupName>,
}

#[async_trait]
impl RouterTrait for ProtocolBasedRouter {
    fn type_name(&self) -> &'static str {
        "ProtocolBased"
    }

    async fn route(
        &self,
        _sql: &str,
        _session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        _auth_ctx: Option<&queryflux_auth::AuthContext>,
    ) -> Result<RoutingDecision> {
        let group = match frontend_protocol {
            FrontendProtocol::TrinoHttp => self.trino_http.clone(),
            FrontendProtocol::PostgresWire => self.postgres_wire.clone(),
            FrontendProtocol::MySqlWire => self.mysql_wire.clone(),
            FrontendProtocol::ClickHouseHttp => self.clickhouse_http.clone(),
            FrontendProtocol::FlightSql => self.flight_sql.clone(),
            FrontendProtocol::SnowflakeHttp => self.snowflake_http.clone(),
            FrontendProtocol::SnowflakeSqlApi => self.snowflake_sql_api.clone(),
        };
        Ok(group.into())
    }
}
