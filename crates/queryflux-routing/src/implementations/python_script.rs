use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use queryflux_auth::AuthContext;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::{RouterTrait, RoutingDecision};

/// Routes queries using a user-supplied Python function.
///
/// The script must define:
/// ```python
/// def route(query: str, ctx: dict) -> str | None:
///     # return a cluster group name, or None to pass to the next router
///     return None
/// ```
///
/// See `docs/routing-and-clusters.md` for the `ctx` schema.
pub struct PythonScriptRouter {
    script: String,
}

impl PythonScriptRouter {
    pub fn new(script: String) -> Self {
        Self { script }
    }

    /// Load from a file path instead of inline script.
    pub fn from_file(path: &str) -> Result<Self> {
        let script = std::fs::read_to_string(path).map_err(|e| {
            QueryFluxError::Routing(format!("Failed to read routing script {path}: {e}"))
        })?;
        Ok(Self::new(script))
    }
}

#[async_trait]
impl RouterTrait for PythonScriptRouter {
    fn type_name(&self) -> &'static str {
        "PythonScript"
    }

    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<RoutingDecision> {
        let sql = sql.to_string();
        let script = self.script.clone();
        let session = session.clone();
        let protocol = frontend_protocol.clone();
        let auth = auth_ctx.cloned();

        tokio::task::spawn_blocking(move || {
            call_python_router(&script, &sql, &session, protocol, auth.as_ref())
        })
        .await
        .map_err(|e| QueryFluxError::Routing(format!("spawn_blocking error: {e}")))?
    }
}

fn protocol_camel(p: FrontendProtocol) -> &'static str {
    match p {
        FrontendProtocol::TrinoHttp => "trinoHttp",
        FrontendProtocol::PostgresWire => "postgresWire",
        FrontendProtocol::MySqlWire => "mysqlWire",
        FrontendProtocol::ClickHouseHttp => "clickHouseHttp",
        FrontendProtocol::FlightSql => "flightSql",
        FrontendProtocol::SnowflakeHttp => "snowflakeHttp",
        FrontendProtocol::SnowflakeSqlApi => "snowflakeSqlApi",
    }
}

fn str_str_dict<'py>(
    py: Python<'py>,
    m: &std::collections::HashMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for (k, v) in m {
        d.set_item(k, v.as_str())?;
    }
    Ok(d)
}

fn string_list<'py>(py: Python<'py>, items: &[String]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for s in items {
        list.append(s.as_str())?;
    }
    Ok(list)
}

fn set_opt_str<'py>(
    ctx: &Bound<'py, PyDict>,
    py: Python<'py>,
    key: &str,
    v: &Option<String>,
) -> PyResult<()> {
    match v {
        Some(s) => ctx.set_item(key, s.as_str()),
        None => ctx.set_item(key, py.None()),
    }
}

fn build_routing_ctx<'py>(
    py: Python<'py>,
    session: &SessionContext,
    protocol: FrontendProtocol,
    auth: Option<&AuthContext>,
) -> PyResult<Bound<'py, PyDict>> {
    let ctx = PyDict::new(py);
    ctx.set_item("protocol", protocol_camel(protocol))?;
    set_opt_str(&ctx, py, "user", &session.user)?;
    set_opt_str(&ctx, py, "database", &session.database)?;
    ctx.set_item("extra", str_str_dict(py, &session.extra)?)?;

    if let Some(a) = auth {
        let ad = PyDict::new(py);
        ad.set_item("user", a.user.as_str())?;
        ad.set_item("groups", string_list(py, &a.groups)?)?;
        ad.set_item("roles", string_list(py, &a.roles)?)?;
        ctx.set_item("auth", ad)?;
    }

    Ok(ctx)
}

fn call_python_router(
    script: &str,
    sql: &str,
    session: &SessionContext,
    protocol: FrontendProtocol,
    auth: Option<&AuthContext>,
) -> Result<RoutingDecision> {
    Python::attach(|py| {
        let globals = PyDict::new(py);
        let cscript = std::ffi::CString::new(script)
            .map_err(|e| QueryFluxError::Routing(format!("Script contains null byte: {e}")))?;
        py.run(&cscript, Some(&globals), None)
            .map_err(|e| QueryFluxError::Routing(format!("Python routing script error: {e}")))?;

        let route_fn = globals
            .get_item("route")
            .map_err(|e| QueryFluxError::Routing(format!("Script has no 'route' function: {e}")))?
            .ok_or_else(|| QueryFluxError::Routing("Script has no 'route' function".to_string()))?;

        let ctx = build_routing_ctx(py, session, protocol, auth).map_err(|e| {
            QueryFluxError::Routing(format!("Failed to build routing ctx for Python: {e}"))
        })?;

        let result = route_fn
            .call1((sql, ctx))
            .map_err(|e| {
                QueryFluxError::Routing(format!(
                    "route(query, ctx) call failed: {e} (expected def route(query: str, ctx: dict) -> str | None)"
                ))
            })?;

        if result.is_none() {
            return Ok(RoutingDecision::NoMatch);
        }

        let group: String = result.extract().map_err(|e| {
            QueryFluxError::Routing(format!("route() must return str or None: {e}"))
        })?;

        Ok(RoutingDecision::Route(ClusterGroupName(group)))
    })
}
