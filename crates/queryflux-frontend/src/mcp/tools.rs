use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use queryflux_auth::{require_query_owner, AuthContext, Credentials};
use queryflux_core::{
    query::{ClusterGroupName, FrontendProtocol, ProxyQueryId},
    session::{AgentContext, SessionContext},
};
use queryflux_routing::ChainRouteResult;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock, ServerCapabilities, ServerInfo},
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler,
};
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::{
    admin::{cancel_executing_query, delete_queued_if_exists, find_executing_query},
    dispatch::execute_to_sink,
    mcp::sink::JsonResultSink,
    state::AppState,
};

const DEFAULT_MAX_ROWS: usize = 1000;
const ABSOLUTE_MAX_ROWS: usize = 100_000;

// ---------------------------------------------------------------------------
// Tool parameter schemas
// ---------------------------------------------------------------------------

/// Optional agent-identity fields shared by every tool. Explicit "handle as argument"
/// path per MCP's own guidance for cross-call state (the newer stateless spec revision
/// deprecates relying on transport session state for this) — works with any MCP client,
/// including ones that don't support setting custom headers per call. When the caller
/// *does* send `X-Agent-Id` / `X-Conversation-Id` / etc. headers, those take precedence
/// over these fields (see `resolve_agent_context`).
#[derive(Debug, Default, Deserialize, schemars::JsonSchema)]
struct AgentContextParams {
    /// Stable identifier for the calling agent (e.g. "data-analyst-v2").
    agent_id: Option<String>,
    /// Groups queries from one agent session/reasoning chain together.
    conversation_id: Option<String>,
    /// Position of this query within the agent's reasoning chain.
    step_index: Option<u32>,
    /// The LLM framework's tool-call id that triggered this request.
    tool_call_id: Option<String>,
    /// Purpose hint: schema_exploration | aggregation | lookup | mutation | unknown.
    /// Inferred from the SQL when omitted.
    query_intent: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ExecuteQueryParams {
    /// SQL to execute.
    sql: String,
    /// Preferred cluster group name. Falls back to normal routing when omitted.
    engine_hint: Option<String>,
    /// Maximum rows to return (default 1000). Bounds the JSON response size; not a
    /// safety mechanism — configure `row_limit` / `read_only` guards for that.
    max_rows: Option<usize>,
    #[serde(flatten)]
    agent: AgentContextParams,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListSchemasParams {
    /// Preferred cluster group name. Falls back to normal routing when omitted.
    engine_hint: Option<String>,
    #[serde(flatten)]
    agent: AgentContextParams,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DescribeTableParams {
    /// Schema/database name, if the engine requires qualification.
    schema: Option<String>,
    /// Table name (schema-qualified if `schema` is omitted and qualification is needed).
    table: String,
    /// Number of sample rows to include alongside the column metadata (default 0 = none).
    sample_rows: Option<usize>,
    /// Preferred cluster group name. Falls back to normal routing when omitted.
    engine_hint: Option<String>,
    #[serde(flatten)]
    agent: AgentContextParams,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ExplainQueryParams {
    /// SQL to explain (not executed).
    sql: String,
    /// Preferred cluster group name. Falls back to normal routing when omitted.
    engine_hint: Option<String>,
    #[serde(flatten)]
    agent: AgentContextParams,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct QueryIdParams {
    /// The query id returned in an `execute_query` result or `list_engines`-style lookup.
    query_id: String,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Extract lowercased HTTP headers from the underlying request, when running over
/// streamable HTTP. Empty for transports that don't carry an HTTP request (none in
/// this build — MCP is HTTP-only here — but this stays defensive rather than panicking).
fn extract_headers(ctx: &RequestContext<RoleServer>) -> HashMap<String, String> {
    ctx.extensions
        .get::<axum::http::request::Parts>()
        .map(|parts| {
            parts
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_ascii_lowercase(),
                        v.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Authenticate the calling MCP client from `Authorization: Bearer <token>`.
async fn authenticate(
    state: &AppState,
    headers: &HashMap<String, String>,
) -> Result<AuthContext, McpError> {
    let token = headers
        .get("authorization")
        .and_then(|v| crate::strip_bearer_prefix(v))
        .map(|t| t.to_string());
    let creds = Credentials {
        username: None,
        password: None,
        bearer_token: token,
    };
    let auth_provider = state.live.read().await.auth_provider.clone();
    auth_provider
        .authenticate(&creds)
        .await
        .map_err(|e| McpError::invalid_request(e.to_string(), None))
}

/// Merge explicit tool-parameter agent fields with any threaded HTTP headers into an
/// `AgentContext`. Headers win on conflict — reuses `AgentContext::from_headers` exactly
/// as every other frontend does, fed from a map with both sources' keys present.
fn resolve_agent_context(
    headers: &HashMap<String, String>,
    params: &AgentContextParams,
) -> Option<AgentContext> {
    let mut merged = HashMap::new();
    if let Some(v) = &params.agent_id {
        merged.insert("agent_id".to_string(), v.clone());
    }
    if let Some(v) = &params.conversation_id {
        merged.insert("conversation_id".to_string(), v.clone());
    }
    if let Some(v) = &params.step_index {
        merged.insert("step_index".to_string(), v.to_string());
    }
    if let Some(v) = &params.tool_call_id {
        merged.insert("tool_call_id".to_string(), v.clone());
    }
    if let Some(v) = &params.query_intent {
        merged.insert("query_intent".to_string(), v.clone());
    }
    // Headers override same-named tool params (x-agent-id beats agent_id, etc.) because
    // AgentContext::from_headers checks the x-prefixed key first.
    merged.extend(headers.iter().map(|(k, v)| (k.clone(), v.clone())));
    AgentContext::from_headers(&merged)
}

/// `agent_context` must already be the fully-resolved value from `resolve_agent_context`
/// (headers-vs-params precedence already applied) — setting it here makes
/// `SessionContext::resolved_agent_context()` return it verbatim, since that method
/// prefers an explicit `agent_context` over re-parsing `extra`. MCP never populates
/// `extra`, so that fallback path is unused; do not rely on it for MCP.
fn session_for(auth: &AuthContext, agent_context: Option<AgentContext>) -> SessionContext {
    SessionContext {
        user: Some(auth.user.clone()),
        agent_context,
        ..Default::default()
    }
}

/// Resolve the target cluster group: use `engine_hint` if it names a real group,
/// otherwise route normally through the configured `RouterChain`.
async fn resolve_group(
    state: &AppState,
    sql: &str,
    session: &SessionContext,
    engine_hint: Option<&str>,
    auth: &AuthContext,
) -> Result<ClusterGroupName, McpError> {
    if let Some(hint) = engine_hint {
        let live = state.live.read().await;
        if live.group_members.contains_key(hint) {
            return Ok(ClusterGroupName(hint.to_string()));
        }
        return Err(McpError::invalid_params(
            format!("Unknown engine group: {hint}"),
            None,
        ));
    }

    let decision = {
        let live = state.live.read().await;
        live.router_chain
            .route(sql, session, &FrontendProtocol::Mcp, Some(auth))
            .await
    }
    .map_err(|e| McpError::internal_error(e.to_string(), None))?;

    match decision {
        ChainRouteResult::Routed(group) => Ok(group),
        ChainRouteResult::Denied { message } => Err(McpError::invalid_request(message, None)),
    }
}

/// Run `sql` through the standard dispatch pipeline (routing, translation, guardrails,
/// execution, persistence) and collect the result as JSON. Every tool goes through this
/// single path so query history, agent-context, and guard enforcement are identical to
/// every other QueryFlux frontend.
async fn run_query(
    state: &Arc<AppState>,
    sql: String,
    session: SessionContext,
    group: ClusterGroupName,
    auth: &AuthContext,
    max_rows: usize,
) -> Result<CallToolResult, McpError> {
    let engine_name = group.0.clone();
    let capped = max_rows.min(ABSOLUTE_MAX_ROWS);
    let mut sink = JsonResultSink::new(capped);
    let start = Instant::now();

    execute_to_sink(
        state,
        sql,
        vec![],
        session,
        FrontendProtocol::Mcp,
        group,
        &mut sink,
        auth,
    )
    .await
    .map_err(|e| McpError::internal_error(e.to_string(), None))?;

    if let Some(err) = &sink.error {
        return Ok(CallToolResult::error(vec![ContentBlock::text(err.clone())]));
    }

    let elapsed = start.elapsed().as_millis() as u64;
    let result = sink.into_result(elapsed, &engine_name);
    Ok(CallToolResult::success(vec![ContentBlock::text(
        serde_json::to_string_pretty(&result).unwrap_or_default(),
    )]))
}

/// Quote a single identifier ANSI-style (double quotes, embedded `"` doubled). Used by
/// `qualify_table` so a `table`/`schema` argument containing a space, reserved word, or
/// `;` stays a single identifier rather than producing unintended SQL — the caller can
/// already run arbitrary SQL via `execute_query`, so this isn't a privilege boundary,
/// just correctness. QueryFlux's translation layer (sqlglot) re-quotes per target engine
/// dialect (backticks for MySQL-family, etc.), so ANSI double-quoting on the way in is
/// the right canonical form regardless of the final backend.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn qualify_table(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) if !s.is_empty() => format!("{}.{}", quote_ident(s), quote_ident(table)),
        _ => quote_ident(table),
    }
}

// ---------------------------------------------------------------------------
// MCP Server — tool handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct QueryFluxMcpServer {
    state: Arc<AppState>,
}

impl QueryFluxMcpServer {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tool_router]
impl QueryFluxMcpServer {
    #[tool(
        description = "Execute a SQL query against a QueryFlux-routed engine. Returns rows as JSON objects keyed by column name, truncated at max_rows (default 1000). Row-level safety policy (read-only, row limits, etc.) is whatever the operator has configured via QueryFlux guardrails — not enforced here."
    )]
    async fn execute_query(
        &self,
        Parameters(ExecuteQueryParams {
            sql,
            engine_hint,
            max_rows,
            agent,
        }): Parameters<ExecuteQueryParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent);
        let session = session_for(&auth, agent_context);
        let group =
            resolve_group(&self.state, &sql, &session, engine_hint.as_deref(), &auth).await?;

        debug!(sql = %sql, group = %group, "MCP execute_query");
        run_query(
            &self.state,
            sql,
            session,
            group,
            &auth,
            max_rows.unwrap_or(DEFAULT_MAX_ROWS),
        )
        .await
    }

    #[tool(
        description = "List available schemas/databases on the routed engine. Queries information_schema.schemata through the normal QueryFlux pipeline — the SQL-standard view every supported engine implements, so this works whether or not dialect translation rewrites it further."
    )]
    async fn list_schemas(
        &self,
        Parameters(ListSchemasParams { engine_hint, agent }): Parameters<ListSchemasParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent);
        let session = session_for(&auth, agent_context);
        let sql = "SELECT schema_name FROM information_schema.schemata".to_string();
        let group =
            resolve_group(&self.state, &sql, &session, engine_hint.as_deref(), &auth).await?;

        run_query(&self.state, sql, session, group, &auth, DEFAULT_MAX_ROWS).await
    }

    #[tool(
        description = "Describe a table's columns (name, type, nullable) and optionally include sample rows in the same call. Runs DESCRIBE <table>, plus a bounded SELECT * ... LIMIT <sample_rows> when sample_rows > 0."
    )]
    async fn describe_table(
        &self,
        Parameters(DescribeTableParams {
            schema,
            table,
            sample_rows,
            engine_hint,
            agent,
        }): Parameters<DescribeTableParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent);
        let qualified = qualify_table(schema.as_deref(), &table);

        let describe_session = session_for(&auth, agent_context.clone());
        let describe_sql = format!("DESCRIBE {qualified}");
        let group = resolve_group(
            &self.state,
            &describe_sql,
            &describe_session,
            engine_hint.as_deref(),
            &auth,
        )
        .await?;

        let engine_name = group.0.clone();
        let mut describe_sink = JsonResultSink::new(1_000);
        let describe_start = Instant::now();
        execute_to_sink(
            &self.state,
            describe_sql,
            vec![],
            describe_session,
            FrontendProtocol::Mcp,
            group.clone(),
            &mut describe_sink,
            &auth,
        )
        .await
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if let Some(err) = &describe_sink.error {
            return Ok(CallToolResult::error(vec![ContentBlock::text(err.clone())]));
        }
        let columns =
            describe_sink.into_result(describe_start.elapsed().as_millis() as u64, &engine_name);

        let sample_rows = sample_rows.unwrap_or(0);
        let sample = if sample_rows > 0 {
            let sample_session = session_for(&auth, agent_context);
            let sample_sql = format!("SELECT * FROM {qualified} LIMIT {sample_rows}");
            let mut sample_sink = JsonResultSink::new(sample_rows);
            let sample_start = Instant::now();
            execute_to_sink(
                &self.state,
                sample_sql,
                vec![],
                sample_session,
                FrontendProtocol::Mcp,
                group,
                &mut sample_sink,
                &auth,
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            if let Some(err) = &sample_sink.error {
                Some(json!({ "error": err }))
            } else {
                Some(
                    sample_sink
                        .into_result(sample_start.elapsed().as_millis() as u64, &engine_name),
                )
            }
        } else {
            None
        };

        let result = json!({
            "table": qualified,
            "columns": columns,
            "sample": sample,
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Return the query plan for a SQL statement without executing it. Runs EXPLAIN <sql> on the routed engine."
    )]
    async fn explain_query(
        &self,
        Parameters(ExplainQueryParams {
            sql,
            engine_hint,
            agent,
        }): Parameters<ExplainQueryParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent);
        let session = session_for(&auth, agent_context);
        let explain_sql = format!("EXPLAIN {sql}");
        let group = resolve_group(
            &self.state,
            &explain_sql,
            &session,
            engine_hint.as_deref(),
            &auth,
        )
        .await?;

        run_query(
            &self.state,
            explain_sql,
            session,
            group,
            &auth,
            DEFAULT_MAX_ROWS,
        )
        .await
    }

    #[tool(
        description = "Check the status of a query submitted via execute_query. Only reports on queries still running or queued — QueryFlux does not currently retain a lookup-by-id history of completed queries, so a finished query reports as not_found_or_completed."
    )]
    async fn get_query_status(
        &self,
        Parameters(QueryIdParams { query_id }): Parameters<QueryIdParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;

        if let Some(executing) = find_executing_query(self.state.persistence.as_ref(), &query_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?
        {
            require_query_owner(&auth, &executing.submitted_by)
                .map_err(|e| McpError::invalid_request(e.to_string(), None))?;
            let result = json!({
                "query_id": query_id,
                "status": "running",
                "cluster_group": executing.cluster_group.0,
                "cluster": executing.cluster_name.0,
                "sql_preview": executing.sql.chars().take(200).collect::<String>(),
            });
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::to_string_pretty(&result).unwrap_or_default(),
            )]));
        }

        if let Some(queued) = self
            .state
            .persistence
            .get_queued(&ProxyQueryId(query_id.clone()))
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?
        {
            require_query_owner(&auth, &queued.submitted_by)
                .map_err(|e| McpError::invalid_request(e.to_string(), None))?;
            let result = json!({
                "query_id": query_id,
                "status": "queued",
                "cluster_group": queued.cluster_group.0,
                "sql_preview": queued.sql.chars().take(200).collect::<String>(),
            });
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::to_string_pretty(&result).unwrap_or_default(),
            )]));
        }

        let result = json!({
            "query_id": query_id,
            "status": "not_found_or_completed",
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Cancel a query submitted via execute_query, whether it is still queued or already running on a backend. Only the agent/user that submitted the query may cancel it."
    )]
    async fn cancel_query(
        &self,
        Parameters(QueryIdParams { query_id }): Parameters<QueryIdParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;

        if let Some(executing) = find_executing_query(self.state.persistence.as_ref(), &query_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?
        {
            require_query_owner(&auth, &executing.submitted_by)
                .map_err(|e| McpError::invalid_request(e.to_string(), None))?;
            cancel_executing_query(
                &self.state,
                FrontendProtocol::Mcp,
                &executing,
                "mcp cancelled",
            )
            .await
            .map_err(|e| McpError::internal_error(e, None))?;
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                json!({ "query_id": query_id, "status": "cancelled" }).to_string(),
            )]));
        }

        if let Some(queued) = delete_queued_if_exists(self.state.persistence.as_ref(), &query_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?
        {
            require_query_owner(&auth, &queued.submitted_by)
                .map_err(|e| McpError::invalid_request(e.to_string(), None))?;
            self.state.record_queued_terminal(
                &queued,
                queryflux_core::query::QueryStatus::Cancelled,
                "mcp cancelled",
            );
            // A worker may have claimed and started executing between the two lookups.
            if let Some(executing) =
                find_executing_query(self.state.persistence.as_ref(), &query_id)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?
            {
                require_query_owner(&auth, &executing.submitted_by)
                    .map_err(|e| McpError::invalid_request(e.to_string(), None))?;
                cancel_executing_query(
                    &self.state,
                    FrontendProtocol::Mcp,
                    &executing,
                    "mcp cancelled",
                )
                .await
                .map_err(|e| McpError::internal_error(e, None))?;
            }
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                json!({ "query_id": query_id, "status": "cancelled" }).to_string(),
            )]));
        }

        Err(McpError::invalid_params(
            format!("query not found: {query_id}"),
            None,
        ))
    }
}

#[tool_handler]
impl ServerHandler for QueryFluxMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "QueryFlux MCP server. Tools: execute_query, list_schemas, describe_table, \
             explain_query, get_query_status, cancel_query. Every tool authenticates via \
             Authorization: Bearer <token> and flows through QueryFlux's normal routing, \
             translation, and guardrail pipeline — no separate MCP-specific policy layer.",
        )
    }
}
