use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use queryflux_auth::{require_query_owner, AuthContext, Credentials};
use queryflux_core::{
    query::{ClusterGroupName, FrontendProtocol, ProxyQueryId, SqlDialect},
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
use serde_json::{json, Value};
use tracing::debug;
use uuid::Uuid;

use crate::{
    admin::{cancel_executing_query, delete_queued_if_exists, find_executing_query},
    dispatch::execute_to_sink,
    hook::{HookBus, HookContext},
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
    /// The SQL dialect `sql` is written in, if it differs from the target engine's own
    /// dialect. MCP has no wire protocol to infer this from (unlike QueryFlux's other
    /// frontends), so by default no translation is attempted at all — sqlglot is never
    /// invoked, and the SQL is sent to the target engine exactly as written. This is a
    /// deliberate no-op, not an assumption that the SQL already matches the target
    /// engine. Set this when the SQL was written for a *different* engine and should be
    /// translated before routing. One of: trino, athena, duckdb, starrocks, clickhouse,
    /// mysql, postgres, sqlite, snowflake, bigquery, databricks, tsql, redshift, exasol,
    /// generic.
    dialect: Option<String>,
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
    /// The SQL dialect `sql` is written in, if it differs from the target engine's own
    /// dialect. Defaults to skipping translation entirely (sqlglot is never invoked) —
    /// see `execute_query`'s `dialect` parameter for the full explanation. One of: trino,
    /// athena, duckdb, starrocks, clickhouse, mysql, postgres, sqlite, snowflake,
    /// bigquery, databricks, tsql, redshift, exasol, generic.
    dialect: Option<String>,
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

/// Validate a caller-supplied `dialect` tool parameter against `SqlDialect::KNOWN_DIALECT_NAMES`
/// and resolve it to the exact string `dispatch::resolve_src_dialect` will hand to sqlglot.
/// Rejects unknown names outright (an `invalid_params` error listing the valid set) rather
/// than passing them through as `SqlDialect::Sqlglot(name)` unvalidated — a typo should fail
/// loudly, not silently be interpreted by sqlglot as some other dialect or fail deep inside
/// translation with a less useful error.
fn resolve_dialect_override(dialect: Option<&str>) -> Result<Option<String>, McpError> {
    let Some(name) = dialect else {
        return Ok(None);
    };
    match SqlDialect::from_known_name(name) {
        Some(parsed) => Ok(Some(parsed.sqlglot_write_name())),
        None => Err(McpError::invalid_params(
            format!(
                "Unknown dialect: {name:?}. Supported: {}",
                SqlDialect::KNOWN_DIALECT_NAMES.join(", ")
            ),
            None,
        )),
    }
}

/// Merge explicit tool-parameter agent fields with any threaded HTTP headers into an
/// `AgentContext`. Headers win on conflict — reuses `AgentContext::from_headers` exactly
/// as every other frontend does, fed from a map with both sources' keys present.
///
/// MCP is inherently agent-facing traffic, but LLM callers inconsistently fill in the
/// free-text `agent_id`/`conversation_id` tool params (or omit them), which would
/// otherwise make the call invisible on the Agents page (it's keyed off
/// `conversation_id`, and `AgentContext::from_headers` requires both fields or neither).
/// So unlike every other frontend, MCP never leaves these unset: `agent_id` defaults to
/// the authenticated identity (`auth.user`, never empty — see `AuthContext::user` docs),
/// and `conversation_id` defaults to the transport's `Mcp-Session-Id` header when present
/// (groups every call within one MCP session together), falling back to a fresh UUID per
/// call when no session id is available (e.g. a stateless transport). Explicit
/// params/headers still win over both defaults.
fn resolve_agent_context(
    headers: &HashMap<String, String>,
    params: &AgentContextParams,
    auth: &AuthContext,
) -> AgentContext {
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

    merged
        .entry("agent_id".to_string())
        .or_insert_with(|| auth.user.clone());
    merged
        .entry("conversation_id".to_string())
        .or_insert_with(|| {
            headers
                .get("mcp-session-id")
                .cloned()
                .unwrap_or_else(|| format!("mcp-{}", Uuid::new_v4()))
        });

    AgentContext::from_headers(&merged)
        .expect("agent_id and conversation_id are populated by the defaults above")
}

/// `agent_context` must already be the fully-resolved value from `resolve_agent_context`
/// (headers-vs-params precedence and defaults already applied) — setting it here makes
/// `SessionContext::resolved_agent_context()` return it verbatim, since that method
/// prefers an explicit `agent_context` over re-parsing `extra`. MCP never populates
/// `extra`, so that fallback path is unused; do not rely on it for MCP.
fn session_for(auth: &AuthContext, agent_context: AgentContext) -> SessionContext {
    SessionContext {
        user: Some(auth.user.clone()),
        agent_context: Some(agent_context),
        ..Default::default()
    }
}

/// Resolve the target cluster group: use `engine_hint` if it names a real group,
/// otherwise route normally through `AppState::route_query` (running before_route /
/// after_route hooks and picking up a `RoutingTrace` like every other frontend).
///
/// Returns `sql` back alongside the group since `before_route` may rewrite it — the
/// `engine_hint` shortcut skips routing (and hooks) entirely, so it returns `sql`
/// unmodified.
async fn resolve_group(
    state: &AppState,
    sql: String,
    session: &SessionContext,
    engine_hint: Option<&str>,
    auth: &AuthContext,
) -> Result<(ClusterGroupName, String), McpError> {
    if let Some(hint) = engine_hint {
        // An explicit engine_hint skips RouterChain evaluation (the caller already
        // named the group), but must not skip before_route/after_route hooks —
        // every other frontend, and MCP calls without a hint, always run them.
        let protocol = FrontendProtocol::Mcp;
        let mut sql = sql;
        if !state.hooks.is_empty() {
            let mut ctx = HookContext {
                sql: sql.clone(),
                session,
                protocol: &protocol,
                group: None,
                cluster: None,
                engine_type: None,
                query_tags: session.tags(),
                auth: Some(auth),
                rows: None,
                execution_ms: None,
            };
            let outcome = state.hooks.before_route(&mut ctx).await;
            sql = ctx.sql;
            if let Some(err) = HookBus::deny_err(&outcome) {
                return Err(McpError::invalid_request(err.to_string(), None));
            }
        }

        if !state.live.read().await.group_members.contains_key(hint) {
            return Err(McpError::invalid_params(
                format!("Unknown engine group: {hint}"),
                None,
            ));
        }
        let group = ClusterGroupName(hint.to_string());

        if !state.hooks.is_empty() {
            let ctx = HookContext {
                sql: sql.clone(),
                session,
                protocol: &protocol,
                group: Some(&group),
                cluster: None,
                engine_type: None,
                query_tags: session.tags(),
                auth: Some(auth),
                rows: None,
                execution_ms: None,
            };
            let outcome = state.hooks.after_route(&ctx).await;
            if let Some(err) = HookBus::deny_err(&outcome) {
                return Err(McpError::invalid_request(err.to_string(), None));
            }
        }

        return Ok((group, sql));
    }

    let (sql, decision, _trace) = state
        .route_query(sql, session, &FrontendProtocol::Mcp, Some(auth))
        .await
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;

    match decision {
        ChainRouteResult::Routed(group) => Ok((group, sql)),
        ChainRouteResult::Denied { message } => Err(McpError::invalid_request(message, None)),
    }
}

/// Insert the resolved `conversation_id` into a tool's JSON result, with a short note
/// telling the caller to reuse it on later calls in this conversation.
///
/// MCP has no reliable transport-level way to correlate tool calls within one chat (see
/// `resolve_agent_context`'s doc comment on why we default `conversation_id` at all), and
/// asking the agent to *invent and remember* an id itself is unreliable — it can forget
/// to generate one, or drift onto a different value on a later call. Echoing back
/// whatever value QueryFlux actually resolved (explicit param, header, or the session/
/// UUID default) gives the agent a concrete value already in its own context to copy
/// forward, rather than something to recall from scratch — the same "mint a handle and
/// have the model pass it back" pattern MCP itself recommends for cross-call state.
fn attach_conversation_hint(result: &mut Value, conversation_id: &str) {
    if let Value::Object(map) = result {
        map.insert("conversation_id".to_string(), json!(conversation_id));
        map.insert(
            "_hint".to_string(),
            json!(
                "Pass this exact conversation_id on your next QueryFlux tool call in this \
                 conversation to keep them grouped together."
            ),
        );
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
    conversation_id: &str,
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
    let mut result = sink.into_result(elapsed, &engine_name);
    attach_conversation_hint(&mut result, conversation_id);
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
        description = "Execute a SQL query against a QueryFlux-routed engine. Returns rows as JSON objects keyed by column name, truncated at max_rows (default 1000). Row-level safety policy (read-only, row limits, etc.) is whatever the operator has configured via QueryFlux guardrails — not enforced here. By default no dialect translation is attempted at all (sqlglot is never invoked); set `dialect` if the SQL was written for a different engine than the one it's routed to. The response includes `conversation_id` — copy that exact value into the `conversation_id` argument on your next QueryFlux tool call in this same conversation, so they're grouped together."
    )]
    async fn execute_query(
        &self,
        Parameters(ExecuteQueryParams {
            sql,
            engine_hint,
            max_rows,
            dialect,
            agent,
        }): Parameters<ExecuteQueryParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let dialect = resolve_dialect_override(dialect.as_deref())?;
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent, &auth);
        let conversation_id = agent_context.conversation_id.clone();
        let mut session = session_for(&auth, agent_context);
        if let Some(d) = dialect {
            session.extra.insert("dialect".to_string(), d);
        }
        let (group, sql) =
            resolve_group(&self.state, sql, &session, engine_hint.as_deref(), &auth).await?;

        debug!(sql = %sql, group = %group, "MCP execute_query");
        run_query(
            &self.state,
            sql,
            session,
            group,
            &auth,
            max_rows.unwrap_or(DEFAULT_MAX_ROWS),
            &conversation_id,
        )
        .await
    }

    #[tool(
        description = "List available schemas/databases on the routed engine. Queries information_schema.schemata through the normal QueryFlux pipeline — the SQL-standard view every supported engine implements, so this works whether or not dialect translation rewrites it further. The response includes `conversation_id` — reuse it on your next QueryFlux tool call in this conversation."
    )]
    async fn list_schemas(
        &self,
        Parameters(ListSchemasParams { engine_hint, agent }): Parameters<ListSchemasParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent, &auth);
        let conversation_id = agent_context.conversation_id.clone();
        let session = session_for(&auth, agent_context);
        let sql = "SELECT schema_name FROM information_schema.schemata".to_string();
        let (group, sql) =
            resolve_group(&self.state, sql, &session, engine_hint.as_deref(), &auth).await?;

        run_query(
            &self.state,
            sql,
            session,
            group,
            &auth,
            DEFAULT_MAX_ROWS,
            &conversation_id,
        )
        .await
    }

    #[tool(
        description = "Describe a table's columns (name, type, nullable) and optionally include sample rows in the same call. Runs DESCRIBE <table>, plus a bounded SELECT * ... LIMIT <sample_rows> when sample_rows > 0. The response includes `conversation_id` — reuse it on your next QueryFlux tool call in this conversation."
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
        let agent_context = resolve_agent_context(&headers, &agent, &auth);
        let conversation_id = agent_context.conversation_id.clone();
        let qualified = qualify_table(schema.as_deref(), &table);

        let describe_session = session_for(&auth, agent_context.clone());
        let describe_sql = format!("DESCRIBE {qualified}");
        let (group, describe_sql) = resolve_group(
            &self.state,
            describe_sql,
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

        let mut result = json!({
            "table": qualified,
            "columns": columns,
            "sample": sample,
        });
        attach_conversation_hint(&mut result, &conversation_id);
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Return the query plan for a SQL statement without executing it. Runs EXPLAIN <sql> on the routed engine. By default no dialect translation is attempted at all (sqlglot is never invoked); set `dialect` if the SQL was written for a different engine than the one it's routed to. The response includes `conversation_id` — reuse it on your next QueryFlux tool call in this conversation."
    )]
    async fn explain_query(
        &self,
        Parameters(ExplainQueryParams {
            sql,
            engine_hint,
            dialect,
            agent,
        }): Parameters<ExplainQueryParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let dialect = resolve_dialect_override(dialect.as_deref())?;
        let headers = extract_headers(&ctx);
        let auth = authenticate(&self.state, &headers).await?;
        let agent_context = resolve_agent_context(&headers, &agent, &auth);
        let conversation_id = agent_context.conversation_id.clone();
        let mut session = session_for(&auth, agent_context);
        if let Some(d) = dialect {
            session.extra.insert("dialect".to_string(), d);
        }
        let explain_sql = format!("EXPLAIN {sql}");
        let (group, explain_sql) = resolve_group(
            &self.state,
            explain_sql,
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
            &conversation_id,
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
             translation, and guardrail pipeline — no separate MCP-specific policy layer. \
             execute_query/list_schemas/describe_table/explain_query responses include a \
             conversation_id field: copy that exact value into the conversation_id argument \
             on every later call in this same conversation, so QueryFlux groups them together.",
        )
    }
}

#[cfg(test)]
mod resolve_dialect_override_tests {
    use super::resolve_dialect_override;

    #[test]
    fn none_stays_none() {
        assert_eq!(resolve_dialect_override(None).unwrap(), None);
    }

    #[test]
    fn known_name_resolves_to_its_sqlglot_write_name() {
        assert_eq!(
            resolve_dialect_override(Some("postgres")).unwrap(),
            Some("postgres".to_string())
        );
        assert_eq!(
            resolve_dialect_override(Some("BigQuery")).unwrap(),
            Some("bigquery".to_string())
        );
    }

    #[test]
    fn alias_resolves_to_the_canonical_sqlglot_write_name() {
        // "postgresql" is an alias for Postgres, but sqlglot's own write name is "postgres".
        assert_eq!(
            resolve_dialect_override(Some("postgresql")).unwrap(),
            Some("postgres".to_string())
        );
    }

    #[test]
    fn generic_resolves_to_empty_string_matching_sqlglots_base_dialect() {
        assert_eq!(
            resolve_dialect_override(Some("generic")).unwrap(),
            Some(String::new())
        );
    }

    #[test]
    fn unknown_name_is_rejected_with_the_known_list_in_the_error() {
        let err = resolve_dialect_override(Some("not-a-real-dialect")).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("not-a-real-dialect"));
        assert!(msg.contains("trino"));
    }
}
