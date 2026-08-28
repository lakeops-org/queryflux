use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::tags::QueryTags;

/// Intent classification for an agent query — used for routing hints and observability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryIntent {
    SchemaExploration,
    Aggregation,
    Lookup,
    Mutation,
    Unknown,
}

impl QueryIntent {
    /// Parse a `X-Query-Intent` header value (case-insensitive).
    pub fn from_header(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "schema_exploration" => Self::SchemaExploration,
            "aggregation" => Self::Aggregation,
            "lookup" => Self::Lookup,
            "mutation" => Self::Mutation,
            _ => Self::Unknown,
        }
    }

    /// Lightweight SQL heuristic — used when `X-Query-Intent` header is absent.
    pub fn infer_from_sql(sql: &str) -> Self {
        let upper = sql.to_uppercase();
        let is_select = upper.trim_start().starts_with("SELECT");
        let has_write = upper.contains("INSERT")
            || upper.contains("UPDATE")
            || upper.contains("DELETE")
            || upper.contains("CREATE")
            || upper.contains("DROP")
            || upper.contains("TRUNCATE");
        if has_write {
            return Self::Mutation;
        }
        if !is_select {
            return Self::Unknown;
        }
        let has_agg = upper.contains("COUNT(")
            || upper.contains("SUM(")
            || upper.contains("AVG(")
            || upper.contains("GROUP BY");
        if has_agg {
            return Self::Aggregation;
        }
        let has_where = upper.contains("WHERE");
        let is_star = upper.contains("SELECT *") || upper.contains("SELECT\n*");
        if is_star && !has_where {
            return Self::SchemaExploration;
        }
        if has_where {
            return Self::Lookup;
        }
        Self::Unknown
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SchemaExploration => "schema_exploration",
            Self::Aggregation => "aggregation",
            Self::Lookup => "lookup",
            Self::Mutation => "mutation",
            Self::Unknown => "unknown",
        }
    }
}

/// Agent identity and conversation tracking — attached to a session when the
/// client provides `X-Agent-Id` / `X-Conversation-Id` headers (or MCP context).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContext {
    pub agent_id: String,
    pub conversation_id: String,
    pub step_index: Option<u32>,
    pub tool_call_id: Option<String>,
    pub query_intent: QueryIntent,
}

impl AgentContext {
    /// Parse from a key-value map of HTTP headers or SQL session params.
    ///
    /// Accepts both HTTP header style (`x-agent-id`) and SQL session param style
    /// (`agent_id`) for each field, so wire-protocol clients (MySQL, Postgres) can
    /// pass agentic context via `SET agent_id = '...'` or startup params without
    /// needing HTTP header support. HTTP-style keys take precedence when both are
    /// present.
    ///
    /// Keys must already be normalised to lowercase. Returns `None` when neither
    /// form of `agent_id` or `conversation_id` is present — both are required.
    pub fn from_headers(headers: &HashMap<String, String>) -> Option<Self> {
        let agent_id = headers
            .get("x-agent-id")
            .or_else(|| headers.get("agent_id"))?
            .clone();
        let conversation_id = headers
            .get("x-conversation-id")
            .or_else(|| headers.get("conversation_id"))?
            .clone();
        let step_index = headers
            .get("x-step-index")
            .or_else(|| headers.get("step_index"))
            .and_then(|v| v.parse::<u32>().ok());
        let tool_call_id = headers
            .get("x-tool-call-id")
            .or_else(|| headers.get("tool_call_id"))
            .cloned();
        let query_intent = headers
            .get("x-query-intent")
            .or_else(|| headers.get("query_intent"))
            .map(|v| QueryIntent::from_header(v))
            .unwrap_or(QueryIntent::Unknown);
        Some(AgentContext {
            agent_id,
            conversation_id,
            step_index,
            tool_call_id,
            query_intent,
        })
    }
}

/// Protocol-agnostic session metadata that travels with a query from the frontend.
///
/// Each frontend extracts the common fields (`user`, `database`, `tags`) at connection
/// time and places all remaining protocol-specific key-value data into `extra`.
/// Key conventions for `extra`:
/// - Trino / ClickHouse HTTP frontends: HTTP header names (lowercase) → values
/// - Postgres wire: startup parameter names → values
/// - MySQL wire: session variable names → values
/// - Snowflake HTTP: session variable names → values
///
/// Used by:
/// - Routers: inspect `user`, `database`, `tags`, or raw `extra` entries
/// - CatalogProvider: extract catalog/database hints for schema lookup
/// - Engine adapters: forward relevant auth/session info to the backend
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionContext {
    /// Resolved user identity (extracted at connection time by the frontend).
    pub user: Option<String>,
    /// Target database/schema hint for routing and catalog providers. For
    /// protocols with a real catalog layer (Trino, Snowflake), this is the
    /// *schema* — the catalog goes in `catalog` below. For protocols without one
    /// (Postgres, MySQL/StarRocks), this is simply the selected database.
    pub database: Option<String>,
    /// Target catalog hint, for protocols that have a separate catalog concept
    /// from database/schema (Trino's `X-Trino-Catalog`, Snowflake's database).
    /// `None` for protocols with no catalog layer — never assume it falls back
    /// to `database`, the two are genuinely different things per-protocol.
    #[serde(default)]
    pub catalog: Option<String>,
    /// Query tags for routing and metrics.
    pub tags: QueryTags,
    /// Protocol-specific key-value data. Frontends own the key conventions.
    pub extra: HashMap<String, String>,
    /// Agent identity and conversation tracking. Present when the client sends
    /// `X-Agent-Id` + `X-Conversation-Id` headers (or MCP context).
    pub agent_context: Option<AgentContext>,
}

impl SessionContext {
    /// Return the pre-extracted query tags for this session.
    pub fn tags(&self) -> &QueryTags {
        &self.tags
    }

    /// Extract the user identity.
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Extract the target database/schema hint (see field doc comment for what
    /// this means per-protocol — it is *not* the catalog for Trino/Snowflake).
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Extract the target catalog hint. `None` for protocols without a catalog
    /// layer (Postgres, MySQL/StarRocks) — never falls back to `database`.
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    /// Resolve agent identity for this session.
    ///
    /// Returns `session.agent_context` if explicitly set (e.g. by the MCP frontend),
    /// otherwise parses from `session.extra` (which all HTTP frontends populate with
    /// lowercased request headers). This means any frontend that stores headers in
    /// `extra` automatically supports agent headers without per-frontend code.
    pub fn resolved_agent_context(&self) -> Option<AgentContext> {
        self.agent_context
            .clone()
            .or_else(|| AgentContext::from_headers(&self.extra))
    }

    /// Extract the client source/application name (used for routing and metrics).
    ///
    /// Checks `extra` for well-known keys in precedence order:
    /// `x-trino-source` (Trino HTTP), `application_name` (Postgres/MySQL/Snowflake),
    /// `client_name` (ClickHouse).
    pub fn client_source(&self) -> Option<&str> {
        self.extra
            .get("x-trino-source")
            .or_else(|| self.extra.get("application_name"))
            .or_else(|| self.extra.get("client_name"))
            .map(|s| s.as_str())
    }

    /// Drop hop-by-hop credentials so they are not persisted on queued/executing rows.
    ///
    /// Client `Authorization` must not be stored in Postgres JSON or replayed onto
    /// a later poller's backend request.
    pub fn without_auth_headers(mut self) -> Self {
        self.extra.remove("authorization");
        self.extra.remove("proxy-authorization");
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(
        user: Option<&str>,
        database: Option<&str>,
        extra: &[(&str, &str)],
    ) -> SessionContext {
        SessionContext {
            user: user.map(str::to_string),
            database: database.map(str::to_string),
            extra: extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn default_is_empty() {
        let s = SessionContext::default();
        assert_eq!(s.user(), None);
        assert_eq!(s.database(), None);
        assert_eq!(s.catalog(), None);
        assert!(s.tags().is_empty());
        assert!(s.extra.is_empty());
        assert_eq!(s.client_source(), None);
    }

    #[test]
    fn catalog_and_database_are_independent() {
        let s = SessionContext {
            catalog: Some("hive".to_string()),
            database: Some("analytics".to_string()),
            ..Default::default()
        };
        assert_eq!(s.catalog(), Some("hive"));
        assert_eq!(s.database(), Some("analytics"));
    }

    #[test]
    fn deserializes_json_missing_catalog_field() {
        // Backward compat: rows persisted before `catalog` existed (e.g. queued
        // queries stored as JSON in Postgres) must still deserialize.
        let json = r#"{"user":"alice","database":"hive","tags":{},"extra":{}}"#;
        let s: SessionContext = serde_json::from_str(json).unwrap();
        assert_eq!(s.database(), Some("hive"));
        assert_eq!(s.catalog(), None);
    }

    #[test]
    fn user_and_database_accessors() {
        let s = session(Some("alice"), Some("tpch"), &[]);
        assert_eq!(s.user(), Some("alice"));
        assert_eq!(s.database(), Some("tpch"));
    }

    #[test]
    fn client_source_trino_header_wins() {
        // x-trino-source takes precedence over application_name and client_name.
        let s = session(
            None,
            None,
            &[
                ("x-trino-source", "my-app"),
                ("application_name", "other"),
                ("client_name", "also-other"),
            ],
        );
        assert_eq!(s.client_source(), Some("my-app"));
    }

    #[test]
    fn client_source_application_name_fallback() {
        let s = session(None, None, &[("application_name", "dbt")]);
        assert_eq!(s.client_source(), Some("dbt"));
    }

    #[test]
    fn client_source_client_name_fallback() {
        let s = session(None, None, &[("client_name", "clickhouse-client")]);
        assert_eq!(s.client_source(), Some("clickhouse-client"));
    }

    #[test]
    fn client_source_none_when_no_known_key() {
        let s = session(None, None, &[("x-custom-header", "value")]);
        assert_eq!(s.client_source(), None);
    }

    #[test]
    fn extra_lookup_is_case_sensitive() {
        // Frontends are responsible for normalising keys (e.g. lowercase headers).
        // SessionContext itself does no case folding.
        let s = session(None, None, &[("X-Trino-Source", "app")]);
        assert_eq!(s.client_source(), None); // uppercase key is not matched
    }

    #[test]
    fn without_auth_headers_strips_credentials_keeps_session() {
        let s = session(
            Some("alice"),
            Some("tpch"),
            &[
                ("authorization", "Bearer stolen"),
                ("proxy-authorization", "Basic abc"),
                ("x-trino-catalog", "hive"),
            ],
        )
        .without_auth_headers();
        assert!(!s.extra.contains_key("authorization"));
        assert!(!s.extra.contains_key("proxy-authorization"));
        assert_eq!(
            s.extra.get("x-trino-catalog").map(String::as_str),
            Some("hive")
        );
        assert_eq!(s.user(), Some("alice"));
    }

    #[test]
    fn resolved_agent_context_from_extra() {
        let s = session(
            None,
            None,
            &[
                ("x-agent-id", "agent-123"),
                ("x-conversation-id", "conv-456"),
                ("x-step-index", "2"),
                ("x-query-intent", "aggregation"),
            ],
        );
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "agent-123");
        assert_eq!(ctx.conversation_id, "conv-456");
        assert_eq!(ctx.step_index, Some(2));
        assert_eq!(ctx.query_intent, QueryIntent::Aggregation);
    }

    #[test]
    fn resolved_agent_context_requires_both_ids() {
        // Missing x-conversation-id → None
        let s = session(None, None, &[("x-agent-id", "agent-123")]);
        assert!(s.resolved_agent_context().is_none());
    }

    #[test]
    fn resolved_agent_context_explicit_wins_over_extra() {
        // Explicitly set agent_context takes priority over extra headers.
        let explicit = AgentContext {
            agent_id: "explicit".to_string(),
            conversation_id: "conv-explicit".to_string(),
            step_index: None,
            tool_call_id: None,
            query_intent: QueryIntent::Lookup,
        };
        let mut s = session(
            None,
            None,
            &[
                ("x-agent-id", "from-header"),
                ("x-conversation-id", "conv-from-header"),
            ],
        );
        s.agent_context = Some(explicit);
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "explicit");
    }

    #[test]
    fn resolved_agent_context_from_sql_style_keys() {
        // SQL session params use snake_case without the x- prefix.
        let s = session(
            None,
            None,
            &[
                ("agent_id", "agent-sql"),
                ("conversation_id", "conv-sql"),
                ("step_index", "3"),
                ("tool_call_id", "call_abc"),
                ("query_intent", "lookup"),
            ],
        );
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "agent-sql");
        assert_eq!(ctx.conversation_id, "conv-sql");
        assert_eq!(ctx.step_index, Some(3));
        assert_eq!(ctx.tool_call_id.as_deref(), Some("call_abc"));
        assert_eq!(ctx.query_intent, QueryIntent::Lookup);
    }

    #[test]
    fn resolved_agent_context_http_style_wins_over_sql_style() {
        // When both forms are present, x- prefixed (HTTP) keys take precedence.
        let s = session(
            None,
            None,
            &[
                ("x-agent-id", "http-agent"),
                ("agent_id", "sql-agent"),
                ("x-conversation-id", "http-conv"),
                ("conversation_id", "sql-conv"),
            ],
        );
        let ctx = s.resolved_agent_context().expect("should resolve");
        assert_eq!(ctx.agent_id, "http-agent");
        assert_eq!(ctx.conversation_id, "http-conv");
    }

    #[test]
    fn resolved_agent_context_sql_style_requires_both_ids() {
        let s = session(None, None, &[("agent_id", "agent-sql")]);
        assert!(s.resolved_agent_context().is_none());
    }
}
