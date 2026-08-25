//! E2e tests for the MCP (Model Context Protocol) frontend.
//!
//! Uses an in-process `ProtocolWireHarness` backed by DuckDB (no docker) and a real
//! `rmcp` streamable-HTTP client — not hand-rolled JSON-RPC — so these tests exercise
//! the actual MCP wire protocol, not just the tool-handler functions directly.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test mcp_tests`

use std::collections::HashMap;
use std::sync::Arc;

use queryflux_e2e_tests::harness::ProtocolWireHarness;
use queryflux_guardrails::{
    built_in::{Guard, ReadOnlyGuard},
    GuardChain,
};
use rmcp::{
    model::{CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation},
    transport::{
        streamable_http_client::StreamableHttpClientTransportConfig, StreamableHttpClientTransport,
    },
    ServiceExt,
};

fn mcp_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}/mcp")
}

async fn connect(
    port: u16,
) -> rmcp::service::RunningService<rmcp::RoleClient, rmcp::model::InitializeRequestParams> {
    let transport = StreamableHttpClientTransport::from_uri(mcp_url(port));
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("queryflux-e2e-tests", "0.0.1"),
    );
    client_info
        .serve(transport)
        .await
        .expect("connect to MCP server")
}

fn call(name: &'static str, args: serde_json::Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name).with_arguments(args.as_object().cloned().unwrap_or_default())
}

fn result_text(result: &rmcp::model::CallToolResult) -> String {
    let v = serde_json::to_value(result).expect("serialize CallToolResult");
    v["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn result_json(result: &rmcp::model::CallToolResult) -> serde_json::Value {
    serde_json::from_str(&result_text(result)).unwrap_or(serde_json::Value::Null)
}

fn is_error(result: &rmcp::model::CallToolResult) -> bool {
    result.is_error.unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Tool happy paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_query_select_one() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1 AS n" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let payload = result_json(&result);
    assert_eq!(payload["rows"][0]["n"], serde_json::json!(1));
    assert_eq!(payload["truncated"], serde_json::json!(false));

    let _ = client.cancel().await;
}

#[tokio::test]
async fn execute_query_respects_max_rows() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let sql = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(n)";
    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": sql, "max_rows": 2 }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let payload = result_json(&result);
    assert_eq!(payload["row_count"], serde_json::json!(2));
    assert_eq!(payload["truncated"], serde_json::json!(true));

    let _ = client.cancel().await;
}

#[tokio::test]
async fn execute_query_rejects_unknown_dialect() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    // An unknown `dialect` value must fail loudly (protocol-level error) rather than
    // being passed through to sqlglot unvalidated.
    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1", "dialect": "not-a-real-dialect" }),
        ))
        .await;
    assert!(
        result.is_err(),
        "expected an unknown dialect to be rejected before execution"
    );

    let _ = client.cancel().await;
}

#[tokio::test]
async fn execute_query_accepts_an_explicit_known_dialect() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    // The harness only routes to DuckDB, so this doesn't exercise a real cross-dialect
    // rewrite — dispatch::resolve_src_dialect_tests covers that logic directly. This
    // proves the MCP-level plumbing (param -> validation -> session.extra -> dispatch)
    // works end-to-end over the real wire protocol without breaking execution.
    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1 AS n", "dialect": "duckdb" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let payload = result_json(&result);
    assert_eq!(payload["rows"][0]["n"], serde_json::json!(1));

    let _ = client.cancel().await;
}

#[tokio::test]
async fn list_schemas_returns_schema_names() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call("list_schemas", serde_json::json!({})))
        .await
        .expect("call_tool list_schemas");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");
    let payload = result_json(&result);
    assert!(payload["columns"].is_array());

    let _ = client.cancel().await;
}

#[tokio::test]
async fn describe_table_with_sample_rows() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    // A table DuckDB always has: information_schema.tables.
    let result = client
        .call_tool(call(
            "describe_table",
            serde_json::json!({
                "schema": "information_schema",
                "table": "tables",
                "sample_rows": 1,
            }),
        ))
        .await
        .expect("call_tool describe_table");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let payload = result_json(&result);
    assert!(payload["columns"]["columns"].is_array());
    assert!(!payload["sample"].is_null());

    let _ = client.cancel().await;
}

#[tokio::test]
async fn explain_query_returns_plan() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "explain_query",
            serde_json::json!({ "sql": "SELECT 1" }),
        ))
        .await
        .expect("call_tool explain_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let _ = client.cancel().await;
}

#[tokio::test]
async fn get_query_status_reports_not_found_for_unknown_id() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "get_query_status",
            serde_json::json!({ "query_id": "does-not-exist" }),
        ))
        .await
        .expect("call_tool get_query_status");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");
    let payload = result_json(&result);
    assert_eq!(
        payload["status"],
        serde_json::json!("not_found_or_completed")
    );

    let _ = client.cancel().await;
}

#[tokio::test]
async fn cancel_query_rejects_unknown_id() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "cancel_query",
            serde_json::json!({ "query_id": "does-not-exist" }),
        ))
        .await;
    // Unknown id is an MCP-level `invalid_params` error (Err), not a tool-level CallToolResult error.
    assert!(
        result.is_err(),
        "expected cancel_query to error on unknown id"
    );

    let _ = client.cancel().await;
}

// ---------------------------------------------------------------------------
// Guardrails — proves MCP flows through the exact same GuardChain as every other
// frontend, using ordinary user-configured guards. No MCP-specific guard code exists;
// this test would fail if that stopped being true.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_query_denied_by_read_only_guard() {
    let guard_chain = Arc::new(GuardChain::new(vec![
        Box::new(ReadOnlyGuard) as Box<dyn Guard>
    ]));
    let h = ProtocolWireHarness::new_with_guard_chain(Some(guard_chain))
        .await
        .expect("harness");
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "DELETE FROM information_schema.tables" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(is_error(&result), "expected read_only guard to deny DELETE");
    assert!(
        result_text(&result)
            .to_lowercase()
            .contains("not permitted"),
        "expected a read_only-style denial reason, got: {}",
        result_text(&result)
    );

    // A normal SELECT through the same guard chain is unaffected.
    let ok = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&ok), "read_only guard should not block SELECT");

    let _ = client.cancel().await;
}

// ---------------------------------------------------------------------------
// Agent context — proves both propagation paths (HTTP headers and explicit tool
// parameters) land in the persisted QueryRecord, mirroring session_context_tests.rs's
// coverage for the other wire protocols.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agent_context_via_tool_params() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    h.clear_records();
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({
                "sql": "SELECT 1",
                "agent_id": "agent-params",
                "conversation_id": "conv-params",
                "step_index": 3,
                "query_intent": "lookup",
            }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let record = h
        .wait_for_record(|r| r.agent_id.as_deref() == Some("agent-params"))
        .await
        .expect("expected a QueryRecord with agent_id=agent-params");
    assert_eq!(record.conversation_id.as_deref(), Some("conv-params"));
    assert_eq!(record.step_index, Some(3));
    assert_eq!(record.query_intent.as_deref(), Some("lookup"));

    let _ = client.cancel().await;
}

#[tokio::test]
async fn agent_context_via_headers_wins_over_params() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    h.clear_records();

    let mut headers = HashMap::new();
    headers.insert(
        reqwest::header::HeaderName::from_static("x-agent-id"),
        reqwest::header::HeaderValue::from_static("agent-header"),
    );
    headers.insert(
        reqwest::header::HeaderName::from_static("x-conversation-id"),
        reqwest::header::HeaderValue::from_static("conv-header"),
    );
    let config =
        StreamableHttpClientTransportConfig::with_uri(mcp_url(h.mcp_port)).custom_headers(headers);
    let transport = StreamableHttpClientTransport::from_config(config);
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("queryflux-e2e-tests", "0.0.1"),
    );
    let client = client_info.serve(transport).await.expect("connect");

    // agent_id supplied both via header (agent-header) and tool param (agent-param) —
    // the header must win, per the documented precedence.
    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({
                "sql": "SELECT 1",
                "agent_id": "agent-param",
                "conversation_id": "conv-param",
            }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let record = h
        .wait_for_record(|r| r.agent_id.is_some())
        .await
        .expect("expected a QueryRecord with agent context");
    assert_eq!(record.agent_id.as_deref(), Some("agent-header"));
    assert_eq!(record.conversation_id.as_deref(), Some("conv-header"));

    let _ = client.cancel().await;
}

/// When the caller supplies no agent params/headers at all, MCP must not drop agent
/// context entirely (that would make the call invisible on the Agents page) — it
/// defaults `agent_id` from the authenticated identity and `conversation_id` from the
/// transport's `Mcp-Session-Id`, which the `rmcp` client transparently attaches after
/// `initialize`. Two calls on the same client/session should therefore share one
/// `conversation_id`, proving the session-based grouping actually works.
#[tokio::test]
async fn agent_context_defaults_when_absent() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    h.clear_records();
    let client = connect(h.mcp_port).await;

    let result = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result), "unexpected tool error: {result:?}");

    let record = h
        .wait_for_record(|r| r.agent_id.is_some())
        .await
        .expect("expected a QueryRecord with a defaulted agent_id");
    // The harness wires a `NoneAuthProvider(required = false)`, and MCP's `authenticate`
    // never supplies a username, so the authenticated identity is deterministically
    // "anonymous" here.
    assert_eq!(record.agent_id.as_deref(), Some("anonymous"));
    let conversation_id = record
        .conversation_id
        .clone()
        .expect("conversation_id should default to the Mcp-Session-Id");

    let result2 = client
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 2" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result2), "unexpected tool error: {result2:?}");

    let record2 = h
        .wait_for_record(|r| r.sql_preview.contains("SELECT 2"))
        .await
        .expect("expected a QueryRecord for the second call");
    assert_eq!(
        record2.conversation_id.as_deref(),
        Some(conversation_id.as_str()),
        "calls on the same MCP session should share the session-derived conversation_id"
    );

    let _ = client.cancel().await;
}

/// Proves the actual "copy it forward" mechanism, not just implicit session-based
/// grouping: extract `conversation_id` from a tool's JSON response (what an agent would
/// read from its own context) and pass that exact value as the explicit `conversation_id`
/// argument on a *new*, otherwise-unrelated MCP connection — the resulting query must
/// still land in the same conversation. This is the behavior the response hint exists to
/// make reliable even when a client doesn't preserve any transport-level session across
/// calls (e.g. reconnects between tool calls).
#[tokio::test]
async fn execute_query_response_conversation_id_can_be_reused_on_a_new_connection() {
    let h = ProtocolWireHarness::new().await.expect("harness");
    h.clear_records();

    let client1 = connect(h.mcp_port).await;
    let result1 = client1
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 1" }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result1), "unexpected tool error: {result1:?}");

    let payload1 = result_json(&result1);
    let conversation_id = payload1["conversation_id"]
        .as_str()
        .expect("response should include conversation_id")
        .to_string();
    assert!(
        !conversation_id.is_empty(),
        "conversation_id should not be empty"
    );
    assert!(
        payload1["_hint"].is_string(),
        "response should include a hint telling the caller to reuse conversation_id"
    );
    let _ = client1.cancel().await;

    // A brand new connection — no shared Mcp-Session-Id with the first one — reuses the
    // conversation_id it read out of the first call's response.
    let client2 = connect(h.mcp_port).await;
    let result2 = client2
        .call_tool(call(
            "execute_query",
            serde_json::json!({ "sql": "SELECT 2", "conversation_id": conversation_id }),
        ))
        .await
        .expect("call_tool execute_query");
    assert!(!is_error(&result2), "unexpected tool error: {result2:?}");

    let record2 = h
        .wait_for_record(|r| r.sql_preview.contains("SELECT 2"))
        .await
        .expect("expected a QueryRecord for the second call");
    assert_eq!(
        record2.conversation_id.as_deref(),
        Some(conversation_id.as_str()),
        "the id copied from the first response should group the second call with the first"
    );

    let _ = client2.cancel().await;
}
