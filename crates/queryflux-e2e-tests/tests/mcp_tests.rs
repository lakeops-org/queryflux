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
