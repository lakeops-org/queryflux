use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    built_in::Guard,
    config::FailBehavior,
    context::{GuardContext, GuardLayer, GuardResult},
};

const DEFAULT_TIMEOUT_MS: u64 = 1_000;
const MAX_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub struct PythonScriptGuard {
    pub script: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct HttpWebhookGuard {
    pub url: String,
    pub timeout_ms: Option<u64>,
    pub retry_count: u32,
    pub fail_behavior: FailBehavior,
    pub headers: HashMap<String, String>,
    pub client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct MisconfiguredGuard {
    pub guard_name: &'static str,
    pub reason: String,
}

#[async_trait]
impl Guard for MisconfiguredGuard {
    fn name(&self) -> &'static str {
        self.guard_name
    }

    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }

    async fn check(&self, _ctx: &GuardContext<'_>) -> GuardResult {
        GuardResult::deny(self.reason.clone(), "GUARD_CONFIG_ERROR")
    }
}

#[async_trait]
impl Guard for PythonScriptGuard {
    fn name(&self) -> &'static str {
        "python_script"
    }

    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }

    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        let script = self.script.clone();
        let payload = guard_payload(ctx);
        let timeout = bounded_timeout(self.timeout_ms);

        let handle = tokio::task::spawn_blocking(move || run_python_guard(&script, payload));
        let abort_handle = handle.abort_handle();

        match tokio::time::timeout(timeout, handle).await {
            Ok(Ok(Ok(guard_result))) => guard_result,
            Ok(Ok(Err(e))) => {
                GuardResult::deny(format!("python guard failed: {e}"), "PYTHON_GUARD_ERROR")
            }
            Ok(Err(e)) => GuardResult::deny(
                format!("python guard task failed: {e}"),
                "PYTHON_GUARD_ERROR",
            ),
            Err(_elapsed) => {
                // Best-effort: abort the blocking task so it is dropped at the next
                // yield/poll boundary. Cannot interrupt native/FFI code mid-flight;
                // for truly untrusted scripts a subprocess boundary is needed.
                abort_handle.abort();
                GuardResult::deny(
                    format!("python guard timed out after {}ms", timeout.as_millis()),
                    "PYTHON_GUARD_TIMEOUT",
                )
            }
        }
    }
}

#[async_trait]
impl Guard for HttpWebhookGuard {
    fn name(&self) -> &'static str {
        "http_webhook"
    }

    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }

    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        let timeout = bounded_timeout(self.timeout_ms);
        let payload = guard_payload(ctx);
        let attempts = self.retry_count.saturating_add(1);
        let mut last_error = String::new();

        for attempt in 0..attempts {
            let mut req = self.client.post(&self.url).timeout(timeout).json(&payload);
            for (name, value) in &self.headers {
                req = req.header(name, value);
            }

            match req.send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<GuardResponse>().await {
                        Ok(verdict) => return verdict.into_result(),
                        Err(e) => {
                            last_error = format!("invalid webhook response: {e}");
                            break;
                        }
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    last_error = format!("webhook returned status {status}");
                    if !status.is_server_error() || attempt + 1 >= attempts {
                        break;
                    }
                }
                Err(e) => {
                    last_error = e.to_string();
                    if attempt + 1 >= attempts {
                        break;
                    }
                }
            }
        }

        self.fail_result(format!(
            "http webhook guard failed after {attempts} attempt(s): {last_error}"
        ))
    }
}

impl HttpWebhookGuard {
    fn fail_result(&self, reason: String) -> GuardResult {
        match self.fail_behavior {
            FailBehavior::Allow => {
                let mut metadata = HashMap::new();
                metadata.insert("webhook_failure".to_string(), reason);
                GuardResult::Allow {
                    metadata: Some(metadata),
                }
            }
            FailBehavior::Deny => GuardResult::deny(reason, "HTTP_WEBHOOK_GUARD_ERROR"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct GuardResponse {
    action: GuardAction,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum GuardAction {
    Allow,
    Warn,
    Deny,
}

impl GuardResponse {
    fn into_result(self) -> GuardResult {
        match self.action {
            GuardAction::Allow => GuardResult::Allow {
                metadata: self.metadata,
            },
            GuardAction::Warn => {
                GuardResult::warn(self.reason.unwrap_or_else(|| "guard warning".to_string()))
            }
            GuardAction::Deny => GuardResult::Deny {
                reason: self
                    .reason
                    .unwrap_or_else(|| "query blocked by external guard".to_string()),
                code: self.code,
            },
        }
    }
}

fn bounded_timeout(timeout_ms: Option<u64>) -> Duration {
    Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS))
}

fn guard_payload(ctx: &GuardContext<'_>) -> serde_json::Value {
    json!({
        "sql": ctx.sql,
        "translated_sql": ctx.translated_sql,
        "engine_type": format!("{:?}", ctx.engine_type),
        "cluster_group": ctx.cluster_group.0,
        "user": ctx.user,
        "agent_context": ctx.agent_context,
        "query_tags": ctx.query_tags,
    })
}

fn run_python_guard(script: &str, payload: serde_json::Value) -> Result<GuardResult, String> {
    Python::attach(|py| {
        let globals = PyDict::new(py);
        let script = std::ffi::CString::new(script)
            .map_err(|e| format!("script contains null byte: {e}"))?;
        py.run(script.as_c_str(), Some(&globals), None)
            .map_err(|e| format!("script error: {e}"))?;

        let check_fn = globals
            .get_item("check")
            .map_err(|e| format!("script has no 'check' function: {e}"))?
            .ok_or_else(|| "script has no 'check' function".to_string())?;

        let json_mod = PyModule::import(py, "json").map_err(|e| format!("import json: {e}"))?;
        let payload_str =
            serde_json::to_string(&payload).map_err(|e| format!("serialize context: {e}"))?;
        let ctx = json_mod
            .getattr("loads")
            .and_then(|loads| loads.call1((payload_str,)))
            .map_err(|e| format!("build Python context: {e}"))?;

        let result = check_fn
            .call1((ctx,))
            .map_err(|e| format!("check(ctx) call failed: {e}"))?;
        if result.is_none() {
            return Ok(GuardResult::allow());
        }

        let result_json: String = json_mod
            .getattr("dumps")
            .and_then(|dumps| dumps.call1((result,)))
            .and_then(|v| v.extract())
            .map_err(|e| format!("serialize check(ctx) result: {e}"))?;
        let response: GuardResponse = serde_json::from_str(&result_json)
            .map_err(|e| format!("invalid check(ctx) result: {e}"))?;
        Ok(response.into_result())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::{
        query::{ClusterGroupName, EngineType},
        tags::QueryTags,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    struct TestCtx {
        sql: String,
        translated_sql: String,
        engine_type: EngineType,
        cluster_group: ClusterGroupName,
        query_tags: QueryTags,
    }

    impl TestCtx {
        fn new(sql: &str) -> Self {
            Self {
                sql: sql.to_string(),
                translated_sql: sql.to_string(),
                engine_type: EngineType::DuckDb,
                cluster_group: ClusterGroupName("default".to_string()),
                query_tags: QueryTags::new(),
            }
        }

        fn ctx(&self) -> GuardContext<'_> {
            GuardContext {
                sql: &self.sql,
                translated_sql: &self.translated_sql,
                engine_type: &self.engine_type,
                cluster_group: &self.cluster_group,
                user: Some("alice"),
                agent_context: None,
                query_tags: &self.query_tags,
            }
        }
    }

    #[tokio::test]
    async fn python_script_guard_allows() {
        let guard = PythonScriptGuard {
            script: "def check(ctx):\n    return {'action': 'allow', 'metadata': {'user': ctx['user']}}\n".to_string(),
            timeout_ms: Some(500),
        };
        let tc = TestCtx::new("SELECT 1");
        let result = guard.check(&tc.ctx()).await;
        assert!(!result.is_deny());
    }

    #[tokio::test]
    async fn python_script_guard_denies() {
        let guard = PythonScriptGuard {
            script: "def check(ctx):\n    return {'action': 'deny', 'reason': 'blocked', 'code': 'BLOCKED'}\n".to_string(),
            timeout_ms: Some(500),
        };
        let tc = TestCtx::new("SELECT 1");
        let result = guard.check(&tc.ctx()).await;
        assert!(matches!(
            result,
            GuardResult::Deny {
                reason,
                code: Some(code)
            } if reason == "blocked" && code == "BLOCKED"
        ));
    }

    #[tokio::test]
    async fn http_webhook_guard_denies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0_u8; 2048];
            let _ = socket.read(&mut buf).await.unwrap();
            let body = r#"{"action":"deny","reason":"blocked by policy","code":"POLICY_DENY"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let guard = HttpWebhookGuard {
            url: format!("http://{addr}/guard"),
            timeout_ms: Some(500),
            retry_count: 0,
            fail_behavior: FailBehavior::Deny,
            headers: HashMap::new(),
            client: reqwest::Client::new(),
        };
        let tc = TestCtx::new("SELECT 1");
        let result = guard.check(&tc.ctx()).await;
        assert!(matches!(
            result,
            GuardResult::Deny {
                reason,
                code: Some(code)
            } if reason == "blocked by policy" && code == "POLICY_DENY"
        ));
    }
}
