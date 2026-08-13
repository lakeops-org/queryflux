//! Minimal Snowflake HTTP wire protocol v1 client for e2e tests.
//!
//! Implements the stateful session flow:
//!   login → (heartbeat | token_request)* → query* → logout
//!
//! Result decoding is intentionally lightweight — tests verify row counts and
//! success/error status rather than Arrow IPC byte contents.

use anyhow::{anyhow, bail, Result};
use reqwest::Client;
use serde_json::{json, Value};

pub struct SnowflakeWireClient {
    client: Client,
    base_url: String,
    pub session_token: Option<String>,
}

pub struct WireQueryResult {
    pub success: bool,
    pub error: Option<String>,
    pub total_rows: u64,
    pub query_id: String,
}

impl SnowflakeWireClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
            session_token: None,
        }
    }

    /// POST /session/v1/login-request — creates a session, stores the token.
    pub async fn login(&mut self, user: &str, password: &str) -> Result<()> {
        let body = json!({
            "data": {
                "LOGIN_NAME": user,
                "PASSWORD": password,
                "CLIENT_APP_ID": "QueryFluxTest",
                "CLIENT_APP_VERSION": "1.0"
            }
        });

        let resp = self
            .client
            .post(format!("{}/session/v1/login-request", self.base_url))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        if resp["success"].as_bool() != Some(true) {
            bail!(
                "login failed: {}",
                resp["message"].as_str().unwrap_or("unknown error")
            );
        }

        let token = resp["data"]["token"]
            .as_str()
            .ok_or_else(|| anyhow!("missing token in login response"))?;
        self.session_token = Some(token.to_string());
        Ok(())
    }

    /// DELETE /session — invalidates the session.
    pub async fn logout(&mut self) -> Result<()> {
        let token = self.token()?;
        self.client
            .delete(format!("{}/session", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .send()
            .await?;
        self.session_token = None;
        Ok(())
    }

    /// GET /session/heartbeat — bumps the idle timer, returns remaining validity seconds.
    pub async fn heartbeat(&self) -> Result<u64> {
        let token = self.token()?;
        let resp = self
            .client
            .get(format!("{}/session/heartbeat", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .send()
            .await?
            .json::<Value>()
            .await?;

        if resp["success"].as_bool() != Some(true) {
            bail!(
                "heartbeat failed: {}",
                resp["message"].as_str().unwrap_or("unknown")
            );
        }
        Ok(resp["data"]["validityInSeconds"].as_u64().unwrap_or(0))
    }

    /// POST /session/token-request — renews the session token, returns remaining validity.
    pub async fn token_request(&self) -> Result<u64> {
        let token = self.token()?;
        let resp = self
            .client
            .post(format!("{}/session/token-request", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .header("Content-Type", "application/json")
            .json(&json!({}))
            .send()
            .await?
            .json::<Value>()
            .await?;

        if resp["success"].as_bool() != Some(true) {
            bail!(
                "token_request failed: {}",
                resp["message"].as_str().unwrap_or("unknown")
            );
        }
        Ok(resp["data"]["validityInSecondsST"].as_u64().unwrap_or(0))
    }

    /// POST /queries/v1/query-request — execute SQL. Requires an active session.
    pub async fn query(&self, sql: &str, bindings: Option<Value>) -> Result<WireQueryResult> {
        let token = self.token()?;
        let mut body = json!({"sqlText": sql});
        if let Some(b) = bindings {
            body["parameterBindings"] = b;
        }

        let resp = self
            .client
            .post(format!("{}/queries/v1/query-request", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        let success = resp["success"].as_bool().unwrap_or(false);
        if !success {
            return Ok(WireQueryResult {
                success: false,
                error: resp["message"].as_str().map(|s| s.to_string()),
                total_rows: 0,
                query_id: resp["data"]["queryId"].as_str().unwrap_or("").to_string(),
            });
        }

        let total_rows = resp["data"]["total"].as_u64().unwrap_or(0);
        let query_id = resp["data"]["queryId"].as_str().unwrap_or("").to_string();

        Ok(WireQueryResult {
            success: true,
            error: None,
            total_rows,
            query_id,
        })
    }

    /// DELETE /queries/v1/:query_id — cancel an in-flight query.
    pub async fn cancel(&self, query_id: &str) -> Result<u16> {
        let token = self.token()?;
        let resp = self
            .client
            .delete(format!("{}/queries/v1/{query_id}", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .send()
            .await?;
        Ok(resp.status().as_u16())
    }

    /// GET /queries/v1/query-monitoring-request — list in-flight query ids.
    pub async fn monitoring_query_ids(&self) -> Result<Vec<String>> {
        let token = self.token()?;
        let resp = self
            .client
            .get(format!(
                "{}/queries/v1/query-monitoring-request",
                self.base_url
            ))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .send()
            .await?
            .json::<Value>()
            .await?;

        if resp["success"].as_bool() != Some(true) {
            bail!(
                "monitoring failed: {}",
                resp["message"].as_str().unwrap_or("unknown")
            );
        }

        Ok(resp["data"]["queries"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|q| q["id"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// POST /queries/v1/query-request — returns the raw HTTP status code.
    /// Used in auth-failure tests where we expect a 401.
    pub async fn query_raw_status(&self, sql: &str) -> Result<u16> {
        let token = self.session_token.as_deref().unwrap_or("invalid");
        let resp = self
            .client
            .post(format!("{}/queries/v1/query-request", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .header("Content-Type", "application/json")
            .json(&json!({"sqlText": sql}))
            .send()
            .await?;
        Ok(resp.status().as_u16())
    }

    fn token(&self) -> Result<&str> {
        self.session_token
            .as_deref()
            .ok_or_else(|| anyhow!("not logged in — call login() first"))
    }
}
