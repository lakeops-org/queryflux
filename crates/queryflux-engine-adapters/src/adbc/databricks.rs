use async_trait::async_trait;
use queryflux_core::query::ClusterName;

use super::introspection::AdbcIntrospection;

/// REST-based introspection for Databricks SQL Warehouses.
/// Avoids waking the warehouse via SQL `SELECT 1` or system tables.
pub struct DatabricksIntrospection {
    http: reqwest::Client,
    workspace_url: String,
    warehouse_id: String,
    auth_token: String,
}

pub fn try_from_adbc_config(
    cluster_name: &ClusterName,
    uri: &str,
    db_kwargs: &[(String, String)],
) -> Option<DatabricksIntrospection> {
    let workspace_url = uri.trim_end_matches('/').to_string();
    if workspace_url.is_empty() {
        tracing::warn!(cluster = %cluster_name.0, "Databricks REST: no workspace URL in ADBC URI");
        return None;
    }

    let find = |key: &str| {
        db_kwargs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };

    let auth_token = find("token")
        .or_else(|| find("access_token"))
        .or_else(|| find("databricks_token"))
        .unwrap_or_default();
    if auth_token.is_empty() {
        tracing::warn!(cluster = %cluster_name.0, "Databricks REST: no auth token found in dbKwargs");
        return None;
    }

    let http_path = find("http_path").unwrap_or_default();
    let warehouse_id = http_path.rsplit('/').next().unwrap_or("").to_string();
    if warehouse_id.is_empty() {
        tracing::warn!(cluster = %cluster_name.0, "Databricks REST: cannot parse warehouse ID from http_path");
        return None;
    }

    tracing::info!(
        cluster = %cluster_name.0,
        workspace = %workspace_url,
        warehouse_id = %warehouse_id,
        "Databricks REST client initialized for health check and reconciliation"
    );

    Some(DatabricksIntrospection {
        http: reqwest::Client::new(),
        workspace_url,
        warehouse_id,
        auth_token,
    })
}

#[async_trait]
impl AdbcIntrospection for DatabricksIntrospection {
    /// Health check via `GET /api/2.0/sql/warehouses/{warehouse_id}`.
    /// Healthy = state is RUNNING, STARTING, or STOPPED (warehouse exists and is accessible).
    async fn health_check(&self) -> bool {
        let url = format!(
            "{}/api/2.0/sql/warehouses/{}",
            self.workspace_url, self.warehouse_id
        );
        match self
            .http
            .get(&url)
            .bearer_auth(&self.auth_token)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    let state = body.get("state").and_then(|v| v.as_str()).unwrap_or("");
                    matches!(state, "RUNNING" | "STARTING" | "STOPPED")
                } else {
                    false
                }
            }
            Ok(resp) => {
                tracing::warn!(
                    warehouse_id = %self.warehouse_id,
                    status = %resp.status(),
                    "Databricks REST health check failed"
                );
                false
            }
            Err(e) => {
                tracing::warn!(
                    warehouse_id = %self.warehouse_id,
                    error = %e,
                    "Databricks REST health check request error"
                );
                false
            }
        }
    }

    /// Reconciliation via `GET /api/2.0/sql/history/queries`.
    /// Returns the count of RUNNING queries for this warehouse.
    async fn fetch_running_query_count(&self) -> Option<u64> {
        let url = format!("{}/api/2.0/sql/history/queries", self.workspace_url);
        let filter = serde_json::json!({
            "filter_by": {
                "statuses": ["RUNNING"],
                "warehouse_ids": [&self.warehouse_id]
            }
        });
        match self
            .http
            .get(&url)
            .bearer_auth(&self.auth_token)
            .json(&filter)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    body.get("res")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.len() as u64)
                } else {
                    None
                }
            }
            Ok(resp) => {
                tracing::warn!(
                    warehouse_id = %self.warehouse_id,
                    status = %resp.status(),
                    "Databricks REST reconcile query failed"
                );
                None
            }
            Err(e) => {
                tracing::warn!(
                    warehouse_id = %self.warehouse_id,
                    error = %e,
                    "Databricks REST reconcile request error"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adbc::sql_helpers;
    use queryflux_core::query::ClusterName;

    #[test]
    fn extracts_warehouse_id_from_http_path() {
        let kwargs = vec![(
            "http_path".into(),
            "/sql/1.0/warehouses/abc123def456".into(),
        )];
        let path = sql_helpers::db_kwarg(&kwargs, "http_path").unwrap();
        let id = path.rsplit('/').next().unwrap();
        assert_eq!(id, "abc123def456");
    }

    #[test]
    fn try_from_requires_token_and_http_path() {
        let name = ClusterName("wh".into());
        assert!(try_from_adbc_config(&name, "https://dbc.example.com", &[],).is_none());
        assert!(try_from_adbc_config(
            &name,
            "https://dbc.example.com",
            &[("token".into(), "tok".into())],
        )
        .is_none());
        assert!(try_from_adbc_config(
            &name,
            "",
            &[
                ("token".into(), "tok".into(),),
                ("http_path".into(), "/sql/1.0/warehouses/id1".into(),)
            ],
        )
        .is_none());
    }

    #[test]
    fn try_from_accepts_token_and_http_path() {
        let name = ClusterName("wh".into());
        let intro = try_from_adbc_config(
            &name,
            "https://dbc.example.com/",
            &[
                ("token".into(), "tok".into()),
                ("http_path".into(), "/sql/1.0/warehouses/wh-id".into()),
            ],
        );
        assert!(intro.is_some());
    }
}
