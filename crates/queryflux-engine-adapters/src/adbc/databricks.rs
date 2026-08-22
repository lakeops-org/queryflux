use async_trait::async_trait;
use queryflux_core::query::ClusterName;

use super::introspection::AdbcIntrospection;

/// Safety cap on `/api/2.0/sql/history/queries` pages walked per reconcile
/// call — bounds worst-case latency on the 30s reconcile tick.
const DATABRICKS_RECONCILE_MAX_PAGES: u32 = 10;

/// Query-string params for `GET /api/2.0/sql/history/queries`. Despite being
/// a `GET`, `filter_by.*` and `page_token` must be sent as flattened URL
/// query parameters, not a JSON request body — confirmed against the
/// Databricks Go/Python/Java SDK source, which all serialize `filter_by` as
/// dotted query params (e.g. `filter_by.warehouse_ids=<id>`) rather than a
/// body. A JSON body silently returns unfiltered/unpaginated results.
fn history_query_params(
    warehouse_id: &str,
    page_token: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut params = vec![
        ("filter_by.statuses", "RUNNING".to_string()),
        ("filter_by.warehouse_ids", warehouse_id.to_string()),
    ];
    if let Some(token) = page_token {
        params.push(("page_token", token.to_string()));
    }
    params
}

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
    ///
    /// The endpoint is paginated (`has_next_page` / `next_page_token`); a single
    /// page can undercount a busy warehouse, so this walks pages up to
    /// [`DATABRICKS_RECONCILE_MAX_PAGES`] — generous for a query count, and
    /// bounded so a runaway `next_page_token` chain can't hang the 30s reconcile tick.
    async fn fetch_running_query_count(&self) -> Option<u64> {
        let url = format!("{}/api/2.0/sql/history/queries", self.workspace_url);
        let mut total = 0u64;
        let mut page_token: Option<String> = None;
        for _ in 0..DATABRICKS_RECONCILE_MAX_PAGES {
            let params = history_query_params(&self.warehouse_id, page_token.as_deref());

            let resp = match self
                .http
                .get(&url)
                .bearer_auth(&self.auth_token)
                .query(&params)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => resp,
                Ok(resp) => {
                    tracing::warn!(
                        warehouse_id = %self.warehouse_id,
                        status = %resp.status(),
                        "Databricks REST reconcile query failed"
                    );
                    return if total > 0 { Some(total) } else { None };
                }
                Err(e) => {
                    tracing::warn!(
                        warehouse_id = %self.warehouse_id,
                        error = %e,
                        "Databricks REST reconcile request error"
                    );
                    return if total > 0 { Some(total) } else { None };
                }
            };

            let Ok(json) = resp.json::<serde_json::Value>().await else {
                return if total > 0 { Some(total) } else { None };
            };
            total += json
                .get("res")
                .and_then(|v| v.as_array())
                .map(|arr| arr.len() as u64)
                .unwrap_or(0);

            let has_next = json
                .get("has_next_page")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !has_next {
                return Some(total);
            }
            page_token = json
                .get("next_page_token")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            if page_token.is_none() {
                return Some(total);
            }
        }
        tracing::warn!(
            warehouse_id = %self.warehouse_id,
            max_pages = DATABRICKS_RECONCILE_MAX_PAGES,
            "Databricks REST reconcile hit page cap — running count may be undercounted"
        );
        Some(total)
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

    #[test]
    fn history_query_params_first_page() {
        let params = history_query_params("wh-id", None);
        assert_eq!(
            params,
            vec![
                ("filter_by.statuses", "RUNNING".to_string()),
                ("filter_by.warehouse_ids", "wh-id".to_string()),
            ]
        );
    }

    #[test]
    fn history_query_params_includes_page_token() {
        let params = history_query_params("wh-id", Some("tok123"));
        assert_eq!(
            params,
            vec![
                ("filter_by.statuses", "RUNNING".to_string()),
                ("filter_by.warehouse_ids", "wh-id".to_string()),
                ("page_token", "tok123".to_string()),
            ]
        );
    }

    #[test]
    fn history_query_params_encode_as_expected_url_query_string() {
        // Regression: filter_by.* and page_token must land in the URL query
        // string (not a JSON body) — this is what a real reqwest request
        // actually sends on the wire.
        let params = history_query_params("wh-id", Some("tok 123"));
        let req = reqwest::Client::new()
            .get("https://dbc.example.com/api/2.0/sql/history/queries")
            .query(&params)
            .build()
            .unwrap();
        let query = req.url().query().unwrap();
        assert!(query.contains("filter_by.statuses=RUNNING"));
        assert!(query.contains("filter_by.warehouse_ids=wh-id"));
        assert!(query.contains("page_token=tok+123") || query.contains("page_token=tok%20123"));
    }
}
