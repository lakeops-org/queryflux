//! Trino transactions retain a coordinator transaction id and service credentials.
use super::*;
use crate::{SyncAdapter, SyncExecution};
use std::sync::atomic::{AtomicBool, Ordering};
fn error(e: impl std::fmt::Display) -> QueryFluxError {
    QueryFluxError::Engine(format!("Trino transaction: {e}"))
}
async fn control(
    adapter: &TrinoAdapter,
    sql: &str,
    session: &SessionContext,
    transaction_id: &str,
) -> Result<Option<String>> {
    let request = adapter
        .http_client
        .post(adapter.trino_url("/v1/statement"))
        .body(sql.to_owned());
    let request = adapter.apply_cluster_auth(request);
    let request = adapter
        .apply_session_headers(
            request,
            session,
            &QueryTags::new(),
            &queryflux_auth::QueryCredentials::ServiceAccount,
        )
        .header("X-Trino-Transaction-Id", transaction_id);
    let mut response = adapter
        .with_control_timeout(request)
        .send()
        .await
        .map_err(error)?;
    let mut started = None;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if !response.status().is_success() {
            return Err(error(format!(
                "control request returned {}",
                response.status()
            )));
        }
        if let Some(id) = response.headers().get("X-Trino-Started-Transaction-Id") {
            started = Some(id.to_str().map_err(error)?.to_owned());
        }
        let body: serde_json::Value = response.json().await.map_err(error)?;
        if let Some(e) = body.get("error") {
            return Err(error(e));
        }
        let Some(next) = body["nextUri"].as_str() else {
            return Ok(started);
        };
        if std::time::Instant::now() >= deadline {
            return Err(error("transaction control timed out"));
        }
        let url = reqwest::Url::parse(next).map_err(error)?;
        let base = reqwest::Url::parse(&adapter.endpoint).map_err(error)?;
        if url.origin() != base.origin() {
            return Err(error("coordinator returned an unexpected poll origin"));
        }
        response = adapter
            .with_control_timeout(adapter.apply_cluster_auth(adapter.http_client.get(next)))
            .send()
            .await
            .map_err(error)?;
    }
}
pub(super) fn service_session(adapter: &TrinoAdapter, session: &SessionContext) -> SessionContext {
    let mut session = session.clone();
    session.extra.retain(|k, _| {
        !matches!(
            k.to_ascii_lowercase().as_str(),
            "authorization" | "x-trino-user"
        )
    });
    match &adapter.auth {
        Some(ClusterAuth::Basic { username, .. }) => {
            session
                .extra
                .insert("x-trino-user".into(), username.clone());
        }
        None => {
            session
                .extra
                .insert("x-trino-user".into(), "queryflux-service".into());
        }
        _ => {} // Token-authenticated Trino derives the service identity from its token.
    }
    session
}
pub async fn open(adapter: TrinoAdapter, session: SessionContext) -> Result<Arc<dyn SyncAdapter>> {
    let mut session = service_session(&adapter, &session);
    session.extra.retain(|k, _| {
        !matches!(
            k.to_ascii_lowercase().as_str(),
            "authorization" | "x-trino-transaction-id"
        )
    });
    let id = control(&adapter, "START TRANSACTION", &session, "NONE")
        .await?
        .ok_or_else(|| error("coordinator did not issue a transaction id"))?;
    Ok(Arc::new(Transaction {
        adapter,
        session,
        id,
        closed: AtomicBool::new(false),
    }))
}
struct Transaction {
    adapter: TrinoAdapter,
    session: SessionContext,
    id: String,
    closed: AtomicBool,
}
#[async_trait]
impl SyncAdapter for Transaction {
    fn engine_type(&self) -> EngineType {
        EngineType::Trino
    }
    fn supports_cancellation(&self) -> bool {
        true
    }
    async fn cancel_query(&self, id: &BackendQueryId) -> Result<()> {
        self.adapter.cancel_query(id, None).await
    }
    async fn finish_transaction(&self, commit: bool) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        control(
            &self.adapter,
            if commit { "COMMIT" } else { "ROLLBACK" },
            &self.session,
            &self.id,
        )
        .await
        .map(|_| ())
    }
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
        params: &queryflux_core::params::QueryParams,
        hints: queryflux_core::sql_classify::ExecutionHints,
        slot: &BackendQueryIdSlot,
    ) -> Result<SyncExecution> {
        if self.closed.load(Ordering::Acquire) {
            return Err(error("transaction closed"));
        }
        if !matches!(
            credentials,
            queryflux_auth::QueryCredentials::ServiceAccount
        ) {
            return Err(error("transaction requires service-account credentials"));
        }
        let mut session = session.clone();
        session.extra.retain(|k, _| {
            !matches!(
                k.to_ascii_lowercase().as_str(),
                "authorization" | "x-trino-transaction-id"
            )
        });
        session
            .extra
            .insert("x-trino-transaction-id".into(), self.id.clone());
        self.adapter
            .execute_as_arrow(sql, &session, credentials, tags, params, hints, slot)
            .await
    }
    async fn health_check(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }
    async fn list_catalogs(&self) -> Result<Vec<String>> {
        self.adapter.list_catalogs().await
    }
    async fn list_databases(&self, c: &str) -> Result<Vec<String>> {
        self.adapter.list_databases(c).await
    }
    async fn list_tables(&self, c: &str, d: &str) -> Result<Vec<String>> {
        self.adapter.list_tables(c, d).await
    }
    async fn describe_table(&self, c: &str, d: &str, t: &str) -> Result<Option<TableSchema>> {
        self.adapter.describe_table(c, d, t).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn trino_transaction_uses_service_auth_and_pinned_id() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for (sql, transaction) in [("START TRANSACTION", "NONE"), ("COMMIT", "txn-test")] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let (header_end, length) = loop {
                    let mut bytes = [0; 1024];
                    let n = stream.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&bytes[..n]);
                    if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..pos]).to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        break (pos + 4, length);
                    }
                };
                while data.len() < header_end + length {
                    let mut bytes = [0; 1024];
                    let n = stream.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&bytes[..n]);
                }
                let headers = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
                assert!(headers.contains("authorization: basic "));
                assert!(headers.contains("x-trino-user: service-user"));
                assert!(!headers.contains("bearer incoming-jwt"));
                assert!(headers.contains(&format!(
                    "x-trino-transaction-id: {}",
                    transaction.to_ascii_lowercase()
                )));
                assert_eq!(&data[header_end..header_end + length], sql.as_bytes());
                let body = "{\"id\":\"control\",\"stats\":{\"state\":\"FINISHED\"}}";
                let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\nX-Trino-Started-Transaction-Id: txn-test\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let adapter = TrinoAdapter::new(
            ClusterName("trino".into()),
            ClusterGroupName("default".into()),
            TrinoConfig {
                endpoint: format!("http://127.0.0.1:{port}"),
                tls_skip_verify: false,
                auth: Some(ClusterAuth::Basic {
                    username: "service-user".into(),
                    password: "test-password".into(),
                }),
            },
        );
        let mut session = SessionContext::default();
        session
            .extra
            .insert("authorization".into(), "Bearer incoming-jwt".into());
        session
            .extra
            .insert("x-trino-user".into(), "untrusted-user".into());
        let transaction = open(adapter, session).await.unwrap();
        transaction.finish_transaction(true).await.unwrap();
        server.await.unwrap();
    }
}
