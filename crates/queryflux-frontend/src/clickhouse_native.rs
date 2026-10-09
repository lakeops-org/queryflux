//! Native ClickHouse ingress. Queries use the same authenticated dispatch as SQL wire.
use crate::{
    dispatch::{execute_to_sink, ResultSink},
    state::AppState,
    FrontendListenerTrait, ShutdownRx,
};
use arrow::{
    array::*,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use opensrv_clickhouse::{
    connection::Connection,
    errors::{Error as NativeError, ServerError},
    types::Block,
    CHContext, ClickHouseMetadata, ClickHouseServer, ClickHouseSession,
};
use queryflux_auth::{provider::WireAuthKind, Credentials};
use queryflux_core::{
    config::FrontendConfig,
    error::{QueryFluxError, Result},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
};
use queryflux_routing::ChainRouteResult;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, Semaphore};

pub struct ClickHouseNativeFrontend {
    state: Arc<AppState>,
    config: FrontendConfig,
}
impl ClickHouseNativeFrontend {
    pub fn new(state: Arc<AppState>, config: FrontendConfig) -> Self {
        Self { state, config }
    }
}

#[async_trait]
impl FrontendListenerTrait for ClickHouseNativeFrontend {
    async fn listen(&self, mut shutdown: ShutdownRx) -> Result<()> {
        let tls = crate::wire_tls::load_tls(self.config.tls.as_ref())?;
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.config.port))
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;
        let limit = self
            .config
            .max_connections
            .filter(|n| *n > 0)
            .unwrap_or(Semaphore::MAX_PERMITS);
        let slots = Arc::new(Semaphore::new(limit));
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, peer) = accepted.map_err(|e| QueryFluxError::Other(e.into()))?;
                    let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                    let tls = tls.clone();
                    let state = self.state.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let secure = tls.is_some();
                        let transport: Box<dyn opensrv_clickhouse::ClickHouseIo> = match tls {
                            Some(tls) => match tokio::time::timeout(std::time::Duration::from_secs(30), tls.accept(stream)).await {
                                Ok(Ok(stream)) => Box::new(stream),
                                _ => return,
                            },
                            None => Box::new(stream),
                        };
                        let session = Arc::new(NativeSession { state, secure, credentials: RwLock::new(None),
                            metadata: ClickHouseMetadata::default().with_name("QueryFlux").with_display_name("QueryFlux") });
                        if let Err(e) = ClickHouseServer::run_on_io(session, transport, peer.to_string()).await {
                            tracing::debug!(%peer, "ClickHouse native connection closed: {e}");
                        }
                    });
                }
            }
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

struct NativeSession {
    state: Arc<AppState>,
    secure: bool,
    credentials: RwLock<Option<Credentials>>,
    metadata: ClickHouseMetadata,
}

#[async_trait]
impl ClickHouseSession for NativeSession {
    fn metadata(&self) -> &ClickHouseMetadata {
        &self.metadata
    }
    async fn authenticate(&self, username: &str, password: &[u8], _: &str) -> bool {
        if password.len() > 16384 {
            return false;
        }
        let Ok(password) = std::str::from_utf8(password) else {
            return false;
        };
        let provider = self.state.live.read().await.auth_provider.clone();
        let kind = provider.wire_auth_kind();
        if kind != WireAuthKind::None && !self.secure {
            return false;
        }
        let mut creds = match native_credentials(kind, username, password) {
            Some(c) => c,
            None => return false,
        };
        if crate::lease::authenticate_initial(&self.state, &mut creds)
            .await
            .is_err()
        {
            return false;
        }
        *self.credentials.write().await = Some(creds);
        true
    }
    async fn execute_query(
        &self,
        ctx: &mut CHContext,
        connection: &mut Connection,
    ) -> opensrv_clickhouse::errors::Result<()> {
        let result = self.dispatch(ctx, connection).await;
        if let Err(e) = result {
            let error = NativeError::Server(ServerError {
                code: 516,
                name: "QueryFluxException".into(),
                message: e.to_string(),
                stack_trace: String::new(),
            });
            connection.write_error(&error).await?;
            return Err(error);
        }
        Ok(())
    }
}

fn native_credentials(kind: WireAuthKind, user: &str, password: &str) -> Option<Credentials> {
    let jwt_marker = user == " JWT AUTHENTICATION ";
    match kind {
        WireAuthKind::Jwt if jwt_marker && !password.is_empty() => Some(Credentials {
            bearer_token: Some(password.into()),
            ..Default::default()
        }),
        WireAuthKind::Jwt => None,
        _ if jwt_marker => None,
        _ => Some(Credentials {
            username: Some(user.into()),
            password: Some(password.into()),
            bearer_token: None,
        }),
    }
}

impl NativeSession {
    async fn dispatch(&self, ctx: &CHContext, connection: &mut Connection) -> Result<()> {
        let creds =
            self.credentials.read().await.clone().ok_or_else(|| {
                QueryFluxError::Auth("native connection is not authenticated".into())
            })?;
        let identity = crate::lease::authenticate(&self.state, &creds).await?;
        let hello = ctx
            .hello
            .as_ref()
            .ok_or_else(|| QueryFluxError::Auth("missing authenticated Hello".into()))?;
        let database = std::str::from_utf8(&hello.default_database)
            .map_err(|e| QueryFluxError::Other(e.into()))?;
        let session = SessionContext {
            user: Some(identity.user.clone()),
            database: (!database.is_empty()).then(|| database.to_owned()),
            extra: HashMap::new(),
            ..Default::default()
        };
        let normalized = ctx.state.query.trim_start().to_ascii_uppercase();
        if normalized.starts_with("INSERT ") || normalized.starts_with("INSERT\n") {
            return Err(QueryFluxError::Engine(
                "ClickHouse native data-block INSERT is not supported".into(),
            ));
        }
        let protocol = FrontendProtocol::ClickHouseNative;
        // Routing is based on the verified identity, never the unauthenticated Hello user.
        let (route, mut trace) = {
            let live = self.state.live.read().await;
            live.router_chain
                .route_with_trace(&ctx.state.query, &session, &protocol, Some(&identity))
                .await?
        };
        let group = match route {
            ChainRouteResult::Routed(group) => group,
            ChainRouteResult::Denied { message } => return Err(QueryFluxError::Auth(message)),
        };
        let group = self
            .state
            .resolve_routed_group(group, &mut trace, &identity)
            .await?;
        let mut sink = NativeSink { connection };
        execute_to_sink(
            &self.state,
            ctx.state.query.clone(),
            vec![],
            session,
            protocol,
            group,
            &mut sink,
            &identity,
        )
        .await
    }
}

struct NativeSink<'a> {
    connection: &'a mut Connection,
}
#[async_trait]
impl ResultSink for NativeSink<'_> {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        if schema.fields().is_empty() {
            return Ok(());
        }
        self.on_batch(&RecordBatch::new_empty(Arc::new(schema.clone())))
            .await
    }
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let block = arrow_block(batch)?;
        self.connection
            .write_block(&block)
            .await
            .map_err(|e| QueryFluxError::Engine(e.to_string()))
    }
    async fn on_complete(&mut self, _: &QueryStats) -> Result<()> {
        Ok(())
    }
    async fn on_error(&mut self, message: &str) -> Result<()> {
        Err(QueryFluxError::Engine(message.into()))
    }
}

fn arrow_block(batch: &RecordBatch) -> Result<Block> {
    let mut block = Block::new();
    for (field, values) in batch.schema().fields().iter().zip(batch.columns()) {
        macro_rules! numeric {
            ($array:ty, $value:ty) => {{
                let a = values
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| QueryFluxError::Engine("Arrow array type mismatch".into()))?;
                if field.is_nullable() {
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| (!a.is_null(i)).then(|| a.value(i)))
                            .collect::<Vec<Option<$value>>>(),
                    )
                } else {
                    if a.null_count() != 0 {
                        return Err(QueryFluxError::Engine(
                            "unexpected null in required column".into(),
                        ));
                    }
                    block.column(
                        field.name(),
                        (0..a.len()).map(|i| a.value(i)).collect::<Vec<$value>>(),
                    )
                }
            }};
        }
        macro_rules! strings {
            ($array:ty) => {{
                let a = values
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| QueryFluxError::Engine("Arrow string type mismatch".into()))?;
                if field.is_nullable() {
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| (!a.is_null(i)).then(|| a.value(i).to_owned()))
                            .collect::<Vec<_>>(),
                    )
                } else {
                    if a.null_count() != 0 {
                        return Err(QueryFluxError::Engine(
                            "unexpected null in required column".into(),
                        ));
                    }
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| a.value(i).to_owned())
                            .collect::<Vec<_>>(),
                    )
                }
            }};
        }
        macro_rules! binary {
            ($array:ty) => {{
                let a = values
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| QueryFluxError::Engine("Arrow binary type mismatch".into()))?;
                if field.is_nullable() {
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| (!a.is_null(i)).then(|| a.value(i).to_owned()))
                            .collect::<Vec<Option<Vec<u8>>>>(),
                    )
                } else {
                    if a.null_count() != 0 {
                        return Err(QueryFluxError::Engine(
                            "unexpected null in required column".into(),
                        ));
                    }
                    block.column(
                        field.name(),
                        (0..a.len()).map(|i| a.value(i)).collect::<Vec<&[u8]>>(),
                    )
                }
            }};
        }
        block = match field.data_type() {
            DataType::Int8 => numeric!(Int8Array, i8),
            DataType::Int16 => numeric!(Int16Array, i16),
            DataType::Int32 => numeric!(Int32Array, i32),
            DataType::Int64 => numeric!(Int64Array, i64),
            DataType::UInt8 => numeric!(UInt8Array, u8),
            DataType::UInt16 => numeric!(UInt16Array, u16),
            DataType::UInt32 => numeric!(UInt32Array, u32),
            DataType::UInt64 => numeric!(UInt64Array, u64),
            DataType::Float32 => numeric!(Float32Array, f32),
            DataType::Float64 => numeric!(Float64Array, f64),
            DataType::Utf8 => strings!(StringArray),
            DataType::LargeUtf8 => strings!(LargeStringArray),
            DataType::Binary => binary!(BinaryArray),
            DataType::LargeBinary => binary!(LargeBinaryArray),
            DataType::Boolean => {
                let a = values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| QueryFluxError::Engine("Arrow boolean type mismatch".into()))?;
                if field.is_nullable() {
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| (!a.is_null(i)).then(|| u8::from(a.value(i))))
                            .collect::<Vec<_>>(),
                    )
                } else {
                    block.column(
                        field.name(),
                        (0..a.len())
                            .map(|i| u8::from(a.value(i)))
                            .collect::<Vec<_>>(),
                    )
                }
            }
            other => {
                return Err(QueryFluxError::Engine(format!(
                "ClickHouse native result type {other} is not supported; refusing lossy conversion"
            )))
            }
        };
    }
    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn jwt_hello_cannot_be_treated_as_a_username_or_password_login() {
        assert!(native_credentials(WireAuthKind::Jwt, " JWT AUTHENTICATION ", "token").is_some());
        assert!(native_credentials(WireAuthKind::Jwt, "alice", "token").is_none());
        assert!(native_credentials(WireAuthKind::None, " JWT AUTHENTICATION ", "token").is_none());
    }
    #[test]
    fn native_results_preserve_values_and_nulls() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("value"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let block = arrow_block(&batch).unwrap();
        assert_eq!(block.row_count(), 2);
        assert_eq!(block.get::<Option<i64>, _>(0, "number").unwrap(), Some(7));
        assert_eq!(block.get::<Option<i64>, _>(1, "number").unwrap(), None);
    }
}
