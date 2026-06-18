//! Arrow Flight SQL frontend.
//!
//! Accepts connections from ADBC clients (pandas, Polars, R, Julia), DBeaver
//! (via Flight SQL plugin), and any other Arrow-native tool.
//!
//! Implements only the minimal-trino RPC surface needed for query execution (V1):
//!   GetFlightInfo(CommandStatementQuery) → FlightInfo with ticket
//!   DoGet(Ticket)                        → FlightData stream (Arrow IPC over gRPC)
//!
//! All other Flight SQL RPCs return Unimplemented.
//!
//! Zero type mapping: RecordBatches flow from the backend adapter directly into
//! the Arrow IPC encoder and out over gRPC without any type inspection.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{
    server::FlightSqlService, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    flight_service_server::FlightServiceServer, FlightDescriptor, FlightEndpoint, FlightInfo,
    SchemaAsIpc, Ticket,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use queryflux_auth::Credentials;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
};

use crate::dispatch::{route_and_execute, ResultSink};
use crate::state::AppState;
use crate::FrontendListenerTrait;

// ── Frontend listener ─────────────────────────────────────────────────────────

pub struct FlightSqlFrontend {
    state: Arc<AppState>,
    port: u16,
}

impl FlightSqlFrontend {
    pub fn new(state: Arc<AppState>, port: u16) -> Self {
        Self { state, port }
    }
}

#[async_trait]
impl FrontendListenerTrait for FlightSqlFrontend {
    async fn listen(&self) -> Result<()> {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| QueryFluxError::Other(e.into()))?;

        info!("Flight SQL frontend listening on {addr}");

        let service = QueryFluxFlightSql::new(self.state.clone());
        let flight_server = FlightServiceServer::new(service);

        tonic::transport::Server::builder()
            .add_service(flight_server)
            .serve(addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))
    }
}

// ── FlightSqlService implementation ──────────────────────────────────────────

#[derive(Clone)]
pub struct QueryFluxFlightSql {
    state: Arc<AppState>,
}

impl QueryFluxFlightSql {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn session_from_request<T>(&self, request: &Request<T>) -> SessionContext {
        // Extract gRPC metadata as key-value headers.
        let headers: HashMap<String, String> = request
            .metadata()
            .iter()
            .filter_map(|kv| match kv {
                tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                    Some((k.as_str().to_string(), v.to_str().ok()?.to_string()))
                }
                _ => None,
            })
            .collect();
        // Extract user from gRPC metadata headers; tags are not yet extracted.
        let user = headers.get("x-trino-user").cloned();
        SessionContext {
            user,
            database: None,
            tags: queryflux_core::tags::QueryTags::new(),
            extra: headers,
            agent_context: None,
        }
    }
}

type FlightDataStream = Pin<
    Box<dyn Stream<Item = std::result::Result<arrow_flight::FlightData, Status>> + Send + 'static>,
>;

#[async_trait]
impl FlightSqlService for QueryFluxFlightSql {
    type FlightService = Self;

    // ── GetFlightInfo(CommandStatementQuery) ──────────────────────────────────

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let sql = &query.query;
        debug!(sql = %sql, "Flight SQL: GetFlightInfo");

        // Encode the SQL as a TicketStatementQuery → Any → bytes → Ticket.
        let ticket_query = TicketStatementQuery {
            statement_handle: sql.as_bytes().to_vec().into(),
        };
        let ticket_bytes = ticket_query.as_any().encode_to_vec();

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_bytes.into(),
            }),
            ..Default::default()
        };

        // Schema is unknown until execution — clients tolerate an empty schema here.
        let flight_info = FlightInfo {
            schema: encode_empty_schema(),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            ..Default::default()
        };

        Ok(Response::new(flight_info))
    }

    // ── DoGet(TicketStatementQuery) ───────────────────────────────────────────

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<FlightDataStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("statement_handle is not valid UTF-8"))?;
        debug!(sql = %sql, "Flight SQL: DoGet");

        let session = self.session_from_request(&request);
        let protocol = FrontendProtocol::FlightSql;

        // Authenticate — extract bearer token from gRPC metadata (Phase 1: NoneAuthProvider).
        let bearer = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|t| t.to_string());
        let creds = Credentials {
            username: session.user().map(|s| s.to_string()),
            bearer_token: bearer,
            ..Default::default()
        };
        let auth_ctx = self
            .state
            .auth_provider
            .authenticate(&creds)
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        // Channel: sink sends RecordBatches; FlightDataEncoderBuilder encodes them.
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<std::result::Result<RecordBatch, FlightError>>();
        let mut sink = FlightSqlResultSink { tx };

        let state2 = self.state.clone();
        let sql2 = sql.clone();

        tokio::spawn(async move {
            let _ = route_and_execute(
                &state2,
                sql2,
                vec![],
                session,
                protocol,
                &mut sink,
                &auth_ctx,
            )
            .await;
            // sink drops here → tx closes → rx stream ends
        });

        let batch_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map(|r| r.map_err(|e| Status::internal(e.to_string())));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    // ── SqlInfo (minimal-trino — return empty) ──────────────────────────────────────

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

// ── FlightSqlResultSink ───────────────────────────────────────────────────────

/// Collects Arrow RecordBatches from `execute_to_sink` and sends them to a channel.
/// `FlightDataEncoderBuilder` on the other end encodes them as Arrow IPC + gRPC FlightData.
struct FlightSqlResultSink {
    tx: UnboundedSender<std::result::Result<RecordBatch, FlightError>>,
}

#[async_trait]
impl ResultSink for FlightSqlResultSink {
    async fn on_schema(&mut self, _schema: &Schema) -> Result<()> {
        // Schema is extracted by FlightDataEncoderBuilder from the first RecordBatch.
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let _ = self.tx.send(Ok(batch.clone()));
        Ok(())
    }

    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        Ok(()) // sink drop closes the channel
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        let _ = self
            .tx
            .send(Err(FlightError::ExternalError(message.to_string().into())));
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Encode an empty Arrow schema as IPC bytes for FlightInfo.schema.
fn encode_empty_schema() -> bytes::Bytes {
    use arrow_ipc::writer::IpcWriteOptions;
    let schema = Schema::empty();
    let options = IpcWriteOptions::default();
    let ipc: arrow_flight::FlightData = SchemaAsIpc::new(&schema, &options).into();
    ipc.data_header
}
