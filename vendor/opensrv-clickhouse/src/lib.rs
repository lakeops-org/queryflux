// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// https://github.com/rust-lang/rust-clippy/issues/8334
#![allow(clippy::ptr_arg)]

use std::sync::Arc;

use errors::Result;
use protocols::Stage;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

use crate::cmd::Cmd;
use crate::connection::Connection;
use crate::protocols::HelloRequest;
use crate::types::Block;
use crate::types::Progress;

pub mod binary;
pub mod cmd;
pub mod connection;
pub mod error_codes;
pub mod errors;
pub mod protocols;
pub mod types;

/// Metadata for ClickHouse
#[derive(Debug, Clone)]
pub struct ClickHouseMetadata {
    name: String,
    display_name: String,
    major_version: u64,
    minor_version: u64,
    patch_version: u64,
    tcp_protocol_version: u64,
    timezone: String,
    has_stack_trace: bool,
}

impl Default for ClickHouseMetadata {
    fn default() -> Self {
        Self {
            name: "clickhouse-server".to_string(),
            display_name: "clickhouse-server".to_string(),
            major_version: 19,
            minor_version: 17,
            patch_version: 1,
            tcp_protocol_version: 54428,
            timezone: "UTC".to_string(),
            has_stack_trace: false,
        }
    }
}

impl ClickHouseMetadata {
    /// ClickHouse DBMS Name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set clickhouse DBMS name.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// ClickHouse DBMS Display Name.
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Set clickhouse DBMS display name.
    pub fn with_display_name(mut self, name: &str) -> Self {
        self.display_name = name.to_string();
        self
    }

    /// ClickHouse's version.
    ///
    /// (major, minor, patch)
    pub fn version(&self) -> (u64, u64, u64) {
        (self.major_version, self.minor_version, self.patch_version)
    }

    /// Set clickhouse major version
    pub fn with_major_version(mut self, v: u64) -> Self {
        self.major_version = v;
        self
    }

    /// Set clickhouse minor version
    pub fn with_minor_version(mut self, v: u64) -> Self {
        self.minor_version = v;
        self
    }

    /// Set clickhouse patch version
    pub fn with_patch_version(mut self, v: u64) -> Self {
        self.patch_version = v;
        self
    }

    /// ClickHouse's tcp_protocol_version
    pub fn tcp_protocol_version(&self) -> u64 {
        self.tcp_protocol_version
    }

    /// Set clickhouse tcp protocol version
    pub fn with_tcp_protocol_version(mut self, v: u64) -> Self {
        self.tcp_protocol_version = v;
        self
    }

    /// ClickHouse's timezone.
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    /// Set clickhouse timezone
    pub fn with_timezone(mut self, v: &str) -> Self {
        self.timezone = v.to_string();
        self
    }

    /// Is this session has stack trace
    pub fn has_stack_trace(&self) -> bool {
        self.has_stack_trace
    }

    /// Enable stack trace for clickhouse
    pub fn with_enable_stack_trace(mut self) -> Self {
        self.has_stack_trace = true;
        self
    }
}

#[async_trait::async_trait]
pub trait ClickHouseSession: Send + Sync {
    async fn authenticate(&self, _username: &str, _password: &[u8], _client_addr: &str) -> bool {
        true
    }

    async fn execute_query(&self, ctx: &mut CHContext, connection: &mut Connection) -> Result<()>;

    fn get_progress(&self) -> Progress {
        Progress::default()
    }

    /// Get ClickHouse metadata.
    fn metadata(&self) -> &ClickHouseMetadata;

    #[deprecated = "use ClickHouseMetadata::has_stack_trace() instead"]
    fn with_stack_trace(&self) -> bool {
        self.metadata().has_stack_trace()
    }

    #[deprecated = "use ClickHouseMetadata::name() instead"]
    fn dbms_name(&self) -> &str {
        self.metadata().name()
    }

    // None is by default, which will use same version as client send
    #[deprecated = "use ClickHouseMetadata::version() instead"]
    fn dbms_version_major(&self) -> u64 {
        self.metadata().version().0
    }

    #[deprecated = "use ClickHouseMetadata::version() instead"]
    fn dbms_version_minor(&self) -> u64 {
        self.metadata().version().1
    }

    #[deprecated = "use ClickHouseMetadata::tcp_protocol_version() instead"]
    fn dbms_tcp_protocol_version(&self) -> u64 {
        self.metadata().tcp_protocol_version()
    }

    #[deprecated = "use ClickHouseMetadata::timezone() instead"]
    fn timezone(&self) -> &str {
        self.metadata().timezone()
    }

    #[deprecated = "use ClickHouseMetadata::display_name() instead"]
    fn server_display_name(&self) -> &str {
        self.metadata().display_name()
    }

    #[deprecated = "use ClickHouseMetadata::version() instead"]
    fn dbms_version_patch(&self) -> u64 {
        self.metadata().version().2
    }
}

#[derive(Default)]
pub struct QueryState {
    pub query_id: String,
    pub stage: Stage,
    pub compression: u64,
    pub query: String,
    pub is_cancelled: bool,
    pub is_connection_closed: bool,
    /// empty or not
    pub is_empty: bool,

    /// Data was sent.
    pub sent_all_data: Arc<Notify>,
    pub out: Option<Sender<Block>>,
}

impl QueryState {
    fn reset(&mut self) {
        self.stage = Stage::Default;
        self.is_cancelled = false;
        self.is_connection_closed = false;
        self.is_empty = false;
        self.out = None;
    }
}

pub struct CHContext {
    pub state: QueryState,

    pub client_revision: u64,
    pub hello: Option<HelloRequest>,
}

impl CHContext {
    pub fn new(state: QueryState) -> Self {
        Self {
            state,
            client_revision: 0,
            hello: None,
        }
    }
}

pub trait ClickHouseIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> ClickHouseIo for T {}

/// A server that speaks the ClickHouseprotocol, and can delegate client commands to a backend
/// that implements [`ClickHouseSession`]
pub struct ClickHouseServer {}

impl ClickHouseServer {
    pub async fn run_on_stream(
        session: Arc<dyn ClickHouseSession>,
        stream: TcpStream,
    ) -> Result<()> {
        let peer = stream.peer_addr()?.to_string();
        Self::run_on_io(session, Box::new(stream), peer).await
    }
}

impl ClickHouseServer {
    pub async fn run_on_io(
        session: Arc<dyn ClickHouseSession>,
        stream: Box<dyn ClickHouseIo>,
        peer: String,
    ) -> Result<()> {
        tracing::debug!("Handle New session");
        let metadata = session.metadata();
        let tz = metadata.timezone().to_string();
        let mut ctx = CHContext::new(QueryState::default());
        let mut connection = Connection::new_io(stream, session, tz, peer)?;

        loop {
            // signal.
            let maybe_packet = tokio::select! {
               res = connection.read_packet(&mut ctx) => res,
            };

            let packet = match maybe_packet {
                Ok(Some(packet)) => packet,
                Err(e) => {
                    ctx.state.reset();
                    connection.write_error(&e).await?;
                    return Err(e);
                }
                Ok(None) => {
                    tracing::debug!("{:?}", "none data reset");
                    ctx.state.reset();
                    return Ok(());
                }
            };
            let cmd = Cmd::create(packet);
            cmd.apply(&mut connection, &mut ctx).await?;
        }
    }
}

#[macro_export]
macro_rules! row {
    () => { $crate::types::RNil };
    ( $i:ident, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($i).into(), $i.into())
    };
    ( $i:ident ) => { row!($i: $i) };

    ( $k:ident: $v:expr ) => {
        $crate::types::RNil.put(stringify!($k).into(), $v.into())
    };

    ( $k:ident: $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($k).into(), $v.into())
    };

    ( $k:expr => $v:expr ) => {
        $crate::types::RNil.put($k.into(), $v.into())
    };

    ( $k:expr => $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put($k.into(), $v.into())
    };
}
