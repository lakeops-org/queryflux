//! TLS and buffered transport shared by the SQL wire listeners.
use queryflux_core::{
    config::FrontendTlsConfig,
    error::{QueryFluxError, Result},
};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadHalf, WriteHalf};
use tokio_rustls::{rustls, TlsAcceptor};

pub trait WireIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> WireIo for T {}
pub type BoxIo = Box<dyn WireIo>;
pub type WireReader = BufReader<ReadHalf<BoxIo>>;
pub type WireWriter = WriteHalf<BoxIo>;

pub fn split(stream: BoxIo) -> (WireReader, WireWriter) {
    let (read, write) = tokio::io::split(stream);
    (BufReader::new(read), write)
}

pub fn load_tls(config: Option<&FrontendTlsConfig>) -> Result<Option<TlsAcceptor>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let load = || -> anyhow::Result<TlsAcceptor> {
        let mut cert = std::io::BufReader::new(std::fs::File::open(&config.cert_file)?);
        let certs = rustls_pemfile::certs(&mut cert).collect::<std::io::Result<Vec<_>>>()?;
        let mut key = std::io::BufReader::new(std::fs::File::open(&config.key_file)?);
        let key = rustls_pemfile::private_key(&mut key)?
            .ok_or_else(|| anyhow::anyhow!("missing TLS private key"))?;
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
        Ok(TlsAcceptor::from(Arc::new(server)))
    };
    load().map(Some).map_err(|e| QueryFluxError::Other(e))
}
