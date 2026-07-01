use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_core::dispatch::QueryStats;
use queryflux_core::error::QueryFluxError;
use tracing::warn;

use crate::CacheWriter;

/// A ResultSink wrapper that tees Arrow batches to both the real downstream
/// sink and a CacheWriter. If the cache writer fails or the result exceeds
/// `max_entry_size_mb`, caching is silently abandoned — the query still
/// succeeds for the client.
pub struct TeeResultSink<'a, S> {
    inner: &'a mut S,
    writer: Box<dyn CacheWriter>,
    max_bytes: Option<u64>,
    /// Set to true when we give up caching (error or size exceeded).
    abandoned: bool,
}

impl<'a, S> TeeResultSink<'a, S> {
    pub fn new(
        inner: &'a mut S,
        writer: Box<dyn CacheWriter>,
        max_entry_size_mb: Option<u64>,
    ) -> Self {
        Self {
            inner,
            writer,
            max_bytes: max_entry_size_mb.map(|mb| mb * 1024 * 1024),
            abandoned: false,
        }
    }

    /// Finalize the cache entry. Call after execution completes.
    /// `success = false` causes the partial file to be cleaned up.
    pub async fn finalize(&mut self, success: bool) {
        if self.abandoned {
            let _ = self.writer.finalize(false).await;
            return;
        }
        if let Err(e) = self.writer.finalize(success).await {
            warn!("cache writer finalize error: {e}");
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.writer.bytes_written()
    }

    fn check_size(&mut self) -> bool {
        if let Some(max) = self.max_bytes {
            if self.writer.bytes_written() > max {
                self.abandoned = true;
                return false;
            }
        }
        true
    }
}

/// Blanket impl: forward all ResultSink calls to inner, and additionally write to the cache.
/// We use a manual async_trait impl because we need the generic S bound.
#[async_trait]
impl<'a, S> queryflux_core::dispatch::ResultSink for TeeResultSink<'a, S>
where
    S: queryflux_core::dispatch::ResultSink + Send,
{
    async fn on_schema(&mut self, schema: &Schema) -> Result<(), QueryFluxError> {
        self.inner.on_schema(schema).await?;
        if !self.abandoned {
            if let Err(e) = self.writer.write_schema(schema).await {
                warn!("cache write_schema error (abandoning): {e}");
                self.abandoned = true;
            }
        }
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<(), QueryFluxError> {
        self.inner.on_batch(batch).await?;
        if !self.abandoned {
            if let Err(e) = self.writer.write_batch(batch).await {
                warn!("cache write_batch error (abandoning): {e}");
                self.abandoned = true;
            } else {
                self.check_size();
            }
        }
        Ok(())
    }

    async fn on_complete(&mut self, stats: &QueryStats) -> Result<(), QueryFluxError> {
        self.inner.on_complete(stats).await
    }

    async fn on_error(&mut self, message: &str) -> Result<(), QueryFluxError> {
        self.abandoned = true;
        self.inner.on_error(message).await
    }
}
