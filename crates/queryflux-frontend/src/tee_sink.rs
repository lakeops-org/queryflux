use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_cache::CacheWriter;
use queryflux_core::error::Result;
use queryflux_core::query::QueryStats;

use crate::dispatch::ResultSink;

/// Wraps a real `ResultSink`, mirroring schema/batch writes to a `CacheWriter`.
///
/// On any cache write failure, caching is silently abandoned (the primary sink
/// is never affected). On completion, finalizes the cache entry.
pub struct TeeResultSink<'a, S: ResultSink> {
    inner: &'a mut S,
    writer: Box<dyn CacheWriter>,
    max_bytes: Option<u64>,
    active: bool,
    committed: bool,
    finalized: bool,
}

impl<'a, S: ResultSink> TeeResultSink<'a, S> {
    pub fn new(inner: &'a mut S, writer: Box<dyn CacheWriter>, max_bytes: Option<u64>) -> Self {
        Self {
            inner,
            writer,
            max_bytes,
            active: true,
            committed: false,
            finalized: false,
        }
    }

    fn check_size(&mut self) -> bool {
        if let Some(max) = self.max_bytes {
            if self.writer.bytes_written() > max {
                self.active = false;
                return true;
            }
        }
        false
    }

    async fn abandon_cache(&mut self) {
        self.active = false;
        if self.finalized {
            return;
        }
        self.finalized = true;
        let _ = self.writer.finalize(false).await;
    }

    /// Finalize the cache entry. Call after on_complete or on failure.
    pub async fn finalize_cache(&mut self, success: bool) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        if self.active && success {
            if self.writer.finalize(true).await.is_ok() {
                self.committed = true;
            }
        } else {
            let _ = self.writer.finalize(false).await;
        }
    }

    pub fn cache_committed(&self) -> bool {
        self.committed
    }
}

#[async_trait]
impl<S: ResultSink> ResultSink for TeeResultSink<'_, S> {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        if self.active && self.writer.write_schema(schema).await.is_err() {
            self.abandon_cache().await;
        }
        self.inner.on_schema(schema).await
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.active && (self.writer.write_batch(batch).await.is_err() || self.check_size()) {
            self.abandon_cache().await;
        }
        self.inner.on_batch(batch).await
    }

    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()> {
        self.inner.on_complete(stats).await
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.abandon_cache().await;
        self.inner.on_error(message).await
    }

    async fn on_native_chunk(
        &mut self,
        chunk: &queryflux_core::native_result::NativeResultChunk,
    ) -> Result<()> {
        self.abandon_cache().await;
        self.inner.on_native_chunk(chunk).await
    }
}
