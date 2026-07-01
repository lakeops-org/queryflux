use anyhow::Result;
use async_trait::async_trait;

use crate::{CacheHitStats, CacheKey, CacheSink, CacheWriter, QueryResultCache};

/// No-op cache used when no cache backend is configured.
pub struct NoopResultCache;

#[async_trait]
impl QueryResultCache for NoopResultCache {
    async fn try_stream_cached(
        &self,
        _key: &CacheKey,
        _sink: &mut dyn CacheSink,
    ) -> Result<Option<CacheHitStats>> {
        Ok(None)
    }

    async fn writer(&self, _key: &CacheKey, _ttl_secs: u64) -> Result<Box<dyn CacheWriter>> {
        Ok(Box::new(NoopCacheWriter))
    }

    async fn invalidate_group(&self, _group: &str) -> Result<u64> {
        Ok(0)
    }

    async fn invalidate_all(&self) -> Result<u64> {
        Ok(0)
    }

    async fn cleanup_expired(&self) -> Result<u64> {
        Ok(0)
    }
}

struct NoopCacheWriter;

#[async_trait]
impl CacheWriter for NoopCacheWriter {
    async fn write_schema(&mut self, _schema: &arrow::datatypes::Schema) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        Ok(())
    }

    async fn finalize(&mut self, _success: bool) -> Result<()> {
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        0
    }
}
