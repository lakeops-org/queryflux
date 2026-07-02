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
        Err(anyhow::anyhow!("cache backend not configured"))
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
