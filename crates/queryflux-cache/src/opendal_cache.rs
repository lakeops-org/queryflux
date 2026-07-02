use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use opendal::Operator;
use queryflux_core::config::{CacheBackendConfig, CacheCompression};
use queryflux_persistence::cache_store::{CacheEntryMeta, CacheStore};
use tracing::{debug, warn};

use crate::{CacheHitStats, CacheKey, CacheSink, CacheWriter, QueryResultCache};

/// Fail open to a cache miss if OpenDAL I/O takes longer than this.
const CACHE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// OpenDAL-backed query result cache.
///
/// Stores Arrow IPC streaming files in the configured backend (fs or S3).
/// Cache entry metadata lives in the persistence layer (`CacheStore`).
pub struct OpenDalResultCache {
    operator: Operator,
    store: Arc<dyn CacheStore>,
    compression: CacheCompression,
}

impl OpenDalResultCache {
    pub fn new(cfg: &CacheBackendConfig, store: Arc<dyn CacheStore>) -> Result<Self> {
        let operator = build_operator(cfg)?;
        Ok(Self {
            operator,
            store,
            compression: cfg.compression.clone(),
        })
    }
}

fn build_operator(cfg: &CacheBackendConfig) -> Result<Operator> {
    let scheme: opendal::Scheme = cfg
        .scheme
        .parse()
        .map_err(|_| anyhow::anyhow!("unsupported OpenDAL scheme: '{}'", cfg.scheme))?;
    Operator::via_iter(
        scheme,
        cfg.options.iter().map(|(k, v)| (k.clone(), v.clone())),
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "failed to build OpenDAL operator (scheme={}): {e}",
            cfg.scheme
        )
    })
}

#[async_trait]
impl QueryResultCache for OpenDalResultCache {
    async fn try_stream_cached(
        &self,
        key: &CacheKey,
        sink: &mut dyn CacheSink,
    ) -> Result<Option<CacheHitStats>> {
        let entry = match self.store.cache_get_valid(&key.hex).await {
            Ok(Some(e)) => e,
            Ok(None) => return Ok(None),
            Err(e) => {
                warn!("cache metadata lookup failed: {e}");
                return Ok(None);
            }
        };

        let path = key.storage_path();
        let data = match tokio::time::timeout(CACHE_IO_TIMEOUT, self.operator.read(&path)).await {
            Ok(Ok(d)) => d.to_vec(),
            Ok(Err(e)) => {
                warn!(path = %path, "cache file read failed: {e}");
                return Ok(None);
            }
            Err(_) => {
                warn!(path = %path, "cache file read timed out");
                return Ok(None);
            }
        };

        let cursor = std::io::Cursor::new(&data);
        let mut reader = match StreamReader::try_new(cursor, None) {
            Ok(r) => r,
            Err(e) => {
                warn!("cache IPC parse error: {e}");
                return Ok(None);
            }
        };

        let schema = reader.schema();
        let mut row_count: u64 = 0;
        let mut started = false;

        for batch_result in reader.by_ref() {
            match batch_result {
                Ok(batch) => {
                    if !started {
                        sink.on_schema(&schema).await?;
                        started = true;
                    }
                    row_count += batch.num_rows() as u64;
                    sink.on_batch(&batch).await?;
                }
                Err(e) => {
                    warn!("cache IPC batch read error: {e}");
                    if started {
                        return Err(anyhow::anyhow!(
                            "cache IPC batch read error after partial replay: {e}"
                        ));
                    }
                    return Ok(None);
                }
            }
        }

        if !started {
            sink.on_schema(&schema).await?;
        }

        debug!(
            key = %key,
            rows = row_count,
            bytes = data.len(),
            "cache hit"
        );

        Ok(Some(CacheHitStats {
            row_count,
            size_bytes: entry.size_bytes.unwrap_or(data.len() as i64) as u64,
        }))
    }

    async fn writer(&self, key: &CacheKey, ttl_secs: u64) -> Result<Box<dyn CacheWriter>> {
        Ok(Box::new(OpenDalCacheWriter {
            operator: self.operator.clone(),
            store: self.store.clone(),
            key: key.clone(),
            ttl_secs,
            ipc_writer: None,
            row_count: 0,
            compression: self.compression.clone(),
        }))
    }

    async fn invalidate_group(&self, group: &str) -> Result<u64> {
        let refs = self.store.cache_delete_by_group(group).await?;
        let count = refs.len() as u64;
        for r in &refs {
            let path = r.storage_path();
            if let Err(e) = self.operator.delete(&path).await {
                warn!(path = %path, "failed to delete cache file: {e}");
            }
        }
        Ok(count)
    }

    async fn invalidate_all(&self) -> Result<u64> {
        let refs = self.store.cache_delete_all().await?;
        let count = refs.len() as u64;
        for r in &refs {
            let path = r.storage_path();
            if let Err(e) = self.operator.delete(&path).await {
                warn!(path = %path, "failed to delete cache file: {e}");
            }
        }
        Ok(count)
    }

    async fn cleanup_expired(&self) -> Result<u64> {
        let refs = self.store.cache_delete_expired().await?;
        let count = refs.len() as u64;
        for r in &refs {
            let path = r.storage_path();
            if let Err(e) = self.operator.delete(&path).await {
                warn!(path = %path, "failed to delete expired cache file: {e}");
            }
        }
        if count > 0 {
            debug!(deleted = count, "cache cleanup completed");
        }
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// CacheWriter — accumulates Arrow IPC data in memory, flushes on finalize
// ---------------------------------------------------------------------------

struct OpenDalCacheWriter {
    operator: Operator,
    store: Arc<dyn CacheStore>,
    key: CacheKey,
    ttl_secs: u64,
    ipc_writer: Option<StreamWriter<Vec<u8>>>,
    row_count: u64,
    compression: CacheCompression,
}

fn ipc_write_options(compression: &CacheCompression) -> arrow::ipc::writer::IpcWriteOptions {
    use arrow::ipc::writer::IpcWriteOptions;

    let compression_codec = match compression {
        CacheCompression::None => None,
        CacheCompression::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
        CacheCompression::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
    };

    let mut opts = IpcWriteOptions::default();
    if let Some(codec) = compression_codec {
        opts = opts.try_with_compression(Some(codec)).unwrap_or_default();
    }
    opts
}

#[async_trait]
impl CacheWriter for OpenDalCacheWriter {
    async fn write_schema(&mut self, schema: &Schema) -> Result<()> {
        let opts = ipc_write_options(&self.compression);
        let writer = StreamWriter::try_new_with_options(Vec::new(), schema, opts)?;
        self.ipc_writer = Some(writer);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(w) = self.ipc_writer.as_mut() {
            self.row_count += batch.num_rows() as u64;
            w.write(batch)?;
        }
        Ok(())
    }

    async fn finalize(&mut self, success: bool) -> Result<()> {
        if !success {
            // Discard partial data
            self.ipc_writer = None;
            return Ok(());
        }

        let Some(writer) = self.ipc_writer.take() else {
            return Ok(());
        };

        let buffer = writer.into_inner()?;
        let size_bytes = buffer.len() as i64;
        let path = self.key.storage_path();

        match tokio::time::timeout(CACHE_IO_TIMEOUT, self.operator.write(&path, buffer)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(anyhow::anyhow!("cache write to {path}: {e}"));
            }
            Err(_) => {
                warn!(path = %path, "cache write timed out");
                return Ok(());
            }
        }

        let now = Utc::now();
        let entry = CacheEntryMeta {
            cache_key: self.key.hex.clone(),
            group_name: self.key.group.clone(),
            created_at: now,
            expires_at: now + Duration::seconds(self.ttl_secs as i64),
            row_count: Some(self.row_count as i64),
            size_bytes: Some(size_bytes),
        };

        if let Err(e) = self.store.cache_upsert(&entry).await {
            warn!(key = %self.key, "cache metadata upsert failed: {e}");
            let _ = self.operator.delete(&path).await;
            return Err(anyhow::anyhow!("cache metadata upsert failed: {e}"));
        }

        debug!(
            key = %self.key,
            rows = self.row_count,
            bytes = size_bytes,
            "cache entry written"
        );

        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.ipc_writer
            .as_ref()
            .map(|w| w.get_ref().len() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use queryflux_core::config::{CacheBackendConfig, CacheCompression};
    use queryflux_core::session::SessionContext;
    use queryflux_persistence::in_memory::InMemoryPersistence;

    use super::*;
    use crate::{CacheKey, CacheSink, QueryResultCache};

    fn test_config(root: &str) -> CacheBackendConfig {
        let mut options = std::collections::HashMap::new();
        options.insert("root".to_string(), root.to_string());
        CacheBackendConfig {
            scheme: "fs".to_string(),
            compression: CacheCompression::None,
            options,
            cleanup_interval_secs: 300,
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()
    }

    struct CollectingSink {
        schema: Option<Arc<Schema>>,
        batches: Vec<RecordBatch>,
    }

    impl CollectingSink {
        fn new() -> Self {
            Self {
                schema: None,
                batches: vec![],
            }
        }
    }

    #[async_trait]
    impl CacheSink for CollectingSink {
        async fn on_schema(&mut self, schema: &Schema) -> anyhow::Result<()> {
            self.schema = Some(Arc::new(schema.clone()));
            Ok(())
        }
        async fn on_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
            self.batches.push(batch.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn roundtrip_write_then_read() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT 1", "test-group", &session, "test-user", &[]);
        let batch = test_batch();

        // Write
        let mut writer = cache.writer(&key, 600).await.unwrap();
        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(true).await.unwrap();

        // Read back
        let mut sink = CollectingSink::new();
        let stats = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(stats.is_some(), "expected cache hit");
        let stats = stats.unwrap();
        assert_eq!(stats.row_count, 3);

        assert!(sink.schema.is_some());
        assert_eq!(sink.batches.len(), 1);
        assert_eq!(sink.batches[0].num_rows(), 3);
        assert_eq!(
            sink.batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "alice"
        );
    }

    #[tokio::test]
    async fn miss_on_unknown_key() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT unknown", "grp", &session, "test-user", &[]);
        let mut sink = CollectingSink::new();
        let result = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(result.is_none(), "expected cache miss");
    }

    #[tokio::test]
    async fn finalize_false_discards_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT failed", "grp", &session, "test-user", &[]);
        let batch = test_batch();

        let mut writer = cache.writer(&key, 600).await.unwrap();
        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(false).await.unwrap(); // discard

        let mut sink = CollectingSink::new();
        let result = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(result.is_none(), "discarded entry should not be found");
    }

    #[tokio::test]
    async fn invalidate_group_removes_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT 1", "my-group", &session, "test-user", &[]);
        let batch = test_batch();

        let mut writer = cache.writer(&key, 600).await.unwrap();
        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(true).await.unwrap();

        let deleted = cache.invalidate_group("my-group").await.unwrap();
        assert_eq!(deleted, 1);

        let mut sink = CollectingSink::new();
        let result = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(result.is_none(), "invalidated entry should miss");
    }

    #[tokio::test]
    async fn roundtrip_with_lz4_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = test_config(tmp.path().to_str().unwrap());
        cfg.compression = CacheCompression::Lz4;
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT compressed", "grp", &session, "test-user", &[]);
        let batch = test_batch();

        let mut writer = cache.writer(&key, 600).await.unwrap();
        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(true).await.unwrap();

        let mut sink = CollectingSink::new();
        let stats = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().row_count, 3);
        assert_eq!(sink.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn roundtrip_multiple_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT multi", "grp", &session, "test-user", &[]);
        let batch = test_batch();

        let mut writer = cache.writer(&key, 600).await.unwrap();
        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finalize(true).await.unwrap();

        let mut sink = CollectingSink::new();
        let stats = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().row_count, 9); // 3 batches × 3 rows
        assert_eq!(sink.batches.len(), 3);
    }

    #[tokio::test]
    async fn invalidate_all_clears_everything() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let batch = test_batch();

        let sqls = [
            "SELECT * FROM table_alpha",
            "SELECT * FROM table_beta",
            "SELECT * FROM table_gamma",
        ];
        for sql in &sqls {
            let key = CacheKey::new(sql, "grp", &session, "test-user", &[]);
            let mut writer = cache.writer(&key, 600).await.unwrap();
            writer.write_schema(batch.schema().as_ref()).await.unwrap();
            writer.write_batch(&batch).await.unwrap();
            writer.finalize(true).await.unwrap();
        }

        // Verify all 3 are independently readable
        for sql in &sqls {
            let key = CacheKey::new(sql, "grp", &session, "test-user", &[]);
            let mut sink = CollectingSink::new();
            let result = cache.try_stream_cached(&key, &mut sink).await.unwrap();
            assert!(
                result.is_some(),
                "entry '{sql}' should exist before invalidation"
            );
        }

        let deleted = cache.invalidate_all().await.unwrap();
        assert_eq!(deleted, 3);

        // Verify all entries are gone
        let key = CacheKey::new(sqls[0], "grp", &session, "test-user", &[]);
        let mut sink = CollectingSink::new();
        let result = cache.try_stream_cached(&key, &mut sink).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn bytes_written_tracks_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path().to_str().unwrap());
        let store: Arc<dyn CacheStore> = Arc::new(InMemoryPersistence::new());
        let cache = OpenDalResultCache::new(&cfg, store).unwrap();

        let session = SessionContext::default();
        let key = CacheKey::new("SELECT bytes", "grp", &session, "test-user", &[]);
        let batch = test_batch();

        let mut writer = cache.writer(&key, 600).await.unwrap();
        assert_eq!(writer.bytes_written(), 0);

        writer.write_schema(batch.schema().as_ref()).await.unwrap();
        let after_schema = writer.bytes_written();
        assert!(after_schema > 0);

        writer.write_batch(&batch).await.unwrap();
        assert!(writer.bytes_written() > after_schema);

        writer.finalize(true).await.unwrap();
    }
}
