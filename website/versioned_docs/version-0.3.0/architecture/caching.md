# Query Cache

QueryFlux can transparently cache deterministic query results in a pluggable storage backend, eliminating repeated backend roundtrips for identical queries.

## How it works

```
Client → QueryFlux
          │
          ├── Cache hit?  → replay Arrow IPC from storage → respond immediately
          │
          └── Cache miss? → execute on backend cluster
                            └── TeeResultSink writes to cache + responds to client
```

1. **Cache key** — computed from the exact SQL text, authenticated user, bind parameters, cluster group name, and current catalog/database. Queries differing only in literal values or parameters produce distinct cache keys.
2. **Determinism guard** — queries containing non-deterministic functions (`NOW()`, `RAND()`, `UUID()`, `CURRENT_TIMESTAMP`, etc.) are never cached.
3. **Arrow IPC storage** — results are stored as streaming Arrow IPC files with optional LZ4 or ZSTD compression.
4. **TTL expiration** — each entry has a configurable time-to-live. A background cleanup task periodically removes expired entries.

## Configuration

### Cache backend (startup-only)

The `cacheBackend` block configures **where** cached data is stored. It uses [Apache OpenDAL](https://opendal.apache.org/) under the hood, so any OpenDAL-supported service can be used without code changes.

| Field | Description |
|-------|-------------|
| `scheme` | OpenDAL service scheme: `fs`, `s3`, `gcs`, `azblob`, `oss`, etc. Default: `fs` |
| `compression` | Arrow IPC body compression: `none`, `lz4` (default), `zstd` |
| `cleanupIntervalSecs` | How often the background task deletes expired entries. Default: `300` |
| `options` | Flat key→value map passed directly to `Operator::via_iter(scheme, options)`. Keys are service-specific — see [OpenDAL service docs](https://opendal.apache.org/services/). |

#### Local filesystem example

```yaml
queryflux:
  cacheBackend:
    scheme: fs
    compression: lz4
    options:
      root: /var/lib/queryflux/cache
```

#### S3 / MinIO example

```yaml
queryflux:
  cacheBackend:
    scheme: s3
    compression: lz4
    options:
      bucket: queryflux-cache
      endpoint: http://minio:9000
      region: us-east-1
      access_key_id: ${MINIO_ACCESS_KEY}
      secret_access_key: ${MINIO_SECRET_KEY}
```

#### Google Cloud Storage example

```yaml
queryflux:
  cacheBackend:
    scheme: gcs
    compression: zstd
    options:
      bucket: my-queryflux-cache
      root: cache/
      credential_path: /etc/queryflux/gcs-key.json
```

### Per-group settings (hot-reloadable)

Each cluster group can independently enable caching and control TTL/size limits. These settings are hot-reloadable via Studio or the admin API.

| Field | Description |
|-------|-------------|
| `enabled` | Whether caching is active for this group. Default: `false` |
| `ttlSecs` | Time-to-live for cached entries in seconds. Default: `300` |
| `maxEntrySizeMb` | Maximum size (in MB) of a single cached result. Results exceeding this limit are not cached (the query still succeeds). Optional — unlimited if omitted. |

```yaml
clusterGroups:
  analytics:
    members: [trino-1, trino-2]
    maxRunningQueries: 50
    cache:
      enabled: true
      ttlSecs: 600
      maxEntrySizeMb: 128
```

## Per-query cache hints

Clients can opt-in to caching on a per-query basis, even when the group-level cache is disabled:

| Mechanism | Example |
|-----------|---------|
| HTTP header | `X-QueryFlux-Cache: true` (optionally `X-QueryFlux-Cache-TTL: 600`) |
| Query tag | Session property `queryflux:cache` with optional value `ttl=N` |
| SQL comment | `/* queryflux:cache:ttl=120 */ SELECT ...` |

Hints are evaluated in the order above (first match wins).

## What gets cached

- Only **deterministic** queries are cached. The determinism check is a lightweight regex that detects common non-deterministic functions.
- Only queries routed through the **sync execution path** (Arrow materialization) are cached. This includes all engines and Trino queries on groups with caching enabled.
- Results exceeding `maxEntrySizeMb` are **not** cached (the query completes normally, only caching is skipped).
- Failed or cancelled queries are **never** cached.

## Cache invalidation

### Admin API

```bash
# Invalidate all cached entries across all groups
curl -X DELETE http://localhost:9000/admin/cache

# Invalidate entries for a specific group
curl -X DELETE http://localhost:9000/admin/cache/analytics
```

### Studio UI

Use the **Clear cache** button in the cluster group editor dialog.

### Automatic expiration

The background cleanup task runs every `cleanupIntervalSecs` seconds and removes entries past their TTL.

## Observability

### Prometheus metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `queryflux_cache_hits_total` | Counter | `cluster_group` | Cache hits served from storage |
| `queryflux_cache_misses_total` | Counter | `cluster_group` | Cache misses (query executed on backend) |
| `queryflux_cache_writes_total` | Counter | `cluster_group` | New entries written to cache |

### Query history

Cached queries appear in Studio's query history with:
- A **CACHED** badge
- Engine type shown as `Cache`
- Cluster shown as `(cache)`
- Execution duration of `0ms`

## Architecture notes

- The cache layer sits **before** cluster slot acquisition — a cache hit never consumes a backend connection.
- Arrow IPC streaming format enables zero-copy replay without deserialization overhead.
- The `TeeResultSink` simultaneously streams results to the client and writes to cache, so there's no latency penalty on cache-miss writes.
- Cache metadata (key, group, TTL, size) is stored in Postgres (or in-memory for dev), while the actual Arrow data lives in the configured OpenDAL backend.
