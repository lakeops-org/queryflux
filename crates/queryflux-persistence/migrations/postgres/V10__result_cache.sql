-- Query result cache support

-- Per-group cache config stored as JSONB on the existing group configs table
ALTER TABLE cluster_group_configs ADD COLUMN IF NOT EXISTS cache JSONB DEFAULT NULL;

-- Cache entry metadata (actual data lives in OpenDAL storage)
CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key   TEXT PRIMARY KEY,
    group_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    row_count   BIGINT,
    size_bytes  BIGINT
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_expires ON cache_entries (expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_entries_group ON cache_entries (group_name);

-- Track cache hits on query records
ALTER TABLE query_records ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN NOT NULL DEFAULT FALSE;
