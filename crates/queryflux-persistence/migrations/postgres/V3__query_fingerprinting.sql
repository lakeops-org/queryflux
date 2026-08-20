-- Add fingerprint hash columns to query_records.
-- Stored as BIGINT (reinterpret u64 bit-pattern as i64; use to_hex() for display).
-- NULL for rows recorded before this migration.

ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS query_hash                BIGINT,
    ADD COLUMN IF NOT EXISTS query_parameterized_hash  BIGINT,
    ADD COLUMN IF NOT EXISTS translated_query_hash     BIGINT;

-- Index on parameterized hash for pattern-based lookups and digest stats joins.
CREATE INDEX idx_query_records_query_parameterized_hash
    ON query_records (query_parameterized_hash)
    WHERE query_parameterized_hash IS NOT NULL;

-- Index on translated hash for cross-dialect pattern matching.
CREATE INDEX  idx_query_records_translated_query_hash
    ON query_records (translated_query_hash)
    WHERE translated_query_hash IS NOT NULL;

-- ---------------------------------------------------------------------------
-- query_digest_stats: one row per unique parameterized query pattern.
-- Like ProxySQL's stats_mysql_query_digest — pre-aggregated for fast reads.
-- ---------------------------------------------------------------------------
CREATE TABLE  query_digest_stats (
    id                       BIGSERIAL PRIMARY KEY,
    query_parameterized_hash BIGINT      NOT NULL,
    digest_text              TEXT        NOT NULL,
    translated_query_hash    BIGINT,
    translated_digest_text   TEXT,
    first_seen               TIMESTAMPTZ NOT NULL,
    last_seen                TIMESTAMPTZ NOT NULL,
    call_count               BIGINT      NOT NULL DEFAULT 0,
    sum_execution_ms         BIGINT      NOT NULL DEFAULT 0,
    sum_rows_returned        BIGINT      NOT NULL DEFAULT 0,
    cluster_group            TEXT        NOT NULL DEFAULT ''
);

-- Unique constraint — used by the ON CONFLICT upsert in record_query.
CREATE UNIQUE INDEX  idx_query_digest_stats_hash
    ON query_digest_stats (query_parameterized_hash);

-- Fast lookups by last activity for the "recent patterns" view.
CREATE INDEX  idx_query_digest_stats_last_seen
    ON query_digest_stats (last_seen DESC);
