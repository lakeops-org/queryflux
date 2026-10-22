-- Runs collected from engines by queryflux-collector (plans/query-enrichment-plan.md).
-- One row per execution on an engine, keyed by the engine's own query id. A run QueryFlux
-- routed is linked to its query_records row through proxy_query_id; runs sent straight to
-- the engine stay unlinked.

CREATE TABLE query_runs (
    cluster           TEXT        NOT NULL,
    engine_query_id   TEXT        NOT NULL,
    engine            TEXT        NOT NULL,
    -- Whether engine_query_id can be matched against query_records.backend_query_id.
    link_by_id        BOOLEAN     NOT NULL DEFAULT false,
    collector         TEXT        NOT NULL,
    proxy_query_id    TEXT,
    matched_by        TEXT,
    user_name         TEXT,
    started_at        TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ,
    status            TEXT,
    statement_type    TEXT,
    sql_hash          BIGINT,
    fast_hash         BIGINT,
    elapsed_ms        BIGINT,
    queued_ms         BIGINT,
    provisioning_ms   BIGINT,
    planning_ms       BIGINT,
    execution_ms      BIGINT,
    cpu_ms            BIGINT,
    bytes_scanned     BIGINT,
    rows_scanned      BIGINT,
    peak_memory_bytes BIGINT,
    spilled_bytes     BIGINT,
    network_bytes     BIGINT,
    rows_returned     BIGINT,
    bytes_returned    BIGINT,
    rows_written      BIGINT,
    bytes_written     BIGINT,
    result_cache_hit  BOOLEAN,
    extra             JSONB       NOT NULL DEFAULT '{}',
    collected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster, engine_query_id)
);

CREATE INDEX query_runs_proxy_query_id ON query_runs (proxy_query_id)
    WHERE proxy_query_id IS NOT NULL;
CREATE INDEX query_runs_ended_at ON query_runs (ended_at DESC);

-- Linking a run to the record QueryFlux wrote for it, from either side.
CREATE INDEX query_records_cluster_backend_id ON query_records (cluster_name, backend_query_id)
    WHERE backend_query_id IS NOT NULL;

-- Where each collector's clock last stopped, per cluster. Advanced in the same
-- transaction that stores a batch, so a restarted collector resumes after the last
-- stored window.
CREATE TABLE collector_watermarks (
    collector  TEXT        NOT NULL,
    cluster    TEXT        NOT NULL,
    clock      TEXT        NOT NULL,
    watermark  TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (collector, cluster, clock)
);
