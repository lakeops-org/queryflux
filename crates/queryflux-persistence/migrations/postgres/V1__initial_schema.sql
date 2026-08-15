-- QueryFlux persistence: full schema for a new database.
-- Engine-specific cluster options live in cluster_configs.config (JSONB).

-- ---------------------------------------------------------------------------
-- Cluster and group configuration
-- ---------------------------------------------------------------------------

CREATE TABLE cluster_configs (
    id                    BIGSERIAL PRIMARY KEY,
    name                  TEXT        NOT NULL UNIQUE,
    engine_key            TEXT        NOT NULL,
    enabled               BOOLEAN     NOT NULL DEFAULT true,
    max_running_queries   BIGINT,
    config                JSONB       NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cluster_group_configs (
    id                      BIGSERIAL PRIMARY KEY,
    name                    TEXT        NOT NULL UNIQUE,
    enabled                 BOOLEAN     NOT NULL DEFAULT true,
    members                 BIGINT[]    NOT NULL DEFAULT '{}',
    max_running_queries     BIGINT      NOT NULL DEFAULT 10,
    max_queued_queries      BIGINT,
    strategy                JSONB,
    allow_groups            TEXT[]      NOT NULL DEFAULT '{}',
    allow_users             TEXT[]      NOT NULL DEFAULT '{}',
    translation_script_ids  BIGINT[]    NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Routing (normalized rules + singleton settings)
-- ---------------------------------------------------------------------------

CREATE TABLE routing_settings (
    singleton                BOOLEAN PRIMARY KEY DEFAULT TRUE,
    routing_fallback         TEXT        NOT NULL DEFAULT '',
    routing_persist_active   BOOLEAN     NOT NULL DEFAULT FALSE,
    fallback_group_id        BIGINT REFERENCES cluster_group_configs (id) ON DELETE SET NULL,
    CONSTRAINT routing_settings_singleton CHECK (singleton = TRUE)
);

INSERT INTO routing_settings (singleton, routing_fallback, routing_persist_active, fallback_group_id)
VALUES (TRUE, '', FALSE, NULL);

CREATE TABLE routing_rules (
    id                    BIGSERIAL PRIMARY KEY,
    sort_order            INT         NOT NULL,
    router_logical_index  INT         NOT NULL DEFAULT 0,
    slice_index           INT         NOT NULL DEFAULT 0,
    definition            JSONB       NOT NULL,
    target_group_id       BIGINT REFERENCES cluster_group_configs (id) ON DELETE RESTRICT,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX routing_rules_sort_order_idx ON routing_rules (sort_order);
CREATE INDEX routing_rules_target_group_id_idx ON routing_rules (target_group_id);

-- ---------------------------------------------------------------------------
-- Security (admin API persistence for security_config)
-- ---------------------------------------------------------------------------

CREATE TABLE security_settings (
    singleton   BOOLEAN PRIMARY KEY DEFAULT TRUE,
    config      JSONB        NOT NULL DEFAULT '{}',
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT security_settings_singleton CHECK (singleton = TRUE)
);

INSERT INTO security_settings (singleton, config) VALUES (TRUE, '{}'::jsonb);

-- ---------------------------------------------------------------------------
-- In-flight query state (cleared when queries complete)
-- ---------------------------------------------------------------------------

CREATE TABLE executing_queries (
    id         TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX executing_queries_created_at ON executing_queries (created_at);

CREATE TABLE queued_queries (
    id         TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX queued_queries_created_at ON queued_queries (created_at);

-- ---------------------------------------------------------------------------
-- Query history and engine stats
-- ---------------------------------------------------------------------------

CREATE TABLE query_records (
    id                     BIGSERIAL PRIMARY KEY,
    proxy_query_id         TEXT         NOT NULL,
    cluster_group          TEXT         NOT NULL,
    cluster_name           TEXT         NOT NULL,
    cluster_group_id       BIGINT REFERENCES cluster_group_configs (id) ON DELETE SET NULL,
    cluster_id             BIGINT REFERENCES cluster_configs (id) ON DELETE SET NULL,
    engine_type            TEXT         NOT NULL,
    frontend_protocol      TEXT         NOT NULL,
    source_dialect         TEXT         NOT NULL,
    target_dialect         TEXT         NOT NULL,
    was_translated         BOOLEAN      NOT NULL DEFAULT false,
    username               TEXT,
    catalog                TEXT,
    db_name                TEXT,
    sql_preview            TEXT         NOT NULL DEFAULT '',
    translated_sql         TEXT,
    status                 TEXT         NOT NULL,
    routing_trace          JSONB,
    queue_duration_ms      BIGINT       NOT NULL DEFAULT 0,
    execution_duration_ms  BIGINT       NOT NULL DEFAULT 0,
    rows_returned          BIGINT,
    error_message          TEXT,
    backend_query_id       TEXT,
    cpu_time_ms            BIGINT,
    processed_rows         BIGINT,
    processed_bytes        BIGINT,
    physical_input_bytes   BIGINT,
    peak_memory_bytes      BIGINT,
    spilled_bytes          BIGINT,
    total_splits           INT,
    engine_elapsed_time_ms BIGINT,
    created_at             TIMESTAMPTZ  NOT NULL
);

CREATE INDEX query_records_created_at ON query_records (created_at DESC);
CREATE INDEX query_records_cluster_group ON query_records (cluster_group, created_at DESC);
CREATE INDEX query_records_status ON query_records (status, created_at DESC);
CREATE INDEX query_records_proxy_id ON query_records (proxy_query_id);
CREATE INDEX query_records_backend_id ON query_records (backend_query_id) WHERE backend_query_id IS NOT NULL;
CREATE INDEX query_records_cluster_group_id ON query_records (cluster_group_id, created_at DESC);
CREATE INDEX query_records_cluster_id ON query_records (cluster_id, created_at DESC);

CREATE TABLE cluster_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    cluster_name        TEXT         NOT NULL,
    group_name          TEXT         NOT NULL,
    engine_type         TEXT         NOT NULL,
    running_queries     INT          NOT NULL,
    queued_queries      INT          NOT NULL,
    max_running_queries INT          NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL
);

CREATE INDEX cluster_snapshots_recorded_at ON cluster_snapshots (recorded_at DESC);
CREATE INDEX cluster_snapshots_group ON cluster_snapshots (group_name, recorded_at DESC);

-- ---------------------------------------------------------------------------
-- User-defined scripts (translation fixups, routing helpers)
-- ---------------------------------------------------------------------------

CREATE TABLE user_scripts (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    description TEXT        NOT NULL DEFAULT '',
    kind        TEXT        NOT NULL CHECK (kind IN ('translation_fixup', 'routing')),
    body        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX user_scripts_kind_idx ON user_scripts (kind);
