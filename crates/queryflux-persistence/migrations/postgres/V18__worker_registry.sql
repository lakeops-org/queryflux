-- Workers that pull config from this control plane (#255): what each reported in its
-- last registration or heartbeat.
CREATE TABLE IF NOT EXISTS worker_registry (
    name                TEXT PRIMARY KEY,
    version             TEXT        NOT NULL DEFAULT '',
    labels              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    coordination_mode   TEXT        NOT NULL DEFAULT 'controlPlane',
    downloaded_revision TEXT,
    active_revision     TEXT,
    last_error          TEXT,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT now()
);
