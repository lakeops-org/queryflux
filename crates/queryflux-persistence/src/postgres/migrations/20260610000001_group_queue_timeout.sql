-- Add configurable queue timeout for wire-protocol queries per cluster group.
-- NULL means wait indefinitely (legacy/default behavior).
ALTER TABLE cluster_group_configs
    ADD COLUMN queue_timeout_ms BIGINT;
