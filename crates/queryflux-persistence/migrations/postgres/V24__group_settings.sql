-- Cluster group settings stored as one JSON object (`GroupSettings` in
-- queryflux-persistence), so a new group setting needs no migration.
-- Holds `maxExecutionSecs` and `capacityWaitTimeoutSecs`; absent keys mean the default.
ALTER TABLE cluster_group_configs
    ADD COLUMN settings JSONB NOT NULL DEFAULT '{}';
