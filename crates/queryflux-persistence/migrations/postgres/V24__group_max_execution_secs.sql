-- Per-group execution limit (`maxExecutionSecs`): queries running longer are cancelled.
-- NULL = no limit.
ALTER TABLE cluster_group_configs
    ADD COLUMN max_execution_secs BIGINT;
