ALTER TABLE cluster_group_configs
    ADD COLUMN IF NOT EXISTS circuit_breaker JSONB DEFAULT NULL;
