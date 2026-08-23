-- Add default_tags to cluster group config.
-- Stored as a JSONB object: keys are tag names, values are strings or NULL (key-only tags).
ALTER TABLE cluster_group_configs
    ADD COLUMN default_tags JSONB NOT NULL DEFAULT '{}';
