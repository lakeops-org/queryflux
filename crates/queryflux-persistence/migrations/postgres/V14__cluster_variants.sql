-- Cluster variants: allow a single cluster config to expand into multiple
-- runtime clusters targeting different sub-resources (e.g. Snowflake warehouses).

ALTER TABLE cluster_configs
  ADD COLUMN IF NOT EXISTS variants JSONB NOT NULL DEFAULT '[]'::jsonb;
