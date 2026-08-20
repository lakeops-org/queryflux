-- Add query_tags JSONB column to query_records.
-- Stores effective tags (group defaults merged with session tags) for each query.
-- GIN index enables efficient tag-based filtering: WHERE query_tags @> '{"team":"eng"}'::jsonb

ALTER TABLE query_records ADD COLUMN query_tags JSONB NOT NULL DEFAULT '{}';

CREATE INDEX idx_query_records_query_tags ON query_records USING gin(query_tags);
