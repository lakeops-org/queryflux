ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS was_rewritten BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS rewritten_sql TEXT;
