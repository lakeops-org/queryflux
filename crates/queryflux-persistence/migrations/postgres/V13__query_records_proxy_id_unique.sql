-- Refinery runs each migration in a transaction; CREATE INDEX CONCURRENTLY is not allowed.
CREATE UNIQUE INDEX IF NOT EXISTS query_records_proxy_query_id_unique
    ON query_records (proxy_query_id);
