-- no-transaction
DROP INDEX IF EXISTS query_records_proxy_id;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS query_records_proxy_query_id_unique
    ON query_records (proxy_query_id);
