-- no-transaction
-- Single statement required: Postgres starts an implicit transaction for
-- multi-statement scripts, which rejects CREATE INDEX CONCURRENTLY.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS query_records_proxy_query_id_unique
    ON query_records (proxy_query_id);
