-- The worker that ran a query (#255). NULL for queries this process ran itself.
-- Join to worker_registry on name for the worker's labels.
ALTER TABLE query_records ADD COLUMN IF NOT EXISTS worker TEXT;

CREATE INDEX IF NOT EXISTS query_records_worker
    ON query_records (worker, created_at DESC) WHERE worker IS NOT NULL;
