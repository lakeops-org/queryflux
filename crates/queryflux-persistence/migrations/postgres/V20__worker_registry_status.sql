-- What a worker reports beyond its revisions (#255): config source and staleness, sync
-- failures, history delivery and frontends. Free-form so workers can add fields without
-- a migration; the admin API reads it as `WorkerStatus`.
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS status JSONB NOT NULL DEFAULT '{}'::jsonb;
