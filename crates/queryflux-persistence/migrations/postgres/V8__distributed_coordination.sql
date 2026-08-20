-- Distributed multi-replica coordination: config revision tracking,
-- global capacity leases, and single-owner queue claims.

-- ---------------------------------------------------------------------------
-- Config revision — singleton counter tracking the global config revision.
-- Every admin write that mutates persisted config bumps this value so that
-- other QueryFlux replicas can detect the change via polling or LISTEN/NOTIFY.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS config_revision (
    id       BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (id),
    -- Read as u64 in Rust; reject negative writes (only possible via manual
    -- tampering — the application only ever increments from 0).
    revision BIGINT      NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO config_revision (id, revision) VALUES (TRUE, 0) ON CONFLICT DO NOTHING;

-- Trigger function: after the revision row is updated, emit a NOTIFY on the
-- 'config_revision_changed' channel with the new revision as payload.
CREATE OR REPLACE FUNCTION notify_config_revision_changed()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('config_revision_changed', NEW.revision::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER config_revision_notify
    AFTER UPDATE ON config_revision
    FOR EACH ROW
    EXECUTE FUNCTION notify_config_revision_changed();

-- ---------------------------------------------------------------------------
-- Capacity leases — each row represents a running query holding a capacity
-- slot. Used by CapacityStore to enforce global max_running_queries across
-- replicas. Replicas renew heartbeat_at for their leases on a timer; leases
-- whose owner stops heartbeating are expired.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cluster_capacity_leases (
    query_id      TEXT        PRIMARY KEY,
    cluster_name  TEXT        NOT NULL,
    instance_id   TEXT        NOT NULL,
    acquired_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX cluster_capacity_leases_cluster
    ON cluster_capacity_leases (cluster_name);

CREATE INDEX cluster_capacity_leases_heartbeat
    ON cluster_capacity_leases (heartbeat_at);

-- CapacityStore::heartbeat renews every lease held by one replica
-- (UPDATE ... WHERE instance_id = $1) on a 60s timer.
CREATE INDEX cluster_capacity_leases_instance
    ON cluster_capacity_leases (instance_id);

-- O(1) admission counter per cluster: try_acquire increments under the row
-- lock in a single statement instead of COUNT(*) under an advisory lock.
-- The leases table stays the ground truth for crash recovery; the single-owner
-- sweep reconciles these counters from it on every expiry cycle.
CREATE TABLE IF NOT EXISTS cluster_capacity_counters (
    cluster_name TEXT   PRIMARY KEY,
    running      BIGINT NOT NULL DEFAULT 0 CHECK (running >= 0)
);

-- ---------------------------------------------------------------------------
-- Queue claims — claim columns on queued_queries so only one replica
-- processes a given queued query.
-- ---------------------------------------------------------------------------

ALTER TABLE queued_queries
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

-- try_claim sets both columns and release_claim clears both; enforce the
-- pairing so the stale-claim predicates (claimed_at < cutoff) can never see
-- a half-written claim from a manual fix-up. No backfill needed: the columns
-- are created in this migration, so no inconsistent rows can pre-exist.
ALTER TABLE queued_queries
    ADD CONSTRAINT queued_queries_claim_pair_chk
    CHECK ((claimed_by IS NULL) = (claimed_at IS NULL));

CREATE INDEX IF NOT EXISTS queued_queries_unclaimed
    ON queued_queries (created_at)
    WHERE claimed_by IS NULL;

-- QueueCoordinator::try_claim / list_unclaimed treat claims older than a
-- cutoff as abandoned (claimed_at < $n); index only the claimed rows.
CREATE INDEX IF NOT EXISTS queued_queries_claimed_at
    ON queued_queries (claimed_at)
    WHERE claimed_by IS NOT NULL;

-- Admission fairness: the gate asks "how many older, actively-polling queued
-- queries does this group have?" before a query may take a freed slot.
-- cluster_group and last_accessed are promoted out of the JSONB blob so the
-- gate is one indexed query; rows written before this migration are backfilled.
ALTER TABLE queued_queries
    ADD COLUMN IF NOT EXISTS cluster_group TEXT,
    ADD COLUMN IF NOT EXISTS last_accessed TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE queued_queries
SET cluster_group = data->>'cluster_group',
    last_accessed = COALESCE((data->>'last_accessed')::timestamptz, last_accessed)
WHERE cluster_group IS NULL;

CREATE INDEX IF NOT EXISTS queued_queries_group_active
    ON queued_queries (cluster_group, last_accessed);
