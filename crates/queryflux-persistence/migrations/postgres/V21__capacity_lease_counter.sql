-- Capacity admission in one round trip (high-throughput plan, WS1).
--
-- Before: try_acquire ran BEGIN, pg_advisory_xact_lock, COUNT(*) over the cluster's
-- leases plus INSERT, then COMMIT: four round trips with the per-cluster lock held across
-- three of them, and a scan that grows with the number of running queries.
--
-- Now `cluster_capacity_counters.leased` holds each cluster's lease count, kept exact by
-- statement triggers on `cluster_capacity_leases`, so every writer (this version's
-- functions, an older replica's plain INSERT and DELETE, the expiry sweep) updates it.
-- `queryflux_capacity_try_acquire` checks it and inserts the lease in one call.

-- Block lease writes from replicas still running the previous version while the
-- triggers are created and the counter is backfilled, so no lease is missed or counted
-- twice. Held until this migration commits; lease writes are short, so they just wait.
LOCK TABLE cluster_capacity_leases IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE cluster_capacity_counters
    ADD COLUMN IF NOT EXISTS leased BIGINT NOT NULL DEFAULT 0;

-- Both triggers update counter rows one at a time in cluster-name order. A statement that
-- touches several clusters (a batch of releases, the expiry sweep) then always locks
-- their rows in the same order, so two such statements can't deadlock.
CREATE OR REPLACE FUNCTION queryflux_capacity_leases_inserted() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT cluster_name, COUNT(*) AS n FROM new_leases
             GROUP BY cluster_name ORDER BY cluster_name LOOP
        INSERT INTO cluster_capacity_counters (cluster_name, leased)
        VALUES (r.cluster_name, r.n)
        ON CONFLICT (cluster_name) DO UPDATE
            SET leased = cluster_capacity_counters.leased + EXCLUDED.leased;
    END LOOP;
    RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION queryflux_capacity_leases_deleted() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT cluster_name, COUNT(*) AS n FROM old_leases
             GROUP BY cluster_name ORDER BY cluster_name LOOP
        -- GREATEST: a counter that drifted low (rows removed with TRUNCATE, which fires
        -- no DELETE trigger) never goes negative. The expiry sweep corrects drift.
        UPDATE cluster_capacity_counters
        SET leased = GREATEST(leased - r.n, 0)
        WHERE cluster_name = r.cluster_name;
    END LOOP;
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS cluster_capacity_leases_count_insert ON cluster_capacity_leases;
CREATE TRIGGER cluster_capacity_leases_count_insert
    AFTER INSERT ON cluster_capacity_leases
    REFERENCING NEW TABLE AS new_leases
    FOR EACH STATEMENT EXECUTE FUNCTION queryflux_capacity_leases_inserted();

DROP TRIGGER IF EXISTS cluster_capacity_leases_count_delete ON cluster_capacity_leases;
CREATE TRIGGER cluster_capacity_leases_count_delete
    AFTER DELETE ON cluster_capacity_leases
    REFERENCING OLD TABLE AS old_leases
    FOR EACH STATEMENT EXECUTE FUNCTION queryflux_capacity_leases_deleted();

-- Backfill from the leases that exist now.
UPDATE cluster_capacity_counters SET leased = 0;
INSERT INTO cluster_capacity_counters (cluster_name, leased)
SELECT cluster_name, COUNT(*) FROM cluster_capacity_leases GROUP BY cluster_name
ON CONFLICT (cluster_name) DO UPDATE SET leased = EXCLUDED.leased;

-- Grant a lease if the cluster has fewer than `p_max` leases. One round trip.
--
-- Still takes the per-cluster advisory lock that the previous version's try_acquire
-- takes, so a fleet in the middle of a rolling upgrade (some replicas counting rows, some
-- reading the counter) never admits past the limit. The lock is now held only for this
-- call and its commit, not across client round trips. Each statement in a plpgsql
-- function takes a new snapshot, so the counter read after the lock sees every lease
-- committed before it.
--
-- Returns false when the cluster is full, and when `p_query_id` already holds a lease
-- (the previous version's behavior; callers never reuse a query id).
CREATE OR REPLACE FUNCTION queryflux_capacity_try_acquire(
    p_cluster TEXT, p_instance TEXT, p_query_id TEXT, p_max BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_leased BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || p_cluster)::bigint);
    SELECT leased INTO v_leased FROM cluster_capacity_counters WHERE cluster_name = p_cluster;
    IF COALESCE(v_leased, 0) >= p_max THEN
        RETURN FALSE;
    END IF;
    INSERT INTO cluster_capacity_leases (query_id, cluster_name, instance_id)
    VALUES (p_query_id, p_cluster, p_instance)
    ON CONFLICT (query_id) DO NOTHING;
    RETURN FOUND;
END
$$;

-- Set a cluster's counter to its actual lease count. Called by the expiry sweep for
-- clusters whose counter looks wrong. The counter row is locked first, in its own
-- statement, so the count below sees every lease whose trigger already ran, and a
-- writer whose trigger is still waiting for the row applies its change afterwards.
CREATE OR REPLACE FUNCTION queryflux_capacity_recount(p_cluster TEXT) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_count BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || p_cluster)::bigint);
    PERFORM 1 FROM cluster_capacity_counters WHERE cluster_name = p_cluster FOR UPDATE;
    SELECT COUNT(*) INTO v_count FROM cluster_capacity_leases WHERE cluster_name = p_cluster;
    UPDATE cluster_capacity_counters SET leased = v_count WHERE cluster_name = p_cluster;
    RETURN v_count;
END
$$;
