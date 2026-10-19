-- Waiting for capacity (high-throughput plan, WS3).
--
-- A query denied capacity used to retry on a timer (100 ms doubling to 2 s): a freed slot
-- could sit idle for up to 2 s, and every waiter retried whether or not anything freed.
-- Now a denied acquire leaves a waiter row, and when leases are released (or the
-- allocator frees quota), the freed slots go straight to the oldest live waiters, as
-- leases created for them. Only those queries are notified
-- (`queryflux_capacity_granted`, payload: their query ids), so a release wakes exactly
-- as many waiters as it freed slots, first come first served across the fleet.
--
-- A lease created for a waiter is unclaimed until that query's next acquire claims it.
-- If the query gave up first, the expiry sweep deletes the unclaimed lease after 10 s.

ALTER TABLE cluster_capacity_leases
    ADD COLUMN IF NOT EXISTS claimed BOOLEAN NOT NULL DEFAULT TRUE;

-- Unclaimed leases are few and short-lived; acquires look for stale ones per cluster.
CREATE INDEX IF NOT EXISTS cluster_capacity_leases_unclaimed
    ON cluster_capacity_leases (cluster_name, acquired_at) WHERE NOT claimed;

-- The cluster's limit as last passed by an acquire, so a release can tell how many
-- slots it freed.
ALTER TABLE cluster_capacity_counters
    ADD COLUMN IF NOT EXISTS max_running BIGINT;

-- Capacity the quota allocator wants to grant but leases hold (see quota.rs,
-- `quota_wanted`). Per-query acquires and hand-overs leave it alone, so it goes to quotas
-- as leases end, instead of every freed slot going back to the line.
ALTER TABLE cluster_capacity_counters
    ADD COLUMN IF NOT EXISTS quota_wanted BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS capacity_waiters (
    query_id      TEXT        PRIMARY KEY,
    cluster_name  TEXT        NOT NULL,
    instance_id   TEXT        NOT NULL,
    -- Set when the query actually waits for a hand-over (wait_for_grant). Only parked
    -- waiters are handed slots or hold the line: an older client that releases its query
    -- right after a denial instead of waiting never parks, so slots aren't handed to a
    -- query that is about to give them straight back.
    parked        BOOLEAN     NOT NULL DEFAULT FALSE,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Each retry refreshes it. A waiter that stops retrying (gave up, client gone) stops
    -- counting after a few seconds and is deleted by the expiry sweep.
    refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS capacity_waiters_cluster
    ON capacity_waiters (cluster_name, enqueued_at);

-- How long a waiter counts without retrying.
CREATE OR REPLACE FUNCTION queryflux_capacity_waiter_ttl() RETURNS INTERVAL
LANGUAGE sql IMMUTABLE AS $$ SELECT interval '6 seconds' $$;

-- Give a cluster's free slots to its oldest live waiters. Takes the cluster's advisory
-- lock, like every acquire, so it never grants past the limit.
CREATE OR REPLACE FUNCTION queryflux_capacity_grant_waiters(p_cluster TEXT) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    v_free BIGINT;
    v_ids TEXT[];
    v_payload TEXT := '';
    v_id TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || p_cluster)::bigint);
    SELECT max_running - leased - quota_committed - quota_wanted INTO v_free
    FROM cluster_capacity_counters WHERE cluster_name = p_cluster;
    IF v_free IS NULL OR v_free <= 0 THEN
        RETURN 0;
    END IF;
    WITH chosen AS (
        DELETE FROM capacity_waiters
        WHERE query_id IN (
            SELECT query_id FROM capacity_waiters
            WHERE cluster_name = p_cluster
              AND parked
              AND refreshed_at > now() - queryflux_capacity_waiter_ttl()
            ORDER BY enqueued_at, query_id
            LIMIT v_free
        )
        RETURNING query_id, instance_id
    ),
    granted AS (
        INSERT INTO cluster_capacity_leases (query_id, cluster_name, instance_id, claimed)
        SELECT query_id, p_cluster, instance_id, FALSE FROM chosen
        ON CONFLICT (query_id) DO NOTHING
        RETURNING query_id
    )
    SELECT array_agg(query_id) INTO v_ids FROM granted;
    IF v_ids IS NULL THEN
        RETURN 0;
    END IF;
    -- Notification payloads are limited to 8000 bytes: send in pieces.
    FOREACH v_id IN ARRAY v_ids LOOP
        IF length(v_payload) + length(v_id) > 7000 THEN
            PERFORM pg_notify('queryflux_capacity_granted', v_payload);
            v_payload := '';
        END IF;
        v_payload := v_payload || CASE WHEN v_payload = '' THEN '' ELSE ' ' END || v_id;
    END LOOP;
    IF v_payload <> '' THEN
        PERFORM pg_notify('queryflux_capacity_granted', v_payload);
    END IF;
    RETURN array_length(v_ids, 1);
END
$$;

-- V21's delete trigger, now handing freed slots to waiters. Takes each cluster's advisory
-- lock before its counter row, in name order: the same order as acquires and the
-- allocator, so none of them deadlock with a release.
CREATE OR REPLACE FUNCTION queryflux_capacity_leases_deleted() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT cluster_name, COUNT(*) AS n FROM old_leases
             GROUP BY cluster_name ORDER BY cluster_name LOOP
        PERFORM pg_advisory_xact_lock(hashtext('capacity:' || r.cluster_name)::bigint);
        UPDATE cluster_capacity_counters
        SET leased = GREATEST(leased - r.n, 0)
        WHERE cluster_name = r.cluster_name;
        PERFORM queryflux_capacity_grant_waiters(r.cluster_name);
    END LOOP;
    RETURN NULL;
END
$$;

-- V22's acquire, first come first served, and claiming leases created for waiters.
--
-- - A lease this query already holds on the cluster (created for it while it waited)
--   is claimed and granted.
-- - Otherwise grant only if fewer live waiters were enqueued before this query than
--   there are free slots. A query never seen before counts as enqueued now, so it
--   doesn't jump ahead of queries already waiting.
-- - Otherwise record or refresh this query as a waiter.
CREATE OR REPLACE FUNCTION queryflux_capacity_try_acquire(
    p_cluster TEXT, p_instance TEXT, p_query_id TEXT, p_max BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_leased BIGINT;
    v_quota BIGINT;
    v_limit BIGINT;
    v_free BIGINT;
    v_since TIMESTAMPTZ;
    v_older BIGINT;
BEGIN
    -- A lease created for this query on another cluster (it waited there, then routing
    -- picked another member of the group): give it back first. This takes that
    -- cluster's lock before this one's, out of name order; it is rare, and Postgres
    -- breaks the deadlock it could cause by failing one call, which the caller retries.
    DELETE FROM cluster_capacity_leases
    WHERE query_id = p_query_id AND cluster_name <> p_cluster AND NOT claimed;

    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || p_cluster)::bigint);

    -- Slots handed to queries that never came back for them, older than 10 s. The
    -- expiry sweep removes these too, but only every couple of minutes.
    DELETE FROM cluster_capacity_leases
    WHERE cluster_name = p_cluster AND NOT claimed
      AND acquired_at < now() - interval '10 seconds';

    UPDATE cluster_capacity_leases SET claimed = TRUE
    WHERE query_id = p_query_id AND cluster_name = p_cluster;
    IF FOUND THEN
        DELETE FROM capacity_waiters WHERE query_id = p_query_id;
        RETURN TRUE;
    END IF;

    SELECT leased, quota_committed + quota_wanted, max_running INTO v_leased, v_quota, v_limit
    FROM cluster_capacity_counters WHERE cluster_name = p_cluster;
    IF v_limit IS DISTINCT FROM p_max THEN
        INSERT INTO cluster_capacity_counters (cluster_name, max_running)
        VALUES (p_cluster, p_max)
        ON CONFLICT (cluster_name) DO UPDATE SET max_running = EXCLUDED.max_running;
    END IF;
    v_free := p_max - COALESCE(v_leased, 0) - COALESCE(v_quota, 0);

    SELECT enqueued_at INTO v_since FROM capacity_waiters WHERE query_id = p_query_id;
    v_since := COALESCE(v_since, now());

    IF v_free > 0 THEN
        -- Only whether at least v_free waiters are ahead matters, so stop counting
        -- there: the cost follows the free slots, not the length of the line.
        SELECT COUNT(*) INTO v_older FROM (
            SELECT 1 FROM capacity_waiters
            WHERE cluster_name = p_cluster
              AND parked
              AND query_id <> p_query_id
              AND enqueued_at < v_since
              AND refreshed_at > now() - queryflux_capacity_waiter_ttl()
            LIMIT v_free
        ) ahead;
        IF v_older < v_free THEN
            INSERT INTO cluster_capacity_leases (query_id, cluster_name, instance_id)
            VALUES (p_query_id, p_cluster, p_instance)
            ON CONFLICT (query_id) DO NOTHING;
            IF FOUND THEN
                DELETE FROM capacity_waiters WHERE query_id = p_query_id;
                RETURN TRUE;
            END IF;
            RETURN FALSE;
        END IF;
    END IF;

    INSERT INTO capacity_waiters (query_id, cluster_name, instance_id, enqueued_at, refreshed_at)
    VALUES (p_query_id, p_cluster, p_instance, v_since, now())
    ON CONFLICT (query_id) DO UPDATE
        SET refreshed_at = now(),
            cluster_name = EXCLUDED.cluster_name,
            instance_id = EXCLUDED.instance_id;
    RETURN FALSE;
END
$$;

-- Release queries: leave the line, then drop their leases.
--
-- First take the advisory lock of every cluster involved, in name order. A hand-over
-- (queryflux_capacity_grant_waiters) holds a cluster's lock while it deletes waiter rows,
-- so a release must hold it before touching those rows, or the two deadlock.
--
-- Then two statements, so the lease delete takes a fresh snapshot: if a release that
-- started earlier is handing one of these queries a slot, the waiter delete waits for
-- it, and the lease delete then sees and removes the lease it created. (One statement
-- would use one snapshot and miss it, leaving the slot held by nobody.)
CREATE OR REPLACE FUNCTION queryflux_capacity_release(p_query_ids TEXT[]) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_n BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || c)::bigint)
    FROM (
        SELECT cluster_name AS c FROM capacity_waiters WHERE query_id = ANY(p_query_ids)
        UNION
        SELECT cluster_name FROM cluster_capacity_leases WHERE query_id = ANY(p_query_ids)
        ORDER BY 1
    ) clusters;
    DELETE FROM capacity_waiters WHERE query_id = ANY(p_query_ids);
    DELETE FROM cluster_capacity_leases WHERE query_id = ANY(p_query_ids);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END
$$;

-- A parked query renewing its wait: refresh its place in line, and if the cluster has
-- free capacity that nobody was handed (its limit went up, or capacity freed without a
-- release), hand it out now. Returns whether the query holds a lease on the cluster.
-- Cheap while the cluster is full: no lock, one row update and two lookups.
CREATE OR REPLACE FUNCTION queryflux_capacity_park(p_cluster TEXT, p_query_id TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_free BIGINT;
BEGIN
    UPDATE capacity_waiters SET parked = TRUE, refreshed_at = now()
    WHERE query_id = p_query_id;
    SELECT max_running - leased - quota_committed - quota_wanted INTO v_free
    FROM cluster_capacity_counters WHERE cluster_name = p_cluster;
    IF v_free > 0 THEN
        PERFORM queryflux_capacity_grant_waiters(p_cluster);
    END IF;
    RETURN EXISTS (
        SELECT 1 FROM cluster_capacity_leases
        WHERE query_id = p_query_id AND cluster_name = p_cluster
    );
END
$$;
