-- Scope Postgres's database-wide coordination primitives to the current schema, so
-- several QueryFlux deployments can share one database with a schema each
-- (`persistence.schema`): advisory locks and NOTIFY channels are database-scoped, not
-- schema-scoped. In `public` every name stays exactly as before, so existing deployments
-- and rolling upgrades keep the same lock keys and channels.

-- The advisory-lock key for `name` in this schema.
CREATE OR REPLACE FUNCTION queryflux_lock_key(name TEXT) RETURNS BIGINT
LANGUAGE sql STABLE AS $$
    SELECT hashtext(CASE WHEN current_schema() = 'public' THEN name
                         ELSE current_schema() || ':' || name END)::bigint
$$;

-- The NOTIFY channel for `base` in this schema. Channel names are limited to 63 bytes,
-- so a non-public schema is shortened to a hash prefix.
CREATE OR REPLACE FUNCTION queryflux_channel(base TEXT) RETURNS TEXT
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN current_schema() = 'public' THEN base
                ELSE 'qf_' || substr(md5(current_schema()), 1, 12) || '_' || base END
$$;

-- notify_config_revision_changed: latest definition from V8__distributed_coordination.sql, with scoped lock keys and channels.
CREATE OR REPLACE FUNCTION notify_config_revision_changed()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(queryflux_channel('config_revision_changed'), NEW.revision::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- queryflux_capacity_recount: latest definition from V21__capacity_lease_counter.sql, with scoped lock keys and channels.
CREATE OR REPLACE FUNCTION queryflux_capacity_recount(p_cluster TEXT) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_count BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(queryflux_lock_key('capacity:' || p_cluster));
    PERFORM 1 FROM cluster_capacity_counters WHERE cluster_name = p_cluster FOR UPDATE;
    SELECT COUNT(*) INTO v_count FROM cluster_capacity_leases WHERE cluster_name = p_cluster;
    UPDATE cluster_capacity_counters SET leased = v_count WHERE cluster_name = p_cluster;
    RETURN v_count;
END
$$;

-- queryflux_capacity_try_acquire: latest definition from V23__capacity_waiters.sql, with scoped lock keys and channels.
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

    PERFORM pg_advisory_xact_lock(queryflux_lock_key('capacity:' || p_cluster));

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

-- queryflux_capacity_grant_waiters: latest definition from V23__capacity_waiters.sql, with scoped lock keys and channels.
CREATE OR REPLACE FUNCTION queryflux_capacity_grant_waiters(p_cluster TEXT) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    v_free BIGINT;
    v_ids TEXT[];
    v_payload TEXT := '';
    v_id TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(queryflux_lock_key('capacity:' || p_cluster));
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
            PERFORM pg_notify(queryflux_channel('queryflux_capacity_granted'), v_payload);
            v_payload := '';
        END IF;
        v_payload := v_payload || CASE WHEN v_payload = '' THEN '' ELSE ' ' END || v_id;
    END LOOP;
    IF v_payload <> '' THEN
        PERFORM pg_notify(queryflux_channel('queryflux_capacity_granted'), v_payload);
    END IF;
    RETURN array_length(v_ids, 1);
END
$$;

-- queryflux_capacity_leases_deleted: latest definition from V23__capacity_waiters.sql, with scoped lock keys and channels.
CREATE OR REPLACE FUNCTION queryflux_capacity_leases_deleted() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT cluster_name, COUNT(*) AS n FROM old_leases
             GROUP BY cluster_name ORDER BY cluster_name LOOP
        PERFORM pg_advisory_xact_lock(queryflux_lock_key('capacity:' || r.cluster_name));
        UPDATE cluster_capacity_counters
        SET leased = GREATEST(leased - r.n, 0)
        WHERE cluster_name = r.cluster_name;
        PERFORM queryflux_capacity_grant_waiters(r.cluster_name);
    END LOOP;
    RETURN NULL;
END
$$;

-- queryflux_capacity_release: latest definition from V23__capacity_waiters.sql, with scoped lock keys and channels.
CREATE OR REPLACE FUNCTION queryflux_capacity_release(p_query_ids TEXT[]) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_n BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(queryflux_lock_key('capacity:' || c))
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
