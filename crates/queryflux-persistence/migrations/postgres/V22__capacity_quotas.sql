-- Capacity quotas (high-throughput plan, WS2). See crates/queryflux-persistence/src/quota.rs.
--
-- Each process that admits queries on quota (a worker, or a replica serving queries) has
-- one row per cluster: its demand, its current grant, and the ceiling the allocator must
-- keep counting until the holder confirms a smaller grant.
CREATE TABLE IF NOT EXISTS capacity_holders (
    cluster_name  TEXT        NOT NULL,
    instance_id   TEXT        NOT NULL,
    -- Reported by the holder.
    running       BIGINT      NOT NULL DEFAULT 0,
    waiting       BIGINT      NOT NULL DEFAULT 0,
    max_running   BIGINT      NOT NULL DEFAULT 0,
    -- The grant sequence the holder last applied.
    seen_seq      BIGINT      NOT NULL DEFAULT 0,
    reported_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Written by the allocator.
    granted       BIGINT      NOT NULL DEFAULT 0,
    grant_seq     BIGINT      NOT NULL DEFAULT 0,
    -- The largest grant the holder may still enforce: raised with every grant, lowered
    -- to the grant once the holder reports it has seen grant_seq.
    ceiling       BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (cluster_name, instance_id)
);

CREATE INDEX IF NOT EXISTS capacity_holders_instance ON capacity_holders (instance_id);
CREATE INDEX IF NOT EXISTS capacity_holders_reported ON capacity_holders (reported_at);

-- Sum of every holder's commit on the cluster, written by the allocator under the
-- cluster's advisory lock. Per-query leases may use only what is left.
ALTER TABLE cluster_capacity_counters
    ADD COLUMN IF NOT EXISTS quota_committed BIGINT NOT NULL DEFAULT 0;

-- Per-query acquire (V21) now leaves room for quotas: leased + quota_committed < max.
-- Without quotas, quota_committed stays 0 and this is the V21 function.
CREATE OR REPLACE FUNCTION queryflux_capacity_try_acquire(
    p_cluster TEXT, p_instance TEXT, p_query_id TEXT, p_max BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_leased BIGINT;
    v_quota BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('capacity:' || p_cluster)::bigint);
    SELECT leased, quota_committed INTO v_leased, v_quota
    FROM cluster_capacity_counters WHERE cluster_name = p_cluster;
    IF COALESCE(v_leased, 0) + COALESCE(v_quota, 0) >= p_max THEN
        RETURN FALSE;
    END IF;
    INSERT INTO cluster_capacity_leases (query_id, cluster_name, instance_id)
    VALUES (p_query_id, p_cluster, p_instance)
    ON CONFLICT (query_id) DO NOTHING;
    RETURN FOUND;
END
$$;
