-- Indexes for the distributed-coordination hot paths.

-- CapacityStore::heartbeat renews every lease held by one replica
-- (UPDATE ... WHERE instance_id = $1) on a 60s timer; without this index the
-- write path degrades to a table scan as lease volume grows.
CREATE INDEX IF NOT EXISTS cluster_capacity_leases_instance
    ON cluster_capacity_leases (instance_id);

-- QueueCoordinator::try_claim / list_unclaimed treat claims older than a
-- cutoff as abandoned (claimed_at < $n); index only the claimed rows.
CREATE INDEX IF NOT EXISTS queued_queries_claimed_at
    ON queued_queries (claimed_at)
    WHERE claimed_by IS NOT NULL;
