-- The fast fingerprint of the original SQL: the key routing rules are looked up by.
ALTER TABLE query_records ADD COLUMN IF NOT EXISTS fast_hash BIGINT;
