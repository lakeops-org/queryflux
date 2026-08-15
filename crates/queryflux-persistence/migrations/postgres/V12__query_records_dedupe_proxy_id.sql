-- Remove duplicate terminal rows before adding a unique index (keep earliest id).
DELETE FROM query_records a
USING query_records b
WHERE a.proxy_query_id = b.proxy_query_id
  AND a.id > b.id;

-- Drop the non-unique index so the concurrent unique index can take its place.
DROP INDEX IF EXISTS query_records_proxy_id;
