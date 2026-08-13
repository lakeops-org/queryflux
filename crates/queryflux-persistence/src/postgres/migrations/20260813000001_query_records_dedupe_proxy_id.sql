-- Remove duplicate terminal rows before adding a unique index (keep earliest id).
DELETE FROM query_records a
USING query_records b
WHERE a.proxy_query_id = b.proxy_query_id
  AND a.id > b.id;
