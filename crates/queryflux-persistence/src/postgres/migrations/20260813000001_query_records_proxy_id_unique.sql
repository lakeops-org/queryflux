-- One terminal audit row per proxy query id (poll completion vs cancel race).
DELETE FROM query_records a
USING query_records b
WHERE a.proxy_query_id = b.proxy_query_id
  AND a.id > b.id;

CREATE UNIQUE INDEX query_records_proxy_query_id_unique ON query_records (proxy_query_id);
