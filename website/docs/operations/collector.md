---
title: Query collector
description: Run queryflux-collector to read what your engines know about each query run (timings, bytes, CPU, memory) and store it next to QueryFlux's own query history.
image: img/queryflux-hero-banner.png
---

# Query collector

`queryflux-collector` reads what an engine knows about each query it ran (queue and planning time, CPU, bytes scanned, peak memory, spill) and sends it to the QueryFlux control plane. Runs QueryFlux routed are linked to the query history record QueryFlux wrote for them; queries sent straight to the engine are kept too.

The collector only polls. Engines are not reconfigured, and QueryFlux runs queries exactly as it would without it: no extra statements, no tags, no changes to the SQL.

It is a separate process from the proxy and the [site agent](./site-agent.md). Run it only where you want engine details; it needs no workers and can run on its own.

## Supported engines

| Engine | Reads | Linked to QueryFlux queries by |
|---|---|---|
| Trino | `GET /v1/query` on the coordinator | Trino query id |

More engines follow. A cluster whose engine has no collector yet is skipped with a warning.

Trino keeps finished queries only until `query.min-expire-age` (15 minutes by default). Keep `pollSecs` well below that, or runs are missed.

## Configuration

```yaml
collector:
  name: site-a-collector        # owns this collector's watermarks upstream
  upstream:                     # the same block as a worker's controlPlane
    url: https://queryflux-control-plane:9000
    auth: { mode: sharedSecret, sharedSecret: <the control plane's workerApi.sharedSecret> }
  pollSecs: 30                  # how often each cluster is read (default 30)
  overlapSecs: 120              # re-read before the watermark, for late runs (default 120)
  lookbackSecs: 900             # first read of a cluster with no watermark (default 900)
clusters:
  trino-a:                      # use the same name as in QueryFlux, so runs link to its queries
    engine: trino
    endpoint: http://trino:8080
    auth: { type: basic, username: monitor, password: <password> }
```

The Trino user needs permission to see every user's queries (with file-based access control, a `query` rule that allows `view` for that user), or only its own queries are returned.

The control plane must run with Postgres persistence and `workerApi.enabled: true`. The collector authenticates with `workerApi.sharedSecret`.

## Running it

```bash
queryflux-collector --config collector.yaml
```

The QueryFlux image includes it:

```bash
docker run --entrypoint /app/queryflux-collector \
  -v $PWD/collector.yaml:/etc/queryflux/collector.yaml \
  ghcr.io/lakeops-org/queryflux --config /etc/queryflux/collector.yaml
```

Run one replica per set of clusters. The collector keeps no state on disk: its watermarks are stored by the control plane together with each batch, so a restarted collector resumes after the last stored batch.

## Where the data goes

- `query_runs`: one row per run, keyed by cluster and the engine's query id. `proxy_query_id` is set when the run belongs to a query QueryFlux routed.
- `collector_watermarks`: where each collector last stopped, per cluster.

```sql
-- QueryFlux's view of a query next to the engine's
SELECT r.proxy_query_id, r.execution_duration_ms, q.cpu_ms, q.bytes_scanned, q.peak_memory_bytes
FROM query_records r
JOIN query_runs q ON q.proxy_query_id = r.proxy_query_id;
```
