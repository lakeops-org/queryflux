---
title: Worker mode
description: Run QueryFlux workers next to your applications. They take config, capacity limits and query history from a control plane and never connect to the database.
image: img/queryflux-hero-banner.png
---

# Worker mode

In worker mode, one QueryFlux deployment owns the config and the others run queries with it:

- The **control plane** owns the config in Postgres. It serves Studio and the admin API, and can run queries itself.
- **Workers** run close to the applications that query them: as a sidecar in each application pod, or as a small service per team or site. A worker holds no config of its own and never connects to Postgres. Instead it:
  - pulls its config from the control plane;
  - shares capacity limits through the control plane;
  - sends its query history to the control plane.

```text
             ┌──────────────────────────── control plane ────────────────────────────┐
  Studio ───►│ admin API   /worker/config   /worker/capacity/*   /worker/query-records │──► Postgres
             └──────▲───────────────▲──────────────────▲─────────────────────▲─────────┘
                    │ config        │ register,        │ capacity leases     │ query history
                    │ (long-poll)   │ heartbeat        │                     │
             ┌──────┴───────────────┴──────────────────┴─────────────────────┴───┐
             │ worker (sidecar or shared service)  ── SQL ──►  Trino, DuckDB, …   │
             └───────────────────────────────────────────────────────────────────┘
                    ▲
                    │ Trino HTTP, Postgres wire, MySQL wire, Flight SQL, …
               application
```

Running QueryFlux without workers works exactly as before. Worker mode is opt-in, through `queryflux.role`.

## When to use it

| Setup | Use |
|-------|-----|
| One QueryFlux deployment, every client reaches it | `role: standalone` (the default). See [Multi-replica operations](./multi-replica) for HA. |
| QueryFlux next to each application, or in networks that cannot reach the database | `role: controlPlane` plus workers |
| Many small QueryFlux deployments that should share one config, one set of limits and one query history | `role: controlPlane` plus workers |

## Roles

| `queryflux.role` | Owns config | Connects to Postgres | Admin API and Studio | Serves workers |
|------------------|-------------|----------------------|----------------------|----------------|
| `standalone` (default) | Yes (YAML or Postgres) | Optional | Yes | With `workerApi.enabled` |
| `controlPlane` | Yes | Yes, for shared capacity | Yes | Yes |
| `worker` | No, pulled from the control plane | **Never** | No: ops listener only | No |

A standalone process with `workerApi.enabled: true` can serve workers too. Without Postgres it serves the config it started with, and workers coordinate with `coordination.mode: local`.

## Configuration

### Control plane

```yaml
queryflux:
  role: controlPlane
  persistence:
    type: postgres
    url: postgres://queryflux:<password>@postgres:5432/queryflux
  adminApi:
    port: 9000
  workerApi:
    enabled: true
    sharedSecret: <long-random-secret>
    history:
      endpoint: null          # optional: workers send history here instead of to the control plane
      redaction: full         # full | maskLiterals | metadata
```

The worker API lives on the admin port, under `/worker/*`. It uses its own bearer secret and does not accept the admin API's credentials.

### Worker

```yaml
queryflux:
  role: worker
  workerName: checkout-7f9c      # optional; defaults to the hostname (the pod name)
  workerLabels: { team: checkout }
  controlPlane:
    url: https://queryflux-control:9000
    auth: { mode: sharedSecret, sharedSecret: <long-random-secret> }
    pollTimeoutSecs: 30          # long-poll hold time
    heartbeatIntervalSecs: 10
    allowInsecureHttp: false     # local demos only: snapshots carry credentials
    peerBootstrap:               # optional: start from a running worker's config
      address: queryflux-workers:9000
      afterSecs: 10
      maxAgeSecs: 86400
  coordination:
    mode: controlPlane           # controlPlane | local
    onUnavailable: failClosed    # failClosed | localLimits
  history:
    endpoint: null               # optional override of the snapshot's endpoint
    spoolMaxRecords: 50000
  frontends:
    trinoHttp: { enabled: true, port: 8080 }
  adminApi:
    port: 9000                   # ops listener: /health, /readyz, /metrics, /worker/status
```

Everything the control plane owns is rejected in a worker config: `persistence` (other than `inMemory`), clusters, groups, routers, `routingFallback`, `auth`, `authorization`, `catalogProvider`, `guardrails`, `accessControl`, `translation.mode`, `translation.errorOnUnsupported` and `workerApi`. Other `translation` settings (`pythonScripts`, `schemaResolutionTimeoutMs`) stay local to the worker for now. `queryflux validate` lists any such settings it finds.

QueryFlux does not expand environment variables in its config file. Mount a worker config that holds the shared secret from a Kubernetes Secret.

## How config reaches workers

1. **Snapshot.** The control plane serves `GET /worker/config`. The response is everything a worker needs to compile its live config: clusters, groups, routers, translation scripts, settings, the history policy and the translation failure policy (`translation.mode` and `errorOnUnsupported`, applied without a restart). Its `revision` is a content hash, so the same config has the same revision on every control plane replica.
2. **Change detection.** The worker long-polls `GET /worker/config/wait?since=<revision>`. The control plane answers as soon as its config changes, usually in under a second. Otherwise it answers after `pollTimeoutSecs` with `304 Not Modified`.
3. **Activation.** A new snapshot is verified (schema version, revision hash) and compiled strictly: an invalid adapter or script rejects the whole snapshot. Only then is it swapped in atomically. A rejected snapshot leaves the previous one active, and the worker reports the error in its heartbeat and in `/worker/status`.
4. **Before the first snapshot.** The worker accepts connections, but queries get a retryable "not configured" error:

   | Protocol | Error |
   |----------|-------|
   | Trino HTTP | `503` with `Retry-After` |
   | Postgres wire | `57P03` |
   | MySQL wire | `1053` |
   | Flight SQL | `UNAVAILABLE` |

   `/readyz` returns `503` until the first snapshot is active. Use it as the startup probe.

While the control plane is unreachable, a worker keeps serving the last config it activated, and retries with backoff (with jitter, up to 60 seconds).

### Starting while the control plane is down

Any config a worker starts with while the control plane is down may be missing recent edits. Two mechanisms keep it as recent as possible, and make its age visible:

- **The control plane serves its last good config.** If building the config from Postgres fails or takes longer than 10 seconds, the control plane serves the last config it built. Run 2+ control-plane replicas, and a Postgres outage does not stop workers from starting.
- **Peer bootstrap** (`controlPlane.peerBootstrap`, off unless set). A worker that has no config, and still cannot sync from the control plane after `afterSecs`, copies the active config from a running worker:
  - **Where it looks:** `address` is resolved and each address is tried in turn. It can be the worker Service, which routes only to ready workers, all of which have a config, or a headless Service.
  - **Authentication:** peers serve `/worker/peer-config` on their ops port, behind the fleet's shared secret.
  - **Age limit:** a copy whose config the control plane last confirmed more than `maxAgeSecs` ago is refused.
  - **What it does not cover:** a full fleet restart during an outage, because no peer has a config to share.

A worker running a copied config reports it:
- `/worker/status` shows `configSource: peer`, `stale: true` and `confirmedAt`, the last time the control plane confirmed that config.
- `queryflux_worker_config_stale` is `1`.

The worker checks the copy with the control plane as soon as the control plane answers. A changed config is fetched and activated, and an unchanged one is confirmed. Either way the stale flag clears. Peer traffic carries the full config, credentials included, so keep the ops port inside the cluster: use a NetworkPolicy, and mTLS from a service mesh.

Each worker registers on start and heartbeats every `heartbeatIntervalSecs`. The control plane's `worker_registry` table holds each worker's name, labels, version, active revision, last error and last-seen time.

## Shared capacity

With `coordination.mode: controlPlane`, the following go through the control plane's `/worker/capacity/*` and `/worker/sweep-lock/*` endpoints:

- the worker's capacity leases;
- engine reconcile locks;
- published engine running counts.

One `maxRunningQueries` therefore holds across the control plane and every worker. This requires the control plane to run with Postgres (distributed mode).

| Situation | Behavior |
|-----------|----------|
| Control plane reachable | Leases are taken and released per query. Leases of a worker that dies expire through the control plane's stale-lease sweep. |
| Control plane down, `onUnavailable: failClosed` (default) | New queries wait in the queue until the capacity wait timeout. Running queries finish. |
| Control plane down, `onUnavailable: localLimits` | New queries are admitted against the worker's own limit. The fleet can exceed the global limit until the control plane returns. |
| `mode: local` | Limits apply to each worker alone. Use this only when one worker serves a group. |

**Fairness.** The shared limit is a hard cap, but queries waiting on different workers are not served in one global order: each worker retries for its own waiting queries. To check whether some workers wait more than others for the same group, compare per worker (per scraped pod):

```promql
# 95th percentile time spent waiting for capacity, per worker
histogram_quantile(0.95, sum by (pod, cluster_group, le) (rate(queryflux_queue_duration_seconds_bucket[10m])))

# how often each worker had local room but the shared limit was full
sum by (pod, cluster_group) (rate(queryflux_capacity_lease_denied_total[10m]))
```

A consistently higher wait on some workers for the same group means waiters are being starved. An ordered wait queue on the control plane is planned for that case.

Each worker sweeps its own zombie and stale queued queries. Those live in its memory.

If two or more workers share clusters with `coordination.mode: local`, the control plane logs a warning, because the effective limit is then multiplied by the number of workers.

## Query history

Workers keep no durable history. Each finished query's record goes upstream:

```text
worker ──► control plane        (POST /worker/query-records, the default)
worker ──► history endpoint     (workerApi.history.endpoint, or the worker's history.endpoint)
```

- **Delivery.** Records are batched (up to 200 per request), queued in a bounded spool (`spoolMaxRecords`) and retried with backoff. When the spool is full, the oldest records are dropped and counted. The query path never waits on the network. A final flush runs after the shutdown drain.
- **Idempotent ingest.** A retried batch is safe: the control plane keeps the first record per proxy query id.
- **Attribution.** Every record is stored with the worker's name (`query_records.worker`, shown in query history). Labels are in `worker_registry`.
- **Redaction**, applied on the worker before a record leaves it:

  | `redaction` | Kept |
  |-------------|------|
  | `full` | Everything |
  | `maskLiterals` | SQL replaced by its parameterized digest. String and number literals masked in other SQL and in error messages. |
  | `metadata` | Fingerprints, timings, status, engine stats. No SQL, error text or routing trace. |

- **Secret handling.** The worker sends the control plane's shared secret only to the control plane, never to a separate history endpoint.

Cluster snapshots are not sent upstream. With shared capacity, the control plane's own snapshots already count every worker's running queries.

## Operating workers

**Ops listener** (the worker's `adminApi.port`):

| Path | Purpose |
|------|---------|
| `/health` | Liveness |
| `/readyz` | `200` once a config snapshot is active |
| `/metrics` | Prometheus metrics for this worker |
| `/worker/status` | Revisions (downloaded, active), config source and staleness, last sync, last error, history policy and delivery counters (`queued`, `sent`, `dropped`, `lastError`) |
| `/worker/peer-config` | With `peerBootstrap` set: the active config, for workers starting without the control plane (shared secret required) |

**Things to alert on:**

- `consecutiveFailures` growing in `/worker/status`, or `queryflux_config_reload_failures_total{stage="worker_activate"}`. The worker is serving a stale config.
- `queryflux_coordination_failures_total` on workers. With `failClosed`, new queries are waiting on the control plane.
- `queryflux_worker_config_stale` at `1` for long. The worker started from a peer's config and the control plane has not answered since.
- `historyUplink.dropped` above zero. The history endpoint was down longer than the spool lasted.

**Upgrades.** The snapshot and history formats are versioned (`schemaVersion`). Upgrade the control plane first. A worker rejects a snapshot version it does not understand and keeps its active config.

**Database migrations.** Upgrading to worker mode runs migrations V18–V20 at startup (with `persistence.autoMigrate`, the default):
- **V18 and V20** add the worker registry and its status column. They're quick.
- **V19** adds `query_records.worker` and builds an index on it. On a large `query_records` table, inserts into query history wait until that index is built.

To avoid a pause in history writes, run `queryflux migrate` in a maintenance window before upgrading, or create the index yourself first with `CREATE INDEX CONCURRENTLY IF NOT EXISTS query_records_worker ON query_records (worker, created_at DESC) WHERE worker IS NOT NULL`. The migration then finds it already there.

The migrations are forward-only: once they've run, an older QueryFlux won't start against that database. Back it up before upgrading.

## Deploying

- **Docker Compose:** [`examples/worker-mode`](https://github.com/lakeops-org/queryflux/tree/main/examples/worker-mode) runs a control plane, Postgres, Studio and two workers.
- **Helm, shared-service workers:** `charts/queryflux/examples/control-plane-values.yaml` and `worker-values.yaml`.
- **Native sidecar** (Kubernetes 1.29+): `charts/queryflux/examples/worker-sidecar-manifest.yaml`. Run the worker as an init container with `restartPolicy: Always` and a `/readyz` startup probe. Kubernetes then starts the application only once its worker has a config, and stops the worker after the application.

See the [Helm chart README](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/README.md#worker-mode).

## Security notes

- Snapshots embed cluster credentials. Serve the control plane's admin port over TLS. `allowInsecureHttp` exists for local demos.
- All workers share one secret. A worker that holds it can read the full config, so run workers only in environments you trust with those credentials.
- Workers never hold database credentials for QueryFlux's own Postgres.

## Limitations

- A running query lives in the memory of the worker that started it. That covers Trino result polls (`nextUri` points at the address the client used) and Snowflake and MCP sessions.
  - For a shared-service worker with more than one replica, set `service.sessionAffinity: ClientIP` in the Helm values (`worker-values.yaml` does), and use sticky sessions on any external load balancer.
  - Sidecar workers are not affected.
  - Forwarding a request to the worker that owns the query, so that any load balancer works, is planned.
- Per-worker credentials, secret references in snapshots, and a site agent that forwards for many workers are planned.
