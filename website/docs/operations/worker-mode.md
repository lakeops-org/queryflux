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

Everything the control plane owns is rejected in a worker config: `persistence` (other than `inMemory`), clusters, groups, routers, `routingFallback`, `auth`, `authorization`, `catalogProvider`, `guardrails`, `accessControl` and `workerApi`. `queryflux validate` lists any such settings it finds.

QueryFlux does not expand environment variables in its config file. Mount a worker config that holds the shared secret from a Kubernetes Secret.

## How config reaches workers

1. **Snapshot.** The control plane serves `GET /worker/config`. The response is everything a worker needs to compile its live config: clusters, groups, routers, translation scripts, settings and the history policy. Its `revision` is a content hash, so the same config has the same revision on every control plane replica.
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
| `/worker/status` | Revisions (downloaded, active), last sync, last error, history policy and delivery counters (`queued`, `sent`, `dropped`, `lastError`) |

**Things to alert on:**

- `consecutiveFailures` growing in `/worker/status`, or `queryflux_config_reload_failures_total{stage="worker_activate"}`. The worker is serving a stale config.
- `queryflux_coordination_failures_total` on workers. With `failClosed`, new queries are waiting on the control plane.
- `historyUplink.dropped` above zero. The history endpoint was down longer than the spool lasted.

**Upgrades.** The snapshot and history formats are versioned (`schemaVersion`). Upgrade the control plane first. A worker rejects a snapshot version it does not understand and keeps its active config.

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

- Snowflake HTTP sessions and MCP sessions live in each worker's memory. For a shared-service worker with more than one replica, use sticky sessions, as described in [Multi-replica operations](./multi-replica).
- Per-worker credentials, secret references in snapshots, and a site agent that forwards for many workers are planned.
