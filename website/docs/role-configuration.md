---
sidebar_position: 2
sidebar_label: Roles reference
title: Configuration by role
description: Every config key for each QueryFlux role (standalone, control plane, worker, hub), with its default and the rules queryflux validate checks.
image: img/queryflux-hero-banner.png
---
# Configuration by role

`queryflux.role` sets what a process does. Keys directly under `queryflux` are shared by every role. Each role except `standalone` also has a section of its own:

```yaml
queryflux:
  role: standalone        # standalone | controlPlane | worker | hub
  workerSecret: <secret>  # shared by a worker and whatever serves it
  # other shared keys: frontends, adminApi, persistence, metrics, capacity, …
  controlPlane: { … }     # only with role: controlPlane
  worker: { … }           # only with role: worker
  hub: { … }              # only with role: hub
```

A role's section set on any other role is an error, for example `queryflux.worker only applies to role: worker`. `queryflux validate --config <file>` reports every problem in the file, and the process refuses to start with any of them.

| Role | What it does | Its section |
|---|---|---|
| `standalone` (default) | A regular proxy: owns its config and serves queries. | none |
| `controlPlane` | Same as standalone, and also serves config, shared capacity and a history endpoint to workers and hubs. | [`controlPlane`](#control-plane) |
| `worker` | Serves queries with config pulled from a control plane or a hub. Owns no database. | [`worker`](#worker) |
| `hub` | Relays one site's workers to a remote control plane. Serves no queries and owns no database. | [`hub`](#hub) |

How the roles work together: [Worker mode](./operations/worker-mode) and [Hub](./operations/hub).

## Shared keys

The keys under `queryflux` (shown here without the `queryflux.` prefix), and which roles use them. "Rejected" means `validate` fails if the key is set. "Ignored" means the key is accepted but has no effect. The [YAML reference](./configuration) documents each key.

| Key | standalone | controlPlane | worker | hub |
|---|---|---|---|---|
| `role` | ✓ | ✓ | ✓ | ✓ |
| `workerSecret` | ignored | accepted from workers | presented upstream | accepted from this site's workers |
| `frontends` | ✓ | ✓ | ✓ | ignored: the hub serves no queries |
| `adminApi.port` | admin API and Studio | admin API, Studio and the worker API (`/worker/*`) | ops listener: `/health`, `/readyz`, `/metrics`, `/worker/status` | the worker API, `/readyz`, `/metrics`, `/hub/status` |
| `adminApi.username`, `password`, `corsAllowedOrigins` | ✓ | ✓ | ignored: no admin API | ignored |
| `externalAddress` | ✓ | ✓ | ✓ | ignored |
| `persistence` | optional | Postgres for shared capacity and Studio edits; without it, workers get the config the process started with | **rejected** unless `inMemory` | **rejected** unless `inMemory` |
| `capacity` | ✓ | ✓ | **rejected**: comes from the control plane | **rejected** |
| `distributed`, `configReloadIntervalSecs`, `queryHistoryRetentionDays` | ✓ | ✓ | ignored: no database | ignored |
| `cacheBackend` | ✓ | ✓ | ✓ (in memory) | ignored |
| `metrics` | ✓ | ✓ | ✓ | ✓ |
| `otlpEndpoint` | ✓ | ✓ | ✓ | ✓ |
| `shutdownDrainTimeoutSecs` | ✓ | ✓ | ✓ | ignored |

### `workerSecret`

The secret between a worker and whatever serves it. A worker sends it upstream as `Authorization: Bearer`; a control plane or hub accepts it from its workers. Use the same value on a worker and its control plane, or on a hub and the workers at its site. Mount it from one Kubernetes Secret into each config.

Some top-level keys come from upstream on a worker or hub, so either rejects them: `clusters`, `clusterGroups`, `routers`, `routingFallback`, `auth`, `authorization`, `catalogProvider`, `guardrails`, `accessControl`, `translation.mode` and `translation.errorOnUnsupported`. A worker keeps the other `translation` settings (`pythonScripts`, `schemaResolutionTimeoutMs`) local.

### `capacity`

How each cluster's `maxRunningQueries` is shared between the processes that admit queries. Used on `standalone` and `controlPlane`. Workers and hubs follow their control plane.

| Key | Default | Meaning |
|---|---|---|
| `mode` | `leases` | `leases`: every admission takes a per-query lease from Postgres or the control plane. `quotas`: each process gets a share of every cluster's limit and admits within it locally, with no round trip; per-query leases cover the rest. |
| `reserveFraction` | `0.15` | Share of each cluster's limit never handed out as quota. It is left for per-query leases (bursts above a process's quota, and older workers). Between 0 and 0.9. |
| `quotaLeaseSecs` | `15` | How long a process may admit on its quota without reaching the allocator. Between 2 and 300. |
| `reportIntervalMs` | `1000` | How often each process reports its demand. Between 100 and 60000, and under half of `quotaLeaseSecs`. Reports go at most 250 ms apart while queries wait. |

## Control plane

`role: controlPlane`. The control plane serves workers and hubs on the admin port, under `/worker/*`. The worker API takes its own bearer credentials, never the admin API's. Put TLS in front of the admin port: snapshots carry engine credentials.

```yaml
queryflux:
  role: controlPlane
  workerSecret: <long-random-secret>
  persistence:
    type: postgres
    url: postgres://queryflux:<password>@postgres:5432/queryflux
  adminApi:
    port: 9000
  capacity:
    mode: quotas
  controlPlane:
    history:
      endpoint: null
      redaction: maskLiterals
```

| Key | Default | Meaning |
|---|---|---|
| `controlPlane.history.endpoint` | the control plane's `/worker/query-records` | Where workers send query history, if not to the control plane. |
| `controlPlane.history.redaction` | `full` | How much SQL leaves each worker: `full`, `maskLiterals` (SQL as its parameterized digest, with literals replaced by `?`), or `metadata` (no SQL or error text). |

Sites and enrolled hubs are not config keys: they live in Postgres and are managed through the admin API (`/admin/sites`), so a hub can be added, limited to some clusters, or revoked without a restart. See [Enrolling a hub](./operations/hub#enrolling-a-hub).

**Rules:** set `workerSecret`, use Postgres persistence, or both. With Postgres and no `workerSecret`, only enrolled hubs can connect, and no worker can connect directly. A hub may also present the `workerSecret`, but then the control plane trusts whatever site it reports. `capacity` must pass the limits above.

## Worker

`role: worker`. A worker starts with no config, pulls it from upstream, and serves queries with it. It keeps serving its active config while upstream is unreachable.

```yaml
queryflux:
  role: worker
  workerSecret: <secret>
  worker:
    name: checkout-7f9c
    labels: { team: checkout }
    labelsFile: /etc/podinfo/labels
    upstream:
      url: https://queryflux-control.example.com
    coordination:
      mode: controlPlane
      onUnavailable: failClosed
    history:
      spoolMaxRecords: 50000
  frontends:
    trinoHttp: { enabled: true, port: 8080 }
  adminApi:
    port: 9000
```

| Key | Default | Meaning |
|---|---|---|
| `worker.name` | the hostname (the pod name) | The name reported upstream and shown in Studio. Behind a hub, it appears upstream as `<site>/<name>`. |
| `worker.labels` | `{}` | Labels reported upstream, shown in Studio and searchable there (team, app, region, …). |
| `worker.labelsFile` | none | A file of `key="value"` lines merged over `labels` and re-read on every heartbeat. Point it at a Kubernetes downward API volume of `metadata.labels`, so each pod reports its own labels. Bookkeeping labels such as `pod-template-hash` are skipped. |
| `worker.upstream` | required | The control plane or hub to pull config from. See [`upstream`](#upstream). |
| `worker.coordination.mode` | `controlPlane` | `controlPlane`: `maxRunningQueries` is shared with every worker and the control plane, through upstream. `local`: limits apply to this worker alone, which is only correct when one worker serves a group. |
| `worker.coordination.onUnavailable` | `failClosed` | What happens when upstream can't be reached for capacity. `failClosed`: new queries on shared groups wait until their capacity wait timeout. `localLimits`: admit against this worker's own limit, which can exceed the global cap. |
| `worker.history.endpoint` | the snapshot's endpoint, else upstream's `/worker/query-records` | Override where this worker sends query records. |
| `worker.history.spoolMaxRecords` | `50000` | Records held in memory while the history endpoint is unreachable. When full, the oldest records are dropped. |

**Rules:** `workerSecret` and `worker.upstream` are required. `persistence` (other than `inMemory`), `capacity` and the keys a worker gets from upstream are rejected.

## Hub

`role: hub`. Run one per site. The site's workers connect to the hub. The hub is the only process at the site that connects to the control plane, and it caches config and query history on disk so the site keeps running through outages.

```yaml
queryflux:
  role: hub
  workerSecret: <what this site's workers present>
  adminApi:
    port: 9000
  hub:
    site: us-east-dc1
    upstream:
      url: https://cp.example.com:9000
      enrollToken: <one-time token for this site>
    stateDir: /var/lib/queryflux/hub
    snapshotCache: true
    historySpool:
      maxBytes: 1073741824
```

| Key | Default | Meaning |
|---|---|---|
| `hub.site` | required | Unique per control plane, with no `/`. This site's workers appear upstream as `<site>/<worker>`. When the hub enrolls, it must be the site its enrollment token was created for. |
| `hub.upstream` | required | The control plane. See [`upstream`](#upstream). |
| `hub.upstream.enrollToken` | one of these two | A one-time token from `POST /admin/sites/{site}/enrollment-tokens`. On first start the hub enrolls its own key with it and keeps the key in `stateDir`; later starts ignore the spent token. A different token makes the hub enroll a new key: how a revoked hub comes back, or a key is rotated. |
| `hub.upstream.secret` | one of these two | Instead of enrolling: the control plane's `workerSecret`. The control plane then trusts whatever site the hub reports. Use a different value from this hub's own `workerSecret`; the hub logs a warning when they match. |
| `hub.stateDir` | `/var/lib/queryflux/hub` | Where the hub's key, the config cache and the history spool live. Use a persistent, encrypted volume: the key is the hub's credential, and the cached config can carry engine credentials. |
| `hub.snapshotCache` | `true` | Keep the last config on disk, so the hub can serve its workers after a restart while upstream is unreachable. |
| `hub.historySpool.maxBytes` | `1073741824` (1 GiB) | When the history spool grows past this, the oldest batches are dropped. |

**Rules:** `workerSecret`, `hub.site`, `hub.upstream`, and one of `hub.upstream.enrollToken` or `hub.upstream.secret` are required, and `stateDir` can't be empty. `persistence` (other than `inMemory`), `capacity` and the keys the hub gets from upstream are rejected.

## `upstream`

`worker.upstream` and `hub.upstream` use the same block. A worker presents `workerSecret` upstream; a hub presents tokens signed with its enrolled key, or `hub.upstream.secret`.

| Key | Default | Meaning |
|---|---|---|
| `url` | required | The upstream's admin port, for example `https://queryflux-control.example.com` or `http://queryflux-hub.<namespace>.svc:9000`. Must be `https://` unless `allowInsecureHttp` is set. |
| `pollTimeoutSecs` | `30` | How long a config long-poll may wait for a new revision. |
| `heartbeatIntervalSecs` | `10` | How often to report status upstream. |
| `allowInsecureHttp` | `false` | Allow `http://`. Use it for local demos, or inside a service mesh that encrypts pod traffic, never across an open network: config carries engine credentials. |
| `peerBootstrap.address` | none (off) | Workers only. `host:port` of other workers' ops listeners, usually a headless Service. A worker that can't reach upstream at startup copies the active config from a peer. |
| `peerBootstrap.afterSecs` | `10` | How long to try upstream at startup before asking peers. |
| `peerBootstrap.maxAgeSecs` | `86400` | Refuse a peer's copy whose config upstream last confirmed longer ago than this. |

QueryFlux does not expand environment variables in its config file. Mount secrets into the config file, for example from a Kubernetes Secret.
