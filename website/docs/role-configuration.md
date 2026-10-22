---
sidebar_position: 2
sidebar_label: Roles reference
title: Configuration by role
description: Every config key for each QueryFlux role (standalone, control plane, worker, site agent), with its default and the rules queryflux validate checks.
image: img/queryflux-hero-banner.png
---
# Configuration by role

`queryflux.role` sets what a process does. Keys directly under `queryflux` are shared by every role. Each role except `standalone` also has a section of its own:

```yaml
queryflux:
  role: standalone        # standalone | controlPlane | worker | siteAgent
  # shared keys: frontends, adminApi, persistence, metrics, capacity, …
  controlPlane: { … }     # only with role: controlPlane
  worker: { … }           # only with role: worker
  siteAgent: { … }        # only with role: siteAgent
```

A role's section set on any other role is an error, for example `queryflux.worker only applies to role: worker`. `queryflux validate --config <file>` reports every problem in the file, and the process refuses to start with any of them.

| Role | What it does | Its section |
|---|---|---|
| `standalone` (default) | A regular proxy: owns its config and serves queries. | none |
| `controlPlane` | Same as standalone, and also serves config, shared capacity and a history endpoint to workers and site agents. | [`controlPlane`](#control-plane) |
| `worker` | Serves queries with config pulled from a control plane or a site agent. Owns no database. | [`worker`](#worker) |
| `siteAgent` | Relays one site's workers to a remote control plane. Serves no queries and owns no database. | [`siteAgent`](#site-agent) |

How the roles work together: [Worker mode](./operations/worker-mode) and [Site agent](./operations/site-agent).

## Shared keys

The keys under `queryflux` (shown here without the `queryflux.` prefix), and which roles use them. "Rejected" means `validate` fails if the key is set. "Ignored" means the key is accepted but has no effect. The [YAML reference](./configuration) documents each key.

| Key | standalone | controlPlane | worker | siteAgent |
|---|---|---|---|---|
| `role` | ✓ | ✓ | ✓ | ✓ |
| `frontends` | ✓ | ✓ | ✓ | ignored: the agent serves no queries |
| `adminApi.port` | admin API and Studio | admin API, Studio and the worker API (`/worker/*`) | ops listener: `/health`, `/readyz`, `/metrics`, `/worker/status` | the worker API, `/readyz`, `/metrics`, `/agent/status` |
| `adminApi.username`, `password`, `corsAllowedOrigins` | ✓ | ✓ | ignored: no admin API | ignored |
| `externalAddress` | ✓ | ✓ | ✓ | ignored |
| `persistence` | optional | Postgres for shared capacity and Studio edits; without it, workers get the config the process started with | **rejected** unless `inMemory` | **rejected** unless `inMemory` |
| `capacity` | ✓ | ✓ | **rejected**: comes from the control plane | **rejected** |
| `distributed`, `configReloadIntervalSecs`, `queryHistoryRetentionDays` | ✓ | ✓ | ignored: no database | ignored |
| `cacheBackend` | ✓ | ✓ | ✓ (in memory) | ignored |
| `metrics` | ✓ | ✓ | ✓ | ✓ |
| `otlpEndpoint` | ✓ | ✓ | ✓ | ✓ |
| `shutdownDrainTimeoutSecs` | ✓ | ✓ | ✓ | ignored |

Some top-level keys come from upstream on a worker or site agent, so either rejects them: `clusters`, `clusterGroups`, `routers`, `routingFallback`, `auth`, `authorization`, `catalogProvider`, `guardrails`, `accessControl`, `translation.mode` and `translation.errorOnUnsupported`. A worker keeps the other `translation` settings (`pythonScripts`, `schemaResolutionTimeoutMs`) local.

### `capacity`

How each cluster's `maxRunningQueries` is shared between the processes that admit queries. Used on `standalone` and `controlPlane`. Workers and site agents follow their control plane.

| Key | Default | Meaning |
|---|---|---|
| `mode` | `leases` | `leases`: every admission takes a per-query lease from Postgres or the control plane. `quotas`: each process gets a share of every cluster's limit and admits within it locally, with no round trip; per-query leases cover the rest. |
| `reserveFraction` | `0.15` | Share of each cluster's limit never handed out as quota. It is left for per-query leases (bursts above a process's quota, and older workers). Between 0 and 0.9. |
| `quotaLeaseSecs` | `15` | How long a process may admit on its quota without reaching the allocator. Between 2 and 300. |
| `reportIntervalMs` | `1000` | How often each process reports its demand. Between 100 and 60000, and under half of `quotaLeaseSecs`. Reports go at most 250 ms apart while queries wait. |

## Control plane

`role: controlPlane`. The control plane serves workers and site agents on the admin port, under `/worker/*`. The worker API takes its own bearer credentials, never the admin API's. Put TLS in front of the admin port: snapshots carry engine credentials.

```yaml
queryflux:
  role: controlPlane
  persistence:
    type: postgres
    url: postgres://queryflux:<password>@postgres:5432/queryflux
  adminApi:
    port: 9000
  capacity:
    mode: quotas
  controlPlane:
    sharedSecret: <long-random-secret>
    sites:
      - name: us-east-dc1
        tokens: [<hash of the site's token>]
        clusters: [trino-us-east]
    history:
      endpoint: null
      redaction: maskLiterals
```

| Key | Default | Meaning |
|---|---|---|
| `controlPlane.sharedSecret` | none | The bearer secret that workers present (their `worker.upstream.auth.sharedSecret`). A site agent may present it too, but then upstream trusts whatever site the agent reports. |
| `controlPlane.sites` | `[]` | Site agents that connect with their own token. See [Site tokens](./operations/site-agent#site-tokens). |
| `controlPlane.sites[].name` | required | The agent's `siteAgent.name`. Must be non-empty, contain no `/`, and be unique. |
| `controlPlane.sites[].tokens` | required | The accepted tokens for this site, each stored as the hex SHA-256 of the token (`printf %s "$TOKEN" \| sha256sum`). List two while rotating. A token may belong to only one site. |
| `controlPlane.sites[].clusters` | all clusters | Limits the site to these clusters: other clusters are removed from the config it gets, and groups keep only these members. A site that agents connect to with the `sharedSecret` instead of a token always gets the whole config. |
| `controlPlane.history.endpoint` | the control plane's `/worker/query-records` | Where workers send query history, if not to the control plane. |
| `controlPlane.history.redaction` | `full` | How much SQL leaves each worker: `full`, `maskLiterals` (SQL as its parameterized digest, with literals replaced by `?`), or `metadata` (no SQL or error text). |

**Rules:** set `sharedSecret`, `sites`, or both. With `sites` set and no `sharedSecret`, only site agents can connect, and no worker can connect directly. `capacity` must pass the limits above.

## Worker

`role: worker`. A worker starts with no config, pulls it from upstream, and serves queries with it. It keeps serving its active config while upstream is unreachable.

```yaml
queryflux:
  role: worker
  worker:
    name: checkout-7f9c
    labels: { team: checkout }
    labelsFile: /etc/podinfo/labels
    upstream:
      url: https://queryflux-control.example.com
      auth: { mode: sharedSecret, sharedSecret: <secret> }
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
| `worker.name` | the hostname (the pod name) | The name reported upstream and shown in Studio. Behind a site agent, it appears upstream as `<site>/<name>`. |
| `worker.labels` | `{}` | Labels reported upstream, shown in Studio and searchable there (team, app, region, …). |
| `worker.labelsFile` | none | A file of `key="value"` lines merged over `labels` and re-read on every heartbeat. Point it at a Kubernetes downward API volume of `metadata.labels`, so each pod reports its own labels. Bookkeeping labels such as `pod-template-hash` are skipped. |
| `worker.upstream` | required | The control plane or site agent to pull config from. See [`upstream`](#upstream). |
| `worker.coordination.mode` | `controlPlane` | `controlPlane`: `maxRunningQueries` is shared with every worker and the control plane, through upstream. `local`: limits apply to this worker alone, which is only correct when one worker serves a group. |
| `worker.coordination.onUnavailable` | `failClosed` | What happens when upstream can't be reached for capacity. `failClosed`: new queries on shared groups wait until their capacity wait timeout. `localLimits`: admit against this worker's own limit, which can exceed the global cap. |
| `worker.history.endpoint` | the snapshot's endpoint, else upstream's `/worker/query-records` | Override where this worker sends query records. |
| `worker.history.spoolMaxRecords` | `50000` | Records held in memory while the history endpoint is unreachable. When full, the oldest records are dropped. |

**Rules:** `worker.upstream` is required. `persistence` (other than `inMemory`), `capacity` and the keys a worker gets from upstream are rejected.

## Site agent

`role: siteAgent`. Run one per site. The site's workers connect to the agent. The agent is the only process at the site that connects to the control plane, and it caches config and query history on disk so the site keeps running through outages.

```yaml
queryflux:
  role: siteAgent
  adminApi:
    port: 9000
  siteAgent:
    name: us-east-dc1
    upstream:
      url: https://cp.example.com:9000
      auth: { mode: sharedSecret, sharedSecret: <this site's token> }
    sharedSecret: <what this site's workers present>
    stateDir: /var/lib/queryflux/site-agent
    snapshotCache: true
    historySpool:
      maxBytes: 1073741824
```

| Key | Default | Meaning |
|---|---|---|
| `siteAgent.name` | required | Unique per control plane, with no `/`. This site's workers appear upstream as `<name>/<worker>`. With [site tokens](./operations/site-agent#site-tokens), it must match the site's `controlPlane.sites[].name`. |
| `siteAgent.upstream` | required | The control plane. `auth.sharedSecret` is this site's token, or the control plane's `controlPlane.sharedSecret`. See [`upstream`](#upstream). |
| `siteAgent.sharedSecret` | required | What this site's workers present (their `worker.upstream.auth.sharedSecret`). Use a different value from the upstream secret, so a worker never holds the site's upstream credential; the agent logs a warning when they match. |
| `siteAgent.stateDir` | `/var/lib/queryflux/site-agent` | Where the config cache and the history spool live. Use a persistent, encrypted volume: the cached config can carry engine credentials. |
| `siteAgent.snapshotCache` | `true` | Keep the last config on disk, so the agent can serve its workers after a restart while upstream is unreachable. |
| `siteAgent.historySpool.maxBytes` | `1073741824` (1 GiB) | When the history spool grows past this, the oldest batches are dropped. |

**Rules:** `name`, `upstream` and `sharedSecret` are required, and `stateDir` can't be empty. `persistence` (other than `inMemory`), `capacity` and the keys the agent gets from upstream are rejected.

## `upstream`

`worker.upstream` and `siteAgent.upstream` use the same block.

| Key | Default | Meaning |
|---|---|---|
| `url` | required | The upstream's admin port, for example `https://queryflux-control.example.com` or `http://queryflux-site-agent.<namespace>.svc:9000`. Must be `https://` unless `allowInsecureHttp` is set. |
| `auth.mode` | required | `sharedSecret`, the only mode. |
| `auth.sharedSecret` | required | Sent as `Authorization: Bearer`. Must be non-empty. |
| `pollTimeoutSecs` | `30` | How long a config long-poll may wait for a new revision. |
| `heartbeatIntervalSecs` | `10` | How often to report status upstream. |
| `allowInsecureHttp` | `false` | Allow `http://`. Use it for local demos, or inside a service mesh that encrypts pod traffic, never across an open network: config carries engine credentials. |
| `peerBootstrap.address` | none (off) | Workers only. `host:port` of other workers' ops listeners, usually a headless Service. A worker that can't reach upstream at startup copies the active config from a peer. |
| `peerBootstrap.afterSecs` | `10` | How long to try upstream at startup before asking peers. |
| `peerBootstrap.maxAgeSecs` | `86400` | Refuse a peer's copy whose config upstream last confirmed longer ago than this. |

QueryFlux does not expand environment variables in its config file. Mount secrets into the config file, for example from a Kubernetes Secret.
