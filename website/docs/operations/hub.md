---
title: Hub
description: Run one QueryFlux hub per site, so workers in a remote network reach the control plane through a single outbound connection, keep running during outages, and never need a route to it.
image: img/queryflux-hero-banner.png
---

# Hub

A **hub** sits between the workers at one site (a data center, a VPC, a customer network) and a control plane that runs somewhere else, for example a hosted one. Workers connect to the hub exactly as they would to a control plane. The hub is the only process at the site that connects upstream, and it only ever opens connections; nothing connects into the site.

<img
  src="/img/docs/hub-topology.svg"
  alt="Hub topology: workers at a site talk to a hub and to query engines; the hub opens outbound connections to the control plane for config, fleet, history, and capacity"
/>

Read [Worker mode](./worker-mode) first: the hub relays that protocol and changes nothing about how workers behave.

## When to use it

| Setup | Use |
|-------|-----|
| Workers and control plane in one network | Workers connect to the control plane directly. No hub. |
| Workers in a network that may only make outbound connections, or far from the control plane | One hub per site. |
| Many workers at one site | One hub, so upstream sees one connection and one credential per site instead of one per worker. |

## What the hub does

| | At the site | Upstream |
|---|---|---|
| **Config** | Workers long-poll the hub. Their waits wake as soon as a new revision reaches it. | The hub long-polls `/worker/config/wait` and serves the snapshot it gets, with upstream's revision. The one exception: when upstream names `controlPlane.history.endpoint`, the hub removes it and re-seals the snapshot, so workers get a new revision ([Query history](#query-history)); `/hub/status` shows both revisions. The last snapshot is cached on disk. |
| **Fleet** | Workers register and heartbeat with the hub. | Every heartbeat interval the hub posts its live workers to `/worker/fleet`, where they appear as `<site>/<worker>` with `parent: <site>`. It also registers itself, with `kind: hub`. |
| **Query history** | Each batch a worker sends is redacted and written to disk before the worker gets its `202`. | A flusher posts the oldest batch upstream and deletes it once accepted. Each record keeps its worker's name, as `<site>/<worker>`. |
| **Capacity** | Workers' acquires, releases, waits, quota reports and sweep locks go to the hub. | The hub forwards each one, so one `maxRunningQueries` holds across every site and the control plane. Each worker's lease and quota holder is sent as `<site>/<instanceId>`, so two sites with a `worker-0` hold separate leases and quotas. |

The hub runs no query frontends and no engine adapters. It needs no database.

## Configuration

Every key, its default and its rules: [Configuration by role](../role-configuration#hub).

### Hub

```yaml
queryflux:
  role: hub
  workerSecret: <what this site's workers present>
  adminApi:
    port: 9000                       # the worker API and /readyz, /metrics, /hub/status
  hub:
    site: us-east-dc1                # unique per upstream; no '/'
    stateDir: /var/lib/queryflux/hub
    snapshotCache: true              # default
    historySpool:
      maxBytes: 1073741824           # default 1 GiB; the oldest batches are dropped beyond it
    upstream:                        # the same block as a worker's upstream
      url: https://cp.example.com:9000
      secret: <site credential>
      pollTimeoutSecs: 30
      heartbeatIntervalSecs: 10
```

- `hub.upstream.secret` is this site's token on the control plane (see [Site tokens](#site-tokens)), or the control plane's `workerSecret`.
- `workerSecret` is what the site's workers present. Use a different value, so a worker never holds the site's upstream credential. The hub logs a warning when the two are equal.
- A hub rejects everything it gets from upstream (`clusters`, `clusterGroups`, `routers`, `auth`, `guardrails`, `capacity`, …), a `persistence` block, and the `worker` and `controlPlane` sections.

### Site tokens

Give each site its own token on the control plane, and list only its SHA-256 there:

```yaml
# control plane
queryflux:
  # workerSecret: omit it to accept hubs only (no worker connects directly)
  controlPlane:
    sites:
      - name: us-east-dc1      # the hub's hub.site
        tokens:                # printf %s "$TOKEN" | sha256sum
          - <64 hex characters>
        clusters: [trino-dc1]  # optional: the only clusters this site gets
```

A caller presenting a site's token is that site's hub, and it can act only for that site:
- Its fleet reports and history must name its own site, or they're refused with `403`. History that names no site is attributed to the token's site.
- Its own record is stored as `<site>/<name>`, with kind `hub` and label `site` set to its site, whatever it reports.
- It can hold capacity only as `<site>/<worker>`, the hub's holder prefix.

With `clusters` set, the site's hub gets a snapshot with only those clusters: other sites' endpoints and credentials never reach it. Groups keep only those members. A group left with no member stays, so routes to it still resolve, and its queries fail at this site with no cluster available. The site gets its own revision, and the fleet view compares the site's hub and workers with it.

To rotate a token, list the new hash next to the old one, restart the control plane, move the hub to the new token, then remove the old hash. Hubs serve their cached config while the control plane restarts. A token can't be recovered from its hash; a lost token is replaced the same way.

### Workers

A worker behind a hub is an ordinary worker whose `worker.upstream.url` points at the hub:

```yaml
queryflux:
  role: worker
  workerSecret: <the hub's workerSecret>
  worker:
    upstream:
      url: http://queryflux-hub.queryflux.svc:9000
    coordination:
      mode: controlPlane
      onUnavailable: localLimits   # or failClosed; see "When upstream is unreachable"
```

### Control plane

Nothing changes. Set `workerSecret` as for workers, and set `capacity.mode: quotas` (see [Capacity](#capacity)).

## Deploying

**Kubernetes.** Run the hub as a StatefulSet, so each replica keeps its history spool and snapshot cache on its own volume:

```bash
kubectl -n queryflux create secret generic queryflux-hub-config \
  --from-file=config.yaml=./charts/queryflux/examples/hub-config.yaml
helm install queryflux-hub ./charts/queryflux -n queryflux \
  -f ./charts/queryflux/examples/hub-values.yaml
```

`examples/hub-values.yaml` sets `workload.kind: StatefulSet`, a `volumeClaimTemplates` entry mounted at `stateDir`, two replicas and a PodDisruptionBudget, and turns off Studio and the query ports. Then point the workers' `worker.upstream.url` at `http://queryflux-hub.<namespace>.svc:9000`.

**Docker Compose.** [`examples/hub`](https://github.com/lakeops-org/queryflux/tree/main/examples/hub) runs a control plane, a hub and two workers on a site network with no route to the control plane.

**High availability.** Run two or more replicas behind one Service. Replicas don't coordinate with each other:

- each relays config from upstream on its own;
- each forwards the capacity calls and history it receives;
- each reports the workers that heartbeat to it.

## Capacity

The hub forwards every capacity call upstream. On its own that is one WAN round trip per acquire. Set `capacity.mode: quotas` on the control plane to avoid it:

- each worker gets a share of every cluster's limit through the hub;
- it admits queries within that share in memory, with no round trip;
- it only reports demand in the background, about once a second.

Only queries above a worker's share take a forwarded per-query lease.

## Query history

- **Redaction at the hub.** The hub applies the served snapshot's `history.redaction` to each batch before writing it to disk. Workers already redact with the same policy, and masking is idempotent, so a worker on an older policy or version is still covered. No unredacted SQL is stored at the site or leaves it.
- **Durability.** A batch is acknowledged to the worker only after it is on disk (temp file, fsync, rename), so it survives a hub restart. Delivery upstream is at least once; the control plane ignores repeats.
- **A full spool** (`historySpool.maxBytes`) drops the oldest batches. Drops are counted in `queryflux_hub_history_dropped_total` and `/hub/status`. Queries are never blocked.
- **A history endpoint named upstream** (`controlPlane.history.endpoint`): workers behind a hub always send to the hub, and the hub sends to that endpoint. To do this, the hub removes the endpoint from the snapshot it serves and re-seals it. In that one case the revision workers report differs from upstream's; `/hub/status` shows both.

## When upstream is unreachable

| Failure | What happens |
|---|---|
| Upstream down | Workers keep their active config; new workers get the hub's copy. History waits on the hub's disk. Capacity: the hub answers capacity calls with an error, and each worker's `worker.coordination.onUnavailable` decides (`failClosed` waits, `localLimits` admits on the worker's own limit). With quotas, workers keep admitting within their share until it expires. |
| Hub restarts, upstream fine | It fetches the snapshot again. Workers re-register within one heartbeat. The spool resumes from disk. |
| Hub restarts, upstream down | It serves the snapshot cached on disk, marked stale (`/hub/status` `stale: true`, `queryflux_hub_snapshot_stale` 1), until upstream confirms it. |
| All hub replicas down | Workers keep serving their active config, as during a control-plane outage. Their history waits in worker memory. Capacity follows `onUnavailable`. |
| Upstream sends a snapshot that fails its integrity check | The hub rejects it and keeps serving the previous one. Workers never see it. |

## Operating the hub

- **Readiness:** `/readyz` is 200 once the hub has a config to serve, from upstream or its cache.
- **Status:** `/hub/status` shows:
  - whether upstream is reachable and the last error;
  - upstream's revision and the served revision, `stale`, and `confirmedAt`;
  - the number of live workers;
  - the history spool: files, bytes, queued records, sent, dropped, last error.
- **Upstream view:** the control plane's fleet API (`/admin/workers`) lists the hub (`kind: hub`) and each worker behind it (`parent: <site>`).

| Metric | Meaning |
|---|---|
| `queryflux_hub_upstream_up` | 1 while the last exchange with upstream succeeded |
| `queryflux_hub_snapshot_age_seconds` | Seconds since upstream last confirmed the served snapshot |
| `queryflux_hub_snapshot_stale` | 1 while serving a cached snapshot upstream has not confirmed |
| `queryflux_hub_workers` | Workers that reported recently |
| `queryflux_hub_history_spool_bytes` | History waiting on disk |
| `queryflux_hub_history_sent_total` | Records delivered upstream |
| `queryflux_hub_history_dropped_total` | Records dropped because the spool was full |

Alert on `queryflux_hub_upstream_up == 0` lasting longer than you expect, and on a growing `queryflux_hub_history_spool_bytes`.

## Security notes

- The cached snapshot can carry engine credentials. The file is written `0600`. Put `stateDir` on an encrypted volume, or set `snapshotCache: false`; without the cache, a hub that restarts while upstream is down has no config to serve.
- Use separate secrets for upstream and for the site's workers.
- Give each site its own token ([Site tokens](#site-tokens)). With only the control plane's `workerSecret`, upstream trusts the `site` each hub reports, and any hub can act for any site.
- Serve the control plane's worker API over TLS: `hub.upstream.url` must be `https://` unless `allowInsecureHttp: true`.

## Limitations

- A site without `controlPlane.sites[].clusters` gets the whole config.
- Capacity is forwarded, not held at the site. A site-wide quota held by the hub, which would keep admitting after per-worker quotas expire during a long outage, is planned.
- Studio actions that must reach engines from inside the site (test a connection, browse a catalog) do not run through the hub yet.
