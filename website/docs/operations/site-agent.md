---
title: Site agent
description: Run one QueryFlux site agent per site, so workers in a remote network reach the control plane through a single outbound connection, keep running during outages, and never need a route to it.
image: img/queryflux-hero-banner.png
---

# Site agent

A **site agent** sits between the workers at one site (a data center, a VPC, a customer network) and a control plane that runs somewhere else, for example a hosted one. Workers connect to the agent exactly as they would to a control plane. The agent is the only process at the site that connects upstream, and it only ever opens connections; nothing connects into the site.

<img
  src="/img/docs/site-agent-topology.svg"
  alt="Site agent topology: workers at a site talk to a site agent and to query engines; the agent opens outbound connections to the control plane for config, fleet, history, and capacity"
/>

Read [Worker mode](./worker-mode) first: the agent relays that protocol and changes nothing about how workers behave.

## When to use it

| Setup | Use |
|-------|-----|
| Workers and control plane in one network | Workers connect to the control plane directly. No agent. |
| Workers in a network that may only make outbound connections, or far from the control plane | One site agent per site. |
| Many workers at one site | One site agent, so upstream sees one connection and one credential per site instead of one per worker. |

## What the agent does

| | At the site | Upstream |
|---|---|---|
| **Config** | Workers long-poll the agent. Their waits wake as soon as a new revision reaches it. | The agent long-polls `/worker/config/wait` and serves the snapshot it gets, with upstream's revision. The one exception: when upstream names `workerApi.history.endpoint`, the agent removes it and re-seals the snapshot, so workers get a new revision ([Query history](#query-history)); `/agent/status` shows both revisions. The last snapshot is cached on disk. |
| **Fleet** | Workers register and heartbeat with the agent. | Every heartbeat interval the agent posts its live workers to `/worker/fleet`, where they appear as `<siteName>/<workerName>` with `parent: <siteName>`. It also registers itself, with `kind: siteAgent`. |
| **Query history** | Each batch a worker sends is redacted and written to disk before the worker gets its `202`. | A flusher posts the oldest batch upstream and deletes it once accepted. Each record keeps its worker's name, as `<siteName>/<workerName>`. |
| **Capacity** | Workers' acquires, releases, waits, quota reports and sweep locks go to the agent. | The agent forwards each one, so one `maxRunningQueries` holds across every site and the control plane. Each worker's lease and quota holder is sent as `<siteName>/<instanceId>`, so two sites with a `worker-0` hold separate leases and quotas. |

The agent runs no query frontends and no engine adapters. It needs no database.

## Configuration

### Site agent

```yaml
queryflux:
  role: siteAgent
  adminApi:
    port: 9000                       # the worker API and /readyz, /metrics, /agent/status
  siteAgent:
    siteName: us-east-dc1            # unique per upstream; no '/'
    stateDir: /var/lib/queryflux/site-agent
    snapshotCache: true              # default
    historySpool:
      maxBytes: 1073741824           # default 1 GiB; the oldest batches are dropped beyond it
    upstream:                        # the same block as a worker's controlPlane
      url: https://cp.example.com:9000
      auth: { mode: sharedSecret, sharedSecret: <site credential> }
      pollTimeoutSecs: 30
      heartbeatIntervalSecs: 10
  workerApi:
    enabled: true
    sharedSecret: <what this site's workers present>
```

- `siteAgent.upstream.auth.sharedSecret` is this site's token on the control plane (see [Site tokens](#site-tokens)), or the control plane's `workerApi.sharedSecret`.
- `workerApi.sharedSecret` is what the site's workers present. Use a different value, so a worker never holds the site's upstream credential. The agent logs a warning when the two are equal.
- A site agent rejects everything it gets from upstream (`clusters`, `clusterGroups`, `routers`, `auth`, `guardrails`, `capacity`, …), a `persistence` block, and worker-only keys (`controlPlane`, `workerName`, `coordination`, `history`).

### Site tokens

Give each site its own token on the control plane, and list only its SHA-256 there:

```yaml
# control plane
queryflux:
  workerApi:
    enabled: true
    # sharedSecret: omit it to accept site agents only (no worker connects directly)
    sites:
      - name: us-east-dc1      # the agent's siteName
        tokens:                # printf %s "$TOKEN" | sha256sum
          - <64 hex characters>
        clusters: [trino-dc1]  # optional: the only clusters this site gets
```

A caller presenting a site's token is that site's agent, and it can act only for that site:
- Its fleet reports and history must name its own site, or they're refused with `403`. History that names no site is attributed to the token's site.
- Its own record is stored as `<site>/<name>`, with kind `siteAgent` and label `site` set to its site, whatever it reports.
- It can hold capacity only as `<site>/<worker>`, the agent's holder prefix.
- Its workers' sweeps (engine reconcile, zombie and stale-queue eviction, …) run under the site's own locks (`<site>/<sweep>`), so one worker per site runs each sweep and sites never wait on each other.

With `clusters` set, the site's agent gets a snapshot with only those clusters: other sites' endpoints and credentials never reach it. Groups keep only those members. A group left with no member stays, so routes to it still resolve, and its queries fail at this site with no cluster available. The site gets its own revision, and the fleet view compares the site's agent and workers with it.

The site can also act only on those clusters. Capacity calls (acquire, release, wait, quota reports) and running counts (read and publish) for any other cluster are refused with `403`. A site without `clusters` isn't limited.

To rotate a token, list the new hash next to the old one, restart the control plane, move the agent to the new token, then remove the old hash. Agents serve their cached config while the control plane restarts. A token can't be recovered from its hash; a lost token is replaced the same way.

### Workers

A worker behind an agent is an ordinary worker whose `controlPlane.url` points at the agent:

```yaml
queryflux:
  role: worker
  controlPlane:
    url: http://queryflux-site-agent.queryflux.svc:9000
    auth: { mode: sharedSecret, sharedSecret: <the agent's workerApi.sharedSecret> }
  coordination:
    mode: controlPlane
    onUnavailable: localLimits   # or failClosed; see "When upstream is unreachable"
```

### Control plane

Nothing changes. Enable `workerApi` as for workers, and set `capacity.mode: quotas` (see [Capacity](#capacity)).

## Deploying

**Kubernetes.** Run the agent as a StatefulSet, so each replica keeps its history spool and snapshot cache on its own volume:

```bash
kubectl -n queryflux create secret generic queryflux-site-agent-config \
  --from-file=config.yaml=./charts/queryflux/examples/site-agent-config.yaml
helm install queryflux-site-agent ./charts/queryflux -n queryflux \
  -f ./charts/queryflux/examples/site-agent-values.yaml
```

`examples/site-agent-values.yaml` sets `workload.kind: StatefulSet`, a `volumeClaimTemplates` entry mounted at `stateDir`, two replicas and a PodDisruptionBudget, and turns off Studio and the query ports. Then point the workers' `controlPlane.url` at `http://queryflux-site-agent.<namespace>.svc:9000`.

**Docker Compose.** [`examples/site-agent`](https://github.com/lakeops-org/queryflux/tree/main/examples/site-agent) runs a control plane, an agent and two workers on a site network with no route to the control plane.

**High availability.** Run two or more replicas behind one Service. Replicas don't coordinate with each other:

- each relays config from upstream on its own;
- each forwards the capacity calls and history it receives;
- each reports the workers that heartbeat to it.

## Capacity

The agent forwards every capacity call upstream. On its own that is one WAN round trip per acquire. Set `capacity.mode: quotas` on the control plane to avoid it:

- each worker gets a share of every cluster's limit through the agent;
- it admits queries within that share in memory, with no round trip;
- it only reports demand in the background, about once a second.

Only queries above a worker's share take a forwarded per-query lease.

## Query history

- **Redaction at the agent.** The agent applies the served snapshot's `history.redaction` to each batch before writing it to disk. Workers already redact with the same policy, and masking is idempotent, so a worker on an older policy or version is still covered. No unredacted SQL is stored at the site or leaves it.
- **Durability.** A batch is acknowledged to the worker only after it is on disk (temp file, fsync, rename), so it survives an agent restart. Delivery upstream is at least once; the control plane ignores repeats.
- **A full spool** (`historySpool.maxBytes`) drops the oldest batches. Drops are counted in `queryflux_agent_history_dropped_total` and `/agent/status`. Queries are never blocked.
- **A history endpoint named upstream** (`workerApi.history.endpoint`): workers behind an agent always send to the agent, and the agent sends to that endpoint. To do this, the agent removes the endpoint from the snapshot it serves and re-seals it. In that one case the revision workers report differs from upstream's; `/agent/status` shows both.

## When upstream is unreachable

| Failure | What happens |
|---|---|
| Upstream down | Workers keep their active config; new workers get the agent's copy. History waits on the agent's disk. Capacity: the agent answers capacity calls with an error, and each worker's `coordination.onUnavailable` decides (`failClosed` waits, `localLimits` admits on the worker's own limit). With quotas, workers keep admitting within their share until it expires. |
| Agent restarts, upstream fine | It fetches the snapshot again. Workers re-register within one heartbeat. The spool resumes from disk. |
| Agent restarts, upstream down | It serves the snapshot cached on disk, marked stale (`/agent/status` `stale: true`, `queryflux_agent_snapshot_stale` 1), until upstream confirms it. |
| All agent replicas down | Workers keep serving their active config, as during a control-plane outage. Their history waits in worker memory. Capacity follows `onUnavailable`. |
| Upstream sends a snapshot that fails its integrity check | The agent rejects it and keeps serving the previous one. Workers never see it. |

## Operating the agent

- **Readiness:** `/readyz` is 200 once the agent has a config to serve, from upstream or its cache.
- **Status:** `/agent/status` shows:
  - whether upstream is reachable and the last error;
  - upstream's revision and the served revision, `stale`, and `confirmedAt`;
  - the number of live workers;
  - the history spool: files, bytes, queued records, sent, dropped, last error.
- **Upstream view:** the control plane's fleet API (`/admin/workers`) lists the agent (`kind: siteAgent`) and each worker behind it (`parent: <siteName>`).

| Metric | Meaning |
|---|---|
| `queryflux_agent_upstream_up` | 1 while the last exchange with upstream succeeded |
| `queryflux_agent_snapshot_age_seconds` | Seconds since upstream last confirmed the served snapshot |
| `queryflux_agent_snapshot_stale` | 1 while serving a cached snapshot upstream has not confirmed |
| `queryflux_agent_workers` | Workers that reported recently |
| `queryflux_agent_history_spool_bytes` | History waiting on disk |
| `queryflux_agent_history_sent_total` | Records delivered upstream |
| `queryflux_agent_history_dropped_total` | Records dropped because the spool was full |

Alert on `queryflux_agent_upstream_up == 0` lasting longer than you expect, and on a growing `queryflux_agent_history_spool_bytes`.

## Security notes

- The cached snapshot can carry engine credentials. The file is written `0600`. Put `stateDir` on an encrypted volume, or set `snapshotCache: false`; without the cache, an agent that restarts while upstream is down has no config to serve.
- Use separate secrets for upstream and for the site's workers.
- Give each site its own token ([Site tokens](#site-tokens)). With only the control plane's `workerApi.sharedSecret`, upstream trusts the `site` each agent reports, and any agent can act for any site.
- Serve the control plane's worker API over TLS: `siteAgent.upstream.url` must be `https://` unless `allowInsecureHttp: true`.

## Limitations

- A site without `workerApi.sites[].clusters` gets the whole config.
- Capacity is forwarded, not held at the site. A site-wide quota held by the agent, which would keep admitting after per-worker quotas expire during a long outage, is planned.
- Studio actions that must reach engines from inside the site (test a connection, browse a catalog) do not run through the agent yet.
