---
title: Worker mode design
description: How QueryFlux workers take config, capacity limits and query history from a control plane — the protocol, failure behavior, and the decisions behind it.
image: img/queryflux-hero-banner.png
---

# Worker mode design

This page explains how worker mode works and why it is built this way. To run it, see [Worker mode operations](../operations/worker-mode).

## Goals

- **Run QueryFlux close to the applications that query it.** That can be a sidecar in every application pod, or a small pool per team or site. Several small processes replace one central proxy on the query path.
- **Keep one place that owns everything shared:** config, capacity limits, and query history. Studio, admins and the routing logic see one system, not a collection of proxies.
- **Workers never touch the database.** A worker needs network access to its clients, its engines and its control plane, nothing else. That makes it safe to run in networks that must not reach the config database.
- **Today's deployments keep working unchanged.** The default role is `standalone`, and a standalone proxy can also serve workers.

**Non-goals:** moving query results through the control plane.

## Roles and topology

| `queryflux.role` | Owns config | Database | Serves |
|---|---|---|---|
| `standalone` (default) | yes (YAML or Postgres) | optional | queries, admin API, Studio |
| `controlPlane` | yes | Postgres | the same, plus workers |
| `worker` | no, pulled from the control plane | **never** | queries, and an ops listener |
| `hub` | no, relayed from the control plane | **never** | the worker API for one site's workers, and ops endpoints |

<img
  src="/img/docs/worker-mode-roles-topology.svg"
  alt="Roles and topology: control plane with Admin API, Studio, and Worker API backed by Postgres; sidecar and pooled workers serve apps and engines and never connect to Postgres"
/>

A worker is the in-memory proxy plus three changes:
- its config comes from the control plane;
- its capacity leases optionally go through the control plane;
- its query history goes upstream.

Its running and queued queries stay in its own memory, the same as an in-memory standalone proxy. It isn't a third architecture.

## Config distribution

### The snapshot

A control plane serves its whole config as one document, `WorkerSnapshotV1` (media type `application/vnd.queryflux.worker-config.v1+json`):
- clusters with their engine config and variants;
- groups, routers and the routing fallback;
- translation scripts;
- settings (guardrails, security, catalog, access control, guard script bodies);
- the query-history policy;
- the translation failure policy.

- **`revision`:** an xxHash64 of the snapshot's canonical JSON, with object keys sorted at every level. The same config therefore has the same revision on every control-plane replica, whatever the `HashMap` iteration order or `serde_json` features.
- **`schemaVersion`:** fields are added, never repurposed. A breaking change is a new version, and a worker rejects versions it doesn't know.
- **Credentials:** they travel inside the snapshot (`credentials: embedded`), over TLS, and are held only in worker memory. A `stripped` mode exists for a future where workers resolve secret references themselves.
- **Consistent reads:** with Postgres, each snapshot is built from one `REPEATABLE READ` read, so an admin edit can't produce a half-old, half-new config. Without Postgres, the snapshot is built once at startup.

### Sync, activation and the last good config

<img
  src="/img/docs/worker-mode-config-sync.svg"
  alt="Config sync sequence: worker fetches a snapshot, long-polls for changes, activates atomically, and receives the last-good snapshot when Postgres is down"
/>

- **Strict compile.** A received snapshot is compiled strictly: one invalid adapter, script or setting rejects the whole snapshot, and the previous config stays active. The worker remembers the rejected revision and doesn't refetch it until the control plane publishes a new one. It reports the error in its heartbeat and in `/worker/status`.
- **Long-poll.** `/worker/config/wait` answers as soon as the control plane reloads its config. Otherwise it answers `304` after `pollTimeoutSecs`. A worker without a revision yet uses a plain `GET`.
- **Before the first activation,** queries get a retryable "not configured yet" error in every protocol, and `/readyz` returns `503`, which is what holds a sidecar's application back:

  | Protocol | Error |
  |---|---|
  | Trino HTTP | `503` with `Retry-After` |
  | Postgres wire | `57P03` |
  | MySQL wire | `1053` |
  | Flight SQL | `UNAVAILABLE` |

- **The control plane serves its last good config** when building a snapshot fails or takes longer than 10 seconds. With two or more control-plane replicas, a Postgres outage therefore doesn't stop workers from getting config.
- **Translation failure policy.** The snapshot carries `translation.mode` and `errorOnUnsupported`, and the worker applies them on activation without a restart.

### Starting while the control plane is down

Any config a worker starts with while the control plane is down may be missing recent edits. The design keeps it as recent as possible and makes that visible:

<img
  src="/img/docs/worker-mode-peer-bootstrap.svg"
  alt="Peer bootstrap sequence: new worker copies config from a running peer when the control plane is unreachable, then confirms when the control plane returns"
/>

- **Where peers come from:** `worker.upstream.peerBootstrap.address` is usually a headless Service. Each address is tried in turn, and only ready workers, which all have a config, are listed.
- **Age limit:** `confirmedAt` is when the control plane last confirmed that config. It is carried along when one peer copies from another, so `maxAgeSecs` bounds the true age.
- **Visibility:** a copied config is reported as `configSource: peer` and `stale: true`, and `queryflux_worker_config_stale` is `1`. Once the control plane answers, the worker confirms or replaces the config with a conditional `GET`. It doesn't use a long-poll here, because a long-poll would delay confirmation.
- **Rejected alternative:** a seed config shipped in a Secret. Edits made in Studio after the export would be silently lost.

## Shared capacity

`maxRunningQueries` caps how many queries run on a cluster at once. Distributed standalone replicas already enforce it through capacity leases in Postgres. Workers use the same lease table, through the control plane:

<img
  src="/img/docs/worker-mode-capacity-lease.svg"
  alt="Capacity lease sequence: client query acquires a lease via the control plane before executing on the engine; fleet-full denials queue and retry"
/>

- **The query path doesn't change.** The worker's `RemoteCoordinationStore` implements the same `CapacityStore` and `SweepCoordinator` traits that dispatch, the slot guard and the engine reconcile loop already use. The control plane exposes its own store at `/worker/capacity/*` and `/worker/sweep-lock/*`.
- **Mixed fleets share one cap.** A control plane that also serves queries uses the lease table directly, and its workers use it through the API, so one `maxRunningQueries` covers all of them.
- **Leases of dead workers expire.** Workers renew their leases on a timer, and the control plane's stale-lease sweep reclaims a worker's leases once its renewals stop. Sweep locks held for a worker are dropped after 5 minutes if never released.
- **Releases are batched.** A worker queues each release and sends them together (`POST /worker/capacity/release-batch`), so a finishing query doesn't wait on the control plane. What a lease costs: [Capacity and throughput](./capacity-and-throughput).
- **Quotas.** With `capacity.mode: quotas` on the control plane, each worker gets a share of every cluster's limit and admits within it without a round trip; per-query leases cover bursts and older workers. See [Capacity quotas](./capacity-and-throughput#capacity-quotas).
- **Local work stays local.** A worker's zombie and stale-queue sweeps clean its own memory, so they never take fleet-wide locks.
- **Cost:** one round trip per query to acquire a lease, with a 5-second timeout. The release is off the query path: a finishing query doesn't wait for it. Releases that finish together share one request; a query that finishes alone still sends its own.

| | `worker.coordination.mode: controlPlane` (default) | `worker.coordination.mode: local` |
|---|---|---|
| Limit applies to | the whole fleet | each worker separately |
| Limit 10, three workers | at most 10 queries | up to 30 queries |
| Control plane unreachable | `onUnavailable: failClosed`: new queries wait. `localLimits`: each worker admits on its own limit. | unaffected |

The control plane warns, in its log and in Studio, when two or more workers are online and any of them uses local limits.

### Fairness

Waiting queries are served first come, first served across the fleet. A query denied a lease joins a line kept in Postgres, and a release hands the freed slot to the oldest query in it. A waiting worker long-polls `POST /worker/capacity/wait` and is woken when its query gets a slot. Details: [Waiting for capacity](./capacity-and-throughput#waiting-for-capacity).

To compare workers, watch, per worker:
- `queryflux_queue_duration_seconds`. Synchronous dispatch records its wait too; it used to record 0.
- `queryflux_capacity_lease_denied_total`.

## Query history

Workers keep no durable history. The control plane is the system of record:

<img
  src="/img/docs/worker-mode-history-pipeline.svg"
  alt="Query history pipeline: finished query, redact, bounded spool, POST query-records, history store used by Studio"
/>

- **The query path never waits on the network.** A finished query is redacted and queued. A flusher sends batches every 2 seconds, and a final flush runs after the shutdown drain. When the endpoint stays down, the oldest records are dropped and counted (`historyUplink.dropped`).
- **Delivery is at least once and ingest is idempotent.** Records are keyed by proxy query id, so a retried batch stores nothing twice.
- **One write per batch.** On Postgres, a batch becomes one multi-row insert into `query_records` (`ON CONFLICT (proxy_query_id) DO NOTHING`) and one multi-row upsert into `query_digest_stats`, in one transaction. A 200-record batch is two statements, not 400. The control plane's own queries take the same path: its buffered store flushes up to 100 records per call.
- **Digest stats are folded in memory.** The batch is folded into one delta per fingerprint (call count, sums, first and last seen, digest text). Only records the insert actually stored are folded, so a retried batch is not counted twice.
- **Backpressure.** The control plane writes at most 2 batches at once. A batch that cannot start within 5 seconds gets `503` with `Retry-After`. The worker keeps it spooled, waits as told (plus some jitter, so the fleet does not come back in one wave), and sends it again. Any other non-2xx answer or network error is retried with backoff up to 60 seconds. Queries never wait on this.
- **Format.** Records travel as a versioned `QueryRecordBatchV1`.
- **Attribution.** Each record is stamped with the sending worker (`query_records.worker`). Workers report config names, not database ids, so ingest resolves ids by name.
- **No double counting.** Ingest writes to the history store only, not to the control plane's Prometheus, because each worker already exports its own metrics.
- **Redaction is decided centrally and applied at the worker:**
  - `maskLiterals` replaces SQL with its parameterized digest, and masks literals in other SQL and in errors.
  - `metadata` drops all SQL, error text and the routing trace.

  The shared secret is sent only to the control plane, never to a separate history endpoint.
- **No cluster snapshots upstream.** With shared capacity, the control plane's own snapshots already count every worker's leases.

## Fleet registry and observability

- **Registration.** Workers register on start and heartbeat every `heartbeatIntervalSecs`, reporting:
  - revisions;
  - config source and staleness;
  - sync times and failures;
  - coordination policy;
  - history delivery counters;
  - enabled frontends.

  The control plane stores this in `worker_registry`.
- **`GET /admin/workers`** returns that registry with two computed fields: online or offline (three heartbeat intervals, at least 30 seconds) and whether each worker runs the current revision. Studio's Workers page and dashboard are built on it, and query history can be filtered by worker.
- **Ops listener.** Each worker's ops listener serves `/health`, `/readyz`, `/metrics` and `/worker/status`, plus `/worker/peer-config` with peer bootstrap on.
- **Key metrics:**
  - `queryflux_worker_config_stale`;
  - `queryflux_config_reload_failures_total{stage="worker_activate"}`;
  - `queryflux_coordination_failures_total`;
  - `queryflux_capacity_lease_denied_total`;
  - `queryflux_queue_duration_seconds`.

## Result cache

Each worker keeps its own result cache. Sharing one through the control plane would move result data through it, which the design rules out.

The cache key includes a fingerprint of the group's member clusters and their config. Repointing a group, or editing one of its clusters, stops serving earlier results. Edits to other groups leave its entries alone.

## Security model

- **Authentication.** Workers authenticate to the control plane with one shared secret (`Authorization: Bearer`), separate from the admin API's credentials. Peers use the same secret for `/worker/peer-config`.
- **Trust.** Snapshots contain engine credentials. Serve the control plane's admin port, and the ops ports when peer bootstrap is on, only over TLS or inside a trusted network. Any process holding the secret can read the full config.
- **Credentials stay in memory.** Workers keep credentials in memory only, never log them, and never show them in `/worker/status`.
- **No Postgres credentials.** Workers never hold credentials for QueryFlux's own database.

## Compatibility and upgrades

- **No change by default.** `role` defaults to `standalone`, so existing deployments behave as before.
- **Versioned wire formats.** The snapshot and the history batch carry `schemaVersion`. Upgrade the control plane first: a worker rejects a snapshot version it doesn't know and keeps its active config.
- **Migrations.** Three migrations add `worker_registry` (V18), `query_records.worker` (V19) and the registry's `status` column (V20).

## Decisions and alternatives

| Decision | Alternative considered | Why |
|---|---|---|
| Workers reach only the control plane's API | Workers read Postgres directly | Fewer credentials and network paths. Workers can run where the database must not be reachable. One place enforces auth and redaction. |
| Whole-config snapshots with a content-hash revision | Deltas, or a database revision counter | Simple to verify and to compare across replicas. A worker can tell whether it already has a config. Configs are small. |
| HTTP long-poll | Postgres `LISTEN/NOTIFY` on workers, or gRPC streams | Works through ordinary proxies and load balancers, needs no database access, and falls back to plain polling. |
| Strict activation, keep the last good config | Lenient compile | A partly applied config on a remote fleet is worse than a slightly old one. |
| A lease per query through the control plane | Pre-assigned quota slices per worker | Exact limit with no rebalancing. Uses the lease table distributed replicas already have. |
| Peer bootstrap plus the control plane's last good config | A seed config in a Secret, or an encrypted disk cache | Edits made in Studio aren't silently lost, and no credentials go to disk. |
| Per-worker result cache, keyed by group config | A cache shared through the control plane | Result data never leaves the worker's network. Edits invalidate exactly the affected group. |
| Measure fairness first | A global wait queue now | It may not be needed, and the metrics show when it is. |

## Hub

A hub ([operations guide](../operations/hub)) lets workers in a remote network reach a control plane through one process that only makes outbound connections. Its design follows from one rule: **workers can't tell.** The hub serves the exact worker API, so a worker needs no change beyond its `worker.upstream.url`.

- **Reuse, not a fork.** The hub is the control plane's `WorkerApi` with different backends:

  | `WorkerApi` field | Control plane | Hub |
  |---|---|---|
  | `snapshot` | built from Postgres, cached until reload | the snapshot fetched from upstream with the worker's own fetch code (`fetch_snapshot`: long-poll, schema and seal checks) |
  | `changes` | bumped on each reload | bumped on each new upstream revision |
  | `registry` | `worker_registry` in Postgres | in memory, posted upstream to `/worker/fleet` |
  | `history` | the history store | a spool on disk, posted upstream |
  | `coordination`, `quotas` | Postgres | the worker's `RemoteCoordinationStore`, pointed upstream |

- **The revision passes through.** The hub serves upstream's sealed snapshot as is, so the revision a worker reports is the one upstream published. The one exception: when upstream names a history endpoint, the hub removes it and re-seals, so workers send history only to the hub, which then relays to that endpoint.
- **History is acknowledged once it is on disk.** Each worker batch is redacted, written durably (temp file, fsync, rename), then acknowledged; a flusher posts the oldest batch upstream with `site` set, so each record keeps its worker's name. Upstream stores it as `<site>/<worker>`.
- **Capacity is forwarded.** One `maxRunningQueries` holds across every site with no state at the hub, so replicas need no leader. With `capacity.mode: quotas` upstream, per-query admission stays local to each worker.
- **Names are qualified upstream.** Workers appear as `<site>/<name>` with `parent` set to the site (migration V24 adds `kind` and `parent` to `worker_registry`), so `worker-0` at two sites doesn't collide.

## Future work

- **Forward to the owning worker.** Put the worker id in query ids, and forward a request to the worker that owns the query, so shared-service workers work behind any load balancer without sticky sessions.
- **Per-worker credentials, secret references and signed snapshots,** for lower-trust workers.
- **Translation scripts and timeout from the control plane:** `translation.pythonScripts` and `schemaResolutionTimeoutMs` still come from each worker's own config.
- **Hub follow-ups:** a site-wide quota held by the hub, so a site keeps admitting through a long upstream outage; Studio actions run at the site (test a connection, browse a catalog); per-site credentials; snapshots filtered per site.
- **Query telemetry:** engine-side details (plans, scan statistics, Iceberg metrics) matched to each query and merged into its history record.
