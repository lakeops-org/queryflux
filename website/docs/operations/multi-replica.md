---
title: Multi-replica operations
description: When and how to run QueryFlux across multiple replicas — Postgres coordination, capacity leases, queue claims, session affinity, and crash recovery.
image: img/queryflux-hero-banner.png
---

# Multi-replica operations

QueryFlux can run as a single process or as a fleet of replicas that share state through Postgres. This runbook covers **when** to scale, **what** must be true for coordination to work, and **how** to operate a multi-replica deployment.

For chart knobs and install notes, see also the [Helm chart README](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/README.md).

## Modes at a glance

| Mode | Persistence | Replicas | Capacity / queue | Safe? |
|------|-------------|----------|------------------|-------|
| Single instance | `inMemory` or Postgres | 1 | Local only | Yes — default evaluation path |
| Multi-replica without Postgres | `inMemory` | ≥ 2 | **Per-pod** (diverges) | **No** — do not do this |
| Coordinated multi-replica | `postgres` | ≥ 2 | Fleet-wide leases + single-owner queue claims | Yes — production HA path |

With `persistence.type: postgres`, QueryFlux enables **distributed mode** automatically (unless you set `queryflux.distributed: false`). In that mode every replica talks to the same database for capacity leases, queue ownership, config revisions, and query history.

## When not to scale

Do **not** set `replicaCount > 1` (or enable HPA) if any of the following hold:

1. **Persistence is still `inMemory`** — each pod has its own queue, history, and config view. The Helm install NOTES warn about this; treat it as a hard stop for production.
2. **Postgres is not highly available or shared** — all replicas must use the **same** database URL. A per-pod Postgres sidecar is not shared state.
3. **Snowflake HTTP is enabled without sticky sessions** — Snowflake sessions live in process memory. Without load-balancer session affinity, clients get random pods and broken sessions. Prefer sticky LB, or keep Snowflake on a single replica until affinity is in place.
4. **Coordination backends cannot keep up** — sustained `queryflux_coordination_failures_total` means global `maxRunningQueries` and single-owner queue claiming are not being enforced.

## Prerequisites checklist

- [ ] `persistence.type: postgres` with one shared HA Postgres reachable from every pod
- [ ] Prefer `config.existingSecret` for the Postgres URL (do not put passwords in a ConfigMap)
- [ ] Helm `replicaCount ≥ 2` (or HPA with `minReplicas ≥ 2`) and a PDB for rolling upgrades
- [ ] `terminationGracePeriodSeconds` ≥ `queryflux.shutdownDrainTimeoutSecs` (defaults **45** ≥ **30**) plus buffer
- [ ] If Snowflake HTTP is enabled: sticky sessions on that port, and set `frontends.snowflakeHttp.sessionAffinityAcknowledged: true` so the requirement is recorded in config
- [ ] Prometheus scrape of admin `/metrics` (ServiceMonitor or equivalent)

Starter values: [`charts/queryflux/examples/production-values.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/production-values.yaml).

## Instance identity

Each process needs a stable-for-its-lifetime **instance ID**:

- Auto: `qf-{HOSTNAME}-{8-char-uuid}` (unique per process incarnation)
- Override: env `QUERYFLUX_INSTANCE_ID`

Use the override only when you need a deterministic ID (for example debugging leases). Never share one instance ID across two concurrent processes.

On graceful shutdown QueryFlux drains in-flight work (`shutdownDrainTimeoutSecs`, default **30s**) and releases capacity leases owned by that instance.

## Global capacity (`maxRunningQueries`)

In distributed mode, group `maxRunningQueries` is enforced **fleet-wide** via Postgres capacity leases (`CapacityStore`), not by summing local counters alone.

| Behavior | Detail |
|----------|--------|
| Admit | Acquire a lease for `(cluster, query_id)` before running |
| Heartbeat | ~**60s** while the query runs |
| Stale expire | Leases not heartbeated for ~**300s** are freed (crash recovery) |
| Coordination failure | Admit path is **fail-closed** (queue / deny) rather than over-admit |

Related group knobs:

- `maxQueuedQueries` — proxy queue depth (enforced from LiveConfig)
- `capacityWaitTimeoutSecs` — max wait for a slot (default **300**)

**Alert on:** `queryflux_coordination_failures_total`, `queryflux_capacity_degraded_total`.

## Queue coordination

Queued Trino HTTP queries are claimed by exactly one replica before dispatch:

- Claim stale timeout ≈ **60s**
- Claim heartbeat ≈ **15s** during a long dispatch
- Stale claims can be taken over if the owning replica died mid-dispatch

Clients that stop polling are cleaned up by the stale queued-query sweeper (idle ≈ **5 minutes**). Capacity wait still uses **creation time** against `capacityWaitTimeoutSecs`, not last-accessed.

## Config reload across replicas

| Trigger | Mechanism |
|---------|-----------|
| Admin write on this replica | Local notify (fast path) |
| Admin write on another replica | Postgres config revision + LISTEN/NOTIFY when available |
| Safety net | Periodic poll (`configReloadIntervalSecs`; omit → **30s**; `0` → poll off) |

Reload keeps **last-good** LiveConfig on failure. Alert on `queryflux_config_reload_failures_total{stage=…}` (`reload`, `auth_rebuild`, `authz_rebuild`, `guard_reload`).

## Session affinity by frontend

| Frontend | Multi-replica affinity needed? |
|----------|--------------------------------|
| Trino HTTP | No — state in Postgres / backend |
| MySQL / Postgres wire | No — connection-scoped |
| Flight SQL | No — connection-scoped |
| Snowflake HTTP | **Yes** — process-local sessions |
| Snowflake SQL API | No — bearer auth per request |

## Crash recovery playbook

1. **Pod killed mid-query** — capacity lease expires (~300s without heartbeat) and the slot returns to the pool; queued claims older than the claim timeout become takeable.
2. **Rolling update** — PDB + drain timeout + grace period let the old pod release leases; new pods join with new instance IDs.
3. **Postgres outage** — coordination fails closed for capacity; expect queue growth / client errors. Fix Postgres before scaling further.
4. **Scale to zero then up** — any leases from dead instances expire; no manual cleanup is required for the happy path.

## Metrics checklist

| Metric | Why it matters |
|--------|----------------|
| `queryflux_running_queries` | Per-group / cluster utilization |
| `queryflux_queued_queries` | Proxy wait depth |
| `queryflux_coordination_failures_total` | Distributed path falling back / failing |
| `queryflux_capacity_degraded_total` | Admits without global lease enforcement |
| `queryflux_config_reload_failures_total` | Soft-failed hot reload |
| `queryflux_auth_failures_total` | Credential stuffing / misconfig |
| `queryflux_queue_rejections_total` | `maxQueuedQueries` hits |

## Related docs

- [Routing & cluster groups](../architecture/routing-and-clusters.md) — routers, strategies, queueing semantics
- [Observability](../architecture/observability.md) — metrics surface
- [Snowflake frontend](../architecture/frontends/snowflake.md) — session affinity
- [Configuration](../configuration.md) — YAML shape
- Helm chart README — replica / persistence / drain invariants
