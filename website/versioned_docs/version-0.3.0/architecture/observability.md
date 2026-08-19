---
title: Observability
description: Prometheus metrics, Grafana dashboards, QueryFlux Studio, and the Admin REST API for operational visibility.
image: img/queryflux-hero-banner.png
---
# Observability

QueryFlux exposes three observability surfaces: **Prometheus metrics** (real-time operational), a **Grafana dashboard** (visual ops view), and **QueryFlux Studio** (admin UI with query history, cluster management, and config).

---

## Prometheus metrics

QueryFlux exposes a `/metrics` endpoint (default port 9000) in standard Prometheus text format.

**Scrape target:** `http://<host>:9000/metrics`

### Exposed metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `queryflux_queries_total` | Counter | `engine_type`, `cluster_group`, `status`, `protocol` | Total queries by outcome and engine |
| `queryflux_query_duration_seconds` | Histogram | `engine_type`, `cluster_group` | End-to-end query duration from proxy receipt to result delivery |
| `queryflux_translated_queries_total` | Counter | `src_dialect`, `tgt_dialect` | Queries where SQL dialect translation ran |
| `queryflux_running_queries` | Gauge | `cluster_group`, `cluster_name` | Currently executing queries per cluster |
| `queryflux_queued_queries` | Gauge | `cluster_group` | Queries waiting for a free cluster slot |
| `queryflux_query_tags_total` | Counter | `tag_key`, `tag_value`, `cluster_group` | Per-tag counter — tracks which workloads (teams, cost centers, etc.) drive load per group. See [Query tags](./query-tags#prometheus). |
| `queryflux_cache_hits_total` | Counter | `cluster_group` | Query result cache hits |
| `queryflux_cache_misses_total` | Counter | `cluster_group` | Query result cache misses |
| `queryflux_cache_writes_total` | Counter | `cluster_group` | Query results written to cache |

The metrics pipeline uses `MultiMetricsStore` to fan out to Prometheus (real-time) and optionally Postgres (historical). `BufferedMetricsStore` wraps the Postgres store to avoid blocking query execution on I/O.

### Prometheus config

The `prometheus/prometheus.yml` file configures a single scrape job:

```yaml
scrape_configs:
  - job_name: queryflux
    static_configs:
      - targets: ["host.docker.internal:9000"]
    metrics_path: /metrics
```

When running via `make env`, Prometheus starts in Docker and reaches QueryFlux on the host via `host.docker.internal`. On Linux, the `docker-compose.yml` adds `extra_hosts: ["host.docker.internal:host-gateway"]` to bridge this.

---

## Grafana dashboard

A pre-built dashboard (`grafana/dashboards/queryflux.json`) is auto-provisioned when Grafana starts via Docker Compose. It auto-refreshes every 10 seconds.

**Access:** http://localhost:3000 (credentials: `admin` / `admin`)

### Panels

| Panel | Type | What it shows |
|-------|------|---------------|
| Query Rate | Stat | Queries per minute, current window |
| Error Rate | Stat | Fraction of failed queries (%) |
| p95 Latency | Stat | 95th-percentile end-to-end duration |
| Translation Rate | Stat | Fraction of queries that ran through sqlglot |
| Query Throughput by Status | Time series | Success / Failed / Cancelled over time |
| Query Throughput by Engine | Time series | Per-engine query rate over time |
| Latency Percentiles (p50 / p95 / p99) | Time series | Latency distribution trends |
| SQL Translations by Dialect Pair | Time series | Translation volume per src→tgt pair |
| Query Throughput per Cluster | Time series | Per-cluster query rate |
| Queued Queries per Cluster Group | Time series | Queue depth per group — spikes indicate saturation |

### Provisioning

Grafana is auto-configured via two provisioning files:

- `grafana/provisioning/datasources/prometheus.yml` — registers the Prometheus datasource pointing at `http://prometheus:9090`
- `grafana/dashboards/` — dashboards loaded automatically at startup; no manual import needed

---

## QueryFlux Studio (admin UI)

QueryFlux Studio is a Next.js web UI served separately from the proxy. It talks to the Admin REST API on port 9000.

**Start:** `cd queryflux-studio && npm run dev` (or build and serve for production)

**Default URL:** http://localhost:3000

:::tip Avoid port conflict with Grafana

Grafana (above) also binds to port 3000. If you run both simultaneously, either start Studio on a different port:

```bash
PORT=3001 npm run dev
```

or remap Grafana in `docker-compose.yml` (`ports: ["3001:3000"]`).

### Pages

| Page | What it shows |
|------|---------------|
| Dashboard | Live query rate, error rate, avg latency, translation rate; cluster health grid; recent queries |
| Clusters | All cluster groups and member clusters — health, running/queued counts, enable/disable, max concurrency |
| Queries | Searchable, filterable query history — SQL, status, duration, engine, routing trace |
| Engines | Engine registry — supported engines, connection types, config fields |

Studio requires **Postgres persistence** to be configured — query history, cluster config, and dashboard stats are read from the DB. Without Postgres, the clusters page works (from in-memory state) but history pages return empty.

---

## Admin REST API

The admin API is served on port 9000 alongside `/metrics`. An OpenAPI spec is available at `http://localhost:9000/openapi.json` and the Swagger UI at `http://localhost:9000/docs`.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/admin/clusters` | Live cluster state (all groups) |
| `PATCH` | `/admin/clusters/{group}/{cluster}` | Update cluster (enable/disable, max concurrency) |
| `GET` | `/admin/queries` | Query history (paginated, filterable) |
| `GET` | `/admin/stats` | Aggregated stats for the last hour |
| `GET` | `/admin/engine-stats?hours=N` | Per-engine aggregated stats |
| `GET` | `/admin/group-stats?hours=N` | Per-cluster-group aggregated stats |
| `GET` | `/admin/engines` | Distinct engine types in query log |
| `GET` | `/admin/engine-registry` | Full engine descriptor catalog |
| `GET/PUT/DELETE` | `/admin/config/clusters/{name}` | Cluster config CRUD (Postgres required) |
| `GET` | `/admin/config/clusters` | List all persisted cluster configs |
| `GET/PUT/DELETE` | `/admin/config/groups/{name}` | Cluster group config CRUD (Postgres required) |
| `GET` | `/admin/config/groups` | List all persisted group configs |
| `DELETE` | `/admin/cache` | Invalidate all cached query results |
| `DELETE` | `/admin/cache/{group}` | Invalidate cached results for a group |
| `GET` | `/openapi.json` | OpenAPI spec |
| `GET` | `/docs` | Swagger UI |

Config CRUD endpoints (`/admin/config/*`) require Postgres persistence. The in-memory store supports reading live cluster state but not persisted config management.
