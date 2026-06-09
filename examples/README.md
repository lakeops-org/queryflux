# Docker Compose examples

Several stacks for **QueryFlux** + **Trino** (and optional add-ons). Run commands **from inside** each example directory so paths like `./config.yaml` resolve.

**Images** in each `docker-compose.yml` may point at a private registry (for example legacy ECR URLs) — switch the `image:` line to **`ghcr.io/<owner>/<repo>:<tag>`** (see [contribute.md](../contribute.md) for release tags) or build from [`docker/`](../docker/).

| Example | Postgres | Best for |
|--------|----------|----------|
| [`minimal/`](minimal-trino/) | Yes | Full Studio (query history, persisted clusters/groups/routing via API), production-like persistence |
| [`minimal-inmemory/`](minimal-inmemory/) | No | Fastest local tryout; config only in `config.yaml`; no shared query history |
| [`with-prometheus-grafana/`](with-prometheus-grafana/) | Yes | Same workload as minimal + **Prometheus** + **Grafana** (repo [`grafana/`](../grafana/), local scrape config); **no Studio** |
| [`full-stack/`](full-stack/) | Yes (host **5433**) | Trino + StarRocks + Iceberg/Lakekeeper + MinIO + TPCH loader |
| [`full-stack-with-prometheus-grafana/`](full-stack-with-prometheus-grafana/) | Yes (host **5433**) | **`full-stack`** + **Prometheus** + **Grafana**; Grafana on **3001** |

`minimal/` and `minimal-inmemory/` use the **same host ports** (8080, 8081, 3000, 9000); **`minimal/`** also maps Postgres to **`localhost:5433`**. **`with-prometheus-grafana`** also uses **3000 for Grafana** (not Studio) — run it alone or change the published Grafana port.

---

## Minimal (`minimal/`)

Postgres + **Trino** + **QueryFlux** + **Studio**. [`config.yaml`](minimal-trino/config.yaml) sends Trino HTTP on `:8080` through QueryFlux to the Trino container. With Postgres, cluster/group rows are **seeded from YAML only when those tables are empty**; after that the **database** is the source of truth. Walkthrough (Trino CLI, `select 1`, Queries UI): [`minimal/README.md`](minimal-trino/README.md).

```bash
cd examples/minimal-trino
docker compose up -d --wait
```

| Service | URL |
|---------|-----|
| SQL (Trino via QueryFlux) | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin API | http://localhost:9000 |
| Studio | http://localhost:3000 |
| Postgres | `localhost:5433` (`queryflux` / `queryflux`, database `queryflux`) |

---

## Minimal in-memory (`minimal-inmemory/`)

**Trino** + **QueryFlux** + **Studio**, **`persistence.type: inMemory`** — no Postgres. All routing/clusters/groups come from [`minimal-inmemory/config.yaml`](minimal-inmemory/config.yaml); **restart QueryFlux** after edits. Studio pages that need Postgres (query list, persisted config CRUD) will not work; see [`minimal-inmemory/README.md`](minimal-inmemory/README.md).

```bash
cd examples/minimal-trino-inmemory
docker compose up -d --wait
```

| Service | URL |
|---------|-----|
| SQL (Trino via QueryFlux) | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin API | http://localhost:9000 |
| Studio | http://localhost:3000 |

---

## With Prometheus + Grafana (`with-prometheus-grafana/`)

**Postgres** + **Trino** + **QueryFlux** plus **Prometheus** and **Grafana**, matching [`docker/docker-compose.yml`](../docker/docker-compose.yml) observability services. Grafana mounts [`grafana/`](../grafana/) from the repo root; Prometheus uses [`with-prometheus-grafana/prometheus.yml`](with-prometheus-grafana/prometheus.yml) to scrape `queryflux:9000` (the root [`prometheus/prometheus.yml`](../prometheus/prometheus.yml) is for QueryFlux on the **host**). Details: [`with-prometheus-grafana/README.md`](with-prometheus-grafana/README.md).

```bash
cd examples/with-prometheus-grafana
docker compose up -d --wait
```

| Service | URL |
|---------|-----|
| Trino via QueryFlux | http://localhost:8080 |
| Trino (direct) | http://localhost:8081 |
| Admin + `/metrics` | http://localhost:9000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

---

## Full stack (`full-stack/`)

Same idea as [`docker/docker-compose.yml`](../docker/docker-compose.yml): **Trino**, **StarRocks**, **Lakekeeper**, **MinIO**, **QueryFlux**, **Studio**. Optional loader brings TPCH into Iceberg via Trino.

```bash
cd examples/full-stack
docker compose up -d --wait
docker compose --profile loader run --rm -T data-loader
docker compose --profile loader run --rm -T starrocks-catalog-setup
```

The loader uses [`docker/fixtures/init.docker-network.sql`](../docker/fixtures/init.docker-network.sql) so object storage is `http://minio:9000` inside the compose network (unlike `docker/fixtures/init.sql`, which targets `host.docker.internal:19000` for hybrid host/DuckDB setups).

| Service | URL |
|---------|-----|
| Trino via QueryFlux | http://localhost:8080 |
| MySQL wire (StarRocks via QueryFlux) | `mysql` client → **localhost:3306** |
| Node.js sample (same MySQL wire) | [`node-starrocks-via-queryflux/`](node-starrocks-via-queryflux/) — `npm install && npm start` |
| Studio | http://localhost:3000 |
| Trino (direct) | http://localhost:8081 |
| MinIO console | http://localhost:19001 |
| Lakekeeper REST | http://localhost:8181 |

QueryFlux Postgres is exposed on **localhost:5433** (same as the main dev compose convention).

---

## Full stack + Prometheus + Grafana (`full-stack-with-prometheus-grafana/`)

Everything in **`full-stack`**, plus **Prometheus** and **Grafana** from [`with-prometheus-grafana/`](with-prometheus-grafana/). Grafana mounts [`grafana/`](../grafana/) from the repo root; Prometheus uses [`full-stack-with-prometheus-grafana/prometheus.yml`](full-stack-with-prometheus-grafana/prometheus.yml) to scrape `queryflux:9000`. **Grafana** is on **3001** so **Studio** keeps **3000**. Details: [`full-stack-with-prometheus-grafana/README.md`](full-stack-with-prometheus-grafana/README.md).

```bash
cd examples/full-stack-with-prometheus-grafana
docker compose up -d --wait
docker compose --profile loader run --rm -T data-loader
docker compose --profile loader run --rm -T starrocks-catalog-setup
```

| Service | URL |
|---------|-----|
| Trino via QueryFlux | http://localhost:8080 |
| MySQL wire (StarRocks via QueryFlux) | `mysql` client → **localhost:3306** |
| Studio | http://localhost:3000 |
| Trino (direct) | http://localhost:8081 |
| Admin + `/metrics` | http://localhost:9000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 |
| MinIO console | http://localhost:19001 |
| Lakekeeper REST | http://localhost:8181 |

---

## Environment overrides

| Variable | Use |
|----------|-----|
| `RUST_LOG` | QueryFlux logging (default in compose: `queryflux=info,queryflux_frontend=info`) |
| `TPCH_SCALE` | Full stack, `loader` profile: `tiny` (default), `sf1`, … — see `docker/fixtures/init.sql` |

