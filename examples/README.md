# Docker Compose examples

Several stacks for **QueryFlux** + **Trino** (and optional add-ons). Run commands **from inside** each example directory so paths like `./config.yaml` resolve.

**Images** in each `docker-compose.yml` may point at a private registry (for example legacy ECR URLs) — switch the `image:` line to **`ghcr.io/<owner>/<repo>:<tag>`** (see [contribute.md](../contribute.md) for release tags) or build from [`docker/`](../docker/).

| Example | Postgres | Best for |
|--------|----------|----------|
| [`minimal/`](minimal-trino/) | Yes | Full Studio (query history, persisted clusters/groups/routing via API), production-like persistence |
| [`minimal-inmemory/`](minimal-inmemory/) | No | Fastest local tryout; config only in `config.yaml`; no shared query history |
| [`with-mcp/`](with-mcp/) | No | MCP frontend + embedded DuckDB — point an AI agent (Cursor, Claude Code, MCP Inspector, ...) at QueryFlux with zero external services |
| [`with-opa/`](with-opa/) | Yes (host **5434**) | OPA data access control (allow/deny, row filters, column masks) + Lakekeeper/Trino; QueryFlux from this branch on the host — see [docs](../website/docs/access-control/opa.md) |
| [`with-opa-oidc/`](with-opa-oidc/) | No | Same OPA demo as `with-opa/`, identity from **Keycloak (OIDC)** instead of a static user list; LDAP config shape documented too |
| [`with-keycloak-oidc/`](with-keycloak-oidc/) | Yes | OIDC authentication with Keycloak as the identity provider — no access control, just auth |
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

## With MCP (`with-mcp/`)

**QueryFlux** with the **MCP frontend** enabled, backed by the **embedded DuckDB** engine — no Trino, no Postgres, nothing else to run. `persistence.type: inMemory` and `auth.provider: none`, same tradeoffs as `minimal-inmemory/`. Point an MCP client (Cursor, Claude Code, Claude Desktop, MCP Inspector) at the server and start running SQL through an agent immediately. Details: [`with-mcp/README.md`](with-mcp/README.md).

```bash
cd examples/with-mcp
docker compose up -d --wait
```

| Service | URL |
|---------|-----|
| MCP endpoint (streamable HTTP) | http://localhost:8811/mcp |
| Admin API | http://localhost:9000 |
| Studio | http://localhost:3000 |

---

## With OPA (`with-opa/`)

**Lakekeeper** + **MinIO** + **Trino** + **OPA** + **QueryFlux Postgres** (`:5434`) in Compose + **QueryFlux on the host** from this branch (the published image does not include `accessControl` yet). Postgres persistence enables query history and Studio **Access Control** saves. QueryFlux resolves Iceberg schema from Lakekeeper for OPA rewrites. Two static users: Alice sees every `customers` row; Bob is row-filtered to `EU` with SSN masked, and `payroll` is denied. Demo UI at **http://127.0.0.1:8183**. Details: [`with-opa/README.md`](with-opa/README.md).

```bash
cd examples/with-opa
docker compose up -d --wait
docker compose --profile seed run --rm data-seed
# from repo root:
cargo run -p queryflux -- --config examples/with-opa/config.yaml
python3 examples/with-opa/demo.py
```

| Service | URL |
|---------|-----|
| Demo UI | http://127.0.0.1:8183 |
| SQL (Trino HTTP via QueryFlux) | http://localhost:8080 |
| Admin API + dry-run | http://localhost:9000 |
| Trino (direct backend) | http://localhost:8081 |
| Lakekeeper REST catalog | http://127.0.0.1:8181 |
| OPA | http://127.0.0.1:8182 |

---

## With OPA + Keycloak OIDC (`with-opa-oidc/`)

Byte-for-byte the same policy/tables/rewrite as `with-opa/` above, but identity
comes from **Keycloak (OIDC)** instead of the static user list — adds a
**Keycloak** service and swaps `auth:` for `provider: oidc`. Same
Alice/Bob outcomes, now driven by a real access token fetched via password
grant instead of HTTP Basic. The README also documents the equivalent
`auth.provider: ldap` config shape for swapping in LDAP instead. Details:
[`with-opa-oidc/README.md`](with-opa-oidc/README.md).

```bash
cd examples/with-opa-oidc
docker compose up -d --wait
docker compose --profile seed run --rm data-seed
# from repo root:
cargo run -p queryflux -- --config examples/with-opa-oidc/config.yaml
python3 examples/with-opa-oidc/demo.py
```

| Service | URL |
|---------|-----|
| Demo UI | http://127.0.0.1:8183 |
| SQL (Trino HTTP via QueryFlux) | http://localhost:8080 |
| Admin API + dry-run | http://localhost:9000 |
| Keycloak (admin `admin`/`admin`) | http://localhost:8180 |
| Trino (direct backend) | http://localhost:8081 |
| Lakekeeper REST catalog | http://127.0.0.1:8181 |
| OPA | http://127.0.0.1:8182 |

---

## With Keycloak OIDC (`with-keycloak-oidc/`)

OIDC authentication only — no access control. **Keycloak** issues JWTs for
two test users (`alice`/`bob`); QueryFlux verifies them via JWKS and gates the
Trino HTTP frontend on a valid Bearer token. Three `queryAuth` backend-identity
modes are covered as reference configs (`passthrough` default,
`config-impersonate.yaml`, `config-token-exchange.yaml`). Details:
[`with-keycloak-oidc/README.md`](with-keycloak-oidc/README.md).

```bash
cd examples/with-keycloak-oidc
docker compose up -d --wait
```

| Service | URL |
|---------|-----|
| SQL (Trino via QueryFlux, OIDC-protected) | http://localhost:8080 |
| Studio | http://localhost:3000 |
| Admin API | http://localhost:9000 |
| Keycloak (admin `admin`/`admin`) | http://localhost:8180 |
| Trino (direct) | http://localhost:8081 |

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

