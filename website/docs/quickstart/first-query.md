---
sidebar_position: 1
sidebar_label: Route your first query
title: Route your first query
description: Start QueryFlux, Trino, Postgres, and Studio with Docker Compose, send SQL through the proxy, and find the query in Studio.
image: img/queryflux-hero-banner.png
---
# Route your first query

In this quickstart you run QueryFlux in front of a single Trino cluster, send SQL to it with the Trino CLI, and find those queries in Studio's query history. It uses the [`examples/minimal-trino`](https://github.com/lakeops-org/queryflux/tree/main/examples/minimal-trino) Docker Compose stack.

## What you'll build

```mermaid
flowchart LR
  cli["Trino CLI<br/>or curl"] -->|"Trino HTTP<br/>:8080"| qf["QueryFlux"]
  qf --> trino["Trino<br/>(direct: :8081)"]
  qf -->|"query history,<br/>cluster config"| pg[("Postgres<br/>:5433")]
  studio["Studio<br/>:3000"] -->|"Admin API :9000"| qf
```

| Service | URL / connection |
| --- | --- |
| Trino HTTP **through** QueryFlux | `http://localhost:8080` |
| Trino direct (bypasses QueryFlux) | `http://localhost:8081` |
| QueryFlux Admin API | `http://localhost:9000` |
| QueryFlux Studio | `http://localhost:3000` (login **admin** / **admin**) |
| Postgres (from host) | `localhost:5433` — user `queryflux`, password `queryflux`, database `queryflux` |

## Prerequisites

- **Docker** and **Docker Compose**
- Ports **8080**, **8081**, **9000**, **3000**, and **5433** free on your machine
- Optional: the [Trino CLI](https://trino.io/docs/current/client/cli.html) on your host (needs Java). If you skip it, you can use the CLI that ships inside the Trino container instead.

:::tip One stack at a time
Several examples, including [Run locally without Docker](./local-duckdb.md), also use ports 8080 and 9000. Stop other QueryFlux stacks before you start this one, or change the published ports in `docker-compose.yml`.
:::

## 1. Start the stack

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux/examples/minimal-trino
docker compose up -d --wait
```

This starts three containers:

- `postgres` — QueryFlux's persistence store (query history, cluster and routing config)
- `trino` — the backend query engine
- `queryflux` — the proxy, Admin API, and Studio in one image

`--wait` returns once QueryFlux passes its health check on `:9000/health`. That only happens after QueryFlux has connected to Postgres and applied its schema migrations, so the first start can take a minute.

## 2. Understand the config

[`examples/minimal-trino/config.yaml`](https://github.com/lakeops-org/queryflux/blob/main/examples/minimal-trino/config.yaml) is mounted into the QueryFlux container:

```yaml title="examples/minimal-trino/config.yaml"
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
  persistence:
    type: postgres
    url: postgres://queryflux:queryflux@postgres:5432/queryflux
  adminApi:
    port: 9000

clusters:
  trino-1:
    engine: trino
    enabled: true
    endpoint: http://trino:8080

clusterGroups:
  trino:
    enabled: true
    maxRunningQueries: 100
    members: [trino-1]
    strategy:
      type: roundRobin

routers:
  - type: protocolBased
    trinoHttp: trino

routingFallback: trino

translation:
  errorOnUnsupported: false
```

- **`frontends.trinoHttp`** — QueryFlux accepts Trino HTTP protocol requests on port 8080. Clients connect here as if it were a Trino coordinator.
- **`clusters.trino-1`** — the backend Trino, reached on the Compose network at `http://trino:8080`.
- **`clusterGroups.trino`** — a group with one member. Routers target groups, not individual clusters; `maxRunningQueries` caps concurrent queries for the group.
- **`routers`** — the `protocolBased` router sends everything arriving on the Trino HTTP frontend to the `trino` group. `routingFallback` catches anything no router matched.
- **`persistence`** — query history and config live in Postgres. On first start, clusters and groups are seeded from this YAML; after that the database is the source of truth, and you change them in Studio or through the Admin API.

The full key reference is in [Configuration](../configuration.md).

## 3. Connect a client

Point the Trino CLI at **QueryFlux** on port 8080, not at Trino on 8081. Use any username; this example has no authentication.

```bash
trino --server http://localhost:8080 --user test
```

If you named the CLI JAR differently, use that filename, or `java -jar <file>` as in the Trino docs.

**No Trino CLI on your host?** The Trino container has one. Open a shell in it and connect to the `queryflux` service by its Compose hostname:

```bash
docker compose exec -it trino bash
trino --server http://queryflux:8080 --user test
```

:::warning Make sure you're going through QueryFlux
These all bypass QueryFlux and talk to Trino directly, so the query never shows up in Studio:

- `http://localhost:8081` from the host
- `http://localhost:8080` or `http://trino:8080` from **inside** the Trino container (there, `localhost` is the Trino coordinator itself)

Use `http://localhost:8080` from the host, or `http://queryflux:8080` from inside the Trino container.
:::

## 4. Run queries

At the `trino>` prompt, start with a trivial query:

```sql
select 1;
```

You should get one row back. The Trino image ships with a built-in `tpch` catalog of generated data, so you can also run real queries without loading anything:

```sql
SHOW CATALOGS;

SELECT count(*) AS n FROM tpch.tiny.customer;

SELECT n.name AS nation, count(*) AS customers
FROM tpch.tiny.customer c
JOIN tpch.tiny.nation n ON c.nationkey = n.nationkey
GROUP BY n.name
ORDER BY customers DESC
LIMIT 5;
```

`tpch.tiny.customer` has 1,500 rows. Type `quit` or press **Ctrl+D** to leave the CLI.

You can also skip the CLI and send a single statement with `curl`:

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: test" \
  -d "SELECT 42"
```

The response is a Trino protocol JSON document. Real clients follow its `nextUri` until the query finishes; the CLI does that for you.

### Confirm QueryFlux handled it

In another terminal, from `examples/minimal-trino`, follow the QueryFlux logs and run a query again:

```bash
docker compose logs -f queryflux
```

Each query through the proxy logs an info line containing `New query submitted`. If nothing appears, your client isn't talking to port 8080 on the `queryflux` service.

## 5. See it in Studio

1. Open [http://localhost:3000](http://localhost:3000) and sign in with **admin** / **admin**.
2. Go to **Queries** ([http://localhost:3000/queries](http://localhost:3000/queries)).
3. Your queries appear in the list. QueryFlux batches history writes to Postgres, so a query can take up to about **5 seconds** to show up after it finishes. Click a row to see its details.

Only traffic through QueryFlux on 8080 is recorded; queries sent straight to Trino on 8081 are not.

If the list stays empty after a refresh, check the QueryFlux logs for `Failed to flush query record`, and check that rows reached Postgres:

```bash
docker compose exec postgres psql -U queryflux -d queryflux \
  -c "SELECT proxy_query_id, status, created_at FROM query_records ORDER BY id DESC LIMIT 5;"
```

While you're in Studio, look at the clusters and routing pages too: they show the `trino-1` cluster, the `trino` group, and the `protocolBased` router seeded from `config.yaml`. See [QueryFlux Studio](../studio.md) for a tour.

:::warning Change the admin password
`admin` / `admin` is a first-boot default. That's fine for a local tryout; for anything else, change it on the **Security** page as described in [QueryFlux Studio](../studio.md#changing-the-admin-password).
:::

## Clean up

From `examples/minimal-trino`, stop the stack:

```bash
docker compose down
```

To also delete the Postgres volume (query history and the cluster config seeded from `config.yaml`):

```bash
docker compose down -v
```

Because Postgres becomes the source of truth after the first start, run `down -v` if you edit `config.yaml` and want the changes picked up on the next start.

## Next steps

- [Run locally without Docker](./local-duckdb.md) — the native binary with an embedded DuckDB engine
- [Route across multiple engines](./multi-engine.md) — add a second engine and route between them
- [Observability](./observability.md) — Prometheus metrics and Grafana dashboards
- [Configuration](../configuration.md) — every config key, including other router types
- [Routing and clusters](../architecture/routing-and-clusters.md) — how routers, groups, and selection strategies fit together
- [Installation](../installation/overview.md) — Docker, Kubernetes, or binary deployments
