---
sidebar_position: 3
sidebar_label: Docker
title: Run QueryFlux with Docker
description: Run the QueryFlux server image with your config.yaml, ports, Postgres persistence, and Docker Compose — Studio is a separate image.
image: img/queryflux-hero-banner.png
---

# Docker

QueryFlux publishes the server image to GitHub Container Registry:

| Image | Contains | Ports |
| --- | --- | --- |
| `ghcr.io/lakeops-org/queryflux:<version>` | Server | `8080`, `9000` (plus any frontends you enable) |
| `ghcr.io/lakeops-org/queryflux-studio:<version>` | QueryFlux Studio (optional) | `3000` |

`latest` is the newest release; builds of `main` that are not releases are published as `main`. Pin a version tag for anything long-lived. Studio is built in its own repo and run next to the server (Compose service or Helm sidecar).

## Run a container

The example config below sends queries to a Trino at `http://trino:8080`. Put QueryFlux and Trino on one Docker network so that name resolves (skip the Trino container if you already have an engine reachable from Docker, and change `endpoint` to its address):

```bash
docker network create queryflux
docker run -d --name trino --network queryflux trinodb/trino
```

The image reads its config from `/etc/queryflux/config.yaml`. Mount yours there:

```bash
docker run -d --name queryflux --network queryflux \
  -p 8080:8080 -p 9000:9000 \
  -v "$PWD/config.yaml:/etc/queryflux/config.yaml:ro" \
  -e QUERYFLUX_ADMIN_USER=admin \
  -e QUERYFLUX_ADMIN_PASSWORD='<strong-password>' \
  ghcr.io/lakeops-org/queryflux:latest
```

A minimal `config.yaml` that sends Trino HTTP traffic to one Trino cluster:

```yaml title="config.yaml"
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
  persistence:
    type: inMemory
  adminApi:
    port: 9000

clusters:
  trino-1:
    engine: trino
    enabled: true
    endpoint: http://trino:8080

clusterGroups:
  trino:
    maxRunningQueries: 100
    members: [trino-1]

routers:
  - type: protocolBased
    trinoHttp: trino

routingFallback: trino
```

Publish a port for every extra frontend you enable (for example `-p 5432:5432` for PostgreSQL wire, `-p 3306:3306` for MySQL wire).

Check it's up:

```bash
curl -s http://localhost:9000/health
curl -X POST http://localhost:8080/v1/statement -H "X-Trino-User: dev" -d "SELECT 42"
```

To run Studio beside the server, start `ghcr.io/lakeops-org/queryflux-studio` with `ADMIN_API_URL` pointing at the admin port (see the Compose examples under [`examples/`](https://github.com/lakeops-org/queryflux/tree/main/examples)).

## Environment variables

| Variable | Purpose |
| --- | --- |
| `QUERYFLUX_ADMIN_USER`, `QUERYFLUX_ADMIN_PASSWORD` | Admin API and Studio login. Set a strong password outside local testing. |
| `RUST_LOG` | Log filter, e.g. `queryflux=info,queryflux_frontend=debug`. |
| `QUERYFLUX_INSTANCE_ID` | Override the replica ID (multi-replica with Postgres only). |
| `ADBC_DRIVERS` / `ADBC_DRIVER_PATH` | Install or locate ADBC drivers — see **[ADBC drivers](/docs/operations/adbc-drivers)**. |

QueryFlux does **not** expand environment variables inside `config.yaml`. Keep secrets in the mounted file and protect it like any other credential.

## Postgres persistence

For more than one container, or to keep query history and Studio edits across restarts, switch to Postgres:

```yaml
queryflux:
  persistence:
    type: postgres
    url: postgres://queryflux:queryflux@postgres:5432/queryflux
```

Schema migrations run at startup by default. Details: **[Multi-replica operations](/docs/operations/multi-replica)**.

## Docker Compose

The repository's [`examples/`](https://github.com/lakeops-org/queryflux/tree/main/examples) directory has ready-made Compose stacks — Trino, Postgres, Studio, Prometheus/Grafana, and a multi-engine Iceberg setup. **[Getting started](/docs/getting-started)** walks through each one.

A minimal service definition to drop into your own Compose file:

```yaml title="docker-compose.yml"
services:
  queryflux:
    image: ghcr.io/lakeops-org/queryflux:latest
    ports:
      - "8080:8080"
      - "9000:9000"
    volumes:
      - ./config.yaml:/etc/queryflux/config.yaml:ro
    environment:
      RUST_LOG: queryflux=info
      QUERYFLUX_ADMIN_USER: admin
      # Required: compose refuses to start until you export a password
      QUERYFLUX_ADMIN_PASSWORD: ${QUERYFLUX_ADMIN_PASSWORD:?set QUERYFLUX_ADMIN_PASSWORD}
    restart: unless-stopped

  queryflux-studio:
    image: ghcr.io/lakeops-org/queryflux-studio:latest
    ports:
      - "3000:3000"
    environment:
      ADMIN_API_URL: http://queryflux:9000
    depends_on:
      - queryflux
    restart: unless-stopped
```

## Next steps

- Going to Kubernetes? Use the **[Helm chart](/docs/installation/kubernetes)**.
- Add clusters and routing rules: **[YAML reference](/docs/configuration)**.
