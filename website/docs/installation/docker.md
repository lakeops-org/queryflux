---
sidebar_position: 3
sidebar_label: Docker
title: Run QueryFlux with Docker
description: Run the QueryFlux container image — Studio-included or server-only -slim — with your config.yaml, ports, Postgres persistence, and Docker Compose.
image: img/queryflux-hero-banner.png
---

# Docker

QueryFlux publishes one image to GitHub Container Registry in two flavours:

| Image | Contains | Ports |
| --- | --- | --- |
| `ghcr.io/lakeops-org/queryflux:<version>` | Server **and** QueryFlux Studio | `8080`, `9000`, `3000` |
| `ghcr.io/lakeops-org/queryflux:<version>-slim` | Server only | `8080`, `9000` |

`latest` and `latest-slim` follow the newest release. Pin a version tag (for example `v0.4.0`) for anything long-lived.

## Run a container

The example config below sends queries to a Trino at `http://trino:8080`. Put QueryFlux and Trino on one Docker network so that name resolves (skip the Trino container if you already have an engine reachable from Docker, and change `endpoint` to its address):

```bash
docker network create queryflux
docker run -d --name trino --network queryflux trinodb/trino
```

The image reads its config from `/etc/queryflux/config.yaml`. Mount yours there:

```bash
docker run -d --name queryflux --network queryflux \
  -p 8080:8080 -p 9000:9000 -p 3000:3000 \
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

Studio is at http://localhost:3000 when you run the non-slim image.

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
      - "3000:3000"
    volumes:
      - ./config.yaml:/etc/queryflux/config.yaml:ro
    environment:
      RUST_LOG: queryflux=info
      QUERYFLUX_ADMIN_USER: admin
      # Required: compose refuses to start until you export a password
      QUERYFLUX_ADMIN_PASSWORD: ${QUERYFLUX_ADMIN_PASSWORD:?set QUERYFLUX_ADMIN_PASSWORD}
    restart: unless-stopped
```

## Next steps

- Going to Kubernetes? Use the **[Helm chart](/docs/installation/kubernetes)**.
- Add clusters and routing rules: **[YAML reference](/docs/configuration)**.
