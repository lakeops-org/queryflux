---
sidebar_position: 1
sidebar_label: Overview
title: Quickstarts
description: Hands-on QueryFlux tutorials — route your first query, run locally, build a multi-engine lakehouse, add monitoring, connect AI agents, and secure with OIDC.
image: img/queryflux-hero-banner.png
---

import {DocCards, DocCard} from '@site/src/components/DocCards';

# Quickstarts

Each quickstart is a complete scenario you can run end to end on a laptop, built on a Docker Compose stack from the repository's [`examples/`](https://github.com/lakeops-org/queryflux/tree/main/examples) directory. Pick the one closest to what you want to do.

<DocCards>
  <DocCard title="Route your first query" to="/docs/quickstart/first-query">Postgres + Trino + QueryFlux + Studio. Send a query through the proxy and find it in query history.</DocCard>
  <DocCard title="Run locally without Docker" to="/docs/quickstart/local-duckdb">Install the binary and query the embedded DuckDB engine — no containers, no other services.</DocCard>
  <DocCard title="Build a multi-engine lakehouse" to="/docs/quickstart/multi-engine">Trino and StarRocks over Iceberg. Route Trino HTTP and MySQL wire clients to different engines.</DocCard>
  <DocCard title="Monitor with Prometheus & Grafana" to="/docs/quickstart/observability">Scrape QueryFlux metrics and watch queues, latency, and routing in Grafana dashboards.</DocCard>
  <DocCard title="Query from an AI agent (MCP)" to="/docs/quickstart/ai-agent-mcp">Point Claude Code, Cursor, or MCP Inspector at QueryFlux and run SQL through an agent.</DocCard>
  <DocCard title="Secure with Keycloak (OIDC)" to="/docs/quickstart/oidc">Authenticate clients with OIDC tokens and pass identity through to the backend.</DocCard>
</DocCards>

## Prerequisites

- **Docker** and **Docker Compose**
- **Git**, to clone the repository
- For [Run locally without Docker](./quickstart/local-duckdb.md): **Rust** and **Python 3.10+** instead of Docker

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux
```

## Five-minute smoke test

If you only want to see QueryFlux answer a query:

```bash
cd examples/minimal-trino
docker compose up -d --wait

curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

Studio is at http://localhost:3000 (login **admin** / **admin**). Stop the stack with `docker compose down -v`.

:::tip Run one stack at a time
Several examples publish the same host ports (8080, 8081, 3000, 9000). Stop one stack before starting another, or change the published ports in its `docker-compose.yml`.
:::

## Ready to deploy?

The quickstarts use throwaway stacks. For a real deployment — Kubernetes with Helm, a standalone Docker container, or a native binary — see **[Installation & Deployment](./installation/overview.md)**.
