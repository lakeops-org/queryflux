---
sidebar_label: Overview
title: Architecture Overview
description: Index of QueryFlux architecture docs — system design, SQL translation, routing, observability, and extending engine support.
image: img/queryflux-hero-banner.png
---
# QueryFlux architecture documentation

This section describes how QueryFlux is put together: why it exists, how SQL is translated, and how traffic is routed to **cluster groups** and individual **clusters**.

| Document | What it covers |
|----------|----------------|
| [Motivation and goals](motivation-and-goals.md) | Problem statement, goals, and how QueryFlux fits multi-engine estates. |
| [System map](system-map.md) | End-to-end query lifecycle, major crates, and component status (high level). |
| [Query translation](query-translation.md) | Dialect detection, sqlglot integration, when translation runs, and schema-aware mode. |
| [Routing and clusters](routing-and-clusters.md) | Router chain, `routingFallback`, cluster groups, load-balancing strategies, and queueing. |
| [Query tags](query-tags.md) | Attaching metadata to queries for routing, observability, and backend forwarding. |
| [Query parameters](query-params.md) | Typed positional bindings — how `?` params flow from frontend to native engine APIs. |
| [Observability](observability.md) | Prometheus metrics, Grafana dashboard, QueryFlux Studio, and the Admin REST API. |
| [Frontends](frontends/overview.md) | Protocol listeners — Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL, and more. Shared dispatch, session model, and per-protocol details. |
| [Extending QueryFlux](adding-support/overview.md) | **[Backend](adding-support/backend.md)** (Rust + Studio) and **[Frontend](adding-support/frontend.md)** (new protocols). |
| [Auth / authz design](auth-authz-design.md) | Authentication and authorization design notes. |

Start with [Motivation and goals](motivation-and-goals.md) if you are new to the project; use [System map](system-map.md) as the single-page map of the system.

The canonical Markdown sources live under [`docs/`](https://github.com/lakeops-org/queryflux/tree/main/docs) in the repository.
