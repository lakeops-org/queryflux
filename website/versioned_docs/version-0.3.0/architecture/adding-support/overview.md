---
sidebar_label: Overview
title: Extending QueryFlux Overview
description: Add a backend engine adapter or a new client protocol — Rust crates, Studio UI, and documentation checklist.
image: img/queryflux-hero-banner.png
---
# Extending QueryFlux

This guide separates two ideas that are easy to conflate:

| Concept | Meaning | Example |
|--------|---------|---------|
| **Backend engine** | A **cluster** type QueryFlux routes queries **to**. It has an adapter that talks to the real database (HTTP, MySQL wire, embedded library, AWS SDK, …). | Trino, DuckDB, StarRocks, Athena |
| **Frontend protocol** | How **clients connect to QueryFlux** (ingress). SQL enters with a `FrontendProtocol` and a default source dialect for translation. | Trino HTTP, PostgreSQL wire, MySQL wire, Flight SQL |

Adding **PostgreSQL wire** as a client entrypoint is **not** the same as adding “PostgreSQL” as a backend: today, `PostgresWire` is already a frontend in `queryflux-frontend`; traffic still lands on the shared dispatch path and is sent to whatever **backend adapter** routing chose (often Trino).

## Guides

| Page | What it covers |
|------|----------------|
| [Backend](backend.md) | Rust adapter (`EngineAdapterTrait`, `EngineAdapterFactory`), `registered_engines` (`all_factories`), persistence, dispatch notes — plus **QueryFlux Studio** (`StudioEngineModule`, catalog, forms). |
| [Frontend](frontend.md) | New ingress protocol: listener, `FrontendProtocol`, dispatch, optional protocol-based routing, admin frontends snapshot. Existing protocols: **[Frontends](../frontends/overview.md)**. |

---

## Related reading

- [Frontends](../frontends/overview.md) — Trino HTTP, Postgres wire, MySQL wire, Flight SQL  
- [system-map.md](../system-map.md) — End-to-end flow  
- [query-translation.md](../query-translation.md) — Dialects and sqlglot  
- [routing-and-clusters.md](../routing-and-clusters.md) — Routers and groups  
- [observability.md](../observability.md) — Admin API (including engine registry JSON)  
