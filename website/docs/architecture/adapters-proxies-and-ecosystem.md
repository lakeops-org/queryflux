---
title: Adapters, proxies, and the ecosystem
description: Why QueryFlux is a purpose-built SQL adapter rather than a fork of Nginx/Envoy/ProxySQL, how it can compose with existing gateways, and how it relates to tools like Hoop.dev.
image: img/queryflux-hero-banner.png
---
# Adapters, proxies, and the ecosystem

This page answers common architecture questions about QueryFlux’s place in the stack: why it is not built as an extension of a generic proxy/gateway, whether it can be bridged with tools like ProxySQL or Envoy, and how it relates to adjacent projects such as Hoop.dev.

For the broader “why QueryFlux exists” story, see [Motivation and goals](motivation-and-goals.md).

---

## Ecosystem position: the adapter as the great equalizer

Open table formats (Iceberg, Delta, Hudi) largely solved **storage** interoperability: one copy of the data, many engines. That was a huge win — and it deliberately made multi-engine estates the default, not the exception.

What it did **not** solve is **compute access**. Engines that share the same lake still speak different wire protocols, different SQL dialects, and different operational models. Clients (BI tools, apps, notebooks, CLIs, agents) still face an N×M wiring problem.

That is where an **adapter** matters more than another standard:

- Standards (SQL, wire protocols, table formats) are necessary but incomplete. Vendors diverge, dialects branch, and “compatible” never means “identical.”
- An adapter sits in the path of real traffic and **absorbs** that divergence — protocol in, protocol out; dialect in, dialect out — so end users keep flexibility without rewriting every client when engines change.
- The travel-adapter analogy is apt: electrical standards exist, but the thing you pack first is the adapter. QueryFlux aims to be that layer for analytical SQL access.

In short: open formats protect data portability; QueryFlux protects **access** portability — independence from any single engine’s protocol, dialect, and capacity model. That is intentional product positioning, not a side effect.

---

## Why not build on Nginx, Kong, Envoy, ProxySQL, etc.?

The short answer: those systems are excellent at what they were designed for. QueryFlux’s hard problems are mostly **orthogonal** to theirs. Building “on top” of them would not remove the hard work — it would relocate it into a plugin/extension model that fights the host, while still owning protocol conversion, SQL semantics, result re-encoding, and engine adapters.

You are not wrong to ask. Leveraging mature infrastructure is usually the right instinct. Here is why that instinct does not map cleanly onto QueryFlux.

### 1. The “missing features” are the product

A natural framing is: take ProxySQL/Envoy/Nginx for routing, rewriting, rate limits, metering — then add protocol conversion and dialect conversion.

In practice, for a multi-engine SQL proxy those “additions” are the majority of the system:

| Concern | Generic HTTP/TCP / ProxySQL strength | QueryFlux requirement |
|---|---|---|
| Connection handling | Excellent | Needed, but not differentiating |
| L4/L7 routing by host/path/header | Excellent | Useful, incomplete — routing is often SQL- and session-aware |
| MySQL/Postgres query rewrite & R/W split | ProxySQL: strong in-family | Needed **across** dialects and engines |
| Protocol conversion (e.g. Postgres client → Doris MySQL FE, Trino HTTP → DuckDB, Snowflake HTTP → Athena) | Out of scope | Core |
| Dialect translation (sqlglot, 31+ dialects) | Out of scope | Core |
| Result shape conversion (Arrow batches ↔ wire-protocol rows ↔ HTTP JSON) | Out of scope | Core |
| Async poll engines (Trino/Athena-style submit + poll) | Not a first-class model | Core for those backends |
| Capacity leases, spillover across heterogeneous groups | Rarely cross-engine | Core |
| Catalog / guardrails / access-control rewrite before dispatch | Different product space | Integrated in the query path |

If you start from Nginx/OpenResty or Envoy, you still implement almost all of QueryFlux inside Lua/Wasm/filters — plus you inherit the host’s upgrade, packaging, and extension constraints. The maintenance burden does not shrink; it **splits** across two codebases with different release cadences.

### 2. Semantic mismatch, not (only) performance

Performance matters, and staying in Rust with a tight path is part of the design. But the primary reason is **semantics**:

- **HTTP gateways** (Nginx, Kong, Envoy Gateway) treat bodies as opaque bytes or HTTP semantics. They do not speak PostgreSQL/MySQL message framing, Trino’s async `nextUri` poll loop, Flight SQL, or Snowflake’s HTTP wire + SQL API v2 on one listener.
- **Envoy** can terminate TCP and do sophisticated L7 for HTTP/gRPC. A full SQL multi-protocol frontend is a large custom filter surface, not a config tweak.
- **ProxySQL** is deep and battle-tested for **MySQL** (and Postgres API support in that lineage) — read/write split, query rules, connection pooling. It is not a general Trino/Athena/DuckDB/ClickHouse/Flight/Snowflake adapter, and it does not do cross-dialect translation as a first-class multi-engine router.
- **DSR / transparent TCP tricks** assume the backend can speak the client’s protocol with byte-identical semantics. The moment you need dialect rewrite or protocol translation, the response path must go through a process that understands both sides. Direct Server Return stops being a free optimization.

So the question is less “can we shave microseconds by forking Nginx?” and more “does the host give us a natural place to own SQL + multi-protocol state machines?” For QueryFlux, the answer was no.

### 3. What we *do* leverage

QueryFlux is not NIH for its own sake. It deliberately builds on existing pieces where the fit is real:

- **sqlglot** for dialect translation (via PyO3) — the hardest text-level SQL problem
- **Tokio / Axum** for async networking and HTTP frontends
- **Prometheus / OpenTelemetry** for metrics and tracing
- **OPA / Cerbos / OpenFGA** (and similar) for policy — QueryFlux extracts tables/columns and delegates decisions; it does not reinvent a full PDP
- **Postgres** for HA state, config, and Studio-backed persistence
- Engine SDKs and wire libraries where appropriate (Athena SDK, MySQL-compatible paths, Arrow/Flight, etc.)

That is the same “focus on the missing middle” idea — applied at the **library and control-plane** layer, not by forking a generic data-plane that was never designed for heterogeneous analytical SQL.

### 4. Long-term maintenance

Forking or deeply extending Nginx/Envoy/ProxySQL looks like less work on day one and often becomes **more** work on day 1,000:

- Every upstream security release becomes a merge problem.
- Extension APIs constrain how you represent sessions, async queries, and streaming results.
- The community around those tools optimizes for HTTP APIs or single-protocol SQL estates — review bandwidth and shared expertise for “Trino HTTP frontend talking to Athena with sqlglot in the middle” is limited.
- A purpose-built proxy keeps one coherent ownership boundary: frontends, routers, translation, adapters, capacity.

ProxySQL (or Envoy) remaining in the estate for what it already does well is still a good idea. Making QueryFlux a tracking fork of one of them is not.

---

## Bridging / linking with existing proxies

Yes — **composition** is welcome. QueryFlux does not need to be the only hop; it needs to be the hop that owns protocol/dialect/engine adaptation when that work is required.

### Sensible patterns

**A. QueryFlux at the edge (common default)**

```
Clients (psql / mysql / Trino / Snowflake / Flight)
        → QueryFlux (protocol + route + translate + capacity)
        → Engines
```

Use this when clients speak mixed protocols or need dialect/protocol conversion. Observability and routing intent live in one place.

**B. Specialized proxy in front for in-family MySQL/Postgres traffic**

```
MySQL/Postgres clients
        → ProxySQL (pooling, R/W split, in-family rewrite)
        → QueryFlux (only when cross-engine / dialect / other protocol needed)
        → Engines
```

Or the reverse for subsets of traffic:

```
ProxySQL
  ├─ no conversion needed → direct to MySQL-compatible OLAP (Doris / StarRocks / …)
  └─ conversion needed    → QueryFlux → target engine
```

That matches the spirit of your proposal: keep ProxySQL’s strengths on the huge MySQL/Postgres share of the pie, and invoke QueryFlux when the adapter is actually needed.

**C. Policy / governance sidecar beside QueryFlux**

```
Clients → Hoop (or similar) → QueryFlux → Engines
```

or

```
Clients → QueryFlux → Engines
              ↑
         OPA/Cerbos (already integrated for table/column/row/mask decisions)
```

Access governance and multi-engine routing are complementary layers (more on Hoop below).

### What about UDS, shared memory, eBPF, DSR?

| Technique | Useful for | Limits in this context |
|---|---|---|
| **Unix domain sockets** | Co-locating ProxySQL ↔ QueryFlux with lower overhead than TCP localhost | Still a second hop; fine when conversion is needed |
| **Shared memory / custom IPC** | Specialized high-QPS internal meshes | High implementation cost; QueryFlux’s measured proxy overhead on simple paths is already sub-millisecond p50 in bench mocks — often not the bottleneck vs engine time |
| **eBPF** | Observability, L4 steering, security hooks | Does not parse SQL dialects or re-encode result protocols |
| **DSR** | L4 paths where backend speaks the client protocol natively | Breaks once QueryFlux (or anyone) must rewrite SQL or re-encode results; the return path must hit the adapter |

**On DSR specifically:** for “SQL conversion without protocol conversion” (e.g. MySQL client → MySQL-speaking StarRocks/Doris with syntax fixes), you still typically need the adapter on the **response** path if errors, metadata, or row encoding must stay consistent after rewrite — and many “MySQL-compatible” systems are compatible-enough, not byte-identical. DSR is attractive on paper; in SQL gateways it is rarely free.

**On protocol conversion without syntax conversion** (e.g. Postgres client → Doris MySQL FE): that *is* a QueryFlux-shaped problem. ProxySQL alone cannot present a Postgres wire face to a MySQL-only frontend. Here QueryFlux (or an equivalent adapter) must terminate Postgres and speak MySQL upstream; DSR to Doris does not help the client.

### Practical guidance

1. Prefer **one semantic owner** per concern: ProxySQL for in-family MySQL/Postgres ops features; QueryFlux for cross-protocol / cross-dialect / heterogeneous engines; Envoy/Nginx for HTTP north-south where that is the API surface.
2. Add a second hop only when it buys a feature you need — not by default.
3. Optimize IPC (UDS, etc.) if profiling shows the hop matters; for analytical queries, engine execution usually dominates.
4. Do not rely on DSR across a translating adapter.

We are open to documenting and refining reference deployments that compose ProxySQL (or Envoy) with QueryFlux for MySQL/Postgres-heavy estates. The overhead goal is “acceptable and measurable,” not “zero hops at all costs.”

---

## Similar / interesting ecosystem tools: Hoop.dev

[Hoop.dev](https://hoop.dev/) (open source: [hoophq/hoop](https://github.com/hoophq/hoop/)) is a layer-7 gateway focused on **access governance** for engineers, service accounts, and AI agents. It terminates multiple wire protocols (databases, SSH/K8s, HTTP/gRPC, and more), and can mask sensitive data in responses, block dangerous commands, require human approval, and record sessions for replay.

That is a valuable — and mostly **complementary** — problem space:

| | Hoop.dev (typical focus) | QueryFlux (typical focus) |
|---|---|---|
| Primary job | Who can do what; DLP/masking; approvals; session audit | Which engine runs the query; dialect/protocol adaptation; capacity |
| Multi-protocol | Broad infra + DB protocols for control | Analytical SQL frontends ↔ heterogeneous engines |
| Dialect / engine routing | Not the core product | Core product |
| Policy | Inline gateway rules + workflows | Delegates to OPA/Cerbos/etc. in the query path; Studio-managed config |

Interesting to study for protocol coverage and agent-era access patterns. Worth collaborating or composing with when an organization wants **governance at the wire** and **multi-engine routing** in the same path:

```
Agent / analyst
    → Hoop (mask, approve, record)
    → QueryFlux (route, translate, queue)
    → Trino / DuckDB / StarRocks / Athena / …
```

QueryFlux should keep learning from adjacent tools without collapsing into them. The adapter lane stays distinct: flexibility across engines for end users.

---

## Summary

1. **Adapter > hope-in-standards alone** for day-to-day portability — QueryFlux is intentionally that equalizer above open table formats.
2. **Not building on Nginx/Kong/Envoy/ProxySQL as the core** is about semantic fit and ownership of SQL multi-protocol adaptation, not dismissal of those projects — and not primarily a microbenchmark flex.
3. **Bridging is encouraged**: ProxySQL (or similar) for in-family MySQL/Postgres features; QueryFlux when conversion or heterogeneous engines appear; UDS/IPC as optional co-location optimizations; DSR only where no semantic translation is required.
4. **Hoop.dev** is a strong peer in governance/DLP/session control; compose rather than conflate.

If you have a concrete ProxySQL ↔ QueryFlux topology (or Doris/StarRocks/OceanBase dialect gaps) you want validated, that is useful input — reference architectures get better when they are grounded in real estates.
