---
sidebar_label: Overview
title: Access Control Overview
description: Data-level access control in QueryFlux — table allow/deny, row filters, and column masks via OPA.
image: img/queryflux-hero-banner.png
---
# Access control

Access control answers a different question from authentication and guardrails:

| Layer | Question | Config |
| --- | --- | --- |
| **Authentication** | Who is this client? | `auth` |
| **Authorization** | Which cluster groups may they reach? | `authorization` |
| **Guardrails** | Is this SQL shaped safely (read-only, LIMIT, …)? | `guardrails` |
| **Access control** | Which **tables/columns** may this identity see, and under what **row filter** or **column mask**? | `accessControl` |

Decisions are delegated to an external **policy provider** — not stored as grants inside QueryFlux config. QueryFlux extracts the tables and columns a query touches, asks the provider, then **allows**, **denies**, or **rewrites** the SQL before dialect translation and engine dispatch.

When `accessControl` is omitted, nothing changes: no provider call, no rewrite guard.

---

## Product model (what QueryFlux configures)

QueryFlux does **not** author policy. It **connects** to OPA and enforces whatever Rego returns (allow, deny, row filters, column masks).

| Question | Answer |
| --- | --- |
| Where does policy live? | In the **OPA bundle** (Rego). Not in QueryFlux YAML, not in Studio. |
| What does Studio configure? | Any number of named OPA connections (URL, decision path, auth), which one is the **default**, and **scope** (which cluster groups call which connection). |
| Where is scope edited? | The **Access Control** page — same pattern as `guardrails.global` + `guardrails.groups`, not on the Clusters / group form. |
| Different rules for different teams? | Prefer branching in Rego on `input.context.clusterGroup` / identity — it's one bundle, one deploy, testable with `opa test`. Reach for a second **connection** (below) only when the constraint is the connection itself: network segmentation, blast-radius isolation, or a per-team OPA during a migration. |
| Multiple access-control engines on one query? | **No** — one `opa_access` decision per query, from exactly one resolved connection. |

At query time: routing picks a **cluster group** → QueryFlux resolves `accessControl.enabled` + `accessControl.groups.<name>.enabled`, and which **connection** the group uses (`groups.<name>.connection`, else `accessControl.defaultConnection`) → if enabled **and** a connection resolves, one call to that connection's provider → rewrite or deny → then guardrails / translation / engine. No name is reserved: with no `defaultConnection` set, a group with no explicit override gets no access control at all.

---

## Use cases

| Pattern | Who connects | What you enforce |
| --- | --- | --- |
| **[Customer API — per-tenant row filters](customer-api-row-filters.md)** | Your backend as a **service account**; end-customer identity stays in your API | OPA turns an allowlisted session param (e.g. `customer=7`) into a row-filter predicate |
| **Internal analysts** | Humans (OIDC / static users) as themselves | Group-based table grants, region filters, column masks — [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa) and [OPA](opa.md) |
| **Support / on-behalf-of** | Support agent or portal service | Same as the customer API: **actor** in `identity`, **subject** in `sessionParams` |

---

## Provider

OPA is the access-control provider. QueryFlux maps its allow / deny / row-filter / column-mask decisions to SQL rewrites.

| Provider | Config key | Docs |
| --- | --- | --- |
| [Open Policy Agent (OPA)](opa.md) | `provider: opa` + `opa:` | [OPA](opa.md) |

---

## What it does

For every evaluated query QueryFlux:

1. Parses the **source** SQL (the dialect the client wrote).
2. Extracts referenced tables and columns (`SELECT *` expanded via the [catalog](../architecture/catalog-integration) when available).
3. Sends one batched request to the configured provider: identity + operation + resources + context.
4. Applies the decision:

| Provider answers | QueryFlux does |
| --- | --- |
| Deny any referenced table | Reject the query before any backend engine. |
| Allow, with a row filter | Splice the filter at each **scan site** for that table (CTEs, subqueries, and self-joins included). |
| Allow, with a column mask | Replace the column with a rendered mask expression at the same scan site. |
| Allow, no filters/masks | Proceed unmodified. |

Enforcement is identical in front of every engine QueryFlux fronts — Trino, DuckDB, StarRocks, ClickHouse, Snowflake, and others — whether or not the engine has native row-level security.

```
Client SQL (source dialect)
        │
        ▼
┌───────────────────────┐
│  Resource extraction  │  ← catalog for SELECT *
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  OPA                  │  ← allow / deny / filters + masks
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  Scan-site rewrite    │  ← still source dialect
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  Dialect translation  │  ← sqlglot, if needed
└───────────┬───────────┘
            ▼
        Backend engine
```

Access control runs **before** dialect translation so filter and mask expressions are authored once in the client's dialect and ride the existing translation pass. After a rewrite, QueryFlux re-classifies the statement and denies if a read somehow became a write.

---

## Identity vs session context

Only the client's **verified** identity is sent as `identity` — the same `AuthContext { user, groups, roles, attributes }` used elsewhere. Never the backend connection credential, never a client-spoofed "run as" principal.

| Field | Source | Role |
| --- | --- | --- |
| `identity.user` / `groups` / `roles` | Auth provider | Actor — who authenticated |
| `identity.attributes` | OIDC `attributeClaims` | ABAC attributes (department, clearance, …) |
| `context.sessionParams` | Allowlisted `SessionContext.extra` keys via `sessionParamKeys` | Extra **context** for policy (e.g. `customer_id`) — never string-substituted into SQL by QueryFlux |

**Delegation pattern** ("connected as user X, fetch data for customer Y"): keep X in `identity`; put Y in an allowlisted session param; the provider decides whether X may act for Y and returns an explicit row-filter expression. QueryFlux does not auto-inject `customer_id = Y` unless the provider returns that filter.

Worked example (API service account + `customer=7`): **[Customer API — per-tenant row filters](customer-api-row-filters)**. Identity construction: [Authentication](../authentication). Rego sketch: [OPA](opa.md#delegation-actor-x-subject-y).

---

## Enabling access control

Minimal skeleton — one connection, used by every cluster group via `defaultConnection`:

```yaml
accessControl:
  enabled: true                     # default for groups without an override
  defaultConnection: prod           # connection groups fall back to; no name is reserved
  connections:
    prod:
      provider: opa
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
        timeoutMs: 1000
      operations: [table.select]        # also: table.insert, table.update, table.delete
      onMissingSchema: evaluate         # evaluate | deny
      failOpen: false                   # OPA error → deny by default
      cacheTtlMs: 5000                  # 0 disables the decision cache
      sessionParamKeys: [customer_id, tenant_id]
  groups:
    trino-prod:
      enabled: true                 # omit to inherit | true | false
      failOpen: false
    sandbox:
      enabled: false                # skip OPA for this group
```

| Setting | Meaning |
| --- | --- |
| `enabled` | Global default: run access control for a cluster group unless `groups.<name>.enabled` overrides. Default: `true` when `accessControl` is set. Set `false` to opt in only where groups explicitly set `enabled: true`. |
| `defaultConnection` | Named entry under `connections` that a group uses when it has no explicit `groups.<name>.connection` override. **Unset means such a group gets no access control at all** — there's nothing to route it to. Must reference a key under `connections` when set. |
| `connections` | Map of named policy-provider connections. No name is reserved — see [Multiple connections](#multiple-connections). |
| `connections.<name>.operations` | Only these namespaced ops hit that connection's provider; others skip the stage. Default: `[table.select]`. |
| `connections.<name>.onMissingSchema` | When columns can't be resolved (`SELECT *` without catalog): `evaluate` still calls the provider with "all columns"; `deny` fails closed. |
| `connections.<name>.failOpen` | Provider timeout/transport error → allow (`true`) or deny (`false`, default) for groups on this connection. |
| `connections.<name>.cacheTtlMs` / `cacheCapacity` | TTL cache of identical decisions on this connection; `0` disables. |
| `connections.<name>.sessionParamKeys` | Which `SessionContext.extra` keys become `context.sessionParams` for this connection. |
| `groups.<name>.enabled` | Per cluster group: inherit global default (omit), force on (`true`), or skip OPA (`false`). |
| `groups.<name>.failOpen` | Override fail-open for one cluster group, regardless of which connection it uses. |
| `groups.<name>.connection` | Which named connection this group uses. Omit to inherit `defaultConnection`. Must reference a key under `connections` when set. |

Full OPA wire format, Rego package shape, and auth to the OPA server: **[OPA provider](opa.md)**.

### Scope by cluster group

A connection can apply fleet-wide via `defaultConnection`, but **not every cluster group has to use it**, and not every group has to use the *same* connection:

| Pattern | Config |
| --- | --- |
| **Most groups on, sandbox off** | `enabled: true` + `groups.sandbox.enabled: false` |
| **Opt-in only** | `enabled: false` + `groups.analytics.enabled: true` for each group that should call OPA |
| **Per-group fail-open** | `groups.<name>.failOpen` overrides that connection's `failOpen` when it errors |
| **A group on a different connection** | `groups.<name>.connection: <name>` — see [Multiple connections](#multiple-connections) |
| **No fleet-wide default at all** | Omit `defaultConnection`; only groups with an explicit `groups.<name>.connection` get access control |

Resolution for group `G` has **two independent gates**, both must pass: (1) `groups.G.enabled` if set, else global `enabled` (default `true`); (2) `groups.G.connection` if set, else `defaultConnection` — if neither names a connection, access control does not apply to `G` regardless of (1). If either gate fails, QueryFlux **skips** access control entirely for that group — no HTTP call, no `opa_access` rewrite.

Configure scope on the **Access Control** page in Studio (or `accessControl.groups` in YAML). The **Clusters** page does not own this setting — group detail shows a read-only **Access control** badge (`OPA on` / `Skipped` / `Off`) and links here; editing stays on Access Control.

### Multiple connections

Most deployments need only one connection, named as `defaultConnection`. A named connection is its own HTTP endpoint, decision cache, and fail-open policy — reach for a second one when the constraint is genuinely the **connection**, not the policy:

- **Network segmentation** — a cluster group in a separate VPC/tenant boundary that can't reach the fleet's OPA.
- **Blast-radius isolation** — a bad bundle push to one team's OPA shouldn't require coordinating a deploy with every other team.
- **Provider migration** — moving one group at a time to a different `PolicyDecisionProvider` (once a second one ships).

For everything else — "analysts see different columns than engineers," "the EU group has a stricter row filter" — branch in Rego on `input.context.clusterGroup` / `input.identity.groups` instead of adding a connection; it's one bundle, one deploy, one thing to `opa test`.

```yaml
accessControl:
  enabled: true
  defaultConnection: prod
  connections:
    prod:
      opa: { url: http://opa.internal:8181, decisionPath: /v1/data/queryflux/access }
    eu-sandbox:
      opa: { url: https://eu-opa.internal, decisionPath: /v1/data/queryflux/access }
  groups:
    trino-prod: {}                        # → "prod" (the default)
    eu-group: { connection: eu-sandbox }  # → "eu-sandbox"
```

Each connection is a full [`OpaProviderConfig`](opa.md) (its own URL, timeout, credentials, `operations`, `onMissingSchema`, cache, `sessionParamKeys`); a group resolves to at most one. Config validation rejects a `groups.<name>.connection` or `defaultConnection` that isn't defined under `connections`. Connection names are arbitrary — `"default"` is not special, just a common label.

Studio's **Access Control** page can add, edit, and remove any number of named connections, and set which one is `defaultConnection`.

---

## Row filters and column masks

Both are applied as **scan-site view substitution** — the table reference becomes `(SELECT <columns> FROM table [WHERE <filter>]) <alias>` — not an `AND` on the outer `WHERE`, and not a rewrite of only the final projection. That keeps filters and masks correct through CTEs, nested/correlated subqueries, self-joins, and renames. Filters apply to real table columns **before** masks replace projected values.

Example — client SQL `SELECT name, ssn FROM customers` with a row filter and an SSN mask becomes roughly:

```sql
SELECT name, ssn FROM (
  SELECT
    name,
    CASE WHEN ssn IS NULL THEN NULL ELSE '****' || substr(ssn, -4) END AS ssn
  FROM customers
  WHERE region = 'EU'
) customers
```

The outer query still projects `name` and `ssn`; the mask and filter live inside the scan subquery.

### Row filters

OPA returns `rowFilters[].expression` — a **boolean** SQL fragment in the **source dialect** (the dialect the client wrote). QueryFlux splices each expression into the inner `WHERE` (AND-combined if there are several).

Same safety rule as CUSTOM masks: QueryFlux does **not** sanitize the expression. Literals you inject (e.g. from `sessionParams`) must be validated in Rego.

### Mask vocabulary

OPA returns `columnMasks[]` with `column` + `type`. QueryFlux renders each mask to a **source-dialect** SQL expression, then substitutes that expression for the column in the scan-site projection (aliased back to the original column name so the rest of the query is unchanged).

| Mask type | OPA fields | What QueryFlux renders |
| --- | --- | --- |
| `NULL` | `column`, `type` | `NULL` |
| `CONSTANT` | `column`, `type`, `value` | Quoted literal from `value` (quotes escaped) |
| `REDACT` | `column`, `type` | `regexp_replace(<col>, '[A-Za-z0-9]', 'x')` |
| `SHOW_LAST_4` | `column`, `type` | `CASE WHEN <col> IS NULL THEN NULL ELSE '****' \|\| substr(<col>, -4) END` |
| `SHOW_FIRST_4` | `column`, `type` | First 4 chars + `'****'` (same CASE shape) |
| `DATE_SHOW_YEAR` | `column`, `type` | `date_trunc('year', <col>)` |
| `HASH` | `column`, `type` | Best-effort SHA-256 per dialect (see below) |
| `CUSTOM` | `column`, `type`, `expression` | **`expression` verbatim** — QueryFlux does not rewrite it |

Named types (`NULL` … `HASH`) are the portable path: QueryFlux builds the SQL from the type and the column name. Prefer them when they fit.

#### `CUSTOM` — author-written SQL

Use `CUSTOM` when named types are not enough (UDFs, engine-specific functions, multi-part expressions). OPA must return:

```json
{
  "column": "ssn",
  "type": "CUSTOM",
  "expression": "CASE WHEN ssn IS NULL THEN NULL ELSE '***-' || substr(ssn, -4) END"
}
```

Rules:

1. **`expression` is required.** Missing `expression` → the query is **denied** (`ACCESS_MASK_RENDER_FAILED`), not silently skipped.
2. **Verbatim splice.** QueryFlux does not wrap, escape, or substitute into the string — it is the SELECT-list expression for that column inside the scan subquery.
3. **Source dialect.** Write Trino SQL if the client speaks Trino HTTP; the expression rides the normal dialect translation pass afterward with the rest of the query.
4. **Bare column names.** Inside the subquery the table is unqualified; reference `ssn`, not `customers.ssn` or an outer alias.
5. **Alias preserved.** After rewrite the projection is still named `ssn` (or whatever `column` was), so joins and outer SELECTs keep working.
6. **Injection.** Same as row filters — never put untrusted client text into `expression`. Validate `sessionParams` in Rego before `sprintf`.

Rego sketch:

```rego
"columnMasks": [{
	"column": "email",
	"type": "CUSTOM",
	"expression": "concat(substr(email, 1, 1), '***@', split_part(email, '@', 2))"
}]
```

#### `HASH` vs `CUSTOM` for hashing

`HASH` is best-effort (`sha256` / `to_hex(sha256(...))` / `sha2` depending on dialect). For a heterogeneous fleet where you need identical semantics, prefer **`CUSTOM`** with an expression you control (or branch in Rego on `input.context.engine`).

Full OPA wire examples: **[OPA provider](opa.md#column-masks)**.

---

## Auditing and Studio (same trail as guardrails)

Access control is **not a second product** — it is the rewriting guard in the [guardrail chain](../architecture/guardrails). Policy lives in OPA; enforcement and audit reuse the same `guard_actions` path as built-in guards such as `read_only`.

Every decision — allow, deny, or rewrite — is recorded as `guard: "opa_access"` on the query record. Denied queries get `status: Denied` and the provider's reason.

QueryFlux Studio already combines both on **Queries**:

| Studio surface | What you see |
| --- | --- |
| **Guard Actions** | `opa_access` with `rewrite` or `deny`, plus metadata: `tables`, `row_filtered`, `masked_columns` — next to any other guards that ran. |
| **Rewritten SQL (access control)** | Source-dialect SQL after row filters / column masks (`was_rewritten`, `rewritten_sql`). |
| **Translated SQL** | Dialect translation only (`was_translated`, `translated_sql`). |
| Query list badges | Separate **rewritten** (amber) and **translated** (violet) chips so an OPA rewrite is not mistaken for sqlglot. |

A query can be both: OPA rewrites first, then sqlglot translates. The **Guardrails** page edits the SQL-shape chain. The **Access Control** page only **connects** QueryFlux to OPA (URL, decision path, credentials) — grants stay in the OPA Rego bundle. Use `POST /admin/access-control/dry-run` to preview a policy change without executing.

---

## Configuring in Studio

The **Access Control** page (`GET`/`PUT /admin/config/access-control`) persists to Postgres (`proxy_settings` table, key `access_control_config`) and hot-reloads the proxy. Requires **`persistence.type: postgres`** (same as Catalog, Guardrails, and cluster saves). The [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa) demo includes a Postgres service on host port **5434** for this.

| Studio section | What it saves |
| --- | --- |
| **Provider** | Disconnect (no `connections`) turns `opa_access` off entirely; otherwise the listed connections stay loaded |
| **Connections** | Add/edit/remove any number of named connections — each one's `opa.{url,decisionPath,timeoutMs}`, bearer / OAuth credentials — and which one is `defaultConnection` |
| **Scope by cluster group** | Global **Enabled by default** + per-group **Inherit / Enabled / Disabled**, plus which connection each group uses (once more than one exists) |

`operations`, `onMissingSchema`, cache, `failOpen`, and `sessionParamKeys` (per connection) are configured via **YAML or a direct `PUT /admin/config/access-control` body**, not exposed in the Studio form yet.

QueryFlux does **not** edit Rego in Studio. Secrets are never returned on GET — leave token fields blank to keep stored values (each connection's `opa.bearerToken` / `opa.clientCredentials.clientSecret` round-trips independently).

YAML is the fallback until you save once via the Admin API; after that the database row wins.

---

## Dry-run

`POST /admin/access-control/dry-run` (Admin API, Basic-auth protected) evaluates a query against the configured provider and returns the decision plus rewritten SQL — without executing or touching a cluster.

If access control is **disabled for the request's `clusterGroup`**, or the group has **no resolvable connection** (no `groups.<name>.connection` override and no `defaultConnection` configured), the response is `{ "outcome": "skip", "reason": "..." }` with a reason distinguishing the two. Every other response also carries `"connection"` — the named connection `clusterGroup` resolved to.

Column masks need a column list for scan-site rewrite. Dry-run resolves that from the live catalog when configured; you can also pass an explicit `schema` map (`table → { column → type }`). Named-column queries (not `SELECT *`) can still rewrite from columns mentioned in the SQL.

Dry-run sends an **empty** `session.extra` map, so `sessionParamKeys` never populate `sessionParams`. Identity-only policies work; [customer-API filters](customer-api-row-filters.md#dry-run-limitation) cannot be previewed via dry-run.

---

## Catalog requirement

Column masks on `SELECT *` need a column list. Configure a [catalog integration](../architecture/catalog-integration) (Iceberg REST, static schema, …) or use `onMissingSchema: deny` so unresolved schemas never silently widen access. Named-column queries can rewrite without full catalog enumeration.

---

## Known limitations

- **Backend views** — policy is asked about the **view name** the query names, not underlying tables. Author grants against what the SQL references.
- **Dynamic SQL** — table names built by string concatenation aren't resolvable by static extraction.
- **One rewriting access-control guard per query** — `opa_access` only, from exactly one resolved connection.
- **`operations`, `onMissingSchema`, cache, `failOpen`, and `sessionParamKeys`** (per connection) are YAML/Admin-API only — not exposed in the Studio connection form yet.
- **Scope is not editable on the cluster group form** — enable/disable/connection per group is only under `accessControl.groups` / the Access Control page (Clusters shows a read-only badge).
- **Session params are not auto-filters** — OPA must return explicit `rowFilters`; QueryFlux never substitutes `sessionParams` into SQL itself.
- **Trino `X-Trino-Session`** is not split into extra keys — use a header whose name matches `sessionParamKeys` (see [Customer API row filters](customer-api-row-filters)).
- **Dry-run** does not accept session params.

---

## Related reading

- [OPA provider](opa.md) — wire format, Rego, demo (`examples/with-opa/`)
- [Customer API row filters](customer-api-row-filters) — service account + `sessionParamKeys`
- [Guardrails](../architecture/guardrails) — SQL-shape safety; access control is one more guard, pre-translation
- [Authentication & identity](../authentication) — how `AuthContext` is built
- [Catalog integration](../architecture/catalog-integration) — schema for `SELECT *` / masks
- [Query translation](../architecture/query-translation) — rewrite-then-translate pipeline
