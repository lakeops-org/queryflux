---
sidebar_label: Overview
title: Access Control Overview
description: Data-level access control in QueryFlux — table allow/deny, row filters, and column masks via a pluggable policy provider (OPA or Cerbos).
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

QueryFlux does **not** author policy. It **connects** to a policy provider — <img src="/img/logos/opa.svg" alt="" class="provider-logo" />[OPA](opa.md) or <img src="/img/logos/cerbos.svg" alt="" class="provider-logo" />[Cerbos](cerbos.md) — and enforces whatever that provider returns (allow, deny, row filters, column masks).

| Question | Answer |
| --- | --- |
| Where does policy live? | In the **provider's own store** — a Rego bundle for OPA, Cerbos policy files for Cerbos. Not in QueryFlux YAML, not in Studio. |
| What does Studio configure? | Any number of named provider connections (URL, credentials, and provider-specific fields), which one is the **default**, and **scope** (which cluster groups call which connection). |
| Where is scope edited? | The **Access Control** page — same pattern as `guardrails.global` + `guardrails.groups`, not on the Clusters / group form. |
| Different rules for different teams? | Prefer branching in the policy itself (Rego: on `input.context.clusterGroup` / identity; Cerbos: on `R.attr`/roles) — it's one bundle, one deploy, testable offline. Reach for a second **connection** (below) only when the constraint is the connection itself: network segmentation, blast-radius isolation, or a per-team provider during a migration. |
| Multiple access-control engines on one query? | **No** — one decision per query, from exactly one resolved connection. The guard action recorded is always named `opa_access` regardless of which provider answered (a naming leftover from when OPA was the only provider). A provider-level error is logged with a `provider` field (`"opa"` or `"cerbos"`) naming which one actually failed. |

At query time: routing picks a **cluster group** → QueryFlux resolves `accessControl.enabled` + `accessControl.groups.<name>.enabled`, and which **connection** the group uses (`groups.<name>.connection`, else `accessControl.defaultConnection`) → if enabled **and** a connection resolves, one call to that connection's provider → rewrite or deny → then guardrails / translation / engine. No name is reserved: with no `defaultConnection` set, a group with no explicit override gets no access control at all.

---

## Use cases

| Pattern | Who connects | What you enforce |
| --- | --- | --- |
| **[Customer API — per-tenant row filters](customer-api-row-filters.md)** | Your backend as a **service account**; end-customer identity stays in your API | The provider turns an allowlisted session param (e.g. `customer=7`) into a row-filter predicate |
| **[Internal analysts](internal-analysts.md)** | Humans (OIDC / static users) as themselves | Group/role-based table grants, region filters, column masks — [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa) / [OPA](opa.md), or [`examples/with-cerbos/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-cerbos) / [Cerbos](cerbos.md) |
| **[Support — on behalf of an account](support-on-behalf-of.md)** | Support agent or desk service | **Actor** in `identity`, **account** in `sessionParams`; row filter + masked payment fields on billing tables |

---

## Provider

QueryFlux ships two access-control providers. Both map to the same allow / deny / row-filter / column-mask model — QueryFlux's SQL-rewrite logic doesn't know or care which one answered.

| Provider | Config key | Docs |
| --- | --- | --- |
| <img src="/img/logos/opa.svg" alt="" class="provider-logo" /> [Open Policy Agent (OPA)](opa.md) | `provider: opa` + `opa:` | [OPA](opa.md) |
| <img src="/img/logos/cerbos.svg" alt="" class="provider-logo" /> [Cerbos](cerbos.md) | `provider: cerbos` + `cerbos:` | [Cerbos](cerbos.md) |

A single connection uses exactly one provider — you cannot mix OPA and Cerbos on the same named connection. Different groups (or a migration in progress) can use different connections on different providers; see [Multiple connections](#multiple-connections).

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
│  Policy provider      │  ← allow / deny / filters + masks
│  (OPA or Cerbos)      │
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

Worked example (API service account + `customer=7`): **[Customer API — per-tenant row filters](customer-api-row-filters)**. Identity construction: [Authentication](../authentication). Rego sketch: [OPA](opa.md#delegation-actor-x-subject-y); the same pattern in CEL: [Cerbos](cerbos.md#delegation-actor-x-subject-y).

---

## Enabling access control

Minimal skeleton — one connection, used by every cluster group via `defaultConnection`. This example uses OPA; swap `opa:` for `cerbos:` and `provider: cerbos` for a Cerbos connection — see [Cerbos](cerbos.md#configuration) for its fields:

```yaml
accessControl:
  enabled: true                     # default for groups without an override
  defaultConnection: prod           # connection groups fall back to; no name is reserved
  connections:
    prod:
      provider: opa                     # or: cerbos
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
        timeoutMs: 1000
      operations: [table.select]        # also: table.insert, table.update, table.delete, table.merge, table.truncate
      onMissingSchema: evaluate         # evaluate | deny
      failOpen: false                   # provider error → deny by default
      cacheTtlMs: 5000                  # 0 disables the decision cache
      sessionParamKeys: [customer_id, tenant_id]
  groups:
    trino-prod:
      enabled: true                 # omit to inherit | true | false
      failOpen: false
    sandbox:
      enabled: false                # skip access control for this group
```

| Setting | Meaning |
| --- | --- |
| `enabled` | Global default: run access control for a cluster group unless `groups.<name>.enabled` overrides. Default: `true` when `accessControl` is set. Set `false` to opt in only where groups explicitly set `enabled: true`. |
| `defaultConnection` | Named entry under `connections` that a group uses when it has no explicit `groups.<name>.connection` override. **Unset means such a group gets no access control at all** — there's nothing to route it to. Must reference a key under `connections` when set. |
| `connections` | Map of named policy-provider connections. No name is reserved — see [Multiple connections](#multiple-connections). |
| `connections.<name>.operations` | Namespaced ops the connection evaluates; others skip the stage. One of `table.select`, `table.insert`, `table.update`, `table.delete`, `table.merge`, `table.truncate` — anything else fails validation. Default: `[table.select]`. See [What policies apply to](#what-policies-apply-to). |
| `connections.<name>.onMissingSchema` | When columns can't be resolved (`SELECT *` without catalog): `evaluate` still calls the provider with "all columns"; `deny` fails closed. |
| `connections.<name>.failOpen` | Provider timeout/transport error → allow (`true`) or deny (`false`, default) for groups on this connection. |
| `connections.<name>.cacheTtlMs` / `cacheCapacity` | TTL cache of identical decisions on this connection; `0` disables. |
| `connections.<name>.sessionParamKeys` | Which `SessionContext.extra` keys become `context.sessionParams` for this connection. |
| `groups.<name>.enabled` | Per cluster group: inherit global default (omit), force on (`true`), or skip access control (`false`). |
| `groups.<name>.failOpen` | Override fail-open for one cluster group, regardless of which connection it uses. |
| `groups.<name>.connection` | Which named connection this group uses. Omit to inherit `defaultConnection`. Must reference a key under `connections` when set. |

Full wire format, policy shape, and server auth for each provider: **[OPA](opa.md)** · **[Cerbos](cerbos.md)**.

### Scope by cluster group

A connection can apply fleet-wide via `defaultConnection`, but **not every cluster group has to use it**, and not every group has to use the *same* connection:

| Pattern | Config |
| --- | --- |
| **Most groups on, sandbox off** | `enabled: true` + `groups.sandbox.enabled: false` |
| **Opt-in only** | `enabled: false` + `groups.analytics.enabled: true` for each group that should call the provider |
| **Per-group fail-open** | `groups.<name>.failOpen` overrides that connection's `failOpen` when it errors |
| **A group on a different connection** | `groups.<name>.connection: <name>` — see [Multiple connections](#multiple-connections) |
| **No fleet-wide default at all** | Omit `defaultConnection`; only groups with an explicit `groups.<name>.connection` get access control |

Resolution for group `G` has **two independent gates**, both must pass: (1) `groups.G.enabled` if set, else global `enabled` (default `true`); (2) `groups.G.connection` if set, else `defaultConnection` — if neither names a connection, access control does not apply to `G` regardless of (1). If either gate fails, QueryFlux **skips** access control entirely for that group — no HTTP call, no `opa_access` rewrite.

Configure scope on the **Access Control** page in Studio (or `accessControl.groups` in YAML). The **Clusters** page does not own this setting — group detail shows a read-only **Access control** badge (`OPA on` / `Cerbos on` / `Skipped` / `Off`) and links here; editing stays on Access Control.

### Multiple connections

Most deployments need only one connection, named as `defaultConnection`. A named connection is its own HTTP endpoint, decision cache, and fail-open policy — reach for a second one when the constraint is genuinely the **connection**, not the policy:

- **Network segmentation** — a cluster group in a separate VPC/tenant boundary that can't reach the fleet's policy server.
- **Blast-radius isolation** — a bad bundle push to one team's server shouldn't require coordinating a deploy with every other team.
- **Provider migration** — moving one group at a time from OPA to Cerbos (or the reverse): the old group's connection keeps `provider: opa`, the new one gets `provider: cerbos`, and you cut over group by group by changing `groups.<name>.connection`.

For everything else — "analysts see different columns than engineers," "the EU group has a stricter row filter" — branch in the policy itself instead of adding a connection: on `input.context.clusterGroup` / `input.identity.groups` in Rego (one bundle, one deploy, one thing to `opa test`), or on `R.attr`/roles in Cerbos.

```yaml
accessControl:
  enabled: true
  defaultConnection: prod
  connections:
    prod:
      opa: { url: http://opa.internal:8181, decisionPath: /v1/data/queryflux/access }
    eu-sandbox:
      opa: { url: https://eu-opa.internal, decisionPath: /v1/data/queryflux/access }
    eu-cerbos-pilot:
      provider: cerbos
      cerbos: { url: http://cerbos.internal:3592 }
  groups:
    trino-prod: {}                              # → "prod" (the default)
    eu-group: { connection: eu-sandbox }         # → "eu-sandbox"
    eu-pilot-group: { connection: eu-cerbos-pilot } # → "eu-cerbos-pilot" (Cerbos)
```

Each connection is a full [`OpaProviderConfig`](opa.md) or [`CerbosProviderConfig`](cerbos.md) (its own URL, timeout, credentials, `operations`, `onMissingSchema`, cache, `sessionParamKeys`); a group resolves to at most one. Config validation rejects a `groups.<name>.connection` or `defaultConnection` that isn't defined under `connections`. Connection names are arbitrary — `"default"` is not special, just a common label.

Studio's **Access Control** page can add, edit, and remove any number of named connections, and set which one is `defaultConnection`.

---

## What policies apply to

Policies govern **reads**. Any table a statement *reads* is evaluated as `table.select` (deny, row filter, column mask) — including reads inside a write: `INSERT … SELECT`, `CREATE TABLE … AS SELECT`, `CREATE VIEW … AS SELECT`, `MERGE … USING`, and subqueries in `UPDATE`/`DELETE`. **This embedded-read guarantee requires `table.select` to be listed in the connection's `operations`** — a connection configured with only `operations: [table.insert]`, say, checks the `INSERT` target but does not evaluate the `SELECT` feeding it, so a protected table's rows could be copied out through it. A protected table cannot be copied out through a write statement as long as `table.select` is enabled.

The **write target** is a second, separate provider call, made only when the statement's own operation is listed in `operations`:

| Statement | Operation | Resource sent | `columns` sent |
| --- | --- | --- | --- |
| `INSERT INTO t (a, b) …` | `table.insert` | `t` | the column list (`null` without one = every column) |
| `UPDATE t SET a = …` | `table.update` | `t` | the `SET` columns — not columns the `WHERE` reads |
| `DELETE FROM t …` | `table.delete` | `t` | `null` (whole row) |
| `MERGE INTO t USING s …` | `table.merge` | `t` | `null` |
| `TRUNCATE TABLE t, u` | `table.truncate` | `t` and `u` | `null` |

So `UPDATE orders SET amount = 0 WHERE id IN (SELECT id FROM customers)` makes two calls: `table.update` for `orders` (columns `["amount"]`) and `table.select` for `customers`. A statement with no reads (`DELETE`, `TRUNCATE`, `INSERT … VALUES`) makes one.

Only allow/deny applies to a write target. A row filter or column mask returned for one is denied (`ACCESS_REWRITE_UNSUPPORTED_FOR_WRITE`) rather than applied — a write has no read site to splice it into. Writes are **not authorized by default**: with the default `operations: [table.select]` no write target is sent to the provider. `operations` is validated at startup; an entry that isn't one of the six above is rejected, since a typo would silently switch that protection off.

Not evaluated at all: `CREATE`/`DROP`/`ALTER` (including the table a `CTAS` or `CREATE VIEW` creates), grants, roles, and session statements. Statements that only name a table without reading it (`DESCRIBE`, `DROP TABLE`) are not treated as reads.

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

The provider's decision carries `rowFilters[].expression` — a **boolean** SQL fragment in the **source dialect** (the dialect the client wrote). For OPA this is a direct field in the decision document; for Cerbos it's carried through a rule's `outputs` (see [Cerbos](cerbos.md#wire-format)). Either way, QueryFlux splices each expression into the inner `WHERE` (AND-combined if there are several).

Same safety rule as CUSTOM masks: QueryFlux does **not** sanitize the expression. Literals you inject (e.g. from `sessionParams`) must be validated in the policy.

### Mask vocabulary

The decision carries `columnMasks[]` with `column` + `type` — a direct field for OPA, carried through `outputs` for Cerbos. QueryFlux renders each mask to a **source-dialect** SQL expression, then substitutes that expression for the column in the scan-site projection (aliased back to the original column name so the rest of the query is unchanged).

| Mask type | Fields | What QueryFlux renders |
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

`HASH` is best-effort (`sha256` / `to_hex(sha256(...))` / `sha2` depending on dialect). For a heterogeneous fleet where you need identical semantics, prefer **`CUSTOM`** with an expression you control (or branch on `input.context.engine` in Rego / `R.attr`/context in Cerbos).

Full wire-level examples: **[OPA](opa.md#column-masks)** · **[Cerbos](cerbos.md#column-masks)**.

---

## Auditing and Studio (same trail as guardrails)

Access control is **not a second product** — it is the rewriting guard in the [guardrail chain](../architecture/guardrails). Policy lives in the provider (OPA or Cerbos); enforcement and audit reuse the same `guard_actions` path as built-in guards such as `read_only`.

Every decision — allow, deny, or rewrite — is recorded as `guard: "opa_access"` on the query record. Denied queries get `status: Denied` and the provider's reason.

QueryFlux Studio already combines both on **Queries**:

| Studio surface | What you see |
| --- | --- |
| **Guard Actions** | `opa_access` with `rewrite` or `deny`, plus metadata: `tables`, `row_filtered`, `masked_columns` — next to any other guards that ran. |
| **Rewritten SQL (access control)** | Source-dialect SQL after row filters / column masks (`was_rewritten`, `rewritten_sql`). |
| **Translated SQL** | Dialect translation only (`was_translated`, `translated_sql`). |
| Query list badges | Separate **rewritten** (amber) and **translated** (violet) chips so an access-control rewrite is not mistaken for sqlglot. |

A query can be both: access control rewrites first, then sqlglot translates. The **Guardrails** page edits the SQL-shape chain. The **Access Control** page only **connects** QueryFlux to the provider (URL, credentials) — grants stay in the provider's own policy store. Use `POST /admin/access-control/dry-run` to preview a policy change without executing.

---

## Configuring in Studio

The **Access Control** page (`GET`/`PUT /admin/config/access-control`) persists to Postgres (`proxy_settings` table, key `access_control_config`) and hot-reloads the proxy. Requires **`persistence.type: postgres`** (same as Catalog, Guardrails, and cluster saves). The [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa) and [`examples/with-cerbos/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-cerbos) demos each include a Postgres service on host port **5434** for this.

| Studio section | What it saves |
| --- | --- |
| **Provider** | Disconnect (no `connections`) turns access control off entirely; otherwise the listed connections stay loaded |
| **Connections** | Add/edit/remove any number of named connections — each picks `provider: opa` or `provider: cerbos` and the matching block (`opa.{url,decisionPath,timeoutMs}` + bearer/OAuth, or `cerbos.{url,checkResourcesPath,timeoutMs}` + optional bearer) — and which one is `defaultConnection` |
| **Scope by cluster group** | Global **Enabled by default** + per-group **Inherit / Enabled / Disabled**, plus which connection each group uses (once more than one exists) |

`operations`, `onMissingSchema`, cache, `failOpen`, and `sessionParamKeys` (per connection) are configured via **YAML or a direct `PUT /admin/config/access-control` body**, not exposed in the Studio form yet.

QueryFlux does **not** edit policy (Rego or Cerbos YAML) in Studio. Secrets are never returned on GET — leave token fields blank to keep stored values (each connection's `opa.bearerToken` / `opa.clientCredentials.clientSecret` / `cerbos.bearerToken` round-trips independently at the API level).

YAML is the fallback until you save once via the Admin API; after that the database row wins.

---

## Dry-run

`POST /admin/access-control/dry-run` (Admin API, Basic-auth protected) evaluates a query against the configured provider and returns the decision plus rewritten SQL — without executing or touching a cluster.

If access control is **disabled for the request's `clusterGroup`**, or the group has **no resolvable connection** (no `groups.<name>.connection` override and no `defaultConnection` configured), the response is `{ "outcome": "skip", "reason": "..." }` with a reason distinguishing the two. Every other response also carries `"connection"` — the named connection `clusterGroup` resolved to.

Column masks need a column list for scan-site rewrite. Dry-run resolves that from the live catalog when configured; you can also pass an explicit `schema` map (`table → { column → type }`). Named-column queries (not `SELECT *`) can still rewrite from columns mentioned in the SQL.

Dry-run sends an **empty** `session.extra` map, so `sessionParamKeys` never populate `sessionParams`. Identity-only policies work; [customer-API filters](customer-api-row-filters.md#dry-run-limitation) cannot be previewed via dry-run.

Against a **Cerbos** connection, the dry-run's `identity.roles` must be non-empty — Cerbos rejects an empty `principal.roles` outright (see [Cerbos — fail-closed defaults](cerbos.md#fail-closed-defaults)). An OPA connection has no such requirement.

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
- **Session params are not auto-filters** — the provider must return an explicit `rowFilters` entry; QueryFlux never substitutes `sessionParams` into SQL itself.
- **Cerbos requires non-empty `principal.roles`** — an identity with no roles resolved is denied locally by `CerbosProvider` without calling Cerbos at all. Not a constraint on OPA connections.
- **Trino `X-Trino-Session`** is not split into extra keys — use a header whose name matches `sessionParamKeys` (see [Customer API row filters](customer-api-row-filters)).
- **Dry-run** does not accept session params.

---

## Related reading

- [OPA provider](opa.md) — wire format, Rego, demo (`examples/with-opa/`)
- [Cerbos provider](cerbos.md) — wire format, policy YAML, demo (`examples/with-cerbos/`)
- [Customer API row filters](customer-api-row-filters) — service account + `sessionParamKeys`
- [Guardrails](../architecture/guardrails) — SQL-shape safety; access control is one more guard, pre-translation
- [Authentication & identity](../authentication) — how `AuthContext` is built
- [Catalog integration](../architecture/catalog-integration) — schema for `SELECT *` / masks
- [Query translation](../architecture/query-translation) — rewrite-then-translate pipeline
