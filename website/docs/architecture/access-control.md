---
description: OPA-backed data access control — table/column allow-deny, row filtering, and column masking, enforced as a pre-translation guard.
---

# Data access control (OPA)

Guardrails (the previous page) answer "is this SQL shaped safely?" — read-only, has a
`WHERE`, has a `LIMIT`. Data access control answers a different question: "is this
**identity** allowed to see this **table/column**, and if so, under what row filter or
column mask?" That decision is delegated to an external policy engine — [Open Policy
Agent](https://www.openpolicyagent.org/) (OPA) in this first implementation — so grants
live in a Rego bundle you author and version outside of QueryFlux, not in QueryFlux's own
config.

Access control is implemented as one more guard (`opa_access`) in the same guard chain
described in [Guardrails](./guardrails), audited the same way, with one difference: it
runs **before** dialect translation, on the SQL exactly as the client wrote it — see
[How it fits with translation](#how-it-fits-with-dialect-translation) below.

---

## What it does

For every query QueryFlux extracts the tables and columns referenced (`SELECT *`
expanded via the catalog when a schema is available), and asks the configured policy
provider one batched question: *for this identity, this operation, and these resources —
allow or deny, and are there any row filters or column masks?*

| Provider answers | QueryFlux does |
|---|---|
| Deny any referenced table | Query is rejected before it reaches any backend engine. |
| Allow, with a row filter | The filter predicate is spliced into the query at the table's scan site — every reference to that table sees only matching rows, including inside CTEs, subqueries, and self-joins. |
| Allow, with a column mask | The masked column is replaced with a rendered expression (`NULL`, a redaction, a partial reveal, a truncated date, a hash, or a custom expression) at the same scan site — every downstream use of that column sees the masked value. |
| Allow, no filters/masks | Query proceeds unmodified. |

This is **binary allow/deny plus SQL rewriting**, enforced identically in front of every
engine QueryFlux fronts — Trino, DuckDB, StarRocks, ClickHouse, and Snowflake — regardless
of whether that engine has its own native row-level security.

---

## Enabling it

```yaml
accessControl:
  provider: opa
  opa:
    url: http://localhost:8181
    decisionPath: /v1/data/queryflux/access
    timeoutMs: 1000
  operations: [table.select]        # default; also: table.insert, table.update, table.delete
  onMissingSchema: evaluate         # evaluate (send all-columns, let policy decide) | deny
  failOpen: false                   # a provider timeout/error denies by default
  cacheTtlMs: 5000                  # identical requests within this window skip the provider; 0 disables
  sessionParamKeys: [customer_id, tenant_id]   # SessionContext.extra keys forwarded as context, never string-substituted into SQL
  groups:
    trino-prod:
      failOpen: false               # per-cluster-group override
```

When `accessControl` is omitted, nothing changes — no provider call is made, no guard is
registered. `onMissingSchema: deny` is the conservative choice when a catalog outage or an
unresolvable table must never silently widen access to "all columns."

---

## Identity sent to the policy engine

The only identity ever sent is the client's **verified** identity — the same
`AuthContext { user, groups, roles }` used for the coarse authorization gate (see
[Authentication, authorization & backend identity](../authentication)) — never the backend
connection credential a cluster is configured to use. OIDC deployments can additionally
populate `AuthContext.attributes` from JWT claims for attribute-based policies:

```yaml
auth:
  provider: oidc
  oidc:
    attributeClaims: [department, clearance_level]
```

Claims named here are copied into `attributes` (dot-notation paths supported, e.g.
`org.department`) and forwarded to the policy engine as ABAC context alongside `groups`
and `roles`. Static, LDAP, and no-auth providers leave `attributes` empty.

---

## Row filtering & column masking mechanics

Both are applied as a **scan-site view substitution** — the referenced table is
rewritten, in place, into `(SELECT <columns> FROM table [WHERE <filter>]) <alias>` — not
as an `AND` tacked onto the outer `WHERE`, and not as a rewrite of the final projection.
Scan-site substitution is the only approach that stays correct through CTEs, nested and
correlated subqueries, self-joins, and column renames: a masked column is masked no matter
how many layers of query wrap around it, and a row filter reads the table's real columns
even when the mask has already replaced one of them in the projection (filter-before-mask
ordering).

Column masks are a **named-type vocabulary**, not opaque provider-authored SQL, so the
mask always renders to something every dialect QueryFlux transpiles through can parse:

| Mask type | Effect |
|---|---|
| `NULL` | Column reads as `NULL`. |
| `CONSTANT` | Column reads as a fixed literal. |
| `REDACT` | Every character replaced (e.g. `xxxxxxxxx`). |
| `SHOW_LAST_4` / `SHOW_FIRST_4` | Only the last/first 4 characters are visible. |
| `DATE_SHOW_YEAR` | A date/timestamp truncated to the year. |
| `HASH` | A one-way hash (best-effort across dialects — prefer `CUSTOM` for a heterogeneous fleet). |
| `CUSTOM` | An author-written SQL expression, used verbatim. |

## How it fits with dialect translation

Row filters and column masks are rendered and spliced into the query **in the source
dialect the client used** — the same SQL QueryFlux's dialect translator (see [Query
translation](./query-translation)) already carries from a client dialect (Trino, Postgres,
MySQL, …) to whatever a target cluster actually speaks. That means the filter/mask
expressions ride the existing translation pass rather than needing their own — one
transpilation, one failure mode, and nothing QueryFlux produces is ever handed to an
engine it can't parse.

As a narrow safety net over that rewrite-then-translate pipeline — not a second policy
decision — QueryFlux re-classifies the statement kind after translation whenever the
access-control guard rewrote a query, and denies if a read somehow became a write. This
only runs on queries the guard actually rewrote, so it costs nothing on the common path.

---

## Auditing

Every decision — allow, deny, or rewrite — is recorded as a `guard_actions` entry on the
query record (`guard: "opa_access"`), exactly like every other guard. A denied query is
recorded with `status: Denied` and the reason returned by the policy engine, queryable
from Studio and the Admin API the same way as any other guard block (see
[Guardrails § Observability](./guardrails#observability)).

## Dry-run

`POST /admin/access-control/dry-run` (Admin API, Basic-auth protected) evaluates a query
against the configured provider and returns the decision plus the rewritten SQL — without
executing anything or touching a real cluster. Use it to test a Rego bundle change before
rolling it out.

---

## Known limitations

- **Backend views**: if a client selects a view that internally reads a policied table,
  the policy engine is asked about the view name, not the table(s) it wraps. Author grants
  against what the query actually names.
- **Dynamic SQL** where a table name isn't literal text (e.g. built via string
  concatenation inside the query) isn't resolvable by static extraction.
- One rewriting guard runs per query in this release; the access-control guard is that
  guard when configured.
