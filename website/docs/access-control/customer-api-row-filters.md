---
sidebar_label: Customer API row filters
title: Customer API — per-tenant row filters
description: Connect to QueryFlux as a service account, pass the customer id your auth service resolved, and let OPA return a row-filter predicate.
image: img/queryflux-hero-banner.png
---
# Customer API — per-tenant row filters

A backend API you expose to customers should **not** connect to QueryFlux as that customer, and it should **not** sprinkle `WHERE customer_id = …` in every handler. Authenticate the caller in **your** API, connect to QueryFlux as a **service account**, and let [OPA](opa.md) return the row filter for the customer you already resolved.

This is the standard **actor vs subject** pattern: who is connected vs whose data this query is about. This walkthrough uses OPA/Rego, but the pattern (session param → row-filter predicate, actor stays in `identity`, subject stays in `sessionParams`) is identical on [Cerbos](cerbos.md#delegation-actor-x-subject-y) — only the policy syntax (CEL, via `outputs`) differs.

```
Customer (JWT / session)
        │
        ▼
┌───────────────────────────┐
│  Your API                 │  auth service → customer_id = 7
│  (only place that trusts  │
│   the end-user token)     │
└─────────────┬─────────────┘
              │  identity: api-service-a
              │  session extra: customer = 7
              ▼
┌───────────────────────────┐
│  QueryFlux                │  sessionParamKeys: [customer]
│  opa_access guard         │  copies extra → OPA sessionParams
└─────────────┬─────────────┘
              ▼
┌───────────────────────────┐
│  OPA                      │  allow + rowFilters:
│                           │  customer_id = '7'
└─────────────┬─────────────┘
              ▼
     rewritten SQL → engine
```

QueryFlux **does not** turn `customer=7` into SQL by itself. `sessionParamKeys` is only an allowlist of which `SessionContext.extra` keys are forwarded to the provider. OPA must return an explicit `rowFilters[].expression`; QueryFlux splices that expression at every scan of the table.

---

## Responsibilities

| Layer | Owns |
| --- | --- |
| **Your API / auth service** | Authenticate the end user. Resolve `customer_id` (e.g. `7`) from the token or session. **Never** take `customer` from an untrusted query string unless you re-check it. |
| **QueryFlux `auth`** | Authenticate **`api-service-a`** (static user, OIDC client credentials, …). That principal is what Studio and `guard_actions` record. |
| **`sessionParamKeys`** | Forward only listed extra keys to OPA (`customer`, `tenant_id`, …). Anything else on the session is invisible to policy. |
| **OPA** | If the actor may query with that subject: `allow` + a predicate such as `customer_id = '7'`. If `customer` is missing or the service account is unknown: **deny**. |
| **QueryFlux rewrite** | Scan-site substitution of the returned filter (and any column masks). |

Do **not** set `identity.user` to `7`. Keep the actor as `api-service-a` so audit trails show the service, not a fake customer login.

---

## Configuration

```yaml
auth:
  provider: static
  required: true
  staticUsers:
    users:
      api-service-a:
        password: "YOUR_SERVICE_PASSWORD_HERE"
        groups: [customer_portal]

accessControl:
  defaultConnection: default
  connections:
    default:
      provider: opa
      opa:
        url: http://opa:8181
        decisionPath: /v1/data/queryflux/access
        timeoutMs: 1000
      operations: [table.select]
      failOpen: false
      onMissingSchema: deny
      # Only these SessionContext.extra keys are sent to OPA.
      sessionParamKeys: [customer]
```

Shared knobs: [Access control overview](overview.md) (`enabled`, per–cluster-group scope on the **Access Control** page, `sessionParamKeys`, …). OPA URL, wire format, and mask types: [OPA provider](opa.md). The API's cluster group must be **enabled** and resolve a connection (via `defaultConnection` above, or its own `groups.<name>.connection`) or access control is skipped for that route.

---

## Rego

OPA sees the service account in `identity` and the customer in `context.sessionParams`. Return a **source-dialect** boolean expression (the dialect the API used, usually Trino if you speak Trino HTTP).

```rego
package queryflux.access

import rego.v1

resources := [d |
	some r in input.action.resources
	d := table_decision(r)
]

is_portal if input.identity.user == "api-service-a"

customer := input.context.sessionParams.customer

# Deny the whole query if the portal did not attach a customer.
table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": "customer session param required",
} if {
	is_portal
	customer == ""
}

table_decision(r) := {
	"table": r.table,
	"allow": true,
	"rowFilters": [{
		"expression": sprintf("customer_id = '%s'", [customer]),
	}],
} if {
	is_portal
	customer != ""
	# Treat the value as an opaque id: digits only (adjust for UUIDs).
	regex.match(`^[0-9]+$`, customer)
}

table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": sprintf("unknown principal %q", [input.identity.user]),
} if {
	not is_portal
}
```

:::warning SQL injection

QueryFlux splices `expression` verbatim. If Rego interpolates `sessionParams` with `sprintf("… '%s' …")` and a caller can set `customer` to `7' OR '1'='1`, the filter is attacker-controlled. Restrict the value in Rego (`regex.match`) **and** only set `extra` after your API has resolved a typed id.

:::

Complete definitions for a given resource must not overlap (OPA eval error). See [OPA — delegation](opa.md#delegation-actor-x-subject-y).

---

## How to put `customer=7` on the session

`sessionParamKeys` looks up **exact keys** on `SessionContext.extra`. How `extra` is filled depends on the frontend.

### Trino HTTP (typical for an internal API)

Every request header is copied into `extra` under the **lowercase header name**. Send a dedicated header whose name matches the allowlist:

```http
POST /v1/statement HTTP/1.1
Authorization: Basic …          # api-service-a
X-Trino-User: api-service-a
customer: 7
Content-Type: text/plain

SELECT order_id, amount FROM lakekeeper.demo.orders
```

```bash
curl -s -u api-service-a:$PASS -X POST http://queryflux:8080/v1/statement \
  -H "X-Trino-User: api-service-a" \
  -H "customer: 7" \
  -d "SELECT order_id, amount FROM lakekeeper.demo.orders"
```

:::caution `X-Trino-Session` is not parsed into extra keys

`X-Trino-Session: customer=7` is stored as a **single** extra entry (`x-trino-session` → `customer=7`). `sessionParamKeys: [customer]` will **not** see it. Use a header named `customer` (or whatever key you allowlisted), not a Trino session property, until QueryFlux parses `X-Trino-Session` into individual extra keys.

:::

Trino JDBC/Python clients send `X-Trino-Session` for `SET SESSION` properties and may **not** let you attach an arbitrary `customer` header. From a backend service, a raw HTTP client (as above) is the reliable path today.

### MySQL wire

Simple `SET` assignments are stored in `extra` (key lowercased):

```sql
SET customer = '7';
SELECT order_id, amount FROM orders;
```

### Postgres wire

**Startup parameters** other than `user` / `database` / `query_tags` flow into `extra`. You can pass `customer=7` on connect (libpq `options` / extra keywords, depending on the driver).

`SET customer = '7'` after connect is acknowledged by the proxy but **not** written to `extra` today — it will not reach OPA.

### Snowflake / MCP / Flight

Snowflake HTTP copies protocol fields (role, warehouse, schema, …) into `extra`, not arbitrary customer ids. MCP and Flight do not expose a first-class “session param for OPA” field. Prefer Trino HTTP from the API, or extend the frontend to set `session.extra["customer"]` after your own auth.

---

## What QueryFlux will and will not do

| QueryFlux does | QueryFlux does not |
| --- | --- |
| Forward allowlisted extra keys as `input.context.sessionParams` | Invent `WHERE customer_id = 7` from the key name |
| Splice OPA's `rowFilters` at every table scan (CTEs, joins, subqueries) | Verify that `api-service-a` is *entitled* to customer `7` — your API or Rego must |
| Record `opa_access` rewrite/deny on the query (Studio Guard Actions) | Substitute `sessionParams` into SQL itself |
| Include `sessionParams` in the OPA decision-cache key | Parse `X-Trino-Session` name/value pairs into extra |
| | Populate dry-run `session.extra` (dry-run identity only — see below) |

---

## Dry-run limitation

`POST /admin/access-control/dry-run` builds an **empty** session extra map. You can preview identity-only policies (Alice vs Bob in [`examples/with-opa`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa)); you cannot pass `customer: "7"` on the dry-run body to exercise this use case. Call OPA directly with a full `input` document, or run a real query through QueryFlux as `api-service-a`.

---

## Checklist

- [ ] End-user auth stays in **your** API; QueryFlux auth is the service account only.
- [ ] `customer` (or `customer_id`) is set on `session.extra` **after** your auth service resolves it.
- [ ] `sessionParamKeys` lists that exact key.
- [ ] Rego denies if the param is missing or not a well-formed id.
- [ ] Rego returns `rowFilters` with a predicate on the **real column name** in the lakehouse tables.
- [ ] `failOpen: false` so an OPA outage does not leak all tenants.
- [ ] Studio **Queries** shows `opa_access` **rewritten** and **Rewritten SQL (access control)** containing `customer_id = '7'`.

---

## Related

- [Access control overview](overview.md) — pipeline, masks, `sessionParamKeys`
- [OPA provider](opa.md) — wire format and general delegation notes
- [Guardrails](../architecture/guardrails) — `opa_access` in the same chain
- [Authentication](../authentication) — service-account `auth` vs backend `queryAuth`
