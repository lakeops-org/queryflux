---
sidebar_label: OPA
title: OPA Access Control Provider
description: Connect QueryFlux to Open Policy Agent — wire format, row filters, column masks, and the local demo. Rego stays in the OPA bundle.
image: img/queryflux-hero-banner.png
---
# OPA provider

<p class="provider-doc-hero">
  <img src="/img/logos/opa.svg" alt="Open Policy Agent" class="provider-logo provider-logo--lg" />
  <span>
    <a href="https://www.openpolicyagent.org/">Open Policy Agent</a> (OPA) is the access-control
    provider shipped with QueryFlux. Grants live in a Rego policy bundle you author and version
    outside QueryFlux; QueryFlux asks OPA on each query and enforces allow, deny, row filters, and
    column masks.
  </span>
</p>

Read the [Access control overview](overview.md) first for the pipeline, identity model, and provider-agnostic config. This page covers the OPA wire contract, Rego shape, server auth, and the runnable demo.

---

## When to use OPA

Use OPA when you want:

- **Central policy-as-code** for table/column grants, row filters, and masks — instead of each app injecting `WHERE` clauses.
- The same decision engine many platforms already run for Kubernetes admission, API gateways, or CI (Conftest).
- Policies that change via Git/bundle rollout **without** redeploying QueryFlux.

OPA does **not** replace frontend authentication (`auth`) or cluster-group authorization (`authorization`). It answers the data-plane question: *given this verified identity and these tables, what may they see?*

---

## Configuration

QueryFlux **connects** to OPA; it does not store or edit Rego. In Studio, **Access Control** lets you add any number of named connections (URL / decision path / credentials each), pick which one is the **default**, and set **scope by cluster group** (global default and per-group inherit / enabled / disabled, plus which connection). The same fields live under `accessControl.connections.<name>:` in YAML — most deployments need only one, named as `defaultConnection`:

```yaml
accessControl:
  enabled: true                     # default for groups without groups.<name>.enabled
  defaultConnection: prod           # groups without an override use this connection
  connections:
    prod:
      provider: opa                 # default when a connection is defined
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
        timeoutMs: 1000
        # bearerToken: "..."
        # clientCredentials:
        #   clientId: ...
        #   clientSecret: ...
        #   tokenEndpoint: https://idp.example/oauth/token
      operations: [table.select]
      onMissingSchema: evaluate
      failOpen: false
      cacheTtlMs: 5000
      sessionParamKeys: [customer_id, tenant_id]
  groups:
    trino-prod:
      enabled: true                 # omit to inherit global enabled
      failOpen: false
    sandbox:
      enabled: false                # skip OPA for this cluster group
```

| Field | Default | Notes |
| --- | --- | --- |
| `connections.<name>.opa.url` | *(required)* | Base URL of the OPA server (`http` or `https`). |
| `connections.<name>.opa.decisionPath` | `/v1/data/queryflux/access` | OPA Data API path for the decision document. Must start with `/`. |
| `connections.<name>.opa.timeoutMs` | `1000` | HTTP timeout for the OPA call on this connection. |
| `connections.<name>.opa.bearerToken` | — | Static bearer token if this OPA requires auth. |
| `connections.<name>.opa.clientCredentials` | — | OAuth2 client-credentials fetch for a bearer token. |

No connection name is reserved. `provider: opa` without an `opa:` block on a connection fails validation at startup, and so does a `defaultConnection` or `groups.<name>.connection` that names a connection not defined under `connections`. Without `defaultConnection` set, a group with no explicit `groups.<name>.connection` override gets **no access control at all**. Shared knobs (`enabled`, `operations`, `failOpen`, `sessionParamKeys`, …) are documented in the [overview](overview.md#enabling-access-control).

### Scope (which groups call which connection)

Scope — which **cluster groups** run access control, and which named connection each uses — is configured under `accessControl.groups` and edited on the **Access Control** page in Studio (not on the Clusters group form). See [Scope by cluster group](overview.md#scope-by-cluster-group) and [Multiple connections](overview.md#multiple-connections).

Different rules per team on the **same** OPA: branch in Rego on `input.context.clusterGroup` and `input.identity.groups` — that's the common case, and it's one bundle to `opa test`. A genuinely different **OPA server** per team (network segmentation, blast-radius isolation, migrating one group to a new OPA deployment) is a second named `connections` entry plus `groups.<name>.connection`, not a separate QueryFlux instance.

---

## Wire format

QueryFlux `POST`s JSON to `{url}{decisionPath}`:

```http
POST /v1/data/queryflux/access
Content-Type: application/json
```

### Request

```json
{
  "input": {
    "identity": {
      "user": "bob",
      "groups": ["analysts"],
      "roles": [],
      "attributes": {}
    },
    "action": {
      "operation": "table.select",
      "resources": [
        {
          "kind": "table",
          "catalog": "lakekeeper",
          "schema": "demo",
          "table": "customers",
          "name": "customers",
          "columns": ["name", "region", "ssn"]
        }
      ]
    },
    "context": {
      "clusterGroup": "trino-opa",
      "engine": "trino",
      "queryId": "",
      "sessionParams": {
        "customer_id": "acct-88213"
      }
    }
  }
}
```

| Field | Notes |
| --- | --- |
| `identity.*` | Verified `AuthContext` only. |
| `action.operation` | `table.select` for tables the statement reads; for a write or DDL target, its own operation (`table.insert`/`update`/`delete`/`merge`/`truncate`/`create`/`drop`/`alter`, `view.create`/`drop`/`alter`, `schema.create`/`drop`, `catalog.create`/`drop`) when enabled in `operations`. A statement with both reads and a target makes one request for each; `CREATE OR REPLACE` also makes the matching `*.drop` request. |
| `action.resources[].kind` | `table` for everything a query reads; DDL can also target a `view`, `schema` or `catalog` (`CREATE DATABASE` is reported as a catalog). |
| `action.resources[].name` | The object's own name — table/view, schema, or catalog. Present for every kind. |
| `action.resources[].table` | Bare or as QueryFlux extracted it; often schema-qualified in practice. **Omitted** for `schema` and `catalog` resources (use `schema` / `catalog` and `name`). |
| `action.resources[].columns` | Named list, or **omitted / null** meaning all columns (`SELECT *` or unresolved schema). |
| `context.sessionParams` | Only keys listed in `sessionParamKeys`. |

### Response

OPA must return a `result` object with a non-empty `resources` array. A missing or empty `result` is treated as **deny-all** (fail closed on undefined policy).

```json
{
  "result": {
    "resources": [
      {
        "table": "customers",
        "allow": true,
        "rowFilters": [
          { "expression": "region = 'EU'" }
        ],
        "columnMasks": [
          { "column": "ssn", "type": "SHOW_LAST_4" }
        ]
      }
    ]
  }
}
```

| Field | Required | Notes |
| --- | --- | --- |
| `table` / `name` | yes (one of them) | Echo the resource's `table` (table resources) or `name` (any kind) — QueryFlux matches decisions back on it. `name` wins if both are sent. An echo that matches no requested resource counts as a missing decision, which denies. |
| `allow` | yes | `false` denies the whole query if any resource is denied. |
| `reason` | no | Surfaced on deny / audit. |
| `rowFilters` | no | Each entry needs `expression` (source-dialect boolean SQL). |
| `columnMasks` | no | See [Column masks](#column-masks) below. |

JSON field names from OPA use camelCase (`rowFilters`, `columnMasks`, `sessionParams`, `clusterGroup`) as in the examples above.

### Column masks

Each entry is:

| Field | When | Notes |
| --- | --- | --- |
| `column` | always | Real table column name (case-insensitive match at rewrite). |
| `type` | always | Named type or `CUSTOM` — see [mask vocabulary](overview.md#mask-vocabulary). |
| `value` | `CONSTANT` only | Literal string; QueryFlux quotes it for SQL. |
| `expression` | `CUSTOM` only | Full source-dialect SQL expression, used **verbatim**. |

**Named type** — QueryFlux builds the SQL:

```json
{ "column": "ssn", "type": "SHOW_LAST_4" }
```

renders to something like `CASE WHEN ssn IS NULL THEN NULL ELSE '****' || substr(ssn, -4) END`, then the scan becomes:

```sql
(SELECT …, <rendered> AS ssn, … FROM customers [WHERE …]) AS customers
```

**`CUSTOM`** — you author the expression; QueryFlux does not modify it:

```json
{
  "column": "ssn",
  "type": "CUSTOM",
  "expression": "CASE WHEN ssn IS NULL THEN NULL ELSE '***-' || substr(ssn, -4) END"
}
```

```rego
table_decision(r) := {
	"table": r.table,
	"allow": true,
	"columnMasks": [{
		"column": "email",
		"type": "CUSTOM",
		"expression": "concat(substr(email, 1, 1), '***@', split_part(email, '@', 2))",
	}],
} if {
	"analysts" in input.identity.groups
	bare(r.table) == "customers"
}
```

Requirements for `CUSTOM`:

- Write in the **client's source dialect** (access control runs before sqlglot).
- Reference the **bare** column name inside the expression (`email`, not `t.email`).
- Omit `expression` → QueryFlux **denies** the query (`ACCESS_MASK_RENDER_FAILED`).
- Do not embed untrusted client strings; validate any `sessionParams` you interpolate.

Prefer named types when they fit. Use `CUSTOM` for UDFs, fleet-specific hash functions, or shapes the named vocabulary cannot express. Prefer `CUSTOM` over `HASH` when engines disagree on hash SQL. Full rewrite semantics: [overview — row filters and column masks](overview.md#row-filters-and-column-masks).

---

## Rego package shape

The default `decisionPath` expects package `queryflux.access` to expose a `resources` rule — one decision object per input resource:

```rego
package queryflux.access

import rego.v1

resources := [d |
	some r in input.action.resources
	d := table_decision(r)
]

# Complete definitions must be mutually exclusive for a given r.
table_decision(r) := {
	"table": r.table,
	"allow": true,
} if {
	"engineers" in input.identity.groups
}

table_decision(r) := {
	"table": r.table,
	"allow": true,
	"rowFilters": [{"expression": "region = 'EU'"}],
	"columnMasks": [{"column": "ssn", "type": "SHOW_LAST_4"}],
} if {
	"analysts" in input.identity.groups
	endswith(r.table, "customers")
}

table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": sprintf("%s denied", [input.identity.user]),
} if {
	not "engineers" in input.identity.groups
	endswith(r.table, "payroll")
}
```

A full demo policy lives at [`examples/with-opa/policy/access.rego`](https://github.com/lakeops-org/queryflux/blob/main/examples/with-opa/policy/access.rego).

### Tips

- Prefer **complete definitions** that don't overlap for the same `r` — overlapping matches are an OPA eval error.
- Match on bare table names or qualified names consistently with what QueryFlux extracts (catalog/schema may be present).
- Put literals you inject into `rowFilters[].expression` or `CUSTOM` `expression` under policy control; never trust raw client SQL for the filter/mask body.
- Prefer named mask types (`SHOW_LAST_4`, `REDACT`, …) when they fit; use `CUSTOM` only when you need a hand-written expression.
- Unit-test with `opa test` and fixtures for each identity × table combination before rolling a bundle.

---

## Delegation (actor X, subject Y)

A common B2B / support pattern: authenticated as **user X**, but the data scope is **customer Y**. Full walkthrough (API service account, how to set `extra`, SQL injection, dry-run gap): **[Customer API — per-tenant row filters](customer-api-row-filters)**.

1. Authenticate X normally (`auth`).
2. Put Y in session context under an allowlisted key, e.g. `customer_id` (your API or frontend sets `SessionContext.extra` after validating X may request Y).
3. Configure `sessionParamKeys: [customer_id]`.
4. Rego checks that X may act for Y, then returns a row filter:

```rego
table_decision(r) := {
	"table": r.table,
	"allow": true,
	"rowFilters": [{
		"expression": sprintf("customer_id = '%s'", [input.context.sessionParams.customer_id]),
	}],
} if {
	"customer_portal" in input.identity.groups
	input.context.sessionParams.customer_id != ""
	# Optional: call into data.entitlements or an external check
	# customer_allowed(input.identity.user, input.context.sessionParams.customer_id)
}
```

**Do not** set `identity.user` to Y unless you intend true, audited impersonation. Keep the actor in identity for audit trails; put the subject in `sessionParams`.

QueryFlux never substitutes `sessionParams` into SQL itself — only expressions returned by OPA are spliced.

:::warning
Validate that X is allowed to access Y **before** or **inside** policy. An unauthenticated client must not be able to set `customer_id` arbitrarily and widen their own scope.
:::

---

## Identity attributes (ABAC)

With OIDC auth, map JWT claims into `identity.attributes`:

```yaml
auth:
  provider: oidc
  oidc:
    attributeClaims: [department, clearance_level]
```

Rego can then branch on `input.identity.attributes.department`, etc. Static / LDAP / no-auth leave `attributes` empty.

---

## Authenticating to OPA

If your OPA (or OPA Gatekeeper / Enterprise) requires a token:

```yaml
accessControl:
  connections:
    default:
      provider: opa
      opa:
        url: https://opa.internal
        decisionPath: /v1/data/queryflux/access
        bearerToken: "eyJ..."           # static
        # or:
        # clientCredentials:
        #   clientId: queryflux
        #   clientSecret: "..."
        #   tokenEndpoint: https://idp.example/oauth/token
```

Each connection authenticates independently — a second named connection to a different OPA server can use its own bearer token or client-credentials, unrelated to `default`'s.

---

## Fail-closed defaults

| Situation | Default behavior |
| --- | --- |
| OPA timeout / HTTP error / unparseable body | **Deny** (`failOpen: false`) |
| Empty / missing `result.resources` | **Deny all** |
| Any resource with `allow: false` | **Deny** whole query |
| Unresolved schema + `onMissingSchema: deny` | **Deny** before calling OPA |

Set `failOpen: true` (or per-group) only when availability must trump enforcement for that path.

---

## Local demo

A Compose stack with Lakekeeper, MinIO, Trino, OPA, and a small UI lives under [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa):

| User | Groups | `customers` | `payroll` |
| --- | --- | --- | --- |
| `alice` | engineers | all rows, SSN visible | allowed |
| `bob` | analysts | `region = 'EU'`, SSN `SHOW_LAST_4` | denied |

```bash
cd examples/with-opa
docker compose up -d --wait
docker compose --profile seed run --rm data-seed

# from repo root — QueryFlux on the host
cargo run -p queryflux -- --config examples/with-opa/config.yaml
```

- Demo UI: `http://127.0.0.1:8183`
- OPA: `http://127.0.0.1:8182`
- QueryFlux Postgres: `postgresql://queryflux:queryflux@127.0.0.1:5434/queryflux` (query history + Studio config)
- Admin dry-run: `POST http://localhost:9000/admin/access-control/dry-run` (Basic `admin` / `admin`)

See the example [README](https://github.com/lakeops-org/queryflux/blob/main/examples/with-opa/README.md) for ports, seeding, and curl recipes.

---

## Dry-run against OPA

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT name, ssn FROM lakekeeper.demo.customers",
    "clusterGroup": "trino-opa",
    "dialect": "trino",
    "identity": { "user": "bob", "groups": ["analysts"] }
  }'
```

If `clusterGroup` has access control disabled, or has no resolvable connection (no `groups.<name>.connection` override and no `defaultConnection` configured), the response is `{ "outcome": "skip", "reason": "..." }` instead of calling OPA. Every other response also carries `"connection"` — the named connection `clusterGroup` resolved to — so a dry-run against a group routed to a non-default connection is unambiguous about which OPA server actually answered.

Or call OPA directly to debug Rego without QueryFlux:

```bash
curl -s http://127.0.0.1:8182/v1/data/queryflux/access \
  -H "Content-Type: application/json" \
  -d '{"input":{"identity":{"user":"bob","groups":["analysts"],"roles":[],"attributes":{}},"action":{"operation":"table.select","resources":[{"table":"customers"}]},"context":{"clusterGroup":"trino-opa","engine":"trino","queryId":"demo","sessionParams":{}}}}'
```

---

## Observability

- Guard action name: `opa_access`
- Decision cache hits skip the OPA round-trip within `cacheTtlMs`
- Provider timeouts and denials appear on the query record like other guard blocks

Studio shows rewritten SQL separately from dialect-translated SQL when both apply.

---

## Related reading

- [Access control overview](overview.md)
- [Customer API row filters](customer-api-row-filters)
- [Guardrails](../architecture/guardrails)
- [Authentication & identity](../authentication)
- [Catalog integration](../architecture/catalog-integration)
- [examples/with-opa](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa)
