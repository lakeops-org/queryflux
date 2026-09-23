---
sidebar_label: Internal analysts
title: Internal analysts — group grants, region filters, column masks
description: Humans connect as themselves (OIDC or static). Policy grants tables by group, adds region row filters, and masks PII columns — no session params, no service account.
image: img/queryflux-hero-banner.png
---
# Internal analysts — group grants, region filters, column masks

Data teams often put **humans** in front of QueryFlux — analysts in a BI tool, engineers in a SQL IDE — and need different table grants, region scopes, and PII visibility per group. Authenticate each person as themselves; let the policy provider decide from their groups (or roles) what they may see.

This is the opposite shape from **[Customer API — per-tenant row filters](customer-api-row-filters.md)**: there is no service account and no `sessionParams` subject. The verified **identity** *is* the subject.

```
Analyst / engineer (OIDC or static login)
        │
        ▼
┌───────────────────────────┐
│  QueryFlux `auth`         │  identity.user + groups / roles
└─────────────┬─────────────┘
              │  cluster group → accessControl connection
              ▼
┌───────────────────────────┐
│  Policy provider          │  table allow/deny
│  (OPA or Cerbos)          │  + row filter (e.g. region = 'EU')
│                           │  + column mask (e.g. SSN SHOW_LAST_4)
└─────────────┬─────────────┘
              ▼
     rewritten SQL → engine
```

---

## When this fits

| Signal | This use case |
| --- | --- |
| Who opens the SQL connection? | The human (or their SSO-backed client) |
| How does policy branch? | `identity.groups` / `identity.roles` (and optional `identity.attributes`) |
| Typical controls | Table grants, region/org row filters, PII column masks |
| `sessionParamKeys` | Usually empty — nothing to forward from session extra |

Prefer the [Customer API](customer-api-row-filters.md) pattern when a **backend** must query on behalf of many tenants while keeping a single service principal in the audit trail.

---

## Responsibilities

| Layer | Owns |
| --- | --- |
| **IdP / static users** | Authenticate Alice and Bob. Map group/role claims into QueryFlux `identity`. |
| **QueryFlux `auth`** | Verify the human; populate `user`, `groups`, `roles`, optional `attributes`. |
| **Access Control scope** | Which cluster groups call which connection (Studio **Access Control** page, or `accessControl.groups`). |
| **Policy provider** | Allow/deny tables; attach row filters and column masks from group membership. |
| **Catalog** | Column lists for `SELECT *` and mask rewrite — configure a [catalog integration](../architecture/catalog-integration), or use `onMissingSchema: deny`. |

---

## Example outcome

Same story as the runnable demos (`examples/with-opa`, `examples/with-cerbos`):

| User | Groups / roles | `customers` | `payroll` |
| --- | --- | --- | --- |
| `alice` | engineers / `engineer` | all rows, SSN visible | allowed |
| `bob` | analysts / `analyst` | `region = 'EU'`, SSN `SHOW_LAST_4` | denied |

Bob's query:

```sql
SELECT name, region, ssn FROM lakekeeper.demo.customers
```

becomes (conceptually) a scan-site rewrite with `region = 'EU'` in the inner `WHERE` and `ssn` replaced by a last-4 mask expression — still in the **source dialect** the client wrote. Alice's identical SQL is unchanged. Bob querying `payroll` is denied before any engine runs.

---

## Configuration sketch

Humans authenticate; access control points at one named connection. Pick OPA or Cerbos for that connection — not both on the same name.

```yaml
auth:
  provider: static          # or oidc — map groups/roles from the IdP
  required: true
  staticUsers:
    users:
      alice:
        password: "YOUR_PASSWORD_HERE"
        groups: [engineers]
        roles: [engineer]
      bob:
        password: "YOUR_PASSWORD_HERE"
        groups: [analysts]
        roles: [analyst]

accessControl:
  enabled: true
  defaultConnection: prod
  connections:
    prod:
      provider: opa         # or: cerbos + cerbos: { url: ... }
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
      operations: [table.select]
      failOpen: false
      onMissingSchema: deny
      # No sessionParamKeys — identity carries everything policy needs.
  groups:
    analysts-bi:
      enabled: true
      connection: prod
    sandbox:
      enabled: false        # skip access control for this cluster group
```

Wire format and policy authoring:

- <img src="/img/logos/opa.svg" alt="" class="provider-logo" /> **[OPA](opa.md)** — Rego decision document; demo [`examples/with-opa/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa)
- <img src="/img/logos/cerbos.svg" alt="" class="provider-logo" /> **[Cerbos](cerbos.md)** — resource policy + `outputs` for filters/masks; demo [`examples/with-cerbos/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-cerbos)

OIDC variant (Keycloak groups → QueryFlux identity): [`examples/with-opa-oidc/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-opa-oidc).

---

## What to put in policy (checklist)

1. **Table grants by group** — engineers may read `customers` and `payroll`; analysts only `customers`.
2. **Row filter for analysts** — e.g. `region = 'EU'` (source-dialect boolean fragment).
3. **Column mask for PII** — e.g. `ssn` → `SHOW_LAST_4` (or `HASH` / `CUSTOM` from the [mask vocabulary](overview.md#mask-vocabulary)).
4. **Fail closed** — unknown tables and unmatched identities deny; keep `failOpen: false` unless a path must stay available when the provider is down.
5. **Catalog** — without resolvable columns, `SELECT *` + masks cannot rewrite safely; demos use Lakekeeper for that reason.

Do **not** rely on clients adding `WHERE region = 'EU'` themselves — the provider returns the filter; QueryFlux splices it at every scan site (CTEs, subqueries, self-joins included).

---

## Dry-run

`POST /admin/access-control/dry-run` works well here because identity alone drives the decision (no session params):

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT name, ssn FROM lakekeeper.demo.customers",
    "clusterGroup": "analysts-bi",
    "dialect": "trino",
    "identity": { "user": "bob", "groups": ["analysts"], "roles": ["analyst"] }
  }'
```

Expect `outcome: "rewrite"` with `rewrittenSql` for Bob, and allow-without-rewrite (or a different shape) for Alice. Cerbos connections need non-empty `identity.roles` — see [Cerbos — fail-closed defaults](cerbos.md#fail-closed-defaults).

---

## Related reading

- [Access control overview](overview.md)
- [Customer API — per-tenant row filters](customer-api-row-filters.md) — service account + `sessionParams`
- [OPA provider](opa.md) · [Cerbos provider](cerbos.md)
- [Authentication](../authentication)
- [Catalog integration](../architecture/catalog-integration)
