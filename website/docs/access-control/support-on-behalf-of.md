---
sidebar_label: Support on-behalf-of
title: Support — query on behalf of an account
description: A support agent or desk service authenticates as themselves, attaches an account_id they already verified, and policy returns a row filter plus masked payment fields.
image: img/queryflux-hero-banner.png
---
# Support — query on behalf of an account

Support tools need to inspect a customer's billing and subscription history **without** logging in as that customer and **without** embedding `WHERE account_id = …` in every handler. Authenticate the **agent** (or a support-desk service account), put the **account** they selected into session context, and let the policy provider return the filter.

Same **actor vs subject** shape as the [Customer API](customer-api-row-filters.md) use case — different domain: accounts, subscriptions, and invoices instead of multi-tenant `customer_id` on app data.

```
Support agent (or desk service)
        │  picks account after their own CRM auth
        ▼
┌───────────────────────────┐
│  Support app / CRM        │  account_id = acct_9f3a  (trusted)
└─────────────┬─────────────┘
              │  identity: sam  (or support-desk)
              │  session extra: account_id = acct_9f3a
              ▼
┌───────────────────────────┐
│  QueryFlux                │  sessionParamKeys: [account_id]
└─────────────┬─────────────┘
              ▼
┌───────────────────────────┐
│  Policy provider          │  allow subscriptions + invoices
│                           │  row filter: account_id = 'acct_9f3a'
│                           │  mask: iban → SHOW_LAST_4
└─────────────┬─────────────┘
              ▼
     rewritten SQL → engine
```

---

## When this fits

| Signal | This use case |
| --- | --- |
| Who opens the SQL connection? | Support human **or** a thin desk service |
| Whose data is the query about? | An **account** chosen in the CRM (not the agent) |
| Audit should show | The agent / desk principal — not a fake customer login |
| Typical tables | `subscriptions`, `invoices`, `support_notes` |

Use [Customer API row filters](customer-api-row-filters.md) when an **external** customer-facing API does the same pattern for product tenants. Use [Internal analysts](internal-analysts.md) when humans query only as themselves with no on-behalf subject.

---

## Example outcome

| Principal | Groups | Session | `subscriptions` / `invoices` | `iban` column |
| --- | --- | --- | --- | --- |
| `sam` | `support` | `account_id=acct_9f3a` | only that account's rows | `SHOW_LAST_4` |
| `sam` | `support` | *(missing account_id)* | **denied** | — |
| `morgan` | `support_leads` | *(any / none)* | all accounts | visible |
| `sam` | `support` | querying `payroll_hr` | **denied** (not a support table) | — |

Sam runs:

```sql
SELECT plan, status, iban FROM billing.subscriptions
```

QueryFlux rewrites the scan with `account_id = 'acct_9f3a'` and masks `iban`. Morgan, in `support_leads`, can run the same SQL across every account with IBAN unmasked for escalation cases your policy allows.

---

## Responsibilities

| Layer | Owns |
| --- | --- |
| **CRM / support app** | Authenticate the agent. Resolve `account_id` from a ticket or search UI. **Never** take it from an untrusted query string without re-checking entitlement. |
| **QueryFlux `auth`** | Authenticate `sam` or `support-desk`. That principal is what Studio and `guard_actions` record. |
| **`sessionParamKeys`** | Forward only `account_id` (and similar allowlisted keys) to the provider. |
| **Policy provider** | Require `account_id` for support agents; optional bypass for support leads; deny unrelated tables; attach IBAN (or similar) masks. |
| **QueryFlux rewrite** | Scan-site row filter + column mask substitution. |

Do **not** set `identity.user` to `acct_9f3a`. Keep the actor as the agent so audit trails show who looked up the account.

---

## Configuration sketch

```yaml
auth:
  provider: static
  required: true
  staticUsers:
    users:
      sam:
        password: "YOUR_PASSWORD_HERE"
        groups: [support]
        roles: [support]
      morgan:
        password: "YOUR_PASSWORD_HERE"
        groups: [support_leads]
        roles: [support_lead]
      support-desk:
        password: "YOUR_PASSWORD_HERE"
        groups: [support]
        roles: [support]

accessControl:
  enabled: true
  defaultConnection: support-pdp
  connections:
    support-pdp:
      provider: opa              # or cerbos + cerbos: { url: ... }
      opa:
        url: http://localhost:8181
        decisionPath: /v1/data/queryflux/access
      operations: [table.select]
      failOpen: false
      onMissingSchema: deny
      sessionParamKeys: [account_id]
  groups:
    support-tools:
      enabled: true
      connection: support-pdp
```

How `account_id` reaches `session.extra` (Trino header, MySQL `SET`, …): same mechanisms as [Customer API — putting the subject on the session](customer-api-row-filters.md#how-to-put-customer7-on-the-session) — use a header or session key named `account_id`, not a free-form string inside SQL.

---

## Policy sketch (OPA / Rego)

```rego
package queryflux.access

import rego.v1

resources := [d |
	some r in input.action.resources
	d := table_decision(r)
]

bare(name) := n if {
	parts := split(name, ".")
	n := parts[count(parts) - 1]
}

is_lead if "support_leads" in input.identity.groups
is_agent if "support" in input.identity.groups

account := input.context.sessionParams.account_id

support_tables := {"subscriptions", "invoices", "support_notes"}

table_decision(r) := {
	"table": r.table,
	"allow": true,
} if {
	is_lead
	bare(r.table) in support_tables
}

table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": "account_id session param required",
} if {
	is_agent
	not is_lead
	account == ""
}

table_decision(r) := {
	"table": r.table,
	"allow": true,
	"rowFilters": [{
		"expression": sprintf("account_id = '%s'", [account]),
	}],
	"columnMasks": [{"column": "iban", "type": "SHOW_LAST_4"}],
} if {
	is_agent
	not is_lead
	regex.match(`^acct_[a-z0-9]+$`, account)
	bare(r.table) in support_tables
}

table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": sprintf("table %q is outside support scope", [r.table]),
} if {
	not bare(r.table) in support_tables
}
```

:::warning SQL injection

QueryFlux splices `expression` verbatim. Validate `account_id` in policy (`regex.match`) **and** only set session extra after the CRM has resolved a real account the agent may open.

:::

Cerbos / CEL: same actor–subject split via `outputs` — see [Cerbos — delegation](cerbos.md#delegation-actor-x-subject-y). Swap the predicate and mask column names to `account_id` / `iban`.

---

## Dry-run limitation

Admin dry-run sends an **empty** session extra map, so you cannot preview `account_id=acct_9f3a` through dry-run alone. Preview lead-vs-agent **table** grants with identity-only dry-run; exercise the row filter with a real query (or a direct call to your policy provider with a full input document).

---

## Checklist

- [ ] Agent auth is separate from the end-customer identity.
- [ ] `account_id` is set on `session.extra` only after CRM entitlement checks.
- [ ] `sessionParamKeys` lists `account_id` exactly.
- [ ] Agents without `account_id` are **denied**; leads may be unrestricted if your policy says so.
- [ ] Support scope is limited to billing/support tables — HR/payroll-style tables stay denied.
- [ ] Payment fields (`iban`, PAN, …) use a column mask for agents.
- [ ] Studio **Queries** shows `opa_access` rewrite and **Rewritten SQL** containing `account_id = 'acct_9f3a'`.

---

## Related reading

- [Access control overview](overview.md)
- [Customer API — per-tenant row filters](customer-api-row-filters.md) — same pattern, product-tenant domain
- [Internal analysts](internal-analysts.md) — humans as themselves, no on-behalf subject
- [OPA provider](opa.md) · [Cerbos provider](cerbos.md)
- [Authentication](../authentication)
