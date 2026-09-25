---
sidebar_label: Cerbos
title: Cerbos Access Control Provider
description: Connect QueryFlux to Cerbos — wire format, the outputs-based row-filter/column-mask contract, server auth, and the local demo.
image: img/queryflux-hero-banner.png
---
# Cerbos provider

<p class="provider-doc-hero">
  <img src="/img/logos/cerbos.svg" alt="Cerbos" class="provider-logo provider-logo--lg" />
  <span>
    <a href="https://cerbos.dev">Cerbos</a> is an access-control provider QueryFlux can connect to.
    Grants live in Cerbos policy files (YAML, CEL conditions) you author and version outside
    QueryFlux; QueryFlux asks Cerbos on each query and enforces allow, deny, row filters, and
    column masks.
  </span>
</p>

Read the [Access control overview](overview.md) first for the pipeline, identity model, and provider-agnostic config. This page covers the Cerbos wire contract, policy shape, server auth, and the runnable demo.

---

## When to use Cerbos

Use Cerbos when you want:

- A policy language built around **roles and resource attributes** (RBAC-first, with derived roles and ABAC conditions).
- Policies as plain YAML files with CEL conditions.
- The same PDP other services in your org already run for API/service authorization.

Cerbos does **not** replace frontend authentication (`auth`) or cluster-group authorization (`authorization`). It answers the data-plane question: *given this verified identity and these tables, what may they see?*

---

## Configuration

QueryFlux **connects** to Cerbos; it does not store or edit Cerbos policy files. Configure a named connection with `provider: cerbos` and a `cerbos:` block:

```yaml
accessControl:
  enabled: true
  defaultConnection: prod
  connections:
    prod:
      provider: cerbos
      cerbos:
        url: http://localhost:3592
        checkResourcesPath: /api/check/resources
        timeoutMs: 1000
        # bearerToken: "..."
      operations: [table.select]
      onMissingSchema: evaluate
      failOpen: false
      cacheTtlMs: 5000
      sessionParamKeys: [customer_id, tenant_id]
  groups:
    trino-prod:
      enabled: true
      failOpen: false
    sandbox:
      enabled: false                 # skip Cerbos for this cluster group
```

| Field | Default | Notes |
| --- | --- | --- |
| `connections.<name>.provider` | — | Set to `cerbos` for this connection. |
| `connections.<name>.cerbos.url` | *(required)* | Base URL of the Cerbos PDP's HTTP API (`http` or `https`). Default Cerbos port is `3592`. |
| `connections.<name>.cerbos.checkResourcesPath` | `/api/check/resources` | Cerbos's `CheckResources` REST path. Must start with `/`. |
| `connections.<name>.cerbos.timeoutMs` | `1000` | HTTP timeout for the Cerbos call on this connection. |
| `connections.<name>.cerbos.bearerToken` | — | Static bearer/API token, for Cerbos Hub/Cloud or a self-hosted PDP fronted by one. Plain self-hosted Cerbos typically needs none — it's usually secured by network policy instead. |

No connection name is reserved. `provider: cerbos` without a `cerbos:` block on a connection fails validation at startup, and so does a `defaultConnection` or `groups.<name>.connection` that names a connection not defined under `connections`. Without `defaultConnection` set, a group with no explicit `groups.<name>.connection` override gets **no access control at all**. Shared knobs (`enabled`, `operations`, `failOpen`, `sessionParamKeys`, …) are documented in the [overview](overview.md#enabling-access-control).

Cerbos PDP auth in QueryFlux is a static `bearerToken` only — there is no OAuth2 client-credentials option for the Cerbos connection.

### Scope (which groups call which connection)

Scope — which **cluster groups** run access control, and which named connection each uses — is configured under `accessControl.groups` and edited on the **Access Control** page in Studio (not on the Clusters group form). See [Scope by cluster group](overview.md#scope-by-cluster-group) and [Multiple connections](overview.md#multiple-connections).

Different rules per team on the **same** Cerbos PDP: branch in the policy on `R.attr` / roles / derived roles — one policy repository, testable offline. A genuinely different **Cerbos server** per team (network segmentation, blast-radius isolation, migrating one group to a new Cerbos deployment) is a second named `connections` entry plus `groups.<name>.connection`.

---

## Wire format

QueryFlux `POST`s JSON to `{url}{checkResourcesPath}` — Cerbos's standard `CheckResources` API:

```http
POST /api/check/resources
Content-Type: application/json
```

### Request

```json
{
  "requestId": "",
  "principal": {
    "id": "bob",
    "roles": ["analyst"],
    "attr": {
      "groups": ["analysts"]
    }
  },
  "resources": [
    {
      "resource": {
        "id": "customers",
        "kind": "table",
        "attr": {
          "catalog": "lakekeeper",
          "schema": "demo",
          "table": "customers",
          "columns": ["name", "region", "ssn"]
        }
      },
      "actions": ["table.select"]
    }
  ]
}
```

| Field | Notes |
| --- | --- |
| `principal.id` / `.roles` | From the verified `AuthContext`. |
| `principal.attr.groups` | `AuthContext.groups`, carried as a free-form attribute — Cerbos's own RBAC matches on `roles`, not `groups`; put group-based logic in a `condition` on `P.attr.groups` if you need it. |
| `resources[].resource.kind` | `"table"` for every table a query reads, and for a table DDL target (`CREATE`/`ALTER`/`DROP TABLE`); a `view`, `schema` or `catalog` DDL target uses that resource kind instead. One Cerbos resource policy per kind governs it generically (matching on `R.attr.catalog` / `R.attr.schema` / `R.attr.table`; `table` is empty for schema/catalog resources). |
| `resources[].resource.attr.columns` | Named list, or **omitted** meaning all columns (`SELECT *` or unresolved schema). |
| `resources[].actions` | Always a single-element array — QueryFlux evaluates one namespaced operation (e.g. `table.select`) per request. A statement that reads tables and also writes one makes one request per operation. Your Cerbos policy must define each operation you enable in `operations` (`table.insert`, `table.update`, …) — an action with no matching rule is `EFFECT_NO_MATCH`, which denies. Each resource is sent under its own Cerbos `kind` — `table`, and for DDL `view`, `schema` or `catalog` (`id` is the object's name) — so enabling `schema.drop` needs a `schema` resource policy. |

:::info Cerbos requires non-empty `principal.roles`
Cerbos's `CheckResources` API rejects an empty `roles` array as an **HTTP 400 validation error** — it is not evaluated as "no rule matches." `CerbosProvider` handles this itself: if `identity.roles` is empty, QueryFlux denies the query **locally**, without calling Cerbos at all, with the reason *"cerbos requires at least one principal role; none were resolved for this identity."* If you see that reason, your auth layer isn't populating `roles` for that user — add `roles:` under `auth.staticUsers.<user>` (or map your real IdP's role claim to it).
:::

### Response

Cerbos's own API contract guarantees exactly one `results` entry per requested resource, in the same order. `CerbosProvider` relies on that ordering to correlate resources back to tables — not on the echoed `resource.id` — so it isn't sensitive to duplicate or unusual table identifiers. If Cerbos ever returns a different number of results than requested (a provider-level anomaly), QueryFlux treats the whole decision as **deny-all**.

```json
{
  "results": [
    {
      "resource": { "id": "customers", "kind": "table" },
      "actions": { "table.select": "EFFECT_ALLOW" },
      "outputs": [
        {
          "src": "resource.table.vdefault#rule-002",
          "val": [
            { "kind": "row_filter", "expression": "region = 'EU'" },
            { "kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4" }
          ]
        }
      ]
    }
  ]
}
```

| Field | Notes |
| --- | --- |
| `results[].actions.<action>` | Only the literal string `"EFFECT_ALLOW"` is treated as allowed. Anything else — `EFFECT_DENY`, `EFFECT_NO_MATCH` (Cerbos's own "no rule matched" default), a missing key, an unrecognized value — denies. |
| `results[].outputs[].val` | See below — QueryFlux's own convention for carrying row filters and column masks through Cerbos's generic outputs mechanism. |

### The `outputs` contract for row filters and column masks

Cerbos's `CheckResources` has **no native concept of a row filter or column mask** — it only returns allow/deny per action, plus a generic `outputs` array collected from whichever policy rules activated. QueryFlux defines a convention for what a rule's output must look like to be understood as a row filter or column mask.

A rule contributes a row filter or column mask by returning a CEL value shaped like:

```json
{ "kind": "row_filter", "expression": "region = 'EU'" }
{ "kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4" }
```

— or a **list** of either, from one rule's single `output.when.ruleActivated` CEL expression (shown combined in the response example above). Multiple activated rules on the same resource all contribute; their outputs accumulate rather than overwrite each other. Any output whose `val` doesn't match either shape (an unrelated audit message, say) is silently ignored — not an error.

Field requirements for `column_mask` follow the [mask vocabulary](overview.md#mask-vocabulary): `column` and `type` always; `value` for `CONSTANT`; `expression` for `CUSTOM`.

### Column masks

Written in a Cerbos policy `output`, a mask output looks like:

```yaml
output:
  when:
    ruleActivated: |
      {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}
```

For `CUSTOM`, `expression` is required (its absence denies the query, `ACCESS_MASK_RENDER_FAILED`), spliced verbatim in the **client's source dialect**, referencing the **bare** column name:

```yaml
output:
  when:
    ruleActivated: |
      {"kind": "column_mask", "column": "email", "type": "CUSTOM",
       "expression": "concat(substr(email, 1, 1), '***@', split_part(email, '@', 2))"}
```

Full rewrite semantics (scan-site substitution, mask rendering, named-type table): [overview — row filters and column masks](overview.md#row-filters-and-column-masks).

---

## Policy shape

The default `checkResourcesPath` expects a Cerbos **resource policy** for the fixed resource kind `table`:

```yaml
apiVersion: api.cerbos.dev/v1
resourcePolicy:
  resource: table
  version: default
  rules:
    - actions: ["table.select"]
      roles: ["engineer"]
      effect: EFFECT_ALLOW
      condition:
        match:
          expr: R.attr.table in ["customers", "payroll"]

    # One rule, one CEL expression, two outputs.
    - actions: ["table.select"]
      roles: ["analyst"]
      effect: EFFECT_ALLOW
      condition:
        match:
          expr: R.attr.table == "customers"
      output:
        when:
          ruleActivated: >
            [
              {"kind": "row_filter", "expression": "region = 'EU'"},
              {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}
            ]

    - actions: ["table.select"]
      roles: ["analyst"]
      effect: EFFECT_DENY
      condition:
        match:
          expr: R.attr.table == "payroll"

    # Everything else — no matching role, or a table this policy doesn't
    # know about — falls through to Cerbos's own default: EFFECT_DENY.
```

A full, validated demo policy (this exact file) lives at [`examples/with-cerbos/policy/table.yaml`](https://github.com/lakeops-org/queryflux/blob/main/examples/with-cerbos/policy/table.yaml).

### Tips

- Match on `R.attr.table` (and `R.attr.catalog` / `R.attr.schema` when you need to disambiguate same-named tables across catalogs) — these are exactly the fields `CerbosProvider` sends, listed under [Wire format](#request) above.
- Prefer Cerbos's native `roles:` matching over branching on `P.attr.groups` in a CEL condition — it's what the policy language is built around, and it's how [`policy/table.yaml`](https://github.com/lakeops-org/queryflux/blob/main/examples/with-cerbos/policy/table.yaml) does it.
- One rule can emit several outputs at once (a row filter plus several column masks) as a single CEL list — you don't need a separate rule per output.
- Cerbos's own default when no rule matches is `EFFECT_DENY` — there's no need to author a catch-all deny rule just to fail closed.
- Never put untrusted client text into a `row_filter` expression or a `CUSTOM` mask's `expression`; validate anything from `sessionParams` inside the policy condition before using it.
- Everything else about Cerbos policy authoring — derived roles, `_schemas`, other resource kinds, scopes, policy testing — works exactly as it would in any other Cerbos deployment. This page's convention only governs the shape of one rule's `output`.

---

## Delegation (actor X, subject Y)

Full walkthrough (API service account, SQL injection caveats): **[Customer API — per-tenant row filters](customer-api-row-filters)**. In Cerbos / CEL:

1. Authenticate X normally (`auth`).
2. Put Y in session context under an allowlisted key, e.g. `customer_id`.
3. Configure `sessionParamKeys: [customer_id]`.
4. The policy checks that X may act for Y, then returns a row filter:

```yaml
- actions: ["table.select"]
  roles: ["customer_portal"]
  effect: EFFECT_ALLOW
  condition:
    match:
      expr: R.attr.sessionParams.customer_id != ""
  output:
    when:
      ruleActivated: |
        {"kind": "row_filter", "expression": "customer_id = '" + R.attr.sessionParams.customer_id + "'"}
```

**Do not** set `identity.user` to Y unless you intend true, audited impersonation. Keep the actor in identity for audit trails; put the subject in `sessionParams`.

QueryFlux never substitutes `sessionParams` into SQL itself — only expressions returned by Cerbos (via `outputs`) are spliced.

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

`identity.attributes` is sent as-is in `principal.attr`, alongside `groups`. A Cerbos policy condition can then branch on `P.attr.department`, `P.attr.clearance_level`, etc. Static / LDAP / no-auth leave it empty.

---

## Authenticating to Cerbos

If your Cerbos PDP (Cerbos Hub/Cloud, or a self-hosted PDP fronted by one) requires a token:

```yaml
accessControl:
  connections:
    default:
      provider: cerbos
      cerbos:
        url: https://cerbos.internal
        bearerToken: "eyJ..."
```

Each connection authenticates independently — a second named connection to a different Cerbos server uses its own bearer token, unrelated to `default`'s. Plain self-hosted Cerbos, secured by network policy instead of application-level auth, needs no `bearerToken` at all.

---

## Fail-closed defaults

| Situation | Default behavior |
| --- | --- |
| Identity with no `roles` resolved | **Deny**, locally, without calling Cerbos — see the [`principal.roles` note](#request) above. |
| Cerbos timeout / HTTP error / unparseable body | **Deny** (`failOpen: false`) |
| Cerbos returns a different number of results than resources requested | **Deny all** |
| Any resource whose action effect isn't exactly `EFFECT_ALLOW` | **Deny** whole query |
| Unresolved schema + `onMissingSchema: deny` | **Deny** before calling Cerbos |

Set `failOpen: true` (or per-group) only when availability must trump enforcement for that path. Note that the empty-`roles` case denies regardless of `failOpen` — it's treated as a well-formed decision, not a provider error.

---

## Local demo

A Compose stack with Lakekeeper, RustFS, Trino, and Cerbos lives under [`examples/with-cerbos/`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-cerbos):

| User | Role | `customers` | `payroll` |
| --- | --- | --- | --- |
| `alice` | `engineer` | all rows, SSN visible | allowed |
| `bob` | `analyst` | `region = 'EU'`, SSN `SHOW_LAST_4` | denied |

```bash
cd examples/with-cerbos
docker compose up -d --wait
docker compose --profile seed run --rm data-seed

# from repo root — QueryFlux on the host
cargo run -p queryflux -- --config examples/with-cerbos/config.yaml
```

- Cerbos HTTP: `http://127.0.0.1:8184`
- Lakekeeper: `http://127.0.0.1:8181`
- QueryFlux Postgres: `postgresql://queryflux:queryflux@127.0.0.1:5434/queryflux` (query history + Studio config)
- Admin dry-run: `POST http://localhost:9000/admin/access-control/dry-run` (Basic `admin` / `admin`)

`examples/with-cerbos/demo.py` covers the same ground, including two direct `CheckResources` calls that print Cerbos's raw response. See the example [README](https://github.com/lakeops-org/queryflux/blob/main/examples/with-cerbos/README.md) for ports, seeding, and curl recipes.

---

## Dry-run against Cerbos

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT name, ssn FROM lakekeeper.demo.customers",
    "clusterGroup": "trino-cerbos",
    "dialect": "trino",
    "identity": { "user": "bob", "groups": ["analysts"], "roles": ["analyst"] }
  }'
```

`identity.roles` is not optional here — omit it and the response is a clean local deny (see [fail-closed defaults](#fail-closed-defaults)), not a call to Cerbos.

If `clusterGroup` has access control disabled, or has no resolvable connection, the response is `{ "outcome": "skip", "reason": "..." }` instead of calling Cerbos. Every other response carries `"connection"` — the named connection `clusterGroup` resolved to.

Or call Cerbos directly to debug policy without QueryFlux:

```bash
curl -s -X POST http://127.0.0.1:8184/api/check/resources \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "demo",
    "principal": {"id": "bob", "roles": ["analyst"], "attr": {}},
    "resources": [{"resource": {"id": "customers", "kind": "table", "attr": {"table": "customers"}}, "actions": ["table.select"]}]
  }'
```

---

## Observability

- Guard action name: `opa_access` (the access-control rewrite/deny guard in query history)
- A provider-level error (timeout, HTTP failure, unparseable body) is logged with `provider: "cerbos"`
- Decision cache hits skip the Cerbos round-trip within `cacheTtlMs`
- Provider timeouts and denials appear on the query record like other guard blocks

Studio shows rewritten SQL separately from dialect-translated SQL when both apply.

---

## Related reading

- [Access control overview](overview.md)
- [Customer API row filters](customer-api-row-filters)
- [Guardrails](../architecture/guardrails)
- [Authentication & identity](../authentication)
- [Catalog integration](../architecture/catalog-integration)
- [examples/with-cerbos](https://github.com/lakeops-org/queryflux/tree/main/examples/with-cerbos)
