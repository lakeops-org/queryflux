---
title: Auth & Authorization Design
description: Authentication providers, authorization policies, backend identity resolution, and the two-credential security model.
image: img/queryflux-hero-banner.png
---
# QueryFlux Auth/AuthZ Design

## The Two-Credential Model (Core Principle)

Every backend cluster has **two distinct credential relationships**, both configured per-cluster in `ClusterConfig`:

**Credential Type 1 — Service Credentials** (`auth`, existing `ClusterAuth`)
- QueryFlux's own service account for the backend
- Used for: health checks, schema/catalog discovery, cluster management
- Static, ops-owned; ideally stored in Secrets Manager (not inline in config)
- Auth types: `basic`, `bearer`, `keyPair` (RSA — new, for Snowflake/Databricks)
- Never changes at request time; independent of which user is running a query

**Credential Type 2 — Query Execution Credentials** (`queryAuth`, new field in `ClusterConfig`)
- The credentials used to execute a specific user's query on the backend
- Configured per-cluster — operators choose which mode their engine supports
- Resolved per-request from `AuthContext` (verified identity) + the configured `queryAuth` type
- Validated at startup: each engine accepts only its supported modes
- Default when omitted: `serviceAccount` (falls back to Type 1 for everything)

`queryAuth` has **four explicit types**: `serviceAccount | passthrough | impersonate | tokenExchange`

**Anonymous requests fail closed for explicit per-user modes.** An unauthenticated (`AuthContext.user == "anonymous"`) request only ever resolves to `serviceAccount` — there is no per-user credential to forward, impersonate, or exchange. If a cluster is explicitly configured with `passthrough`, `impersonate`, or `tokenExchange`, an anonymous request is rejected with an auth error rather than silently executing under the service account.

`passthrough` forwards the client's own credential (Bearer/Basic) to the backend unchanged — the backend authenticates the user's own token, not a QueryFlux-vouched identity. It is **Trino-only in this release**; startup validation rejects it for engines whose adapters don't yet consume `QueryCredentials` (see the compatibility table below).

Before `passthrough` existed as an explicit type, the Trino adapter forwarded all headers stored in `SessionContext.extra` verbatim whenever a cluster had no HTTP-setting `auth` of its own — including the client's `Authorization` header — regardless of `queryAuth`. That implicit behavior is **deprecated but still supported** for `serviceAccount`/omitted `queryAuth` on Trino clusters, for backward compatibility (a startup warning is emitted). New configs should set `queryAuth.type: passthrough` explicitly instead of relying on the implicit fallback.

Health checks always use Type 1 (`auth`) directly, never `queryAuth`. This ensures they work even when a user's token is expired or missing.

```yaml
# config.yaml — per-cluster dual credentials (camelCase to match existing serde config)
clusters:
  trino-prod:
    engine: trino
    endpoint: https://trino.internal:8443
    auth:                           # Type 1 — service credentials
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:                      # Type 2 — query execution mode
      type: impersonate             # service account + X-Trino-User header

  clickhouse-prod:
    engine: clickHouse
    endpoint: http://clickhouse:8123
    auth:
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:
      type: serviceAccount          # only viable option for ClickHouse

  snowflake-prod:                   # future adapter
    engine: snowflake
    endpoint: https://myaccount.snowflakecomputing.com
    auth:
      type: keyPair                 # RSA key-pair, Snowflake standard
      username: QF_SVC
      privateKeyPem: "..."
    queryAuth:
      type: tokenExchange
      tokenEndpoint: https://keycloak.internal/realms/my-realm/protocol/openid-connect/token
      clientId: queryflux-gateway
      clientSecret: "..."

  # Trino→Trino same-IdP: forward the client's own credential explicitly.
  trino-analytics:
    engine: trino
    endpoint: https://trino-analytics.internal:8443
    auth:
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:
      type: passthrough               # forward the client's Authorization unchanged
```

**`queryAuth` engine compatibility** (startup validation rejects unsupported combinations):

| Engine | `serviceAccount` | `passthrough` | `impersonate` | `tokenExchange` |
|--------|:---:|:---:|:---:|:---:|
| Trino | ✅ | ✅ | ✅ `X-Trino-User` (needs Trino file-based ACL) | ✅ |
| ClickHouse | ✅ | ❌ not yet wired | ✅ `EXECUTE AS` (self-hosted 25.11+ only, see below) | ❌ not yet wired |
| StarRocks (MySQL wire) | ✅ | ✅ LDAP `COM_CHANGE_USER` (requires TLS; see below) | ❌ no wire mechanism | ❌ no wire mechanism |
| StarRocks (HTTP, future) | ✅ | — | — | — |
| Snowflake (ADBC) | ✅ | ❌ not yet wired | ❌ | ✅ per-identity connection pool |
| Databricks (ADBC, future) | ✅ | ❌ not yet wired | ❌ | ❌ not yet wired |
| DuckDB | ✅ (no-op) | — | — | — |

"Not yet wired" means the adapter does not yet consume `QueryCredentials` for that mode — startup validation rejects the config rather than silently accepting it and doing nothing. See the backend-identity plan for the phase that adds each one.

**`BackendIdentityResolver` pseudocode:**

At this point the caller has already: selected **`ClusterGroupMember`**, merged **`connection`** hints for the adapter, and resolved **which `ClusterAuth` / profile** supplies Type 1 material.

```
fn resolve(auth_ctx: &AuthContext, cluster: &ClusterConfig, type1: &ClusterAuth) -> QueryCredentials {
    // If no user identity available, always fall back to service account
    if auth_ctx is NoneIdentity {
        return ServiceAccountCreds(type1.clone())
    }

    match cluster.queryAuth.type {
        serviceAccount =>
            ServiceAccountCreds(type1.clone())

        passthrough =>
            // No-op at the resolver: the adapter/dispatch layer resolves the actual
            // forwarded credential from SessionContext.extra["authorization"], or a
            // Bearer built from auth_ctx.raw_token when that header is missing.
            // Fails closed (rejects the query) if neither is available — never
            // silently substitutes ServiceAccountCreds.
            PassthroughCreds

        impersonate =>
            // Use Type 1 credentials on the wire; inject user identity separately.
            // IMPORTANT: suppress the client's Authorization header — do NOT forward it.
            // Only Type 1 (service account) auth reaches the backend.
            // User identity is injected via an engine-specific mechanism AFTER
            // authentication: Trino sets X-Trino-User; ClickHouse wraps the SQL as
            // `EXECUTE AS {user} {sql}`.
            ImpersonateCreds {
                service_auth: type1.clone(),  // resolved profile or cluster.auth
                user: auth_ctx.user.clone(),
            }

        tokenExchange =>
            // Exchange auth_ctx.raw_token at the configured OAuth endpoint.
            // Fails closed (rejects the query) if raw_token is None or the exchange
            // fails — never silently substitutes ServiceAccountCreds, since that would
            // submit the query under the wrong principal.
            // Per-provider contract: see Layer 3 → tokenExchange section.
            exchange_token(auth_ctx.raw_token?, cluster.queryAuth.token_exchange_config)
    }
}
```

**Mixed `queryAuth` within one cluster group:** Allowed — each cluster carries its own config. If a group uses `engineAffinity` or `weighted` strategy with members of different `queryAuth` types, the resolver uses whichever cluster was selected. Operators should ensure all members of a group use the same `queryAuth` type unless they explicitly want per-cluster behaviour; a startup warning is emitted if a group has members with mixed types.

---

## Cluster group membership: array of connection options

**Problem:** `members: [ "cluster-a", "cluster-b" ]` is not enough when the **same logical cluster** (same endpoint) participates in **multiple groups** with different **team defaults** (e.g. Snowflake **role/warehouse**, or which **auth profile** to prefer).

**Model:** `clusterGroups[].members` becomes an **array of objects** — each entry is **one connection option** in the group’s pool (ordering preserved for failover / round-robin / weighted).

Each **`ClusterGroupMember`** (name TBD) contains at minimum:

- **`cluster`** — required; name of an entry in `clusters`.
- **`connection`** — optional, **engine-specific** non-secret hints merged at dispatch after a member is selected (or used to disambiguate defaults). Validated at startup: fields must match the **referenced cluster’s `engine`**; unknown or wrong-engine fields are **rejected** (fail fast).
- **`weight`** — optional; for weighted strategies within the group.
- **`defaultAuthProfile`** — optional; names a profile defined on that cluster (see below). Supplies the **group-scoped default** when this member is the path into the cluster (“team A uses role ANALYST”).

**Backward compatibility:** Config loader may accept **either** a bare string (`"trino-prod"`) or a full object, so existing YAML keeps working during migration.

### Per-engine `connection` shapes (all supported types)

Each engine exposes a **closed set** of connection option types. Implement as a **serde tagged enum** `EngineConnectionOptions` (or nested `Option` structs with validation) so only valid combinations deserialize.

| Engine | Purpose of group-level `connection` | Typical fields (non-secret) | `ClusterAuth` (Type 1) variants |
|--------|-------------------------------------|------------------------------|----------------------------------|
| **Trino** | Session context defaults for this group’s path | `catalog`, `schema`, optional `sessionProperties` map | `basic`, `bearer` |
| **ClickHouse** | Default database / settings | `database`, optional `role` (if using CH RBAC features) | `basic` (maps to `X-ClickHouse-User` / `Key`) |
| **StarRocks (MySQL wire)** | Default catalog/db context | `database` | `basic` (user/password) |
| **StarRocks (HTTP, future)** | JWT-forwarding session hints | TBD aligned with StarRocks HTTP API | `basic`, `bearer` |
| **Snowflake** | Same account URL, different **team slice** | `role`, `warehouse`, `database`, `schema` | `keyPair` (recommended), future password-based if needed |
| **Databricks (future)** | Warehouse / HTTP context | `warehouseId`, `catalog`, `httpPath` (product-specific) | `bearer`, OAuth client creds via `queryAuth` |
| **DuckDB** | Usually none (file path on cluster) | rarely `attach` hints | N/A (embedded) |

**Rule:** Secrets (**passwords, PEMs, client secrets**) stay on **`clusters[].auth`** or **`authProfiles`** via **inline (dev) or `secretRef` (prod)** — not duplicated per group. Group `connection` carries **which role/warehouse/catalog** to use, not private keys.

**Teams / groups pattern:** Create **one cluster group per team** (or workload). Each group lists the same Snowflake `cluster` name once, with different `connection.role` / `warehouse` and/or `defaultAuthProfile`. Combined with **allowGroups** / OpenFGA, “Alice may only hit `group:team-analytics`” implies she only gets **that group’s** Snowflake role default.

---

## Auth profiles (cluster-scoped, named Type 1 variants)

When one cluster needs **multiple service identities or Snowflake logins** (different key-pair users, or same user + different static contexts), define **`authProfiles`** on **`ClusterConfig`**:

```yaml
clusters:
  snowflake-prod:
    engine: snowflake
    endpoint: https://xyz.snowflakecomputing.com
    defaultAuthProfile: svc_readonly
    authProfiles:
      svc_readonly:
        type: keyPair
        username: QF_READONLY
        privateKeySecretRef: { provider: vault, path: secret/data/qf/sf-readonly, field: key }
      svc_etl:
        type: keyPair
        username: QF_ETL
        privateKeySecretRef: { provider: vault, path: secret/data/qf/sf-etl, field: key }
    queryAuth:
      type: serviceAccount   # or tokenExchange — profile picks which Type 1 for serviceAccount path
```

**Resolution order after cluster + group member are known:**

1. **`defaultAuthProfile`** on the **group member** entry (if set) — team/unit-specific service account.
2. Else **`defaultAuthProfile`** on the **cluster** (if set) — cluster-level default.
3. Else legacy single **`auth`** block on the cluster.

The client **never** influences which auth profile is used. Clients influence *routing* (which group to target) via the existing router chain — headers, client tags, regex, protocol — but the auth/authz layer must approve access to that group. Once a group is selected, the auth profile is entirely determined by operator config. This separation means the group IS the privilege boundary: being authorized for `team-a-snowflake` group means you get the `svc_readonly` service account; being authorized for `team-etl` means you get `svc_etl`. No escalation possible from the client side.

Config is invalid (startup error) if a `defaultAuthProfile` name references a profile not defined on that cluster's `authProfiles`.

---

## Default routing (when no router matches)

When the router chain evaluates all configured routers (protocol-based, header, user-group, query-regex, client-tags, python-script) and **none produces a group selection**, QueryFlux applies a two-step fallback rather than blindly using the static `routingFallback` config key:

1. **Authorization-aware first-fit**: enumerate all cluster groups in config order. For each group, check whether `AuthContext` is authorized (OpenFGA check or `allowGroups`/`allowUsers` match). Pick the **first group the user is authorized for**.
2. **Static fallback** (`routingFallback`): if the user is not authorized for any group (or has no identity), fall back to the static `routingFallback` group — same behavior as today, but only reached when step 1 finds nothing.

**Why this order?** Clients route implicitly via router rules when they send headers/tags/regex-matching SQL. When they send nothing, they still belong to some team — the authorization layer already knows which groups they may access. Picking the first authorized group gives users a deterministic default without requiring them to always specify routing hints. The static `routingFallback` remains for unauthenticated or unauthorized requests (e.g. health probers, legacy clients with no identity).

**Startup constraint:** If `auth.required: true`, an unauthenticated request is rejected before routing — `routingFallback` is not reached. If `auth.required: false`, `NoneAuthProvider` still derives a user from `sessionCtx.user()`; the first-fit check runs against that identity.

**Config remains unchanged**: `routingFallback` is still a required top-level string. No new config key needed — the behavior is implicit when the router chain produces no match and an `AuthContext` is available.

```
RouterChain result = None
  → for each group in config order:
       if authz.check(auth_ctx, group) == allowed → use this group
  → if none found → use routingFallback
```

---

## End-to-end dispatch (single query)

1. **Authenticate** → `AuthContext`.
2. **Route** → cluster **group** name (router chain → authorization-aware first-fit → `routingFallback`).
3. **Authorize** → user may use that group (OpenFGA or `allowUsers` / `allowGroups`).
4. **ClusterManager** → pick **one member** of the group (strategy: RR, weighted, failover, engine affinity).
5. **Merge connection context** → load `ClusterConfig` for `member.cluster`, apply `member.connection` (engine-validated) + `member.defaultAuthProfile`.
6. **Resolve profile** → pick `auth` material (single `auth` or named `authProfiles`).
7. **`BackendIdentityResolver`** → `QueryCredentials` from `queryAuth` + `AuthContext` (token exchange, impersonate, service account, implicit header forward).
8. **Adapter** → submit query with merged **wire auth + engine session hints** (role, catalog, etc., per adapter).

Audit logs should record: **`auth_ctx.user`**, **group**, **cluster**, **resolved profile**, **member index or id** (if useful).

---

## Secret storage (operations)

| Approach | Use when |
|----------|----------|
| **Vault / cloud Secrets Manager** (`secretRef` on `auth` / `authProfiles`) | Production default; rotation and audit at the secrets layer. |
| **Envelope encryption in Postgres** (ciphertext in DB, DEK wrapped by **KMS**) | Policy requires all config in DB; avoid a single static app-wide passphrase without rotation. |
| **Plain YAML / plain DB columns** | Dev and test only. |

QueryFlux should resolve `secretRef` at **startup or config reload**, not on every query, unless operators explicitly need dynamic secrets.

---

## Context

QueryFlux is a universal SQL proxy routing queries across heterogeneous backends (Trino, DuckDB, StarRocks, ClickHouse, and future cloud platforms). Today there is **no verified frontend authentication** — on Trino HTTP, client headers may be forwarded to a Trino backend; there is no gateway-level JWT validation or OpenFGA. As it grows to multi-tenant use, it needs:

1. **Frontend auth**: verify who the user is (AuthProvider — pluggable)
2. **Authorization**: decide what they can access (OpenFGA or simple policy)
3. **Backend identity**: propagate the right credentials to each engine per its capabilities

### Multi-engine routing (frontend A → backend D)

The design fits **heterogeneous routing**: any supported frontend (Trino HTTP, Postgres wire, …) can target any supported backend cluster type, as long as routers and SQL translation allow it. **Gateway auth and authz** depend only on `AuthContext` and cluster group — not on whether the backend is Trino or ClickHouse. **Backend identity** is always resolved **per selected cluster** via `queryAuth` + engine capabilities: the same user may hit Trino with forwarded JWT and ClickHouse with a **service account** in the same deployment.

### Operator choices: static backend creds vs forwarding client creds

| Approach | Meaning | When to use |
|----------|---------|-------------|
| **Static Type 1 only** (`queryAuth` omitted or `serviceAccount`) | No per-request resolution for the wire: every query uses `clusters[].auth`. User identity may still exist in `AuthContext` for audit, authz, and metrics. | Default for ClickHouse, StarRocks MySQL wire, DuckDB; safe baseline everywhere. |
| **Implicit header forwarding** (Trino adapter today) | Client `Authorization` / `X-Trino-*` from `SessionContext` are applied after cluster auth — client's `Authorization` **wins** if present. No separate `queryAuth` type; not “free security.” | Same-IdP Trino→Trino, dev, or locked-down networks where routing is narrow. |
| **`impersonate`** | Type 1 only on the wire + `X-Trino-User`; client `Authorization` **must** be suppressed. | Trino with file-based ACL when JWT passthrough is not used. |

**Authorization (`provider: none`) and passthrough are independent.** Turning off OpenFGA/simple lists does **not** make forwarded client creds a substitute for gateway policy: anyone who can reach the gateway may get queries routed per router rules, and the **backend** decides what those creds allow. Do **not** auto-enable “forward everything” based solely on `authorization: none`. Prefer **explicit** per-cluster behavior (`queryAuth` + adapter rules). Emit a **startup warning** when `authorization.provider: none` and implicit `Authorization` forwarding is active on a frontend that routes to **multiple** cluster groups (broad blast radius).

**`auth.required: true` with `NoneAuthProvider`** still means **no cryptographic proof** of identity — only network trust. Document clearly for operators.

---

## Architecture Overview

```
Client (any protocol)
  ↓
Frontend Listener
  ├─ Extract Credentials (protocol-specific)     ← raw material for AuthProvider
  └─ Build SessionContext (unverified, as today)
  ↓
AuthProvider.authenticate(credentials) → AuthContext
  { user, groups, roles, raw_token }
  Pluggable: None | Static | OIDC | LDAP
  ↓
RouterChain → ClusterGroup selection
  (routers can inspect AuthContext.user/groups)
  ↓
OpenFGA / Policy check
  "can user X execute queries on cluster group Y?" → allowed | 403
  ↓
ClusterManager → pick GroupMember (cluster name + connection options + optional defaultAuthProfile)
  ↓
Merge ClusterConfig + member.connection (engine-specific hints) + resolved auth profile
  ↓
BackendIdentityResolver(AuthContext, cluster.queryAuth) → QueryCredentials
  serviceAccount  → cluster.auth (Type 1)
  impersonate     → cluster.auth + user identity header (suppress client Authorization)
  tokenExchange   → exchange raw_token at OAuth endpoint
  (implicit: Trino adapter forwards SessionContext headers unchanged when no suppression needed)
  ↓
Adapter.submit_query(sql, QueryCredentials)       ← Type 2 used here
Adapter.health_check() uses cluster.auth (Type 1) ← always independent
  ↓
Backend Engine
```

---

## Layer 1: Frontend Authentication

### `AuthProvider` trait (new `queryflux-auth` crate)

```rust
trait AuthProvider: Send + Sync {
    async fn authenticate(&self, creds: &Credentials) -> Result<AuthContext>;
}

struct Credentials {
    username: Option<String>,
    password: Option<String>,       // from Basic auth or wire handshake
    bearer_token: Option<String>,   // from Authorization: Bearer
    // Future: extensible fields or a sealed enum for mTLS principal, Kerberos, IAM delegation, etc.
}

struct AuthContext {
    user: String,
    groups: Vec<String>,
    roles: Vec<String>,
    raw_token: Option<String>,      // original JWT, needed for tokenExchange
}
```

**Why gateway auth if clients already send credentials?** Client material (Basic, Bearer, wire username) is **input**. **AuthProvider** answers: is it **valid** (signature, LDAP bind, static password), and what is the **canonical subject** for policy? **Authorization** answers: what may that subject do **at QueryFlux** (which cluster groups)? **Query resolution** answers: what credentials go **on the wire to this engine** (often Type 1 only). Unverified headers (e.g. `X-Trino-User` alone) are trivial to forge from any client that can reach the gateway — so multi-tenant or untrusted networks need **verified** auth, not only forwarding.

**Implementations:**
- `NoneAuthProvider` — derives identity from `sessionCtx.user()` only; no cryptographic verification. `auth.required: true` with this provider does **not** add JWT/signature checks — it only enforces that a username is present unless paired with **network trust** (VPC, mTLS at the load balancer). Make this explicit in operator docs.
- `StaticAuthProvider` — user/password map in config (dev/simple deployments)
- `OidcAuthProvider` — validates JWT signature against JWKS endpoint; extracts groups/roles from claims
- `LdapAuthProvider` — binds with user credentials to verify; extracts group membership from DN

**Credential extraction per protocol (no password verification for wire protocols):**
- `TrinoHttp`: parse `Authorization` header → Basic or Bearer → `Credentials`
- `PostgresWire`: capture `user` from startup message → `Credentials { username, .. }`
- `MySqlWire`: capture `user` from handshake → `Credentials { username, .. }`
- `ArrowFlightSQL`: gRPC metadata bearer token → `Credentials { bearer_token, .. }`

### Auth config block (in `queryflux-core/src/config.rs`):

```yaml
auth:
  provider: none | static | oidc | ldap
  required: true   # with NoneProvider: network-trust only, not cryptographic assurance
  # IdP roles/groups that may cancel any query (not poll another user's results).
  operatorRoles: [queryflux-operator]
  operatorGroups: [platform-ops]
  oidc:
    issuer: https://...
    jwksUri: https://...
    audience: queryflux
    groupsClaim: groups
    rolesClaim: roles
  ldap:
    url: ldap://...
    bindDn: cn=svc,...
    userSearchBase: ou=users,...
  static:
    users:
      alice: { password: "...", groups: [analysts] }
```

### Keycloak as OIDC provider

Keycloak maps directly onto `OidcAuthProvider` — no special code:

```yaml
auth:
  provider: oidc
  oidc:
    issuer: https://keycloak.internal/realms/my-realm
    jwksUri: https://keycloak.internal/realms/my-realm/protocol/openid-connect/certs
    audience: queryflux-client
    groupsClaim: groups           # requires "Group Membership" token mapper on Keycloak client
    rolesClaim: realm_access.roles
```

Keycloak also enables `tokenExchange` for backends: QueryFlux exchanges the user's access token for a backend-scoped token (requires Keycloak `token-exchange` preview feature and "Token Exchange" permission on the target client). This is configured in `clusters[].queryAuth`, not here.

---

## Layer 2: Authorization via OpenFGA

OpenFGA implements Google Zanzibar-style fine-grained authorization, stored and managed outside QueryFlux code.

**Scope:** OpenFGA (and simple allowlists) answer **gateway** questions — e.g. “may this subject run queries against **cluster group** G?” They do **not** replace **engine-native** RBAC (Trino system access control, ClickHouse users, Ranger on StarRocks, etc.). Table/column policies remain on the engines unless the model is extended and kept in sync deliberately.

**Authorization Model:**

```
type user
type group
  relations
    define member: [user]
type cluster_group
  relations
    define reader: [user, group#member]
    define writer: [user, group#member]
    define admin:  [user, group#member]
```

**Check at dispatch time** (after routing, before query execution):

```rust
openfga_client.check(
    user:     format!("user:{}", auth_ctx.user),
    relation: "reader",
    object:   format!("cluster_group:{}", selected_group),
).await?  // → allowed | 403
```

**Tuple lifecycle (who writes authorization data):**
- **Bootstrap**: an init script or migration tool writes tuples from a seed file when QueryFlux first starts against a new OpenFGA store
- **Admin API**: `POST /admin/authz/tuples` (new endpoint) allows operators to grant/revoke access at runtime without redeploy
- **IdP sync (optional)**: a background task reads group memberships from the IdP (LDAP, Keycloak) and syncs group-member tuples into OpenFGA on a configured interval
- **Manual**: operators use the OpenFGA CLI or Playground directly against the OpenFGA store

**Config:**

```yaml
authorization:
  provider: openfga | none
  openfga:
    url: http://openfga:8080
    storeId: "..."
    credentials:
      method: api_key
      apiKey: "..."
```

**Fallback when `provider: none`**: simple `allowGroups`/`allowUsers` lists on each `clusterGroup` (same as trino-gateway's role approach). No external dependency.

```yaml
clusterGroups:
  analytics:
    members:
      - cluster: trino-prod
        connection:
          type: trino
          catalog: hive
          schema: default
      - cluster: clickhouse-prod
        connection:
          type: clickHouse
          database: analytics
    authorization:             # used only when provider: none
      allowGroups: [analysts, admins]
      allowUsers: [svc-etl]

  team-a-snowflake:
    members:
      - cluster: snowflake-prod
        defaultAuthProfile: svc_readonly
        connection:
          type: snowflake
          role: ANALYST_TEAM_A
          warehouse: WH_TEAM_A
    authorization:
      allowGroups: [team-a]

  team-b-snowflake:
    members:
      - cluster: snowflake-prod
        defaultAuthProfile: svc_etl
        connection:
          type: snowflake
          role: ETL_TEAM_B
          warehouse: WH_TEAM_B
    authorization:
      allowGroups: [team-b]
```

**Note:** `connection.type` should align with the cluster’s engine for that member; startup validation rejects mismatches. Bare strings in `members` remain supported for backward compatibility during migration.

---

## Layer 3: Backend Identity (`queryAuth` modes)

All modes configured under `clusters[].queryAuth` (per-cluster, not per-group).

### Deprecated: implicit header forwarding (Trino HTTP → Trino, no config needed)

Before `passthrough` existed as an explicit `queryAuth` type, a Trino cluster with no HTTP-setting `cluster.auth` of its own would forward all headers stored in `SessionContext.extra` to the backend — including `Authorization` — regardless of `queryAuth`. This still works today for backward compatibility (a startup warning is emitted), but new configs should use `queryAuth: passthrough` explicitly instead — see the mode below.

When `queryAuth: impersonate` is set on a Trino cluster, the adapter **must suppress the client's `Authorization` header** and use only Type 1 credentials for authentication. The `X-Trino-User` injection happens after the service account auth is applied. Failing to suppress the client `Authorization` would cause the backend to see conflicting auth credentials.

### Mode: `serviceAccount`

Use Type 1 credentials (`cluster.auth`) for query execution. User identity is known to QueryFlux (logged in audit/metrics) but the backend sees only the service account.

Works for all engines. Default when `queryAuth` is omitted.

### Mode: `passthrough` (Trino, StarRocks)

Forward the client's own credential unchanged. The shape of "the client's own credential" is engine-specific — `enrich_session_for_passthrough` (`queryflux-engine-adapters::wire_auth`) populates both shapes from `AuthContext` before the adapter runs, and each adapter reads whichever one applies:

- **Trino (HTTP-shaped):** the value already captured in `SessionContext.extra["authorization"]` (the client's original header), or — if that's missing but the frontend authenticated via OIDC — a `Bearer {auth_ctx.raw_token}` injected by dispatch. The backend authenticates the user's own token; QueryFlux does not vouch for the identity beyond forwarding it.
- **StarRocks (MySQL-wire-shaped):** `session.extra["passthrough_username"]`/`["passthrough_password"]`, sourced from `AuthContext.raw_password` (only ever populated by `LdapAuthProvider`, which just verified it via a real LDAP bind) and applied via `COM_CHANGE_USER` — see "StarRocks" below for why this shape is necessary and what it costs.

**Fails closed:** if the engine-appropriate credential isn't available, the query is rejected with an auth error rather than silently falling back to `serviceAccount`.

Startup validation rejects `passthrough` for engines whose adapters don't yet consume `QueryCredentials` — see the compatibility table above.

### Mode: `impersonate` (Trino, ClickHouse)

Service account authenticates to the backend; user identity injected via an engine-specific mechanism — a header for Trino, a SQL prefix for ClickHouse.

**Trino:**

1. Remove client's `Authorization` header from the outgoing request
2. Apply `cluster.auth` (Type 1, Basic or Bearer) as the backend authentication
3. Set `X-Trino-User: {auth_ctx.user}` header

**Trino-side requirement** — Trino's built-in access control **prohibits impersonation by default**. File-based access control must be configured:

```json
{ "impersonation": [{ "original_user": "qf_svc", "new_user": ".*", "allow": true }] }
```
```properties
http-server.access-control.config-files=/etc/trino/rules.json
```

This is high operator burden. For OIDC deployments where Trino is configured with JWT auth pointing to the same IdP, prefer explicit `queryAuth: passthrough` over `impersonate` — omitting `queryAuth` relies on the deprecated implicit forwarding path (see above); new configs should never depend on it.

**ClickHouse:**

The adapter wraps the outgoing SQL as `EXECUTE AS {user} {sql}` — the service account (`cluster.auth`) still authenticates the HTTP request; `EXECUTE AS` re-scopes just that one query to run as `user`. Only safe for a single statement per request, which matches how QueryFlux's ClickHouse HTTP path already works.

**ClickHouse-side requirements**, verified against the current upstream docs and changelog (not guessed):
- **ClickHouse 25.11 or newer** — `EXECUTE AS` was added in that release ([`EXECUTE AS` documentation](https://clickhouse.com/docs/sql-reference/statements/execute_as), [25.11 release notes](https://clickhouse.com/blog/clickhouse-release-25-11)).
- **Self-hosted only** — the current ClickHouse docs state `EXECUTE AS` is not supported on ClickHouse Cloud.
- Server setting `access_control_improvements.allow_impersonate_user = 1`.
- `GRANT IMPERSONATE ON {user} TO {service_account}` (or `GRANT IMPERSONATE ON * TO {service_account}` for all users).

QueryFlux cannot verify any of these remotely at startup — an unsupported version, a Cloud endpoint, or a missing grant all surface as an ordinary ClickHouse query error at run time (not a startup failure), since `EXECUTE AS` is just SQL syntax as far as the adapter is concerned.

**Cancelling an impersonated query needs `KILL QUERY` too.** `EXECUTE AS` re-scopes the query to run as `user`, so ClickHouse attributes it to `user`, not the service account. `cancel_query` issues `KILL QUERY WHERE query_id = …` using the service account's own connection (it doesn't re-apply per-query wire auth — there's nothing to re-apply for `impersonate`, since the identity is baked into the SQL text at submit time, not carried as a separate credential). By default, ClickHouse's `KILL QUERY` privilege only lets a user kill their *own* queries; killing a different user's requires the global `KILL QUERY` privilege, granted with `GRANT KILL QUERY ON *.* TO {service_account}`. Without it, cancelling an impersonated query fails server-side (`User {service_account} attempts to kill query created by {user}`) — caught by the existing best-effort cancel error handling (logged, ignored), so the query keeps running until it finishes on its own.

**Only Trino and ClickHouse support `impersonate`.** StarRocks has no equivalent over MySQL wire (no trusted-proxy mechanism). Startup validation rejects `impersonate` for any other engine type.

### Mode: `tokenExchange` (Trino only in this release)

QueryFlux exchanges the user's OIDC JWT (`auth_ctx.raw_token`) for a backend-specific OAuth access token via RFC 8693 token exchange. Requires `OidcAuthProvider` on the frontend (so `raw_token` is populated). **Fails closed:** if `raw_token` is absent or the exchange fails, the query is rejected with an auth error — it never silently falls back to `serviceAccount`, since that would submit the query under the wrong principal.

Startup validation rejects `tokenExchange` for engines whose adapters don't yet consume it — see the compatibility table above.

**Per-provider contract:**

| Provider | Grant type | Subject token type | Audience / scope |
|----------|-----------|-------------------|-----------------|
| Keycloak token exchange | `urn:ietf:params:oauth:grant-type:token-exchange` | `urn:ietf:params:oauth:token-type:access_token` | `audience: &lt;target-client-id&gt;` |

The provider must be registered as an OAuth client in the same IdP as QueryFlux. The exchanged token is used as `Authorization: Bearer &lt;exchanged_token&gt;` in the adapter request. Exchanged tokens are cached (keyed by the full effective grant — user, endpoint, client, audience, scope, and a digest of the raw subject token — until `expires_in` minus a buffer) to avoid an exchange call on every query.

```yaml
clusters:
  trino-1:
    engine: trino
    auth:
      type: bearer
      token: "..."               # QueryFlux's own service credential to Trino
    queryAuth:
      type: tokenExchange
      tokenEndpoint: https://keycloak.internal/realms/my-realm/protocol/openid-connect/token
      clientId: queryflux-gateway
      clientSecret: "..."
      targetAudience: trino-client   # Keycloak target client ID
```

#### Future: Snowflake, Databricks

Non-Trino OAuth-based engines are natural `tokenExchange` targets once their adapters consume `QueryCredentials::Bearer`:

| Provider | Grant type | Subject token type | Audience / scope |
|----------|-----------|-------------------|-----------------|
| Snowflake external OAuth | `urn:ietf:params:oauth:grant-type:token-exchange` | `urn:ietf:params:oauth:token-type:access_token` | `scope: session:role:&lt;ROLE&gt;` |
| Databricks OAuth U2M | `urn:ietf:params:oauth:grant-type:token-exchange` | `urn:ietf:params:oauth:token-type:access_token` | `scope: all-apis` |

The same `queryAuth: tokenExchange` config shape applies — only the `targetAudience`/`scope` and the engine's own OAuth registration differ.

### Persisted wire credentials (poll/cancel reuse)

Poll and cancel requests don't repeat the client's original headers, so the resolved wire credential is persisted on `ExecutingQuery.wire_auth` (`StoredWireAuth::Authorization` for passthrough/tokenExchange's `Authorization` value, `StoredWireAuth::ImpersonateUser` for impersonate's username) at submit time and re-applied on every subsequent poll/cancel — including after a replica restart, since `ExecutingQuery` is durably stored.

**Residual risk:** with a Postgres persistence backend, `ExecutingQuery` (including `wire_auth`) is serialized into the `executing_queries.data` JSONB column **unencrypted**. A `StoredWireAuth::Authorization` value is a live, reusable bearer/basic credential for the duration of the query — anyone with read access to that column (DB backups, replication, a database-level compromise) could reuse it. This applies to `passthrough`, `tokenExchange`, **and** the deprecated implicit-forwarding path under `serviceAccount` (see above) — all three can produce a `StoredWireAuth::Authorization` value, and all three get persisted the same way. `impersonate`'s `StoredWireAuth::ImpersonateUser` is just a username, not a credential, so it isn't part of this risk. This is an accepted, documented gap, not an oversight:

- The row is deleted as soon as the query reaches a terminal state — every exit path (normal completion, client/admin cancel, and the zombie-eviction sweep for a disconnected client) calls `persistence.delete` on it. This bounds, but does not eliminate, the exposure window: a backup or replica snapshot taken while the query is still running would still capture the row.
- `StoredWireAuth`'s `Debug` implementation is hand-written to redact the `Authorization` value, so it never leaks through incidental `{:?}` logging even though it isn't encrypted at rest.
- Admin-facing DTOs and query-history conversions never include `wire_auth` — it exists only in the internal `ExecutingQuery` record.
- Operators running `passthrough`/`tokenExchange` in production should restrict read access to the `executing_queries` table (and its backups/replicas) to the application role — treat it as sensitive as the secrets store, since it transiently holds equivalent material.
- Encryption-at-rest for this column (envelope encryption via a KMS, or a short-lived protected-credential-store reference instead of the raw value) is a real follow-up, deliberately not implemented in this release — it needs an explicit key-management decision this codebase doesn't otherwise make. **This is an accepted release gap, not a completed mitigation** — treat `passthrough`/`tokenExchange` with a Postgres backend as requiring the DB-access restriction above until encryption-at-rest ships; get explicit security sign-off before relying on them in an environment where `executing_queries` backups/replicas aren't already tightly restricted.

---

## Engine-Specific Notes

### ClickHouse
- No JWT/OIDC support; `X-ClickHouse-User` + `X-ClickHouse-Key` are full auth credentials (username + password), not impersonation headers — there is no trusted-proxy header on this engine
- `impersonate` **is** viable via the `EXECUTE AS {user} {sql}` SQL statement — see "Mode: `impersonate`" above for the version/config/grant requirements (self-hosted ClickHouse 25.11+ only)
- `passthrough`/`tokenExchange`: not wired in this release
- ClickHouse Cloud: further restricted to password-only (no LDAP/Kerberos/cert), and does not support `EXECUTE AS` at all — `impersonate` is a self-hosted-only capability here

**Trino HTTP frontend → ClickHouse backend, `serviceAccount`:** Gateway **auth** still produces `AuthContext` (who the analyst is for authz and audit). Gateway **queryAuth** for the ClickHouse cluster resolves to **Type 1 service credentials** only. ClickHouse sees the service user, not the Trino username, unless `queryAuth: impersonate` is configured (see above) or operators add a custom integration (password mirroring, external authenticator).

### StarRocks
- MySQL wire (port 9030, current adapter): `serviceAccount` always works. `passthrough` is wired via LDAP `COM_CHANGE_USER` — see below. No impersonation mechanism (no `EXECUTE AS`-equivalent) exists on this protocol; `impersonate` is not viable for StarRocks.
- **`passthrough` (LDAP `authentication_ldap_simple`) — wired, gated on TLS:**
  - MySQL wire protocol authenticates the *whole connection*, not the individual query — there's no per-statement identity header (Trino) or SQL prefix (ClickHouse). The only way to make StarRocks enforce a specific user's permissions is a connection actually authenticated as that user.
  - Mechanism: `COM_CHANGE_USER` — the same one [ProxySQL](https://proxysql.com/) uses for per-user backend connections — re-authenticates an existing connection instead of opening a new one. `mysql_async` supports it natively via `Conn::change_user()`; no fork needed (see the JWT path below for the contrast).
  - StarRocks LDAP auth negotiates the standard `mysql_clear_password` plugin — confirmed via StarRocks' own docs — which `mysql_async` also supports, gated behind `enable_cleartext_plugin(true)`.
  - **Sends the password with no hashing or encryption at the MySQL protocol layer.** `StarRocksAdapter::new` refuses to start a `passthrough`-configured cluster unless the endpoint URL already requests TLS (`?require_ssl=true`) — checked against the built `mysql_async::Opts`, not assumed.
  - The re-authenticated connection is **not returned to the shared pool** (`mysql_async` evicts it automatically once its options change) — every passthrough query pays a fresh `COM_CHANGE_USER` handshake. Cheaper than a new TCP+TLS connection, not free; a per-identity reuse cache (mirroring the ADBC/Snowflake sub-pool) is the natural next step if this becomes a bottleneck.
  - `AuthContext.raw_password` — new, and populated **only** by `LdapAuthProvider` (the password was just verified by a real LDAP bind). `StaticAuthProvider` deliberately never populates it: its password map is QueryFlux's own local config, not necessarily valid against any backend.
- **StarRocks 3.5+'s JWT MySQL auth (`authentication_jwt` on the user, `authentication_openid-connect_client` plugin on the wire) is verified infeasible for `mysql_native` as built — this is the mechanism that stays undone, not LDAP.** Checked both sides:
  - StarRocks' own docs specify the client must implement the `authentication_openid-connect-client` plugin and pass the token via `--authentication-openid-connect-client-id-token-file=<path>` — Oracle's official `mysql` CLI **9.2+** does this; it is a vendor-specific extension, not standard MySQL wire protocol.
  - `mysql_async` 0.36.1 hardcodes exactly five auth plugins (`mysql_native_password`, `caching_sha2_password`, `mysql_old_password`, `mysql_clear_password`, `ed25519`). Confirmed at the source level: any other plugin name — including StarRocks' JWT plugin — falls into `AuthPlugin::Other`, whose `gen_data()` returns `None` (mysql_common `packets/mod.rs`) and whose continuation-handshake arm returns `DriverError::UnknownAuthPlugin` outright (mysql_async `conn/mod.rs`). The connection attempt fails immediately; there is no configuration path around this, unlike LDAP's `mysql_clear_password`.
  - Making this work means implementing an undocumented (from `mysql_async`'s side) vendor protocol extension — either patching/forking `mysql_async` or hand-rolling this part of the MySQL wire protocol.
- HTTP API (ports 8030/8040, future adapter): StarRocks natively supports JWT and OAuth 2.0 over this interface too; a future HTTP adapter could use implicit header forwarding when StarRocks and QueryFlux share an IdP — the likely path to JWT-based StarRocks passthrough, since the MySQL-wire route is blocked.

### Snowflake (ADBC driver)
- No header-based impersonation on the ADBC wire — `impersonate` is rejected at startup for every ADBC driver
- `tokenExchange` is wired: the resolved OAuth token is set via the driver's own connection options (`adbc.snowflake.sql.auth_type=auth_oauth`, `adbc.snowflake.sql.client_option.auth_token`), each user's token requiring its own `ManagedDatabase` (see the per-identity sub-pool design in the backend-identity plan's Phase 3a) since ADBC bakes connection options in at open time, not per-checkout
- Service account (`serviceAccount` mode, Type 1) should use key-pair auth (RSA JWT), not password — Snowflake's recommended pattern for automated connections
- Private keys must not be stored in config files in production; use `secretRef` to Secrets Manager
- `passthrough` is not wired for ADBC in this release (only `tokenExchange`, only for the `snowflake` driver)

### Trino
- Implicit header forwarding works for same-IdP deployments
- `impersonate` requires Trino file-based ACL — high operator burden, document clearly
- For `impersonate`: suppress client `Authorization`; apply service account auth; inject `X-Trino-User`

---

## Snowflake Key-Pair Auth (`ClusterAuth` extension)

The existing `ClusterAuth` only supports `Basic` and `Bearer`. A `KeyPair` variant is needed for Snowflake (and Databricks):

```rust
pub enum ClusterAuth {
    Basic   { username: String, password: String },
    Bearer  { token: String },
    KeyPair {                              // NEW
        username: String,
        private_key_pem: String,          // PEM string or secretRef
        private_key_passphrase: Option<String>,
    },
}
```

Future: support `secretRef` on any auth type so private keys are fetched from AWS Secrets Manager / Vault at startup, not stored in YAML:

```yaml
auth:
  type: keyPair
  username: QF_SVC
  privateKeySecretRef:
    provider: awsSecretsManager
    secretId: "arn:aws:secretsmanager:us-east-1:123:secret:qf-snowflake-key"
    field: private_key
```

---

## What Changes Where

### New: `crates/queryflux-auth/`
- `AuthProvider` trait, `Credentials` struct, `AuthContext` struct
- `NoneAuthProvider`, `StaticAuthProvider`, `OidcAuthProvider`, `LdapAuthProvider`
- `BackendIdentityResolver` — takes `(AuthContext, QueryAuthConfig, ResolvedProfile)` → `QueryCredentials`
- **`ConnectionContextMerge`** (or inline in dispatch) — merges `ClusterGroupMember.connection` into adapter-facing session hints
- `OpenFgaAuthorizationClient` — wraps OpenFGA HTTP API
- `SimpleAuthorizationPolicy` — fallback allowGroups/allowUsers
- Optional: **`SecretResolver` trait** — resolves `secretRef` to material for `ClusterAuth` / profiles at load time

### `queryflux-core/src/config.rs`
- Add `AuthConfig` (provider + per-provider sub-configs)
- Add `AuthorizationConfig` (openFga | none) to `ProxyConfig`
- Add `QueryAuthConfig` enum (`serviceAccount | passthrough | impersonate | tokenExchange`) to `ClusterConfig`
- Add **`authProfiles`** map + **`defaultAuthProfile`** optional field on `ClusterConfig`; support **`secretRef`** on credential fields (resolve at load/reload)
- Replace `ClusterGroupConfig.members: Vec<String>` with **`Vec<ClusterGroupMember>`**: `{ cluster, connection?: EngineConnectionOptions, weight?, defaultAuthProfile? }`; serde **untagged** or custom deserializer to accept **legacy string OR object**
- Add **`EngineConnectionOptions`** as a **tagged enum** (or per-engine struct union) listing **all supported per-engine connection types**; startup validation: each member’s `connection` matches `clusters[cluster].engine`
- Add `authorization` block (`allowGroups`/`allowUsers` fallback) to `ClusterGroupConfig`
- Extend `ClusterAuth` with `KeyPair` variant
- `QueryAuthConfig` validated at startup against engine type; error on unsupported combination

### `queryflux-core/src/session.rs`
- No change. `SessionContext` stays as-is (unverified protocol metadata).
- `AuthContext` lives in `queryflux-auth`.

### `queryflux-frontend/src/state.rs`
- Add `auth_provider: Arc<dyn AuthProvider>`
- Add `authorization: Arc<dyn AuthorizationChecker>`

### `queryflux-frontend/src/dispatch.rs`
- Accept `AuthContext` in `dispatch_query()` and `execute_to_sink()`
- **First step in `dispatch_query()`**: call `state.authorization.check(auth_ctx, group)` → 403 if denied (before `acquire_cluster`)
- After cluster pick: thread **`ClusterGroupMember`** (or equivalent) so adapters receive **merged** engine session hints (`connection`) + **resolved profile** (`auth` / `authProfiles`)
- Pass `QueryCredentials` (resolved by `BackendIdentityResolver`) to adapter alongside `SessionContext` (both needed until Phase 3b replaces session hints with `EngineConnectionOptions`)

### `queryflux-routing/src/lib.rs` (RouterTrait)
- Update `RouterTrait.route()` signature to accept `Option<&AuthContext>` alongside `&SessionContext` and `&FrontendProtocol`
- `UserGroup` router and any future identity-aware router must use verified `AuthContext.user`, not `session.user()` (which is unverified)
- `NoneAuthProvider` still produces an `AuthContext` derived from `session.user()`, so the interface is consistent regardless of provider

### `queryflux-frontend/src/trino_http/handlers.rs`
- Extract `Authorization` header → `Credentials` → `auth_provider.authenticate()` → `AuthContext` (before calling `route_with_trace()`)
- Pass `&auth_ctx` to routers
- **Default routing** (here, not in dispatch): if `route_with_trace()` returns `used_fallback == true`, iterate `state.group_configs` in config order; call `state.authorization.check(auth_ctx, group)` for each; pick first authorized group; only use static `routingFallback` if none found. `state` needs ordered group config list for this (add to `AppState`).
- Thread `AuthContext` through to dispatch

### `queryflux-frontend/src/postgres_wire/mod.rs`
- Capture `user` from startup message → `Credentials { username, .. }` → `auth_provider.authenticate()`
- Same default routing logic as Trino HTTP handler
- Thread `AuthContext` through

### `queryflux-engine-adapters/src/lib.rs`
- Add `QueryCredentials` enum alongside `SessionContext` — **not replacing it**. Until Phase 3b (EngineConnectionOptions), `SessionContext` still carries session hints (catalog, schema, X-Trino-* headers). Adapters need both: `QueryCredentials` for wire auth, `SessionContext` for session setup.
- Update `submit_query` / `execute_as_arrow` to accept `&QueryCredentials` as an additional parameter

### `queryflux-engine-adapters/src/trino/mod.rs`
- `serviceAccount`: apply `cluster.auth` (Basic/Bearer); session headers forwarded as today
- `impersonate`: apply `cluster.auth`; **remove** client `Authorization` from headers; add `X-Trino-User: {user}`

### `queryflux-engine-adapters/src/clickhouse/mod.rs` *(future — no module exists yet)*
- `serviceAccount` only: `X-ClickHouse-User` + `X-ClickHouse-Key` from `cluster.auth`

---

## Phased Implementation

### Phase 1 — Foundation: AuthContext plumbing + NoneProvider
- Define `AuthContext` / `Credentials` / `AuthProvider` / `QueryCredentials` types
- `NoneAuthProvider`: identity from `sessionCtx.user()`, no verification (current behaviour)
- Thread `AuthContext` and `QueryCredentials` through dispatch and adapter calls
- No behaviour change; all existing deployments unaffected

### Phase 2 — Frontend auth (Trino HTTP first)
- `OidcAuthProvider`: JWT validation via JWKS, groups/roles extraction
- `StaticAuthProvider`: config-driven user/password map
- Extract `Authorization` header in Trino HTTP handlers → `Credentials`

### Phase 3 — Authorization
- Simple `allowGroups`/`allowUsers` policy per cluster group (no external dep)
- OpenFGA client integration as optional provider
- Admin API endpoint for tuple management

### Phase 3b — Structured group members & per-engine connection options
- Migrate `members` to `Vec<ClusterGroupMember>` with backward-compatible deserializer for string entries
- Implement `EngineConnectionOptions` tagged enum covering **all engines** in the compatibility table; reject cross-engine field sets at startup
- Plumb **merged connection context** from selected member into dispatch and adapters (Trino catalog/schema, Snowflake role/warehouse, etc.)

### Phase 3c — Auth profiles + secretRef *(can overlap with Phase 3b)*
- `authProfiles` / `defaultAuthProfile` on `ClusterConfig` and `ClusterGroupMember`
- Profile resolution order: group member default → cluster default → single `auth` (no client influence)
- `secretRef` resolution from Vault / AWS Secrets Manager at config load

### Phase 4 — `impersonate` mode for Trino
- `BackendIdentityResolver` producing `ImpersonateCreds`
- Trino adapter: suppress client `Authorization`, apply service account auth, inject `X-Trino-User`
- Startup validation: reject `impersonate` for non-Trino engines

### Phase 5 — LDAP + wire protocol auth
- `LdapAuthProvider`
- PG/MySQL wire: OIDC bearer as session parameter

### Phase 6 — `tokenExchange` + cloud adapters
- `tokenExchange` resolver with per-provider contract and token caching
- Snowflake adapter (REST API, key-pair auth)
- Databricks adapter (SQL Warehouses REST or Arrow Flight SQL)

---

## Key Files

| File | Change |
|------|--------|
| [queryflux-core/src/config.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/config.rs) | Add AuthConfig, QueryAuthConfig (3 variants), AuthorizationConfig; `ClusterGroupMember`, `EngineConnectionOptions` (per-engine variants); `authProfiles` + `defaultAuthProfile`; `secretRef`; extend ClusterAuth with KeyPair |
| [queryflux-core/src/session.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/session.rs) | No change — AuthContext is in queryflux-auth |
| [queryflux-frontend/src/state.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/state.rs) | Add auth_provider, authorization checker |
| [queryflux-frontend/src/dispatch.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/dispatch.rs) | Thread AuthContext + QueryCredentials; authz check as first step before acquire_cluster |
| [queryflux-frontend/src/trino_http/handlers.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/trino_http/handlers.rs) | Authenticate before routing; pass AuthContext to routers; authorization-aware first-fit when used_fallback==true |
| [queryflux-frontend/src/postgres_wire/mod.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-frontend/src/postgres_wire/mod.rs) | Same as Trino HTTP: authenticate, pass AuthContext to routers, default routing |
| [queryflux-routing/src/lib.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-routing/src/lib.rs) | Add `Option<&AuthContext>` to `RouterTrait.route()` signature; UserGroup router uses verified identity |
| [queryflux-engine-adapters/src/lib.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/lib.rs) | Add QueryCredentials alongside SessionContext in submit_query / execute_as_arrow signatures |
| [queryflux-engine-adapters/src/trino/mod.rs](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/trino/mod.rs) | serviceAccount (current behaviour) + impersonate (suppress + inject) |
| New: `crates/queryflux-auth/` | AuthProvider trait + all implementations + BackendIdentityResolver + OpenFGA client |
