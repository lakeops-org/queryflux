---
sidebar_position: 3
sidebar_label: Authentication & identity
title: Authentication, Authorization & Backend Identity
description: Configure who can reach QueryFlux, what they can do, and which identity your queries run as on Trino, ClickHouse, StarRocks, and Snowflake.
image: img/queryflux-hero-banner.png
---
# Authentication, authorization & backend identity

QueryFlux separates three questions, each configured independently:

| Question | Config key | Answers |
| --- | --- | --- |
| **Who is this client?** | `auth` | none, static users, OIDC, LDAP |
| **What are they allowed to do?** | `authorization` | allow-all, per-group allow-lists, OpenFGA |
| **Which identity reaches the backend engine?** | `clusters[].queryAuth` | the service account, the client's own credential, an impersonated user, or an exchanged token |

The first two gate access to QueryFlux itself. The third — **backend identity** — decides what Trino, ClickHouse, StarRocks, or Snowflake see as the query's principal, which matters for the backend's own audit log, row-level security, and access control.

---

## Frontend authentication (`auth`)

```yaml
auth:
  provider: oidc          # none | static | oidc | ldap
  required: true
  oidc:
    issuer: https://keycloak.internal/realms/my-realm
    jwksUri: https://keycloak.internal/realms/my-realm/protocol/openid-connect/certs
    audience: queryflux
    groupsClaim: groups
    rolesClaim: roles
```

| Provider | Use when |
| --- | --- |
| `none` | Local development, or the network path to QueryFlux is already trusted (e.g. a private VPC with its own perimeter) |
| `static` | A small, fixed set of service users — bcrypt-hashed passwords in config |
| `oidc` | You already run an IdP (Keycloak, Okta, Auth0, Entra ID) and want SSO |
| `ldap` | Active Directory / OpenLDAP-backed organizations |

`auth.required: true` rejects unauthenticated requests on every enabled frontend. `AuthContext` (the verified identity — user, groups, roles, and the raw OIDC token when applicable) is what the rest of this page builds on.

---

## Authorization (`authorization`)

```yaml
authorization:
  provider: none   # none | openfga

clusterGroups:
  analytics:
    authorization:
      allowGroups: [data-team, analysts]
      allowUsers: [oncall-bot]
```

`provider: none` with no `allowGroups`/`allowUsers` on any group is **allow-all** — anyone who authenticates to QueryFlux can route to anything. Add per-group allow-lists to restrict that without standing up a full policy engine, or set `provider: openfga` for Zanzibar-style fine-grained authorization.

:::warning
`authorization` gates **routing** — which cluster group a client can reach. It does not decide which identity a query runs under on the backend; that's `queryAuth`, below. A client authorized to route to a `passthrough` cluster still only sees what its own backend credential is allowed to see.
:::

---

## Backend identity (`queryAuth`)

This is the piece that decides what the **engine itself** sees as the query's user — independent of how the client authenticated to QueryFlux.

```yaml
clusters:
  trino-1:
    engine: trino
    endpoint: https://trino.internal:8443
    auth:                      # Type 1 — QueryFlux's own service credential
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:                 # Type 2 — which identity the query runs as
      type: impersonate
```

| Mode | The backend sees | Engines |
| --- | --- | --- |
| `serviceAccount` *(default)* | QueryFlux's own service credential only — the user is known to QueryFlux for audit/routing, not proven to the backend | All |
| `passthrough` | The client's own credential, forwarded unchanged | Trino, StarRocks (MySQL wire, LDAP-backed) |
| `impersonate` | The service account authenticates; the real user is injected via an engine-specific mechanism (`X-Trino-User`, ClickHouse `EXECUTE AS`) | Trino, ClickHouse |
| `tokenExchange` | A backend-scoped OAuth token, exchanged (RFC 8693) from the client's own token | Trino, Snowflake (ADBC) |

### Which mode should I use?

- **Same IdP on both sides, engine trusts bearer tokens directly** (Trino with JWT auth against the same Keycloak realm) → `passthrough`. Simplest — the engine validates the token itself, QueryFlux just forwards it.
- **Engine needs a fixed service principal but you still want per-user attribution in its own audit log** (Trino/ClickHouse ACLs, `system.query_log`) → `impersonate`.
- **Engine speaks OAuth but not your IdP's token format directly** (Snowflake external OAuth, a Trino cluster in a different trust domain) → `tokenExchange`.
- **Engine has no per-user wire mechanism, or you haven't set any of this up yet** → `serviceAccount`. This is the default and always works; it's the starting point, not a fallback to be embarrassed about.

Every mode other than `serviceAccount` **fails closed**: if the client has no forwardable credential, the exchange fails, or the caller is unauthenticated, the query is rejected — QueryFlux never silently downgrades to the service account and submits under the wrong principal.

### Engine-specific requirements

Backend identity needs a small amount of setup on the engine side — QueryFlux can't configure your engine's own access control for you:

- **Trino `impersonate`** — requires file-based access control granting the service account impersonation rights (`http-server.access-control.config-files`). Trino denies impersonation by default.
- **ClickHouse `impersonate`** — requires ClickHouse **25.11+, self-hosted** (not supported on ClickHouse Cloud), `access_control_improvements.allow_impersonate_user = 1`, and `GRANT IMPERSONATE ON {user} TO {service_account}`. Cancelling an impersonated query additionally needs `GRANT KILL QUERY ON *.*` on the service account.
- **StarRocks `passthrough`** — authenticates each query on a dedicated connection as the target user via `authentication_ldap_simple`. Requires the cluster endpoint to use TLS (`?require_ssl=true`) and the passthrough user to hold `OPERATE` for `cancel_query` to work.
- **Snowflake `tokenExchange`** — the exchanged token is sent as `adbc.snowflake.sql.client_option.auth_token` with `auth_type=auth_oauth`; register QueryFlux as an OAuth client in the same IdP as the target Snowflake OAuth integration.

Full per-engine wiring, the resolver's fail-closed contract, and the `queryAuth` × engine compatibility matrix: **[Auth & authorization design](/docs/architecture/auth-authz-design)**.

---

## Try it

**[`examples/with-keycloak-oidc`](https://github.com/lakeops-org/queryflux/tree/main/examples/with-keycloak-oidc)** is a runnable Docker Compose stack: Keycloak as the IdP, Trino as the backend, and three ready-to-swap configs — `config.yaml` (`passthrough`), `config-impersonate.yaml`, `config-token-exchange.yaml`.

```bash
cd examples/with-keycloak-oidc
docker compose up -d --wait

TOKEN=$(curl -s http://localhost:8180/realms/queryflux/protocol/openid-connect/token \
  -d grant_type=password -d client_id=queryflux \
  -d username=alice -d password=alice | jq -r .access_token)

curl -X POST http://localhost:8080/v1/statement \
  -H "Authorization: Bearer $TOKEN" -d "SELECT current_user"
```

See the example's own README for the full walkthrough, including how to swap between the three `queryAuth` modes.
