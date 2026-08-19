# QueryFlux + Keycloak OIDC

OIDC authentication with [Keycloak](https://www.keycloak.org/) as the identity provider.
Queries to the Trino HTTP frontend require a valid JWT bearer token.

## What's included

| Service        | URL                          | Credentials         |
|----------------|------------------------------|----------------------|
| QueryFlux SQL  | `http://localhost:8080`      | Bearer token (below) |
| Studio         | `http://localhost:3000`      | `admin` / `admin`    |
| Admin API      | `http://localhost:9000`      | `admin` / `admin`    |
| Keycloak Admin | `http://localhost:8180`      | `admin` / `admin`    |
| Trino direct   | `http://localhost:8081`      | (no auth)            |

### Pre-configured test users

| User    | Password | Groups           | Roles              |
|---------|----------|------------------|---------------------|
| `alice` | `alice`  | `data-team`      | `engineer`, `admin` |
| `bob`   | `bob`    | `analytics-team` | `analyst`           |

Both groups are allowed on the `trino` cluster group via `allowGroups`.

## Start

```bash
docker compose up -d --wait
```

Keycloak takes ~30s to start and import the realm. QueryFlux waits for it before starting.

## Get a token and query

```bash
# Get a token for alice
TOKEN=$(curl -s http://localhost:8180/realms/queryflux/protocol/openid-connect/token \
  -d grant_type=password \
  -d client_id=queryflux \
  -d username=alice \
  -d password=alice | jq -r .access_token)

# Query via QueryFlux (authenticated)
curl -X POST http://localhost:8080/v1/statement \
  -H "Authorization: Bearer $TOKEN" \
  -d "SELECT 1 AS hello"
```

### Query as bob

```bash
TOKEN=$(curl -s http://localhost:8180/realms/queryflux/protocol/openid-connect/token \
  -d grant_type=password \
  -d client_id=queryflux \
  -d username=bob \
  -d password=bob | jq -r .access_token)

curl -X POST http://localhost:8080/v1/statement \
  -H "Authorization: Bearer $TOKEN" \
  -d "SELECT current_user"
```

### Unauthenticated request (should fail)

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: anonymous" \
  -d "SELECT 1"
# Returns a Trino error response with "OIDC authentication required"
```

## Inspect the JWT

Paste the token at [jwt.io](https://jwt.io) or decode it locally:

```bash
python3 -c 'import base64,json,sys; p=sys.stdin.read().strip().split(".")[1]; print(json.dumps(json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4))), indent=2))' <<<"$TOKEN"
```

(JWT payloads are base64url-encoded with padding stripped — plain `base64 -d` can reject a
valid token that contains `-`/`_` characters or needs padding restored.)

You'll see claims like:

```json
{
  "sub": "...",
  "preferred_username": "alice",
  "groups": ["data-team"],
  "roles": ["engineer", "admin"],
  "aud": "queryflux",
  "iss": "http://keycloak:8080/realms/queryflux"
}
```

QueryFlux extracts `sub` as the user identity, `groups` for authorization, and `roles` for
identity-aware routing (when configured).

## Configuration

The OIDC config in `config.yaml`:

```yaml
auth:
  provider: oidc
  required: true
  oidc:
    issuer: http://keycloak:8080/realms/queryflux
    jwksUri: http://keycloak:8080/realms/queryflux/protocol/openid-connect/certs
    audience: queryflux
    groupsClaim: groups
    rolesClaim: roles
```

The `allowGroups` on the cluster group restricts which Keycloak groups can run queries:

```yaml
clusterGroups:
  trino:
    authorization:
      allowGroups: [data-team, analytics-team]
```

## Backend identity modes

This example's `config.yaml` uses `queryAuth: passthrough` — QueryFlux forwards the
client's own Keycloak Bearer token to Trino unchanged. Trino has three other `queryAuth`
modes available (see
[auth-authz-design.md](../../website/docs/architecture/auth-authz-design.md) for the
full picture):

> **Production hardening:** the Trino container in this stack has no authentication
> configured at all (see the "Trino direct | (no auth)" row above) — it accepts the
> forwarded token, and any other request, without validating it. The security boundary
> in this demo is entirely QueryFlux's own OIDC verification; Trino itself does not check
> the token's signature, issuer, or audience. `passthrough` and `tokenExchange` only
> become a real backend-level security boundary once Trino is configured with its own
> JWT or OAuth2 authenticator validating against the same Keycloak realm — see
> [Trino's authentication docs](https://trino.io/docs/current/security/authentication-types.html).
> Without that, a network path that reaches Trino directly (bypassing QueryFlux, as the
> `8081` port here demonstrates) has no authentication at all.

| Mode | File | Backend sees |
|------|------|--------------|
| `serviceAccount` | *(omit `queryAuth`, or set explicitly)* | QueryFlux's own service account only — user known to QueryFlux for audit, not proven to Trino |
| `passthrough` | `config.yaml` (default here) | The client's own Bearer token, forwarded as-is |
| `impersonate` | `config-impersonate.yaml` | QueryFlux's service account authenticates; the real user is injected via `X-Trino-User` |
| `tokenExchange` | `config-token-exchange.yaml` | A Trino-scoped token exchanged (RFC 8693) from the client's Keycloak JWT |

`config-impersonate.yaml` and `config-token-exchange.yaml` are **reference configs**,
not turnkey demos — each needs additional Trino/Keycloak setup this minimal
docker-compose stack doesn't include (a Trino password authenticator + impersonation
ACL for the former, Keycloak's token-exchange feature + a confidential client for the
latter). Each file documents exactly what's missing in a trailing comment block.

To try one, swap it in and restart just the `queryflux` service:

```bash
cp config-impersonate.yaml config.yaml   # or config-token-exchange.yaml
docker compose up -d --force-recreate queryflux
```

## Keycloak administration

Open `http://localhost:8180` and log in with `admin` / `admin` to manage the `queryflux` realm:
add users, change group memberships, add client scopes, etc.

## Teardown

```bash
docker compose down -v
```
