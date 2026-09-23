# QueryFlux + OPA data access control, identity from Keycloak (OIDC)

The same row-filtering/column-masking demo as
[`examples/with-opa`](../with-opa/README.md), with one change: identity comes
from a real IdP ([Keycloak](https://www.keycloak.org/), via OIDC) instead of a
static user list in `config.yaml`. The policy, tables, and OPA setup are
byte-for-byte identical — only `auth:` differs. Full docs:
**[Access control](../../website/docs/access-control/overview.md)** ·
**[OPA provider](../../website/docs/access-control/opa.md)** ·
**[Authentication](../../website/docs/authentication.md)**.

Compose runs **Keycloak**, **Lakekeeper**, **MinIO**, **Trino**, and **OPA**;
QueryFlux runs **on the host from this branch** (the published
`ghcr.io/lakeops-org/queryflux:latest` image does not include `accessControl`
yet).

QueryFlux resolves table schema from the **Lakekeeper Iceberg REST catalog**.
Without a catalog (`config-no-catalog.yaml`), column masks and `SELECT *`
rewrites cannot resolve columns and queries are **denied** (`onMissingSchema:
deny`).

Host ports: QueryFlux SQL `:8080`, Admin `:9000`, Keycloak `:8180`, Trino
direct `:8081`, Lakekeeper `:8181`, OPA `:8182` (avoids colliding with
Lakekeeper on `8181`), demo UI `:8183`, QueryFlux Postgres `:5434`
(`queryflux` / `queryflux`).

## What the policy does

Unchanged from `with-opa` — same [`policy/access.rego`](policy/access.rego),
same tables. Only the group names in Keycloak had to match what the Rego
already checks (`engineers` / `analysts`):

| User | Password | Group | `customers` | `payroll` |
|------|----------|-------|-------------|-----------|
| `alice` | `alice` | `engineers` | all rows, SSN visible | allowed |
| `bob` | `bob` | `analysts` | `region = 'EU'` and SSN `SHOW_LAST_4` | denied |

OPA is started with `--watch`, so edits to `policy/access.rego` apply on the
next query (`cacheTtlMs: 0` in config).

## Start

From the **repository root** (QueryFlux needs sqlglot for the rewrite path):

```bash
# 1. Keycloak + Lakekeeper + Trino + OPA + demo UI
cd examples/with-opa-oidc
docker compose up -d --wait   # Keycloak takes ~30s to import the realm
docker compose --profile seed run --rm data-seed
# UI: http://127.0.0.1:8183  (Ask OPA works immediately)

# 2. Python deps used by translation / scan-site rewrite
cd ../..
queryflux --install-deps   # or: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 3. QueryFlux from this branch — verifies Keycloak JWTs, Postgres on :5434,
#    catalogProvider at Lakekeeper :8181
cargo run -p queryflux -- --config examples/with-opa-oidc/config-no-catalog.yaml
```

Open **http://127.0.0.1:8183**. **Run query** fetches a real Keycloak token
for the selected identity's password, then executes SQL through QueryFlux
(Trino HTTP) as that user. Dry-run resolves schema from the live catalog. Ask
OPA talks to the engine only — neither needs a token.

In another terminal:

```bash
cd examples/with-opa-oidc
python3 demo.py
```

You should see Alice's three customer rows with full SSNs, Bob's two EU rows
with masked SSNs, Alice's payroll, and Bob denied on payroll — same output as
`with-opa`'s `demo.py`, just fetching a real access token for each user first.

### Without catalog (fail-closed)

```bash
cargo run -p queryflux -- --config examples/with-opa-oidc/config-no-catalog.yaml
```

Bob's masked `SELECT * FROM lakekeeper.demo.customers` is denied because
QueryFlux cannot enumerate columns for the rewrite.

### Manual queries

Get a token, then use it as a Bearer token — no username/password sent to
QueryFlux itself:

```bash
TOKEN=$(curl -s http://localhost:8180/realms/queryflux/protocol/openid-connect/token \
  -d grant_type=password -d client_id=queryflux \
  -d username=alice -d password=alice | jq -r .access_token)

# Alice — all regions, unmasked
curl -s -X POST http://localhost:8080/v1/statement \
  -H "Authorization: Bearer $TOKEN" \
  -d "SELECT name, region, ssn FROM lakekeeper.demo.customers ORDER BY id"
```

```bash
TOKEN=$(curl -s http://localhost:8180/realms/queryflux/protocol/openid-connect/token \
  -d grant_type=password -d client_id=queryflux \
  -d username=bob -d password=bob | jq -r .access_token)

# Bob — should error (payroll)
curl -s -X POST http://localhost:8080/v1/statement \
  -H "Authorization: Bearer $TOKEN" \
  -d "SELECT * FROM lakekeeper.demo.payroll"
```

Dry-run and OPA-direct curls are unchanged from `with-opa` — identity is
passed directly in the request body / Rego input, not derived from a token:

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT name, ssn FROM lakekeeper.demo.customers","clusterGroup":"trino-opa","dialect":"trino","identity":{"user":"bob","groups":["analysts"]}}'

curl -s http://127.0.0.1:8182/v1/data/queryflux/access \
  -H "Content-Type: application/json" \
  -d '{"input":{"identity":{"user":"bob","groups":["analysts"],"roles":[],"attributes":{}},"action":{"operation":"table.select","resources":[{"table":"customers"}]},"context":{"clusterGroup":"trino-opa","engine":"trino","queryId":"demo","sessionParams":{}}}}'
```

## The OIDC config

```yaml
auth:
  provider: oidc
  required: true
  oidc:
    issuer: http://keycloak:8080/realms/queryflux
    jwksUri: http://localhost:8180/realms/queryflux/protocol/openid-connect/certs
    audience: queryflux
    groupsClaim: groups
    rolesClaim: roles
```

Note the two URLs point at **different hosts on purpose**, and getting this
backwards is the most common way to break this example:

- **`issuer`** is never fetched over the network — it's a plain string
  compared against the JWT's `iss` claim (`jsonwebtoken`'s `set_issuer`). It
  must match what Keycloak actually *bakes into tokens*, which
  `docker-compose.yml`'s `KC_HOSTNAME: http://keycloak:8080` pins to the
  docker-network name regardless of which URL a client used to request the
  token — so this stays `keycloak:8080` even though QueryFlux itself never
  talks to that host.
- **`jwksUri`** *is* fetched — QueryFlux (running on the host, not inside the
  compose network) makes a real HTTP GET to it to fetch Keycloak's signing
  keys. It must be reachable from the host, hence `localhost:8180` (the port
  Compose maps Keycloak to on the host).

`groupsClaim`/`rolesClaim` map JWT claims to `AuthContext.groups`/`.roles` —
`policy/access.rego` reads `input.identity.groups`, fed straight from the
verified token. `queryAuth: impersonate` on the Trino cluster is unchanged
from `with-opa`: Trino has no OIDC awareness of its own, so QueryFlux injects
`X-Trino-User` from its own verified identity rather than forwarding the
Keycloak token to Trino.

There's also a commented-out `attributeClaims` in `config.yaml` — OIDC claims
can be copied into `AuthContext.attributes` for ABAC policies (Rego reads
`input.identity.attributes`), which `access.rego` doesn't use in this demo
(group-based only) but is there to try: add a user-attribute protocol mapper
in Keycloak, uncomment the field, and extend the Rego to key off it.

## LDAP instead of OIDC

QueryFlux's LDAP provider (`auth.provider: ldap`, bind + group-membership
lookup) is fully implemented and usable the same way — swap the `auth:` block
for something like:

```yaml
auth:
  provider: ldap
  required: true
  ldap:
    url: ldap://ldap.internal:389
    bindDn: cn=svc,ou=serviceaccounts,dc=example,dc=com
    bindPassword: ${LDAP_BIND_PASSWORD}
    userSearchBase: ou=users,dc=example,dc=com
    userSearchFilter: "(uid={})"
    groupNameAttribute: cn
```

This directory doesn't include a working OpenLDAP container or seed data —
unlike the Keycloak setup above, it isn't a turnkey demo, just the config
shape. To try it for real: add an `osixia/openldap` (or similar) service to
`docker-compose.yml`, seed it with `alice`/`bob` under groups named
`engineers`/`analysts` (matching `policy/access.rego`), and point `ldap.url`
at it. Everything downstream — OPA, the catalog, the rewrite — is identical
once `AuthContext.groups` comes out right.

## Postgres (QueryFlux)

```bash
psql postgresql://queryflux:queryflux@127.0.0.1:5434/queryflux \
  -c "SELECT proxy_query_id, status, cluster_group FROM query_records ORDER BY id DESC LIMIT 5;"
```

Point QueryFlux Studio at Admin `http://localhost:9000` to edit access
control; saves go to `proxy_settings` in this database.

## Keycloak administration

Open `http://localhost:8180` and log in with `admin` / `admin` to manage the
`queryflux` realm: add users, change group memberships, add client scopes,
etc.

## Stop

```bash
docker compose -f examples/with-opa-oidc/docker-compose.yml down
```

Iceberg data persists in the Compose MinIO volume until `docker compose down
-v` (or you remove volumes). QueryFlux history and Studio config persist in
the `queryflux-pg` volume until you remove it. Re-run `data-seed` after a
fresh stack to recreate tables.
