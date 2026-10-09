# Native CLI JWT ingress and protocol support (development branch)

Configure QueryFlux's existing `auth.provider: oidc` with the Knorket token issuer,
JWKS URL, and audience. Use the token's `sub` as the gateway identity. Knorket's
`role` claim is a string; configure `rolesClaim: role` if it is needed by policy.
The CLI obtains a JWT beforehand; these listeners do not perform browser login.

PostgreSQL and MySQL wire listeners require TLS when authentication is configured.
Add this to each wire frontend's configuration (PEM paths on the QueryFlux host):

```yaml
frontends:
  postgresWire:
    enabled: true
    port: 5432
    tls: { certFile: /etc/queryflux/server.pem, keyFile: /etc/queryflux/server-key.pem }
  mysqlWire:
    enabled: true
    port: 3306
    tls: { certFile: /etc/queryflux/server.pem, keyFile: /etc/queryflux/server-key.pem }
  flightSql:
    enabled: true
    port: 8815
    tls: { certFile: /etc/queryflux/server.pem, keyFile: /etc/queryflux/server-key.pem }
  clickhouseNative:
    enabled: true
    port: 9440
    tls: { certFile: /etc/queryflux/server.pem, keyFile: /etc/queryflux/server-key.pem }
```

This is the `queryflux.frontends` fragment of the normal configuration. Native
ClickHouse uses the existing protocol router's `clickhouseHttp` destination.
Compound router rules can identify the protocol as `clickhouseNative`.

## Client commands

The examples assume an access token in a private file named `access-token` and a
trusted gateway TLS certificate. Use the actual database, account, and host values.

```sh
# PostgreSQL: AuthenticationCleartextPassword carries the JWT inside TLS.
PGPASSWORD="$(cat access-token)" psql \
  'host=gateway.example port=5432 user=subject database=analytics sslmode=verify-full'

# StarRocks clients: MySQL 9.2+ OIDC plugin reads the access token file.
mysql --host=gateway.example --port=3306 --user=subject \
  --ssl-mode=VERIFY_IDENTITY --authentication-openid-connect-client-id-token-file=access-token

# ClickHouse native JWT Hello, over TLS.
clickhouse-client --host gateway.example --port 9440 --secure \
  --jwt "$(cat access-token)" --query 'SELECT 1'

# Snowflake CLI sends AUTHENTICATOR=OAUTH and TOKEN to the v1 login endpoint.
snow sql --host gateway.example --port 443 --account gateway --user subject \
  --authenticator oauth --token-file-path access-token --query 'SELECT 1'
```

Snowflake requires HTTPS termination in front of its HTTP listener. Keep the
listener private behind that TLS ingress. `SNOWFLAKE_JWT` is Snowflake key-pair
authentication, not the Knorket OIDC access token mode.

JWT verification occurs before wire authentication succeeds. PostgreSQL, MySQL,
and ClickHouse revalidate credentials for each dispatched query. Snowflake
revalidates the original JWT for queries, polling, cancellation, heartbeat, and
session renewal. Renewable connections resolve their current Knorket JWT through the bounded lease before these checks. Routing uses the verified subject rather than a supplied username.

For this integration, Queryflux authenticates and authorizes users; databases use
configured backend service-account credentials (`queryAuth: {type: serviceAccount}`). The
user JWT is not needed by the database. Configure issuer/JWKS/audience at Queryflux.
The optional backend modes below are separate capabilities and are not needed for
this deployment. Existing `serviceAccount` and Snowflake `tokenExchange` paths remain available.
ClickHouse HTTP adapters now accept bearer credentials for backend passthrough or
exchange. StarRocks' MySQL adapter can answer an OIDC auth-plugin challenge with
the verified or exchanged token. Those backend modes require a database configured
to accept the corresponding token issuer and audience. The PostgreSQL backend
adapter still uses its configured backend credentials; this change does not add
PostgreSQL 18 backend OAUTHBEARER negotiation.

## Automatic native session renewal

Hyperlake `connect --native` now maintains a bounded session lease. It registers a valid Knorket JWT at `POST /v1/session-leases`, receives a signed one-use connection JWT and an independent renewal secret, then starts the native client using that connection JWT. All four wire frontends verify the backing OIDC grant and atomically bind the ticket for one login. Existing plain Knorket JWT logins retain normal expiry behavior.

Five minutes before the current JWT expires, Hyperlake mints a fresh Knorket JWT and calls `POST /v1/session-leases/{id}` with the renewal secret and fresh token. Queryflux preserves subject, issuer, audience, tenant, cluster, actor and agent context; current roles/attributes may change and are reevaluated on each request. The lease's current verified JWT supplies upstream token exchange/passthrough material. The internal connection reference and renewal secret are never sent upstream.

Leases remain valid until the current JWT expires and have an eight-hour absolute lifetime. Refresh starts at JWT `exp` minus five minutes; transient failures retry every ten seconds until current expiry. There is no one-minute heartbeat or separate three-minute lease timeout. An expired lease cannot be renewed. Hyperlake closes the native process if refresh has not succeeded before token expiry and calls `DELETE /v1/session-leases/{id}` on exit. Gateway checks deny subsequent requests after revocation or expiry; an already executing request is not retroactively unauthorized by this mechanism. Native cancellation limitations still apply.

These routes are added to the Trino HTTP listener. Expose them through **HTTPS**, keep its underlying HTTP port private, and pass `--renewal-url https://GATEWAY` to Hyperlake when it cannot infer an HTTPS Trino endpoint. Enable `queryflux.frontends.trinoHttp` even for psql/MySQL/ClickHouse/Snowflake launchers. The native and HTTP listeners must reach the **same replica** because lease state is in memory. Restart requires reconnecting. Shared-state renewal across arbitrary load-balanced replicas is not implemented.

```sh
hyperlake connect --native --cluster CLUSTER_ID --client psql \
  --renewal-url https://gateway.example
```

## Added protocol functionality

PostgreSQL now handles Parse, Bind, Describe, Execute, Close, Flush, and Sync, with
named/unnamed statements and portals, an error state until Sync, and suspended
portals that resume without executing the backend query again. Parameter replacement
uses the SQL AST. Text values and common binary scalar inputs/results (boolean,
smallint/integer/bigint, float/double, text) are supported. Binary date/time/decimal,
array, UUID, and bytea binding/result encoding remain unsupported. Statement Describe
with unknown parameter types does not infer PostgreSQL parameter OIDs. Backend
metadata must be available through `describe_query` (ADBC or DuckDB currently);
Trino and other adapters return an explicit unsupported error for this operation.
Metadata uses the backend's SQL dialect; cross-dialect Describe translation is not
implemented yet. Portal results are bounded to 64 MiB; simple queries stream.

PostgreSQL COPY TO STDOUT streams default text results from a table or SELECT.
COPY FROM STDIN decodes default text rows (including escapes and NULL), dispatches
batch INSERTs with current gateway authorization, and uses a pinned transaction so
failed uploads roll back. COPY FROM requires a transactional backend (ADBC with
transaction support, DuckDB, or Trino with a suitable connector). CSV, binary,
custom delimiters/options, and extended-protocol COPY remain unsupported. A COPY
row is limited to 8 MiB. A simple Query message accepts one SQL statement; clients
must submit transaction statements separately. COPY FROM is a compatibility path,
not the database's optimized native bulk ingestion API.

BEGIN/COMMIT/ROLLBACK retain one ADBC/DuckDB connection or one Trino coordinator
transaction id. Queries keep using Queryflux's policy and dispatch pipeline and
remain pinned to the original cluster group/cluster. Cache reads and writes are
bypassed inside transactions. Failed or cancelled operations require rollback;
PostgreSQL reports I/T/E in ReadyForQuery. PostgreSQL disconnect and Snowflake
logout roll back. Transactions expire after 30 minutes of inactivity or eight
hours overall; expired transactions require explicit rollback before further
queries. The registry is capped at 1024 active or expired sessions; expired-session markers
remain until rollback/disconnect to prevent a later query silently becoming
autocommit. State is process-local and requires replica affinity. Other adapters,
savepoints, two-phase commit, and isolation-level options are unsupported.

Snowflake wire `asyncExec: true` and SQL API `?async=true` run detached from the
submission request and return query handles. Native connector results are available
at `/queries/{id}/result`; SQL API results at `/api/v2/statements/{handle}`. Polls
validate current authentication and stable issuer/subject/tenant/cluster/agent
ownership. Query status includes terminal success/error, and native session BEGIN,
COMMIT, and ROLLBACK use the same pinned transaction machinery. Finish a transaction
before changing role/warehouse/namespace. Stateless SQL API transaction commands
are rejected explicitly. Async results expire after 15 minutes, have a 128-entry
limit and a 64 MiB aggregate storage cap. Inline results are limited to 16 MiB of
Arrow input (32 MiB encoded JSON); asynchronous queries have a 14-minute gateway deadline.
No partitioned/chunked results, async persistence, request-id deduplication, or
multi-statement SQL API batches are implemented yet.

Cancellation now declares backend capabilities. ClickHouse HTTP, StarRocks, DuckDB,
and Trino support backend cancellation; ADBC and other unsupported sync adapters
return an error rather than reporting a no-op as success. Snowflake cancellation
conservatively rejects groups containing an unsupported adapter. PostgreSQL simple
query CancelRequest uses a random per-connection secret and forwards cancellation
through the existing dispatch guard when supported. PostgreSQL extended-query and
COPY cancellation have not been implemented; ClickHouse native cancellation also
remains unsupported.

## Flight SQL

Flight SQL is included for Arrow/ADBC/JDBC clients and columnar result streaming.
Configure `queryflux.frontends.flightSql` as shown above. Send `Authorization:
Bearer <current Knorket JWT>` as gRPC metadata on every RPC. Flight SQL credential
authentication requires native listener TLS. Both GetFlightInfo and DoGet verify
JWTs; tickets are opaque, one-use, valid for five minutes, and bound to the verified
issuer/subject/tenant/cluster/actor/agent identity. Authorization metadata is not
forwarded upstream. Result streaming has bounded buffering and preserves schemas
for empty result sets. BeginTransaction/EndTransaction RPCs are implemented and
statement queries can carry the resulting transaction id.

The frontend still has a limited Flight SQL RPC surface: statement query fetch and
transaction actions. Prepared-statement RPCs, catalog/schema/table metadata RPCs,
DoPut updates/ingestion, savepoints, and handshake-based login are not implemented.
GetFlightInfo retains the upstream empty-schema response; the actual schema arrives
in DoGet. Test the specific ADBC/JDBC client before advertising general compatibility.
Hyperlake has no Flight SQL native executable launcher; applications need their own
OIDC login/refresh flow and must attach fresh JWTs on subsequent RPCs. Existing
Hyperlake native CLI renewal still refreshes five minutes before JWT expiry.

## Validation and remaining deployment checks

Changes are in the local `feat/cli-oidc-auth` branch, based on upstream main
`13a038fabf7a14008d3104904c113d811bef6839`. This is a development implementation; no deployment has been performed.
All 535 authentication (39), engine-adapter (281), and frontend (215) library
tests pass. `cargo check -p queryflux -p queryflux-frontend --tests`, formatting,
and staged/unstaged whitespace checks pass. Both packaged patches pass reverse
application checks against their respective working trees. PostgreSQL
socket tests exercise suspended portals and binary integer bindings/results. The
installed psql passed local Queryflux-to-DuckDB tests covering rollback, COPY FROM
STDIN, SELECT, and COPY TO STDOUT. A separate native psql test uses verified TLS,
a local JWKS endpoint, and an RS256 JWT: the verified JWT subject is authorized
even with a different startup username, while a forged signature is rejected.
This validates gateway authentication locally; it does not establish production
Knorket or live PostgreSQL database interoperability. A mock Trino coordinator
verifies configured service-account identity and the pinned transaction id.

Live Knorket-issued JWT/TLS runs against the intended PostgreSQL, ClickHouse, Trino,
StarRocks, and Snowflake deployments are still required. The MySQL OIDC plugin,
ClickHouse CLI, Snowflake CLI, and Flight SQL client compatibility matrix remains
unvalidated. ClickHouse native result support is limited to integer, float, boolean,
string/binary, and nullable values; data-block INSERT and native cancellation are
unsupported. Its vendored protocol library negotiates revision 54428. Unsupported
result types fail explicitly. See `vendor/README.queryflux.md`.

These changes address ingress TLS/authentication aspects of upstream issue #277.
Individual user database authentication/passthrough across every engine is outside
this integration's service-account architecture and is not claimed to be solved.
