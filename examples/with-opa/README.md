# QueryFlux + OPA data access control

Local walkthrough of table allow/deny, row filters, and column masks from
[PR #234](https://github.com/lakeops-org/queryflux/pull/234). Full docs:
**[Access control](../../website/docs/access-control/overview.md)** ·
**[OPA provider](../../website/docs/access-control/opa.md)**.

Compose runs **Lakekeeper**, **MinIO**, **Trino**, and **OPA**; QueryFlux runs
**on the host from this branch** (the published `ghcr.io/lakeops-org/queryflux:latest`
image does not include `accessControl` yet).

QueryFlux resolves table schema from the **Lakekeeper Iceberg REST catalog** —
there is no static schema in the demo UI. Without a catalog (`config-no-catalog.yaml`),
column masks and `SELECT *` rewrites cannot resolve columns and queries are
**denied** (`onMissingSchema: deny`).

Host ports: QueryFlux SQL `:8080`, Admin `:9000`, Trino direct `:8081`,
Lakekeeper `:8181`, OPA `:8182` (8182 avoids colliding with Lakekeeper on 8181),
demo UI `:8183`, QueryFlux Postgres `:5434` (`queryflux` / `queryflux`).

QueryFlux uses **Postgres persistence** so query history and Studio config saves
(access control, guardrails, catalog) survive restarts. Migrations run automatically
on startup (`autoMigrate` default). Lakekeeper still uses its own Postgres
(`lakekeeper-db`); OPA loads Rego from `./policy`.

## What the policy does

| User | Password | Groups | `customers` | `payroll` |
|------|----------|--------|-------------|-----------|
| `alice` | `alice` | `engineers` | all rows, SSN visible | allowed |
| `bob` | `bob` | `analysts` | `region = 'EU'` and SSN `SHOW_LAST_4` | denied |

Tables live in Iceberg as `lakekeeper.demo.customers` and `lakekeeper.demo.payroll`.
Rego: [`policy/access.rego`](policy/access.rego). OPA is started with `--watch`,
so edits to that file apply on the next query (`cacheTtlMs: 0` in config).

## Start

From the **repository root** (QueryFlux needs sqlglot for the rewrite path):

```bash
# 1. Lakekeeper + Trino + OPA + demo UI
cd examples/with-opa
docker compose up -d --wait
docker compose --profile seed run --rm data-seed
# UI: http://127.0.0.1:8183  (Ask OPA works immediately)

# 2. Python deps used by translation / scan-site rewrite
cd ../..
queryflux --install-deps   # or: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 3. QueryFlux from this branch — Postgres on :5434, catalogProvider at Lakekeeper :8181
#    (trino-1 uses queryAuth: impersonate — alice/bob auth is gateway-only)
cargo run -p queryflux -- --config examples/with-opa/config.yaml
```

Open **http://127.0.0.1:8183**. **Run query** executes SQL as alice or bob through
QueryFlux (Trino HTTP). **Seed data** reloads row data from [`seed.sql`](seed.sql)
(tables must already exist from `data-seed`). Dry-run resolves schema from the live
catalog. Ask OPA talks to the engine only.

In another terminal:

```bash
cd examples/with-opa
python3 demo.py
```

You should see Alice's three customer rows with full SSNs, Bob's two EU rows
with masked SSNs, Alice's payroll, and Bob denied on payroll. `demo.py` also
hits `POST /admin/access-control/dry-run` (no static schema payload).

### Without catalog (fail-closed)

Same Compose stack, but QueryFlux has no `catalogProvider`:

```bash
cargo run -p queryflux -- --config examples/with-opa/config-no-catalog.yaml
```

Bob's masked `SELECT * FROM lakekeeper.demo.customers` is denied because QueryFlux
cannot enumerate columns for the rewrite.

### Manual queries

Trino HTTP Basic auth (static users). Follow `nextUri` until it disappears, or
use `demo.py`.

```bash
# Alice — all regions, unmasked
curl -s -u alice:alice -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: alice" \
  -d "SELECT name, region, ssn FROM lakekeeper.demo.customers ORDER BY id"

# Bob — should error (payroll)
curl -s -u bob:bob -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: bob" \
  -d "SELECT * FROM lakekeeper.demo.payroll"
```

Dry-run (Admin Basic `admin` / `admin`; schema comes from Lakekeeper):

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT name, ssn FROM lakekeeper.demo.customers","clusterGroup":"trino-opa","dialect":"trino","identity":{"user":"bob","groups":["analysts"]}}'
```

OPA itself:

```bash
curl -s http://127.0.0.1:8182/v1/data/queryflux/access \
  -H "Content-Type: application/json" \
  -d '{"input":{"identity":{"user":"bob","groups":["analysts"],"roles":[],"attributes":{}},"action":{"operation":"table.select","resources":[{"table":"customers"}]},"context":{"clusterGroup":"trino-opa","engine":"trino","queryId":"demo","sessionParams":{}}}}'
```

## Postgres (QueryFlux)

```bash
psql postgresql://queryflux:queryflux@127.0.0.1:5434/queryflux \
  -c "SELECT proxy_query_id, status, cluster_group FROM query_records ORDER BY id DESC LIMIT 5;"
```

Point QueryFlux Studio at Admin `http://localhost:9000` to edit access control;
saves go to `proxy_settings` in this database.

## Stop

```bash
docker compose -f examples/with-opa/docker-compose.yml down
```

Iceberg data persists in the Compose MinIO volume until `docker compose down -v`
(or you remove volumes). QueryFlux history and Studio config persist in the
`queryflux-pg` volume until you remove it. Re-run `data-seed` after a fresh stack
to recreate tables.
