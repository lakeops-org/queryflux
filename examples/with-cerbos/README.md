# QueryFlux + Cerbos data access control

A local, runnable demo of table allow/deny, row filters, and column masking
with **[Cerbos](https://cerbos.dev)** as the policy engine. Same scenario as
[`examples/with-opa`](../with-opa/README.md), same Lakekeeper/Trino stack —
only the policy engine differs. Full docs:
**[Access control](../../website/docs/access-control/overview.md)**.

## The scenario

Two Iceberg tables, `customers` and `payroll`, two users:

| User | Password | Role | `customers` | `payroll` |
|------|----------|------|-------------|-----------|
| `alice` | `alice` | `engineer` | every row, SSN in full | allowed |
| `bob` | `bob` | `analyst` | only `region = 'EU'` rows, SSN as `****1234` | denied |

The policy that produces this lives in one file:
[`policy/table.yaml`](policy/table.yaml). Everything below is one way or
another of exercising that same file.

## Architecture

```
you ── SQL ──▶ QueryFlux (:8080) ── SQL ──▶ Trino (:8081) ── Iceberg ──▶ Lakekeeper (:8181) + MinIO
                    │
                    │  "can bob select from customers, and with what
                    │   row filter / column mask?"
                    ▼
                Cerbos PDP (:8184)
                    │
                    ▼
              policy/table.yaml
```

QueryFlux asks Cerbos **before** running the query, rewrites the SQL with
whatever row filter / column mask Cerbos returned, and only then sends it to
Trino. QueryFlux itself runs **on the host, from this branch** — the
published `ghcr.io/lakeops-org/queryflux:latest` image doesn't yet include
the Cerbos provider. Everything else (Lakekeeper, MinIO, Trino, Cerbos, and
QueryFlux's own Postgres) runs in Docker Compose.

## Run it

Three steps, each in the order shown.

**1. Start the backing services and load the demo tables** (from this
directory):

```bash
docker compose up -d --wait
docker compose --profile seed run --rm data-seed
```

**2. Install the Python deps QueryFlux's SQL rewriter needs** (from the
repository root):

```bash
cd ../..
queryflux --install-deps   # or: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

**3. Start QueryFlux against this example's config:**

```bash
cargo run -p queryflux -- --config examples/with-cerbos/config.yaml
```

Leave that running, and in another terminal:

```bash
cd examples/with-cerbos
python3 demo.py
```

### What you should see

- Alice's three `customers` rows, SSNs unmasked.
- Bob's two `customers` rows (`region = 'EU'` only), SSNs as `****3333` /
  `****9999`.
- Alice's `payroll` rows; Bob denied (`HTTP 403`) on `payroll`.
- A dry-run showing the **exact rewritten SQL** Cerbos's decision produced.
- Two raw `CheckResources` calls to Cerbos, showing the JSON Cerbos actually
  returns — including the combined row-filter + column-mask output.

If any of that doesn't match, see [Troubleshooting](#troubleshooting).

Useful flags: `python3 demo.py --skip-seed` (tables already loaded),
`python3 demo.py --dry-run-only` (skip the live queries).

## Fail-closed without a catalog

QueryFlux resolves table schema from Lakekeeper to build column masks and
`SELECT *` rewrites. Run the same stack with no `catalogProvider` configured
at all:

```bash
cargo run -p queryflux -- --config examples/with-cerbos/config-no-catalog.yaml
```

With no catalog, QueryFlux can't enumerate `customers`'s columns, so
`onMissingSchema: deny` denies **any** query on it up front — before Cerbos
is even asked, and regardless of who's asking. This check is generic (it's
the same for the OPA example, and for any engine); it isn't a Cerbos
peculiarity.

## Manual requests

Trino HTTP uses Basic auth for the demo's static users. `curl` needs to
follow `nextUri` itself, or just use `demo.py`.

```bash
# Alice — every region, SSN unmasked
curl -s -u alice:alice -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: alice" \
  -d "SELECT name, region, ssn FROM lakekeeper.demo.customers ORDER BY id"

# Bob — denied (payroll)
curl -s -u bob:bob -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: bob" \
  -d "SELECT * FROM lakekeeper.demo.payroll"
```

Admin dry-run (Basic `admin` / `admin`; resolves schema from Lakekeeper;
`identity.roles` is required — see [below](#a-cerbos-constraint-worth-knowing)):

```bash
curl -s -u admin:admin -X POST http://localhost:9000/admin/access-control/dry-run \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT name, ssn FROM lakekeeper.demo.customers","clusterGroup":"trino-cerbos","dialect":"trino","identity":{"user":"bob","groups":["analysts"],"roles":["analyst"]}}'
```

Cerbos directly — the exact `CheckResources` shape `CerbosProvider` sends:

```bash
curl -s -X POST http://127.0.0.1:8184/api/check/resources \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "manual-test",
    "principal": {"id": "bob", "roles": ["analyst"], "attr": {}},
    "resources": [{"resource": {"id": "customers", "kind": "table", "attr": {"table": "customers"}}, "actions": ["table.select"]}]
  }'
```

## Reference: how Cerbos carries row filters and masks

Cerbos's `CheckResources` API has **no built-in concept of a row filter or
column mask** — it only returns `EFFECT_ALLOW` / `EFFECT_DENY` per action,
plus a generic `outputs` array collected from whichever policy rules fired.
QueryFlux's `CerbosProvider` defines its own convention on top of that
generic channel — this is *not* a Cerbos standard, it's how QueryFlux
specifically interprets a rule's output (see
[`providers/cerbos/wire.rs`](../../crates/queryflux-access-control/src/providers/cerbos/wire.rs)
for the authoritative version). A rule contributes a filter or mask by
returning a value shaped like:

```json
{"kind": "row_filter", "expression": "region = 'EU'"}
{"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}
```

— or a **list** of either from one rule (see `rule-002` in
[`policy/table.yaml`](policy/table.yaml), which returns both from a single
CEL expression). Everything else about Cerbos — other resource kinds,
derived roles, `_schemas` — works exactly as it would in any other Cerbos
deployment; this convention only governs how one rule's `output` is shaped.

### A Cerbos constraint worth knowing

Cerbos hard-requires `principal.roles` to be **non-empty** — an empty array
is rejected as an HTTP 400 validation error, not evaluated as "no rule
matched." `CerbosProvider` handles this itself: an identity with no roles is
denied locally, without ever calling Cerbos, so it degrades to a clear,
explained deny instead of a confusing provider error. If you ever see the
reason *"cerbos requires at least one principal role"*, it means your auth
layer isn't populating `roles` for that user — add `roles:` under
`auth.staticUsers.<user>` (or map your real IdP's role claim to it).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `demo.py` hangs on "not ready" | A container isn't healthy yet — `docker compose ps` and `docker compose logs <service>`. |
| Every query denied, including Alice's | `onMissingSchema: deny` firing with no catalog reachable — check `catalogProvider` in `config.yaml` points at `http://127.0.0.1:8181/catalog` and Lakekeeper is healthy. |
| Dry-run says *"cerbos requires at least one principal role"* | Your dry-run/identity payload has no `roles` — see [above](#a-cerbos-constraint-worth-knowing). |
| `seed failed` in `demo.py` | Tables don't exist yet — run `docker compose --profile seed run --rm data-seed` first. |

## Inspecting state

```bash
# QueryFlux's own history/config Postgres
psql postgresql://queryflux:queryflux@127.0.0.1:5434/queryflux \
  -c "SELECT proxy_query_id, status, cluster_group FROM query_records ORDER BY id DESC LIMIT 5;"
```

QueryFlux Studio, pointed at Admin `http://localhost:9000`, can edit access
control live; saves go to `proxy_settings` in that same database.

## Stop

```bash
docker compose down          # keep Lakekeeper metadata, MinIO objects, and QueryFlux history
docker compose down -v       # also wipe them
```

Trino itself has no persistent storage — its dynamically-registered catalog
does not survive `down` either way, so re-run
`docker compose --profile seed run --rm data-seed` after **any** restart
(`down`/`up` or `down -v`/`up`) to re-register the catalog and, on a
volume-wiped stack, recreate the demo tables.

## Ports

| Service | Port |
|---|---|
| QueryFlux SQL (Trino wire) | `8080` |
| QueryFlux Admin | `9000` |
| Trino (direct) | `8081` |
| Lakekeeper | `8181` |
| Cerbos HTTP | `8184` |
| Cerbos gRPC (unused here) | `8185` |
| QueryFlux Postgres | `5434` |

(Cerbos is on `8184`, not its default `3592`, only to avoid colliding with
Lakekeeper on `8181` when both are mapped to the host.)

There's no bundled UI here, unlike `with-opa`'s `ui/` — `demo.py` and the
`curl` commands above cover the same ground. If you want the UI, copy
`with-opa/ui/` and swap its "Ask OPA" panel for a `CheckResources` call (see
`ask_cerbos` in [`demo.py`](demo.py) for the exact request shape).
