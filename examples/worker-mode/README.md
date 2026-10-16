# Worker mode example

One **control plane** and two **workers**:

- The control plane owns the config in Postgres, serves Studio and the admin API, and runs queries itself.
- The workers hold no config and never connect to Postgres. Each one:
  - pulls its config from the control plane;
  - takes capacity leases from it, so `maxRunningQueries` holds across all three processes;
  - sends it query history.

Background: [Worker mode](../../website/docs/operations/worker-mode.md).

## Start the stack

```bash
docker compose up -d --wait
```

Worker mode needs an image that includes it. To run a local build:

```bash
docker build -f docker/queryflux/Dockerfile --target runtime-server -t queryflux:dev .   # from the repo root
QUERYFLUX_IMAGE=queryflux:dev docker compose up -d --wait
```

| Service | URL |
|--------|-----|
| SQL via the control plane | http://localhost:8080 |
| SQL via worker-a / worker-b | http://localhost:8090 / http://localhost:8091 |
| Worker status | http://localhost:9090/worker/status, http://localhost:9091/worker/status |
| Admin API (control plane) | http://localhost:9000 |
| Studio | http://localhost:3000 |
| Postgres | `localhost:5435` (`queryflux` / `queryflux`) |

## Things to try

**Run a query on a worker.** It shows up in the control plane's query history, with the worker's name. Its SQL is masked because the control plane sets `workerApi.history.redaction: maskLiterals`.

```bash
./trino --server http://localhost:8090 --user demo --execute "SELECT 42"
```

**Change the config once.** Edit the `duckdb-1` cluster or the `analytics` group in Studio. Both workers activate the new revision within about a second. Check `activeRevision` in `/worker/status`.

**Watch the shared limit.** `maxRunningQueries: 2` covers the control plane and both workers together:

- run two slow queries on worker-a;
- a third query on worker-b waits until one of them finishes.

**Stop the control plane.** Run `docker compose stop control-plane`. The workers keep serving the config they already have, and:

- new queries wait for capacity, because of `onUnavailable: failClosed`;
- query history spools in memory until the control plane is back.

Set `onUnavailable: localLimits` on a worker to admit queries on its own limit instead.

## Files

| File | Role |
|------|------|
| `control-plane.yaml` | `role: controlPlane`, Postgres, `workerApi` (shared secret, history redaction), the seed config |
| `worker-a.yaml`, `worker-b.yaml` | `role: worker`, `controlPlane` URL and secret, `coordination`, frontends |
