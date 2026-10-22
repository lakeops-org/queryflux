# Hub example

A **control plane** standing in for a remote one, one **hub**, and two **workers**:

- The workers run on a `site` network with **no route to the control plane**. They talk only to the hub, exactly as they would to a control plane.
- The hub is on both networks. It is the only process that connects upstream. It relays:
  - config down;
  - fleet status, query history (spooled on disk) and capacity calls up.

Background: [Hub](../../website/docs/operations/hub.md), [Worker mode](../../website/docs/operations/worker-mode.md).

## Start the stack

```bash
docker compose up -d --wait
```

The hub needs an image that includes it. To run a local build:

```bash
# from the repo root
docker build -f docker/queryflux/Dockerfile --target runtime-server -t queryflux:dev .
cd examples/hub && QUERYFLUX_IMAGE=queryflux:dev docker compose up -d --wait
```

| Service | URL |
|--------|-----|
| SQL via worker-a / worker-b | http://localhost:8090 / http://localhost:8091 |
| Hub status | http://localhost:9100/hub/status |
| Worker status | http://localhost:9090/worker/status, http://localhost:9091/worker/status |
| Admin API (control plane) | http://localhost:9000 |
| Studio | http://localhost:3000 |

## Things to try

**Run a query on a worker.**

```bash
./trino --server http://localhost:8090 --user demo --execute "SELECT 42, 'secret'"
```

It shows up in the control plane's query history as `demo-site/worker-a`, with its literals masked (`maskLiterals`).

**See the fleet upstream.** `curl -u admin:admin localhost:9000/admin/workers` lists:

- the hub (`kind: hub`);
- `demo-site/worker-a` and `demo-site/worker-b`, both with `parent: demo-site`.

**Change the config once.** Edit the `duckdb-1` cluster in Studio. The hub picks up the new revision through its long-poll, and both workers activate it within about a second. Compare `servedRevision` in `/hub/status` with `activeRevision` in each `/worker/status`.

**Cut the site off.** Run `docker compose stop control-plane`, then:

- Queries keep running: the workers use `onUnavailable: localLimits`.
- `/hub/status` shows `upstreamUp: false`.
- Each batch of query history is a file in the hub's spool (`history.files`).
- Start the control plane again: the spool drains, and the records arrive with their workers' names.

**Restart the hub while upstream is down.** Run `docker compose stop control-plane && docker compose restart hub`. The hub serves the snapshot cached on its volume, marked `stale: true`, until the control plane answers.

## Files

| File | Role |
|------|------|
| `control-plane.yaml` | `role: controlPlane`: Postgres, `controlPlane`, `capacity.mode: quotas` |
| `hub.yaml` | `role: hub`: `hub.upstream` (with this site's `secret`), `stateDir`, `workerSecret` for the site's workers |
| `worker-a.yaml`, `worker-b.yaml` | `role: worker` with `worker.upstream.url` pointing at the hub |
