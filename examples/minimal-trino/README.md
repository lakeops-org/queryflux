# Minimal QueryFlux example

Stack: **Postgres**, **Trino**, **QueryFlux** (Trino HTTP on port 8080), and **Studio** (UI on port 3000). Traffic to `http://localhost:8080` goes through QueryFlux to the Trino container.

## Start the stack

From this directory:

```bash
docker compose up -d --wait
```

| Service | URL |
|--------|-----|
| Trino over HTTP via QueryFlux | http://localhost:8080 |
| Trino (direct, bypasses QueryFlux) | http://localhost:8081 |
| QueryFlux admin API | http://localhost:9000 |
| QueryFlux Studio | http://localhost:3000 |
| Postgres (from host) | `localhost:5433` → user `queryflux`, password `queryflux`, db `queryflux` |

## Run SQL with the Trino CLI

Traffic must go to **QueryFlux** on the Trino HTTP port (`8080` in this compose file), not straight to the Trino coordinator. QueryFlux then routes to the `trino` service. Use any username for local dev.

Install the CLI from Trino’s docs if you run it on your machine: **[Command line interface](https://trino.io/docs/current/client/cli.html)** (requirements, download, and `java -jar` fallback on Windows).

### From your machine (host)

After you have the executable JAR, make it runnable (the docs often rename it to `trino`). From the **host**, QueryFlux is published on `localhost:8080`:

```bash
./trino --server http://localhost:8080 --user test
```

If you did not rename the JAR, use `./<that-filename>` or `java -jar …` as in the Trino docs.

### From inside the Trino container

The `trinodb/trino` image includes the **`trino`** CLI on `PATH`. You can open a shell **in the Trino service** and point the CLI at the **QueryFlux** hostname on the Compose network (not `localhost` — inside that container, `localhost` is only the Trino process itself).

From the **same directory** as this example’s `docker compose` file:

```bash
docker compose exec -it trino bash
```

If `bash` is not available, try `docker compose exec -it trino sh`.

Then run the client against QueryFlux (service name `queryflux`, internal port `8080`):

```bash
trino --server http://queryflux:8080 --user test
```

At the `trino>` prompt:

```sql
select 1;
```

You should see one row. Type `quit` or **Ctrl+D** to exit the CLI, then `exit` to leave the container shell.

**Contrast:** `trino --server http://localhost:8080` **inside** the Trino container talks only to the coordinator in that same container (bypasses QueryFlux). `trino --server http://queryflux:8080` sends traffic **through QueryFlux**, which matches how clients use `http://localhost:8080` on the host.

### If queries seem to skip QueryFlux

Use this port/hostname cheat sheet for this compose file:

| You run the CLI on… | Use this `--server` | Goes through QueryFlux? |
|---------------------|---------------------|-------------------------|
| **Host** | `http://localhost:8080` | Yes |
| **Host** | `http://localhost:8081` | **No** — that is Trino mapped direct (see table above) |
| **Inside `trino` container** | `http://queryflux:8080` | Yes |
| **Inside `trino` container** | `http://localhost:8080` or `http://trino:8080` | **No** — that is the coordinator inside the same pod/service |

Typical mistakes: choosing **8081** on the host “because Trino”, or inside the Trino container using **localhost** (or **`trino`**) **:8080** — those hit Trino only.

**Check:** in another terminal, `docker compose logs -f queryflux` and submit `select 1;` against QueryFlux. You should see an info line like `New query submitted`. If nothing appears on QueryFlux logs, the CLI is not talking to port **8080** on the **queryflux** service (host: `localhost:8080`; in-container: `http://queryflux:8080`).

## See the query in Studio

1. Open **http://localhost:3000** and go to **Queries** (or open **http://localhost:3000/queries**).
2. After the query **finishes** (the Trino CLI must poll to completion), wait **a few seconds** — QueryFlux batches writes to Postgres; rows can appear up to roughly **5 seconds** later.
3. Click a row to open details if your Studio build supports a detail view.

History is only recorded for traffic through **QueryFlux** (`localhost:8080`), not Trino direct (`8081`). If the list stays empty: refresh after ~5s, check `docker compose logs queryflux` for `New query submitted`, and for errors like `Failed to flush query record` or `Insert query_records`. Confirm rows exist in Postgres:

```bash
docker compose exec postgres psql -U queryflux -d queryflux -c "SELECT proxy_query_id, status, created_at FROM query_records ORDER BY id DESC LIMIT 5;"
```

From your machine (Postgres is published on **5433**):

```bash
psql postgresql://queryflux:queryflux@localhost:5433/queryflux -c "SELECT COUNT(*) FROM query_records;"
```

## Database migrations

Schema changes are **not** applied by Postgres’s `docker-entrypoint-initdb.d`. By default (`autoMigrate: true`), they run **inside the QueryFlux process** the first time it connects: embedded **sqlx** migrations create/update tables (including `_sqlx_migrations`, `query_records`, cluster config tables, etc.). If migration fails, QueryFlux exits and you’ll see **`Migration failed`** in `docker compose logs queryflux`.

You can also apply migrations as a one-shot without starting the server:

```bash
queryflux migrate -c /path/to/config.yaml
```

For multi-replica rollouts, set `autoMigrate: false` under `persistence` (with `type: postgres`) and run `queryflux migrate` (or a Kubernetes Job with the same image/command) **before** rolling pods so only one process applies DDL.

`docker compose up -d --wait` waits until **QueryFlux** reports healthy on `:9000/health` — that only happens after migrations (when auto-migrate is on) and startup succeed. If the DB volume is empty and you only inspect Postgres before QueryFlux has finished starting, tables may not exist yet.

If migrations never appear to run with a **pre-built registry image**, ensure that image was produced from this repo’s `docker/queryflux/Dockerfile` so the binary includes the migration bundle; otherwise build locally (see repo `docker/queryflux/Dockerfile` header) and set the image in `docker-compose.yml`.

## Stop and reset

```bash
docker compose down
```

To wipe persisted QueryFlux state (cluster config seeded from `config.yaml`, query history, etc.):

```bash
docker compose down -v
```

## Configuration note

`config.yaml` in this folder is mounted into the Queryflux container. With **Postgres** persistence enabled, cluster and group rows are **seeded from YAML when the tables are empty**; after that, the database is the source of truth for those objects. Use Studio or the admin API to change them on a long-lived volume.
