# embed-queryflux

Minimal demonstration of the `QueryFlux::builder()` embedding API — every kind of
compiled-in plugin the builder supports, wired up in one small binary:

| Plugin | This example's version | Registered via |
|---|---|---|
| Backend engine | in-memory DuckDB, from `queryflux-config.yaml`'s built-in factory | `.with_builtin_plugins()` |
| Guard | [`NoDdlGuard`](src/main.rs) — denies `DROP`/`CREATE`/`ALTER`/`TRUNCATE` | `.guard(...)` |
| Router | [`LoggingRouter`](src/main.rs) — logs every query, defers to the configured fallback | `.router_prepend(...)` |
| Hook | [`AuditHook`](src/main.rs) — logs `before_execute` / `after_execute` / `on_error` | `.hook(...)` |
| Frontend | [`TinyHttpFrontend`](src/main.rs) — `POST /query` with a raw SQL body | `.frontend(...)` |

## Run it

```sh
cargo run -p embed-queryflux
```

This starts three listeners:

- `:8080` — the built-in Trino HTTP frontend (point any Trino client at it).
- `:9000` — the admin API (metrics, Swagger UI at `/docs`).
- `:8090` — the extra frontend registered by this example.

Query through the extra frontend:

```sh
curl -X POST localhost:8090/query -d 'SELECT 42'
# {"rows":1}

curl -X POST localhost:8090/query -d 'DROP TABLE t'
# {"error":"DDL is disabled in this deployment"}
```

Set `RUST_LOG=info` to see `LoggingRouter` and `AuditHook` fire on every query.

## What to look at

- [`src/main.rs`](src/main.rs) — the whole example is one file; start with `fn main()`
  at the bottom to see the builder chain, then read each plugin type top to bottom.
- [`queryflux-config.yaml`](queryflux-config.yaml) — the YAML side: one in-memory DuckDB cluster, one
  group, an empty `routers:` list (so `LoggingRouter`, prepended in code, is the only
  router — it always falls through to `routingFallback: analytics`).

See [`website/docs/architecture/embedding.md`](../../website/docs/architecture/embedding.md)
for the full reference.
