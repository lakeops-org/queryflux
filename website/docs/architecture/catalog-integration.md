# Catalog Provider

QueryFlux can discover table and column metadata from a pluggable catalog provider, feeding [schema-aware SQL translation](./query-translation) so `sqlglot`'s optimizer can qualify columns and types instead of just transpiling syntax.

## How it works

```
Client → QueryFlux
          │
          ├── extract table refs from SQL (sqlglot, excludes CTE aliases)
          │
          ├── catalog.get_schemas_for_query(catalog, database, tables)  ── timeout-guarded
          │
          └── SchemaContext (table → column → type) → sqlglot.optimizer.optimize(schema=…)
```

1. **Table-ref extraction** — before translation, QueryFlux parses the SQL (via the same `sqlglot` binding used for dialect translation) to find every table it references, excluding names that resolve to a CTE defined in the same query.
2. **Catalog lookup** — those table names are looked up via the configured `CatalogProvider`, timeout-guarded so a slow or unreachable catalog never blocks a query.
3. **Schema-aware translation** — the result populates a `SchemaContext` that `sqlglot`'s optimizer uses to resolve column references and types, producing more accurate translated SQL than dialect-only transpilation.
4. **Fails open, always** — a parse failure, catalog error, or timeout at any step degrades to the same dialect-only translation QueryFlux always did before this feature existed. Schema-aware translation can only ever *improve* accuracy, never break a query.

With no `catalogProvider` configured (the default), a `NullCatalogProvider` returns empty results and step 1 is skipped entirely — a query pays no extra cost.

## Configuration

`catalogProvider` is a single top-level YAML key, hot-reloadable via the [admin API](#admin-api) or Studio's **Catalog** page. It's a tagged union (`type:` selects the variant); `fallback` wraps two more `catalogProvider` values recursively.

Every real (network-calling) provider carries its own optional `cache` field — caching is a property of that provider's config, not a separate wrapper type to remember to nest.

### `null` (default)

No catalog configured — dialect-only translation, same as omitting `catalogProvider` entirely.

```yaml
catalogProvider:
  type: null
```

### `glue`

Talks directly to the [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html#data-catalog-intro) API — format-agnostic, so it sees Hive/Parquet, CSV/JSON, and Iceberg tables alike (unlike going through Iceberg's own Glue catalog client, which would only see Iceberg-format tables). Glue has no catalog concept of its own — every database/table lives under the caller's AWS account, so `list_catalogs()` always reports the single synthetic name `AwsDataCatalog`, matching the convention the Athena backend adapter already uses.

| Field | Description |
|-------|-------------|
| `region` | AWS region (optional — falls back to the default region resolution) |
| `auth` | Optional, same shape as engine cluster `auth` (`accessKey` or `roleArn`; `basic`/`bearer`/`keyPair` don't apply to AWS). Omitted: the default AWS credential chain (env vars, ECS task role, EC2 instance profile, ...). |
| `cache` | Optional `{ ttlSeconds, maxEntries }` — see [Caching](#caching) below. Strongly recommended: omitting it logs a startup warning. |

```yaml
catalogProvider:
  type: glue
  region: us-east-1
  auth:
    type: roleArn
    roleArn: arn:aws:iam::123456789012:role/queryflux-glue-readonly
    externalId: ${GLUE_EXTERNAL_ID}
  cache:
    ttlSeconds: 300
    maxEntries: 10000
```

Column types come straight from Glue's own type strings (e.g. `bigint`, `struct<a:int>`) rather than a normalized SQL type — `sqlglot`'s optimizer accepts most of them as-is. Partition keys are included alongside regular columns, since they're valid in `WHERE`/`SELECT` on a Hive-style partitioned table. Nullability isn't exposed by Glue's `Column` type, so every column defaults to nullable.

### Caching

Every network-calling provider's `cache` field wraps it in a TTL + capacity-bounded cache. Only successful lookups are cached — an error is never pinned for `ttlSeconds`, so a transient catalog outage self-heals on the next call. Table schemas change far less often than query results, so a longer TTL than you'd use for a [query result cache](./caching) — several minutes to hours — is usually safe.

| Field | Description |
|-------|-------------|
| `ttlSeconds` | How long a cached result stays valid |
| `maxEntries` | Capacity bound (FIFO eviction beyond this) |

A config for a network-calling provider with no `cache` set logs a startup warning recommending one; Studio's Catalog page defaults the "AWS Glue" picker to a cache-enabled config for the same reason (uncheck the box there, or omit `cache` in YAML, if you actually want every lookup to hit the backing service directly).

### `fallback`

Tries `primary` first, falls through to `secondary` on any error, and — for a single table lookup specifically — when `primary` doesn't have that table (an empty `list_tables`/`list_databases` result does **not** trigger fallback, since that's a legitimate answer, not a "try harder" signal). Each side configures its own `cache` independently — `fallback` composes two *sources*, it isn't itself a caching concern.

| Field | Description |
|-------|-------------|
| `primary` | Tried first |
| `secondary` | Used on `primary` error, or a missing single table |

```yaml
catalogProvider:
  type: fallback
  primary:
    type: glue
    region: us-east-1
    cache: { ttlSeconds: 300, maxEntries: 10000 }
  secondary:
    # a bare YAML `null` parses as YAML's null type, not the string tag
    # this enum's `type` field needs — quote it.
    type: "null"
```

### Declared but not yet implemented

These variants parse and build successfully, but currently degrade to a no-op provider (same behavior as `type: null`) with a startup warning — real integrations land in a follow-up release. Both already carry the same `cache` field `glue` does, so no config migration is needed once they're implemented:

| Type | Fields | Notes |
|------|--------|-------|
| `engineDelegate` | `clusterGroup`, `cache` | Delegates to a cluster group's own adapter (Trino, DuckDB, StarRocks, Athena, ClickHouse) |
| `hiveMetastore` | `uri`, `cache` | Hive Metastore (Thrift) |

Use [`/admin/config/catalog/test`](#admin-api) to check whether a given config actually does anything, rather than silently degrading unnoticed.

## Admin API

```bash
# Read the current config (persisted override, or the startup YAML value)
curl -u admin:admin http://localhost:9000/admin/config/catalog

# Replace it — structurally validated, then persisted and hot-reloaded
curl -u admin:admin -X PUT http://localhost:9000/admin/config/catalog \
  -H 'content-type: application/json' \
  -d '{"type": "glue", "region": "us-east-1", "cache": {"ttlSeconds": 300, "maxEntries": 10000}}'

# Build a config and smoke-test it (list_catalogs()) without persisting it
curl -u admin:admin -X POST http://localhost:9000/admin/config/catalog/test \
  -H 'content-type: application/json' \
  -d '{"config": {"type": "glue", "region": "us-east-1"}}'
```

A `PUT` never fails startup or a running proxy: an invalid `type` is rejected with `400`, but a *structurally valid* config for an unimplemented provider (e.g. `hiveMetastore`) is accepted — it will simply degrade to a no-op at build time. A `glue` config that's structurally valid but fails to actually build (unreachable AWS, bad credentials) degrades the same way. Use the `/test` endpoint first if you want to know that ahead of saving.

Studio's **Catalog** page (left nav) is a thin UI over these same three endpoints — a `type:` picker per provider, a cache checkbox on every provider that has one, recursive nesting for `fallback`, and the same test-connection button.

## Architecture notes

- `CatalogProvider` (`queryflux_core::catalog`) is the one generic trait every integration implements — `list_catalogs`, `list_databases`, `list_tables`, `get_table_schema`, plus a default `get_schemas_for_query` that batches `get_table_schema` calls.
- The live provider is hot-reloadable: it lives on `LiveConfig` (not a static `AppState` field), carried forward on a reload unless `catalog_config` has a new, successfully-parsed value — a reload must never silently regress to `NullCatalogProvider` and quietly stop discovering schema for already-working translation.
- Real integrations are deliberately **engine-independent** — catalog discovery must work even when no query engine is configured or healthy. `glue` (and a future `hiveMetastore`) talk directly to the catalog service; `engineDelegate` (not yet implemented) is the one intentional exception, an opt-in convenience that delegates to an already-configured cluster group's adapter.
- See [`architecture/query-translation`](./query-translation) for how `SchemaContext` flows into `sqlglot`'s optimizer, and [`architecture/system-map`](./system-map) for where this fits in the overall request path.
