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

### `hiveMetastore`

Talks the *raw* Hive Metastore Thrift protocol directly — format-agnostic, so it sees plain Hive/Parquet tables as well as Iceberg-format tables registered in HMS (unlike going through Iceberg's own HMS catalog client, which only understands the latter).

| Field | Description |
|-------|-------------|
| `uri` | `thrift://host:port` (the `thrift://` scheme is optional) |
| `cache` | Optional `{ ttlSeconds, maxEntries }` — see [Caching](#caching) below. Strongly recommended: omitting it logs a startup warning. |

```yaml
catalogProvider:
  type: hiveMetastore
  uri: thrift://localhost:9083
  cache:
    ttlSeconds: 300
    maxEntries: 10000
```

HMS has no catalog concept of its own — every database/table lives under one metastore, so `list_catalogs()` reports a single synthetic name (`hive_metastore`). Column types come straight from HMS's own Hive type strings, same convention as `glue`. Partition keys are included alongside regular columns. A missing table (HMS's `NoSuchObjectException`) is treated as a normal "not found" answer, not an error.

### `icebergRest`

Speaks the [Iceberg REST Catalog protocol](https://iceberg.apache.org/spec/#rest-catalog) — served by Polaris, Tabular, Unity's REST endpoint, or Snowflake's own Horizon endpoint for Snowflake-managed Iceberg tables. Unlike `glue`/`hiveMetastore`, this is the one integration built on the upstream `iceberg`/`iceberg-catalog-rest` crates rather than a hand-rolled client — the REST protocol genuinely *is* Iceberg-specific (a REST catalog endpoint only ever serves Iceberg tables), so there's no format-agnostic reason to avoid it here.

| Field | Description |
|-------|-------------|
| `uri` | The catalog's REST endpoint |
| `warehouse` | Optional warehouse location, if the catalog server requires one |
| `catalogName` | The protocol has no "list catalogs" call — one REST endpoint *is* one catalog — so this is just echoed back by `list_catalogs()` |
| `auth` | Optional: `oauth2ClientCredentials` (`clientId`/`clientSecret`) or `bearerToken` (`token`). Omitted for an unauthenticated catalog server. |
| `cache` | Optional `{ ttlSeconds, maxEntries }` — see [Caching](#caching) below. Strongly recommended: omitting it logs a startup warning. |

```yaml
catalogProvider:
  type: icebergRest
  uri: https://polaris.example.com/api/catalog
  warehouse: s3://my-bucket/warehouse
  catalogName: prod
  auth:
    type: oauth2ClientCredentials
    clientId: ${ICEBERG_REST_CLIENT_ID}
    clientSecret: ${ICEBERG_REST_CLIENT_SECRET}
  cache:
    ttlSeconds: 300
    maxEntries: 10000
```

A database maps onto a (possibly multi-level) Iceberg namespace — dotted components round-trip, e.g. `"a.b"` ↔ namespace `["a", "b"]`. Column types come from Iceberg's own typed schema (`iceberg::spec::PrimitiveType`), mapped to a SQL type name (`Decimal{precision,scale}` → `DECIMAL(p,s)`, `Timestamptz` → `TIMESTAMP WITH TIME ZONE`, etc.); a nested struct/list/map column falls back to Iceberg's own rendering of that type rather than failing the whole lookup.

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

A `PUT` never fails startup or a running proxy: an invalid `type` is rejected with `400`, but a *structurally valid* config that fails to actually build (unreachable endpoint, bad credentials, unresolvable host) degrades to a no-op provider at build time rather than refusing to save. Use the `/test` endpoint first if you want to know that ahead of saving.

Studio's **Catalog** page (left nav) is a thin UI over these same three endpoints — a `type:` picker per provider, a cache checkbox on every provider that has one, recursive nesting for `fallback`, and the same test-connection button.

## Architecture notes

- `CatalogProvider` (`queryflux_core::catalog`) is the one generic trait every integration implements — `list_catalogs`, `list_databases`, `list_tables`, `get_table_schema`, plus a default `get_schemas_for_query` that batches `get_table_schema` calls.
- The live provider is hot-reloadable: it lives on `LiveConfig` (not a static `AppState` field), carried forward on a reload unless `catalog_config` has a new, successfully-parsed value — a reload must never silently regress to `NullCatalogProvider` and quietly stop discovering schema for already-working translation.
- Every integration is deliberately **engine-independent** — catalog discovery works even when no query engine is configured or healthy. `glue`, `hiveMetastore`, and `icebergRest` all talk directly to the catalog service, never through a routed query.
- Catalogs are format-agnostic integrations, not Iceberg wrappers: `glue` and `hiveMetastore` use their own native (non-Iceberg) clients because both catalog tables of any format, not just Iceberg — only `icebergRest` is built on the upstream `iceberg`/`iceberg-catalog-rest` crates, because that protocol genuinely is Iceberg-only.
- See [`architecture/query-translation`](./query-translation) for how `SchemaContext` flows into `sqlglot`'s optimizer, and [`architecture/system-map`](./system-map) for where this fits in the overall request path.
