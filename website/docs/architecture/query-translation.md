---
title: Query Translation
description: How QueryFlux converts SQL between dialects with sqlglot — when translation runs, supported dialects, and bypass rules.
image: img/queryflux-hero-banner.png
---
# Query translation

This document explains **how** QueryFlux converts SQL between dialects, **when** that happens, and how it fits into the query path.

## Role in the pipeline

Translation runs **after** routing has chosen a **cluster group** and **after** the cluster manager has selected a **concrete cluster** (adapter), but **before** the SQL is submitted or executed on the backend.

Conceptually:

```
Client SQL
  → routers pick cluster group
  → cluster manager picks cluster (adapter)
  → translate(client dialect → engine dialect)   ← this document
  → adapter.submit_query / execute_as_arrow
```

The implementation lives mainly in the `queryflux-translation` crate (`TranslationService`, `SqlglotTranslator`) and is invoked from shared dispatch code in `queryflux-frontend` (`dispatch_query`, `execute_to_sink`).

## Source and target dialects

- **Source dialect** comes from the **frontend protocol**: each `FrontendProtocol` has a `default_dialect()` (e.g. Trino HTTP → Trino, MySQL wire → MySQL). See `queryflux_core::query::FrontendProtocol`.
- **Target dialect** comes from the **engine type** of the chosen adapter: `EngineType::dialect()` (e.g. DuckDB → DuckDB, StarRocks → StarRocks). See `queryflux_core::query::EngineType`.

If source and target are considered **compatible** and no fixup scripts are configured, translation is skipped entirely (no sqlglot call). Notably, **MySQL and StarRocks** are treated as mutually compatible in `SqlDialect::is_compatible_with`, reflecting similar client SQL expectations.

## TranslationService and sqlglot

`TranslationService` is the façade used by the frontend:

- **`new_sqlglot(python_scripts)`** — Verifies that Python can import `sqlglot` (via PyO3) and stores the global fixup scripts. If that fails at startup, the service retains the configured policy and fixups. Required translation passes the original SQL through in `bestEffort` mode and rejects the query in `strict` mode.
- **`maybe_translate(sql, src, tgt, schema, group_fixups)`** — If dialects are compatible and no fixups are configured, returns the original string. Otherwise it constructs a `SqlglotTranslator` with **global** YAML `translation.pythonScripts` plus **per-group** fixup scripts from Postgres (`user_scripts` rows attached to the cluster group, ordered by their position in `translation_script_ids`), then runs translation.

`SqlglotTranslator` runs work on a **blocking thread pool** (`spawn_blocking`) because it holds the Python GIL. Inside Python it either:

1. **Dialect-only** — When `SchemaContext` is empty: parse one executable statement in the source dialect and emit it in the target dialect, using sqlglot's SQL generator.
2. **Schema-aware** — When tables/columns are populated: build a `MappingSchema`, run `sqlglot.optimizer.optimize` on a copy of the parsed statement, then emit SQL with the target dialect. If optimization fails, it **falls back** to dialect-only behavior (with a warning).

The Rust type `SchemaContext` (`queryflux_translation::SchemaContext`) carries optional catalog/database and a map of **table → column → SQL type string** for sqlglot's schema-aware path.

### Current default on the hot path

Dispatch resolves schema through the configured catalog provider, with a bounded timeout. Missing schema, catalog failures, and timeouts produce an empty `SchemaContext` and dialect-only fallback (`no_schema`). If schema-aware optimization fails, dialect-only translation is retried from the original SQL (`optimize_error`). Both fallbacks are allowed in strict mode if dialect translation succeeds.

## Passthrough and performance

When the client dialect matches the engine (e.g. Trino client → Trino cluster) and no fixups are configured, `maybe_translate` returns immediately with **no Python work**. That keeps the common "Trino in, Trino out" case cheap.

## Configuration

`translation` in the root config (`queryflux_core::config::TranslationConfig`) includes:

- **`mode`** — `bestEffort` (default) forwards the original SQL if required translation is unavailable or fails. `strict` rejects the query with a translation error before backend submission. Strict mode also asks sqlglot to raise errors for unsupported constructs. Translation is required when dialects differ or fixup scripts must run; compatible dialects without fixups and MCP queries without a declared dialect still bypass translation.
- **`errorOnUnsupported`** — Legacy option. `true` enables the same fail-closed policy as `mode: strict`; `false` does not override strict mode.
- **`pythonScripts`** — List of global Python transform scripts run after sqlglot translation. See the next section.

Required translation supports one SQL statement per query. Batches and syntax that
sqlglot only understands as an opaque command are translation failures: strict mode
rejects them, while best-effort forwards the complete original SQL and records
`transpile_error`. Empty statements and standalone comments do not count as additional
executable statements; comments are retained during translation.

See `config.local.yaml` / your deployment YAML for concrete values.

## Observability

The Prometheus endpoint exports these counters (using the standard `queryflux_` prefix):

- `queryflux_translation_skipped_total{reason="sqlglot_unavailable"|"transpile_error"|"not_needed"}` counts queries that could not translate or did not require translation. Strict rejections are counted too. `transpile_error` includes parsing, SQL generation, and fixup failures.
- `queryflux_translation_fallback_dialect_only_total{reason="no_schema"|"optimize_error"}` counts successful dialect-only fallbacks.

Dispatch logs degradation at WARN with `query_fingerprint`, `reason`, and `rejected`. Expected `not_needed` skips increment the counter without a warning. The fingerprint uses the existing parameterized fast hash; the warning does not include SQL text.

Query history exposes a `translation` object, for example:

```json
{"translation": {"status": "fallback", "reason": "no_schema"}}
```

`status` is `yes`, `no`, or `fallback`. Successful schema-aware translation and fixup-only execution have status `yes` and a null reason. The outcome describes execution of the translation stage even when the resulting SQL is identical. The existing `was_translated` flag continues to mean that SQL text changed. `translation: null` means the stage was not reached or the record predates this field; older rows are not backfilled with guessed outcomes. Cache hits bypass translation and also have a null outcome. Metadata survives asynchronous polling, cancellation, and completion and is returned by the Admin API consumed by the separately maintained Studio application.

```yaml
translation:
  mode: strict
```

This policy is loaded at startup; restart QueryFlux after changing it.

## Python transform scripts

After sqlglot finishes translation, QueryFlux runs each script in order — first the global `translation.pythonScripts` from YAML, then any per-group scripts attached to the cluster group via the Admin UI. This is an escape hatch for structural transformations that sqlglot does not handle on its own — things like stripping catalog prefixes, renaming functions, or applying environment-specific rewrites.

### Script contract

Each script must define a `transform` function:

```python
def transform(sql: str, src: str, dst: str) -> str:
    ...
```

| Parameter | Type  | Description                                                          |
|-----------|-------|-----------------------------------------------------------------------|
| `sql`     | `str` | The **already-translated** SQL text                                   |
| `src`     | `str` | Source dialect name (sqlglot name, e.g. `"trino"`)                    |
| `dst`     | `str` | Target dialect name (sqlglot name, e.g. `"athena"`)                   |

**Returns** the (possibly modified) SQL text as a `str`.

Scripts are handed plain SQL text, not a live AST object — this keeps them decoupled from whatever SQL library QueryFlux uses internally to translate. If you need real AST-level control, `import sqlglot` (or any other parser) yourself inside the script, parse `sql`, mutate your own tree, and return `.sql(dialect=dst)`. That tool choice is entirely up to the script and never binds QueryFlux to it.

Top-level imports and helper functions are fully supported — the script is executed as a module before `transform` is called. Scripts run **in order**, and each one receives the previous script's *returned string* as its `sql` argument.

### When scripts run

Configured scripts run even when the dialects are compatible. Use `src`/`dst` guards to apply logic only to specific pairs. MCP queries without a declared source dialect bypass translation and fixups.

### Example — strip catalog prefix for Athena

Trino clients use three-part names (`catalog.database.table`). Athena has no catalog layer and expects `database.table`. sqlglot preserves the catalog structurally, so a transform script is needed:

```yaml
translation:
  pythonScripts:
    - |
      import sqlglot
      import sqlglot.expressions as exp

      def transform(sql: str, src: str, dst: str) -> str:
          if dst != "athena":
              return sql
          ast = sqlglot.parse_one(sql, dialect=dst)
          for table in ast.find_all(exp.Table):
              table.set("catalog", None)
          return ast.sql(dialect=dst)
```

### Example — multiple scripts

Scripts are composable. Each receives the **string** returned by the previous script, so they chain:

```yaml
translation:
  pythonScripts:
    - |
      import sqlglot
      import sqlglot.expressions as exp

      def transform(sql: str, src: str, dst: str) -> str:
          # Strip catalog when targeting Athena (any source dialect)
          if dst != "athena":
              return sql
          ast = sqlglot.parse_one(sql, dialect=dst)
          for table in ast.find_all(exp.Table):
              table.set("catalog", None)
          return ast.sql(dialect=dst)
    - |
      import sqlglot
      import sqlglot.expressions as exp

      def transform(sql: str, src: str, dst: str) -> str:
          # Force uppercase schema names in DuckDB (environment-specific convention)
          if dst != "duckdb":
              return sql
          ast = sqlglot.parse_one(sql, dialect=dst)
          for table in ast.find_all(exp.Table):
              db = table.args.get("db")
              if db:
                  db.set("this", db.name.upper())
          return ast.sql(dialect=dst)
```

### Per-group scripts

In addition to global YAML scripts, you can attach **reusable scripts** to individual cluster groups via the Admin UI (**Scripts** page → **Groups** page). Per-group scripts run after the global ones and follow the same `transform(sql, src, dst) -> str` contract. This is useful when different groups target different engines and need distinct fixups.

### Error handling

If a script raises a Python exception, the outcome records `transpile_error`. In `bestEffort` mode, the original SQL is forwarded; in `strict` mode (or with `errorOnUnsupported: true`), the query is rejected before backend submission. Configured scripts run even for compatible dialects. MCP queries without a declared source dialect bypass translation and scripts.

### Implementation notes

- Scripts run inside a `spawn_blocking` task on Tokio's blocking thread pool because they hold the Python GIL.
- Each script is executed in its own globals dict (same approach as `PythonScriptRouter`), so imports and helper functions defined at module level work correctly.
- QueryFlux never parses or holds an AST across scripts — `sql` is already the sqlglot-translated text in the **target dialect** when scripts start running, and only the returned string from each `transform()` call crosses back. A script that parses `sql` with sqlglot (or anything else) is working with its own private tree, not something QueryFlux passes between scripts.
- A script's `transform()` must return a `str`; returning anything else (including `None`, e.g. a script with no `return` statement) is a `Translation` error.

## Failure modes

- **sqlglot missing** — The service retains the configured policy and fixups. When translation is required, `bestEffort` forwards the original SQL and records `sqlglot_unavailable`; `strict` rejects the query before backend submission. Queries that do not require translation can still proceed.
- **Translation errors** — Parsing, SQL generation, and fixup failures record `transpile_error`. In `bestEffort` mode, dispatch forwards the original SQL. In `strict` mode, dispatch releases the acquired cluster slot and returns a translation error before backend submission. The legacy `errorOnUnsupported: true` option also enables strict behavior. Successful dialect-only fallbacks remain allowed in both modes.

For how routing picks the group and cluster **before** translation, see [routing-and-clusters.md](routing-and-clusters.md).
