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

If source and target are considered **compatible**, translation is skipped entirely (no sqlglot call). Notably, **MySQL and StarRocks** are treated as mutually compatible in `SqlDialect::is_compatible_with`, reflecting similar client SQL expectations.

## TranslationService and sqlglot

`TranslationService` is the façade used by the frontend:

- **`new_sqlglot(python_scripts)`** — Verifies that Python can import `sqlglot` (via PyO3) and stores the global fixup scripts. If that fails at startup, the binary logs a warning and falls back to **`disabled()`**, which passes SQL through unchanged.
- **`maybe_translate(sql, src, tgt, schema, group_fixups)`** — If translation is disabled, or dialects are compatible, returns the original string. Otherwise it constructs a `SqlglotTranslator` with **global** YAML `translation.pythonScripts` plus **per-group** fixup scripts from Postgres (`user_scripts` rows attached to the cluster group, ordered by their position in `translation_script_ids`), then runs translation.

`SqlglotTranslator` runs work on a **blocking thread pool** (`spawn_blocking`) because it holds the Python GIL. Inside Python it either:

1. **Dialect-only** — When `SchemaContext` is empty: `sqlglot.transpile(sql, read=<src>, write=<tgt>)`.
2. **Schema-aware** — When tables/columns are populated: parse with `parse_one`, build a `MappingSchema`, run `sqlglot.optimizer.optimize`, then emit SQL with the target dialect. If optimization fails, it **falls back** to dialect-only behavior (with a warning).

The Rust type `SchemaContext` (`queryflux_translation::SchemaContext`) carries optional catalog/database and a map of **table → column → SQL type string** for sqlglot's schema-aware path.

### Current default on the hot path

Today, dispatch passes **`SchemaContext::default()`** (empty tables). So in production query paths you get **dialect-only** transpilation. The schema-aware branch is **implemented** in `sqlglot.rs` and is ready for future wiring (e.g. catalog providers or static schema config) to populate `SchemaContext` before `maybe_translate`.

## Passthrough and performance

When the client dialect matches the engine (e.g. Trino client → Trino cluster), `maybe_translate` returns immediately with **no Python work**. That keeps the common "Trino in, Trino out" case cheap.

## Configuration

`translation` in the root config (`queryflux_core::config::TranslationConfig`) includes:

- **`errorOnUnsupported`** — Controls strictness when sqlglot cannot translate a construct. `false` (default) passes the original construct through best-effort; `true` fails the query.
- **`pythonScripts`** — List of global Python transform scripts run after sqlglot translation. See the next section.

See `config.local.yaml` / your deployment YAML for concrete values.

## Python transform scripts

After sqlglot finishes translation, QueryFlux runs each script in order — first the global `translation.pythonScripts` from YAML, then any per-group scripts attached to the cluster group via the Admin UI. This is an escape hatch for structural transformations that sqlglot does not handle on its own — things like stripping catalog prefixes, renaming functions, or applying environment-specific rewrites.

### Script contract

Each script must define a `transform` function:

```python
def transform(ast, src: str, dst: str) -> None:
    ...
```

| Parameter | Type             | Description                                                 |
|-----------|------------------|-------------------------------------------------------------|
| `ast`     | `sqlglot.Expression` | Root AST node of the **already-translated** SQL — mutate in-place |
| `src`     | `str`            | Source dialect name (sqlglot name, e.g. `"trino"`)          |
| `dst`     | `str`            | Target dialect name (sqlglot name, e.g. `"athena"`)         |

Top-level imports and helper functions are fully supported — the script is executed as a module before `transform` is called. QueryFlux re-serializes the AST using the target dialect once, **after all scripts have run**.

### When scripts run

Scripts run for **every translation** where `src != dst`. They do not run when dialects are compatible and translation is skipped. Use `src`/`dst` guards to apply logic only to specific pairs.

### Example — strip catalog prefix for Athena

Trino clients use three-part names (`catalog.database.table`). Athena has no catalog layer and expects `database.table`. sqlglot preserves the catalog structurally, so a transform script is needed:

```yaml
translation:
  pythonScripts:
    - |
      import sqlglot.expressions as exp

      def transform(ast, src: str, dst: str) -> None:
          if dst == "athena":
              for table in ast.find_all(exp.Table):
                  table.set("catalog", None)
```

### Example — multiple scripts

Scripts are composable. Each sees the same `ast` (as mutated by previous scripts), so they chain:

```yaml
translation:
  pythonScripts:
    - |
      import sqlglot.expressions as exp

      def transform(ast, src: str, dst: str) -> None:
          # Strip catalog when targeting Athena (any source dialect)
          if dst == "athena":
              for table in ast.find_all(exp.Table):
                  table.set("catalog", None)
    - |
      import sqlglot.expressions as exp

      def transform(ast, src: str, dst: str) -> None:
          # Force uppercase schema names in DuckDB (environment-specific convention)
          if dst == "duckdb":
              for table in ast.find_all(exp.Table):
                  db = table.args.get("db")
                  if db:
                      db.set("this", db.name.upper())
```

### Per-group scripts

In addition to global YAML scripts, you can attach **reusable scripts** to individual cluster groups via the Admin UI (**Scripts** page → **Groups** page). Per-group scripts run after the global ones and follow the same `transform(ast, src, dst)` contract. This is useful when different groups target different engines and need distinct fixups.

### Error handling

If a script raises a Python exception, the query fails with a `Translation` error and the SQL is **not** sent to the backend. The error message includes the script index and the Python traceback. Scripts do not affect queries that skip translation (compatible dialects).

### Implementation notes

- Scripts run inside a `spawn_blocking` task on Tokio's blocking thread pool because they hold the Python GIL.
- Each script is executed in its own globals dict (same approach as `PythonScriptRouter`), so imports and helper functions defined at module level work correctly.
- The `ast` is parsed from the sqlglot-translated SQL in the **target dialect** before scripts run. Mutations do not need to account for the source dialect's syntax.
- Re-serialization happens once at the end via `ast.sql(dialect=dst)`, keeping overhead independent of the number of scripts.

## Failure modes

- **sqlglot missing** — Startup degrades to a disabled translation service; SQL is sent as-is, which may fail on the backend if dialects differ.
- **Translation errors** — Dispatch releases the acquired cluster slot and returns an error to the client (async Trino path logs and propagates; sync `execute_to_sink` path reports via the result sink).

For how routing picks the group and cluster **before** translation, see [routing-and-clusters.md](routing-and-clusters.md).
