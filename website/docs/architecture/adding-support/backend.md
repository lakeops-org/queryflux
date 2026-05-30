---
title: Add a Backend Engine
description: Step-by-step guide to adding a query backend — Rust adapter, engine registration, persistence, and Studio UI.
image: img/queryflux-hero-banner.png
---
# Adding a new backend (engine)

This page is for contributors who want QueryFlux to **route SQL to a new database or engine** (for example a new OLAP system). It is **not** about adding a new **client protocol** (Trino HTTP, Postgres wire, etc.); for that, see **[Frontend](frontend.md)** and **[Frontends](../frontends/overview.md)**.

**What you are building**

- A Rust **adapter** that implements how QueryFlux talks to that engine (submit query, poll if async, health check, optional catalog discovery).
- Registration so the **binary** can construct that adapter from cluster config (from **Postgres** or from **YAML**).
- A **descriptor** that describes connection fields for the Admin API and (usually) **QueryFlux Studio** forms.

When you are done, operators can define a **cluster** whose `engine` is your engine, point it at endpoints and credentials, and have traffic routed to your adapter.

---

## How config reaches your code

Engines are **compiled in**, not loaded as plugins.

| Source | What happens |
|--------|----------------|
| **Postgres** (`cluster_configs` table) | Each row has `engine_key` (string) and `config` (JSON). QueryFlux finds your **`EngineAdapterFactory`** by `engine_key` and calls **`build_from_config_json`**. Your adapter reads whatever JSON keys it needs. The persistence crate does **not** need to know your field names. |
| **YAML** (`config.yaml` clusters) | Clusters are deserialized into **`ClusterConfig`**. **`build_adapter`** matches on **`EngineConfig`** and calls your **`try_from_cluster_config`**. |

So you implement **two constructors** on the adapter (plus a small **factory** type — see below): one from **JSON** (DB), one from **`ClusterConfig`** (YAML). Same engine, two entry points.

---

## Follow this order (Rust)

Treat **[Trino](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/trino/mod.rs)** as the default template (sync HTTP). Use **[Athena](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/athena/mod.rs)** if your setup is **async** (e.g. cloud SDK init).

### Step 1 — Name the engine in core (`queryflux-core`)

1. Add **`EngineConfig::YourEngine`** in `crates/queryflux-core/src/config.rs` (serde uses **camelCase** in JSON, e.g. `myEngine`).
2. If metrics, translation, or dispatch need to distinguish this engine, add **`EngineType::YourEngine`** in `crates/queryflux-core/src/query.rs`.
3. In `crates/queryflux-core/src/engine_registry.rs`, wire:
   - **`engine_key(&EngineConfig)`** → stable string (must match DB column and Studio `engineKey`).
   - **`parse_engine_key(&str)`** → parse that string back to **`EngineConfig`** (needed when reading rows / API).
   - **`impl From<&EngineConfig> for EngineType`**.
4. Implement **`EngineType::dialect()`** for your variant if SQL should be translated to a specific target dialect; see [query-translation.md](../query-translation.md).

### Step 2 — Optional fields on `ClusterConfig`

Add top-level fields on **`ClusterConfig`** only if **YAML** users need them and they are shared across documentation. Many engines only need keys inside the JSON blob for Postgres; those are parsed in **`try_from_config_json`**, not necessarily on **`ClusterConfig`**.

### Step 3 — Adapter module (`queryflux-engine-adapters`)

Adapters implement one of two traits depending on their execution model:

| Trait | Used when | Examples |
|-------|-----------|---------|
| `SyncAdapter` | Engine returns results synchronously (single round-trip or blocking call) | DuckDB, StarRocks, ADBC engines |
| `AsyncAdapter` | Engine uses a submit-then-poll lifecycle across multiple HTTP requests | Trino, Athena |

**Steps:**

1. Add `src/your_engine/mod.rs` (or similar).
2. Implement **`SyncAdapter`** or **`AsyncAdapter`** — pick the one that matches your engine's execution model. Copy the shape from StarRocks (`SyncAdapter`) or Trino (`AsyncAdapter`).
   - Required methods: `execute_as_arrow` / `submit_query` + `poll_query` + `cancel_query`, `health_check`, `engine_type`, catalog helpers (`list_catalogs`, `list_databases`, `list_tables`, `describe_table`).
3. **Declare `connection_format()`** — this is how dispatch knows which result-encoding path to use:

   ```rust
   fn connection_format(&self) -> ConnectionFormat {
       ConnectionFormat::MysqlWire   // for mysql_async-backed engines
       // ConnectionFormat::Arrow    // default — ADBC, DuckDB, in-process
       // ConnectionFormat::PostgresWire // for tokio_postgres-backed engines
   }
   ```

   If you return anything other than the default `Arrow`, you **must** also override **`execute_native`** to produce a `NativeExecution` stream. The shared helpers in `queryflux-engine-adapters::mysql_native` (for `mysql_async`) and `queryflux-engine-adapters::pg_native` (for `tokio_postgres`) cover the common cases — delegate to them rather than implementing row conversion yourself.

4. Implement **`descriptor() -> EngineDescriptor`**: `engine_key`, `display_name`, `connection_type`, `supported_auth`, **`config_fields`** (this is the schema for forms and `/admin/engine-registry`), `implemented: true` once wired.
5. Implement **`try_from_config_json(..., json: &serde_json::Value, ...)`** for the DB path. Use **`queryflux_core::engine_registry`**: `json_str`, `json_bool`, **`parse_auth_from_config_json`** where auth matches existing patterns.
6. Implement **`try_from_cluster_config(..., cfg: &ClusterConfig, ...)`** for YAML.
7. Add **`YourEngineFactory`** (empty struct) and **`impl EngineAdapterFactory`** in the same module: `engine_key()`, `descriptor()`, `build_from_config_json` delegating to `try_from_config_json` and returning `AdapterKind::Sync(...)` or `AdapterKind::Async(...)`. For async construction (Athena-style), `try_from_config_json` is `async`; the trait is `async_trait`-based.
8. Export the module from `crates/queryflux-engine-adapters/src/lib.rs` and add **Cargo.toml** dependencies for any new client libraries.

Use **`QueryFluxError::Engine(format!(...))`** and include the **`cluster_name_str`** argument in messages so logs show which cluster failed.

### Step 4 — Register the factory (`queryflux` binary)

In **`crates/queryflux/src/registered_engines.rs`**:

- Append **`Box::new(YourEngineFactory)`** to **`all_factories()`**. That automatically registers the descriptor and DB-path construction.
- Add a **`match`** arm in **`build_adapter`** for **`EngineConfig::YourEngine`** that calls **`try_from_cluster_config`** (YAML path).

Do **not** add adapter construction logic in **`main.rs`** beyond what already exists.

### Step 5 — Persistence

You normally **do not** edit `queryflux-persistence` for engine-specific JSON keys that live only inside the `cluster_configs.config` JSONB blob. If you add new **top-level** **`ClusterConfig`** fields (in `queryflux-core`) that YAML seeding must persist, extend **`UpsertClusterConfig::from_core`** so first-run Postgres seeding writes them into that JSON. You **do** extend **`parse_engine_key`** (and thus **`engine_key`**) in core so the `engine_key` column is recognized; **`UpsertClusterConfig::from_core`** sets the column from **`engine_key(&EngineConfig)`**.

### Step 6 — Frontends and tests

- **`queryflux-frontend`**: Most engines need **no** change; execution goes through **`dispatch_query`** / **`execute_to_sink`**. The native path (zero Arrow) is activated purely by returning the right `ConnectionFormat` in your adapter — no frontend changes required.
- **E2E**: Add tests under **`crates/queryflux-e2e-tests`** if you can run the engine in Docker; see **`docker/test/docker-compose.test.yml`**.
- Update **[system-map.md](../system-map.md)** if you maintain a supported-engines list there.

### Unimplemented placeholder

Until the adapter exists, **`EngineConfig::ClickHouse`** (or similar) may **`bail!`** inside **`build_adapter`**. Replace that with a real arm when you implement the adapter.

---

## QueryFlux Studio (optional but typical)

Studio lives in **`queryflux-studio/`** at the repo root (Next.js). It talks to the Admin API; it does **not** embed Rust. Today, **Rust `descriptor()` and the TypeScript `descriptor` must match by hand** (same `engineKey`, field keys, auth). The proxy also serves **`GET /admin/engine-registry`**.

**Minimum Studio work**

1. **`queryflux-studio/lib/studio-engines/engines/<engine>.ts`** — export a **`StudioEngineModule`** with **`descriptor`** (mirror Rust), **`catalog`**, and optional **`validateFlat`**, **`customFormId`**, **`engineAffinity`**, **`extraTypeAliases`**.
2. **`queryflux-studio/lib/studio-engines/manifest.ts`** — import and append to **`STUDIO_ENGINE_MODULES`**.
3. **`queryflux-studio/components/engine-catalog.ts`** — add **`{ k: "studio", engineKey: "<same as Rust>" }`** to **`ENGINE_CATALOG_SLOTS`** so the engine appears in the picker.

**If you add new top-level keys** inside the persisted `config` JSON that the flat form must edit, update **`queryflux-studio/lib/cluster-persist-form.ts`** (`MANAGED_CONFIG_JSON_KEYS`, flat ↔ JSON helpers, **`buildValidateShape`**).

**If the generic form is not enough**, register a custom component in **`queryflux-studio/components/cluster-config/studio-engine-forms.tsx`** and set **`customFormId`** on the module.

| User-facing area | Main file(s) |
|------------------|----------------|
| Add / edit cluster forms | `components/cluster-config/engine-cluster-config.tsx`, `components/add-cluster-dialog.tsx`, `app/clusters/clusters-grid.tsx` |
| Engine affinity in groups | `lib/cluster-group-strategy.ts` (driven by manifest) |
| Types for auth / connection | `lib/engine-registry-types.ts` |

---

## Checklists

### Rust

- [ ] `EngineConfig` + `EngineType` + `engine_key` / `parse_engine_key` / `From<&EngineConfig> for EngineType` + `dialect()` if needed
- [ ] `SyncAdapter` or `AsyncAdapter` + `connection_format()` (+ `execute_native` if non-Arrow) + `descriptor()` + `try_from_config_json` + `try_from_cluster_config`
- [ ] `YourEngineFactory` + `EngineAdapterFactory` returning `AdapterKind::Sync` or `AdapterKind::Async`
- [ ] `registered_engines.rs`: `all_factories()` + `build_adapter` YAML arm
- [ ] `cargo build -p queryflux` and smoke-test Postgres + YAML cluster load

### Studio

- [ ] `lib/studio-engines/engines/<engine>.ts` + `manifest.ts`
- [ ] `components/engine-catalog.ts` studio slot
- [ ] `engine-registry-types.ts` if you added auth/connection enums
- [ ] `cluster-persist-form.ts` only if new persisted JSON keys
- [ ] `studio-engine-forms.tsx` only if `customFormId`

---

## Related reading

- [Extending QueryFlux — overview](overview.md)
- [Frontend](frontend.md)
- [query-translation.md](../query-translation.md)
- [routing-and-clusters.md](../routing-and-clusters.md)
- [observability.md](../observability.md)

**Key Rust files**

- [`crates/queryflux/src/registered_engines.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux/src/registered_engines.rs) — `all_factories`, `build_adapter`, `build_adapter_from_record`
- [`crates/queryflux-engine-adapters/src/lib.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-engine-adapters/src/lib.rs) — `EngineAdapterFactory`, `SyncAdapter`, `AsyncAdapter`, `ConnectionFormat`, `AdapterKind`
- [`crates/queryflux-core/src/engine_registry.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-core/src/engine_registry.rs) — `engine_key`, `parse_engine_key`, `parse_auth_from_config_json`
- [`crates/queryflux-persistence/src/cluster_config.rs`](https://github.com/lakeops-org/queryflux/blob/main/crates/queryflux-persistence/src/cluster_config.rs) — row types; engine config stored as JSONB
