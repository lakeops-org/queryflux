# QueryFlux Studio

Next.js admin UI for QueryFlux: clusters, groups, routing, scripts, and query history. It talks to the **Admin REST API** (default `http://localhost:9000` via `ADMIN_API_URL`), not to backend engines directly.

## Run locally

```bash
cd ui/queryflux-studio
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). Postgres-backed features (query history, persisted cluster config) need the proxy configured with Postgres as documented in the repo root [README.md](../../README.md) and [docs/observability.md](../../docs/observability.md).

## Adding or changing a backend in the UI

Use the **QueryFlux Studio** section in **[website/docs/architecture/adding-support/backend.md](../website/docs/architecture/adding-support/backend.md)**. Short version:

1. Add **`lib/studio-engines/engines/<engine>.ts`** exporting a **`StudioEngineModule`** (descriptor + catalog metadata + optional validation and custom form id).
2. Register it in **`lib/studio-engines/manifest.ts`**.
3. Add a **`{ k: "studio", engineKey: "…" }`** entry to **`ENGINE_CATALOG_SLOTS`** in **`components/engine-catalog.ts`** where the card should appear.
4. If the cluster form is not generic: set **`customFormId`** on the module and register the component in **`components/cluster-config/studio-engine-forms.tsx`**.
5. If persisted **`config`** JSON uses new top-level keys, extend **`lib/cluster-persist-form.ts`**.

**Derived pieces** (usually no manual edits): **`lib/engine-registry.ts`** builds **`ENGINE_REGISTRY`** from the manifest; **`ENGINE_AFFINITY_OPTIONS`** comes from **`buildEngineAffinityOptionsFromManifest()`**; **`validateEngineSpecific`** dispatches to each module’s **`validateFlat`**.

## Layout (high level)

| Path | Role |
|------|------|
| `lib/studio-engines/` | Per-engine modules, manifest, catalog bridge, flat validation |
| `lib/engine-registry.ts` | Registry helpers + `ENGINE_REGISTRY` from manifest |
| `lib/engine-registry-types.ts` | `EngineDescriptor` and field/auth unions |
| `components/engine-catalog.ts` | Engines grid + `findEngineByType` (studio slots + static dialects) |
| `components/cluster-config/` | Cluster form router, generic row renderer, custom engine panels, SaaS warehouse editor, health/reconcile fields |
| `lib/adbc-saas-variants.ts` | Warehouse variant rows ↔ `ClusterVariant` conversion and validation |
| `lib/cluster-persist-form.ts` | Flat form ↔ persisted `config` JSON (including `variants`, `healthCheckQuery`, `reconcileQuery`) |

For Next.js framework docs, see [nextjs.org/docs](https://nextjs.org/docs).
