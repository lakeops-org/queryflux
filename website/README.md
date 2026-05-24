# QueryFlux documentation site

This directory is a [Docusaurus](https://docusaurus.io/) site that mirrors the root [`README.md`](https://github.com/lakeops-org/queryflux/blob/main/README.md) and the [`docs/`](https://github.com/lakeops-org/queryflux/tree/main/docs) Markdown in `docs/` here. **Canonical sources** stay in the repository root (`README.md`, `development.md`, `contribute.md`, `docs/`); edit those and refresh the copies under `website/docs/` when they drift, or automate sync in CI if you prefer.

## Commands

```bash
npm install   # if node_modules is missing
npm start     # dev server (default http://localhost:3000)
npm run build # static output in build/
npm run serve # preview production build (search works here)
```

## Search

Local search uses [`@cmfcmf/docusaurus-search-local`](https://github.com/cmfcmf/docusaurus-search-local) ([Docusaurus: local search](https://docusaurus.io/docs/search#using-local-search)): the index is built at compile time and shipped with the site—no Algolia or other hosted service.

The search bar appears in the navbar after a **production build**. It does **not** work with `npm start` (dev mode); use `npm run build` then `npm run serve` to try it locally. Versioned docs are supported (results follow the doc version you are viewing).

## SEO

Global metadata, JSON-LD, and `static/robots.txt` follow [Docusaurus SEO](https://docusaurus.io/docs/seo). **`seoConfig.ts`** sets `noIndex` on the **Next** draft and all **older** doc versions (everything in `versions.json` except the first entry). Only the **latest release** at `/docs/` is indexable; the sitemap plugin omits `noIndex` pages automatically. At build time, **`robotsTxtPlugin`** writes `build/robots.txt` with `Sitemap:` derived from `url` + `baseUrl` in `docusaurus.config.ts` (update those if the domain changes). Per-page titles, descriptions, and OG images live in **`seo/docSeo.json`** — run **`npm run seo:apply-doc-meta`** after adding docs.

## Versioning

Docs follow [Docusaurus versioning](https://docusaurus.io/docs/versioning): **`docs/`** is the **Next** draft (`/docs/next/...`). Published snapshots live under **`versioned_docs/`** and **`versions.json`**.

When a release is ready to freeze:

```bash
npm run docs:version 0.2.0   # example; use your semver
```

Prepend the new version to `versions.json` automatically. On the next **`npm run build`**, the previous latest moves under `/docs/<version>/` and gets **`noIndex`** — no manual SEO edits needed.

Then edit **`sidebars.ts`** only for **Next**; for an older release, edit **`versioned_sidebars/version-X-sidebars.json`** and files under **`versioned_docs/version-X/`**. Keep **`sidebars.ts`** and versioned sidebars in sync (same categories: Guides, Reference, Architecture, Frontends, Extending, Project).

## Deployment URL

`docusaurus.config.ts` sets `url` and `baseUrl` for publication (e.g. GitHub Pages project sites often use `baseUrl: '/queryflux/'`). Adjust those values to match your hosting layout.
