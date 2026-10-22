# QueryFlux documentation site

This directory is a [Docusaurus](https://docusaurus.io/) site that mirrors the root [`README.md`](https://github.com/lakeops-org/queryflux/blob/main/README.md) and the [`docs/`](https://github.com/lakeops-org/queryflux/tree/main/docs) Markdown in `docs/` here. **Canonical sources** stay in the repository root (`README.md`, `development.md`, `contribute.md`, `docs/`); edit those and refresh the copies under `website/docs/` when they drift, or automate sync in CI if you prefer.

## Commands

```bash
npm install   # if node_modules is missing
npm start     # dev server (default http://localhost:3000)
npm run build # static output in build/
npm run serve # preview production build (search works here)
```

## Analytics

**[OpenPanel](https://openpanel.dev)** (see `src/components/OpenPanelInit.tsx`) is enabled only when `OPENPANEL_CLIENT_ID` is set at build/start time. That id is a **public** project key (like GA’s `G-…`); it ends up in the static JS bundle. Abuse is limited by the OpenPanel **CORS / domain allowlist** — never put `clientSecret` in the website build.

```bash
# Local (optional — analytics off if unset)
export OPENPANEL_CLIENT_ID=<from OpenPanel → Project → Settings → API Keys>
npm start
```

In GitHub Actions, set repository/org **variable** `OPENPANEL_CLIENT_ID` (Settings → Secrets and variables → Actions → Variables). `deploy-website.yml` passes it into the build. Prefer a **variable** over a secret: the value is public in the built site anyway, and an unset Actions expression becomes `""` (we treat that as “off”). Disable explicitly with `OPENPANEL_DISABLED=true`.

**Google Analytics 4** uses [`@docusaurus/plugin-google-gtag`](https://docusaurus.io/docs/api/plugins/@docusaurus/plugin-google-gtag) via the classic preset’s `gtag` option (no extra npm install — it ships with `@docusaurus/preset-classic`). The plugin is **inactive in development** and only runs in production builds.

```js
// website/docusaurus.config.ts (preset-classic)
gtag: {
  trackingID: 'G-XXXXXXXXXX', // from env GA_MEASUREMENT_ID
  anonymizeIP: true,
},
```

Enable by setting a measurement ID at **build/deploy** time:

```bash
# GA4 → Admin → Data streams → Web → Measurement ID
export GA_MEASUREMENT_ID=G-XXXXXXXXXX
npm run build   # then npm run serve to verify (production mode)
```

In GitHub Actions, set repository/org **variable** `GA_MEASUREMENT_ID` (same place as `OPENPANEL_CLIENT_ID`). `deploy-website.yml` passes it into the build. Verify with [Google Tag Assistant](https://tagassistant.google.com/).

OpenPanel and GA4 can run together; they are independent.

## Search

Local search uses [`@cmfcmf/docusaurus-search-local`](https://github.com/cmfcmf/docusaurus-search-local) ([Docusaurus: local search](https://docusaurus.io/docs/search#using-local-search)): the index is built at compile time and shipped with the site—no Algolia or other hosted service.

The search bar appears in the navbar after a **production build**. It does **not** work with `npm start` (dev mode); use `npm run build` then `npm run serve` to try it locally. Versioned docs are supported (results follow the doc version you are viewing).

## SEO

Global metadata, JSON-LD, and `static/robots.txt` follow [Docusaurus SEO](https://docusaurus.io/docs/seo). **`seoConfig.ts`** sets `noIndex` on the **Next** draft and all **older** doc versions (everything in `versions.json` except the first entry). Only the **latest release** at `/docs/` is indexable; the sitemap plugin omits `noIndex` pages and thin blog utility URLs (`/blog/tags`, `/blog/archive`, `/blog/authors`). Priorities favor home, getting started, and blog posts. At build time, **`robotsTxtPlugin`** writes `build/robots.txt` with matching `Disallow` rules and a `Sitemap:` line from `url` + `baseUrl`. Per-doc titles, descriptions, and OG images live in **`seo/docSeo.json`** — run **`npm run seo:apply-doc-meta`** after adding docs. Blog posts should set `title` (~50–60 chars before `| QueryFlux`), `description` (~150–160 chars), `keywords`, and `image`. After building, run **`npm run seo:check-blog`** to verify every post has those fields and appears in `sitemap.xml`.

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
