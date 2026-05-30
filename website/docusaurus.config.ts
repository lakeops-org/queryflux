import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {
  buildDocsVersionConfig,
  buildSitemapConfig,
  robotsTxtPlugin,
} from './seoConfig';

const siteUrl = 'https://queryflux.dev';
const siteCanonicalUrl = siteUrl;
const siteDescription =
  'Universal SQL query proxy and router in Rust. One endpoint for Trino, PostgreSQL, MySQL, Snowflake HTTP, Flight, and more — with routing, queueing, and observability.';

const config: Config = {
  title: 'QueryFlux',
  tagline: 'Universal SQL query proxy and router in Rust',
  favicon: 'img/queryflux-hero-banner.png',

  url: siteUrl,
  baseUrl: '/',

  // Used by `npm run deploy` to pick the target repo — must match `git remote` (org/repo).
  organizationName: 'lakeops-org',
  projectName: 'queryflux',

  // https://docusaurus.io/docs/deployment#deploying-to-github-pages
  trailingSlash: false,

  onBrokenLinks: 'throw',

  // https://docusaurus.io/docs/seo — global <head> injection
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'preconnect',
        href: 'https://github.com',
      },
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareSourceCode',
        name: 'QueryFlux',
        description:
          'Universal SQL query proxy and router in Rust. Front protocols include Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake HTTP wire + SQL API v2, and Arrow Flight; backends include Trino, DuckDB, StarRocks, and more with routing and sqlglot dialect translation.',
        url: siteCanonicalUrl,
        codeRepository: 'https://github.com/lakeops-org/queryflux',
        license: 'https://www.apache.org/licenses/LICENSE-2.0',
        programmingLanguage: 'Rust',
      }),
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'WebSite',
        name: 'QueryFlux',
        url: siteCanonicalUrl,
        description:
          'Documentation and resources for QueryFlux, a universal SQL query proxy and router.',
      }),
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // https://docusaurus.io/docs/search#using-local-search — offline index, no Algolia
  plugins: [
    robotsTxtPlugin,
    [
      '@cmfcmf/docusaurus-search-local',
      {
        indexBlog: false,
        indexPages: true,
        language: 'en',
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/lakeops-org/queryflux/tree/main/website/',
          // https://docusaurus.io/docs/versioning — latest = first entry in versions.json
          // SEO: only latest is indexable; see seoConfig.ts (reads versions.json each build)
          versions: buildDocsVersionConfig(),
        },
        blog: false,
        sitemap: buildSitemapConfig(),
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Default og/twitter image (homepage hero banner); width/height in metadata below.
    image: 'img/queryflux-hero-banner.png',
    // New visitors start in dark; do not follow OS prefers-color-scheme.
    colorMode: {
      defaultMode: 'dark',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'QueryFlux',
      hideOnScroll: false,
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
          className: 'navbar-item-docs-mobile',
        },
        {
          href: 'https://lakeops.dev/blog/category/queryflux',
          label: 'Blog',
          position: 'left',
        },
        {
          to: '/community',
          label: 'Community',
          position: 'left',
        },
        {
          type: 'docsVersionDropdown',
          position: 'left',
          className: 'navbar-item-collapse-mobile',
        },
        {
          type: 'search',
          position: 'left',
        },
        {
          href: 'https://github.com/lakeops-org/queryflux/tree/main',
          position: 'right',
          className: 'navbar-link-github navbar-item-collapse-mobile',
          'aria-label': 'QueryFlux repository on GitHub',
          html: '<svg width="22" height="22" viewBox="0 0 98 96" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path fill="currentColor" fill-rule="evenodd" clip-rule="evenodd" d="M48.854 0C21.839 0 0 22 0 49.217c0 21.756 13.993 40.172 33.405 46.69 2.427.49 3.316-1.059 3.316-2.362 0-1.141-.08-5.052-.08-9.127-13.59 2.934-16.42-5.867-16.42-5.867-2.184-5.704-5.42-7.17-5.42-7.17-4.448-3.015.324-3.015.324-3.015 4.934.326 7.523 5.052 7.523 5.052 4.367 7.496 11.404 5.378 14.235 4.074.404-3.178 1.699-5.378 3.074-6.6-10.839-1.195-22.179-5.378-22.179-24.057 0-5.378 1.939-9.778 5.014-13.173-.503-1.196-2.184-6.02.478-12.518 0 0 4.075-1.302 13.406 4.994 4.002-1.079 8.29-1.619 12.548-1.619 4.259 0 8.546.54 12.548 1.619 9.318-6.296 13.393-4.994 13.393-4.994 2.662 6.498 1.003 11.322.478 12.518 3.08 3.395 5.014 7.795 5.014 13.173 0 18.795-11.354 22.848-22.194 24.043 1.741 1.508 3.302 4.407 3.302 8.927 0 6.434-.057 11.621-.057 13.173 0 1.304.869 2.852 3.316 2.367 19.394-6.518 33.382-24.934 33.382-46.69C97.708 22 75.788 0 48.854 0z"/></svg>',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Overview', to: '/docs/intro'},
            {label: 'Getting started', to: '/docs/getting-started'},
            {label: 'Studio', to: '/docs/studio'},
            {label: 'Configuration', to: '/docs/configuration'},
            {label: 'Architecture', to: '/docs/architecture/overview'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'Community', to: '/community'},
            {label: 'Development', to: '/docs/development'},
            {label: 'Contribute', to: '/docs/contribute'},
            {label: 'Roadmap', to: '/docs/roadmap'},
          ],
        },
        {
          title: 'Repository',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/lakeops-org/queryflux',
            },
            {
              label: 'Issues',
              href: 'https://github.com/lakeops-org/queryflux/issues',
            },
            {
              label: 'Discussions',
              href: 'https://github.com/lakeops-org/queryflux/discussions',
            },
          ],
        },
        {
          title: null,
          className: 'footer__col--lakeops',
          items: [],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} QueryFlux contributors. QueryFlux documentation — Apache-2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'yaml', 'bash', 'python'],
    },
    // https://docusaurus.io/docs/seo#global-metadata — site-wide defaults; per-page title/description/OG come from Layout or doc frontmatter
    metadata: [
      {
        name: 'keywords',
        content:
          'LakeOps, QueryFlux, SQL proxy, query router, Trino, PostgreSQL, MySQL, Arrow Flight, DuckDB, StarRocks, Rust, sqlglot, data lake',
      },
      {
        name: 'description',
        content: siteDescription,
      },
      {
        name: 'robots',
        content: 'index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1',
      },
      {
        name: 'googlebot',
        content: 'index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1',
      },
      {property: 'og:site_name', content: 'QueryFlux'},
      {property: 'og:type', content: 'website'},
      {property: 'og:locale', content: 'en_US'},
      {property: 'og:image:alt', content: 'QueryFlux multi-engine query routing overview'},
      {name: 'twitter:card', content: 'summary_large_image'},
      {name: 'twitter:site', content: '@lakeops'},
      {name: 'twitter:image:alt', content: 'QueryFlux multi-engine query routing overview'},
      {property: 'og:image:width', content: '1024'},
      {property: 'og:image:height', content: '682'},
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;
