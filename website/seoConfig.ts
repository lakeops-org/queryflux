import fs from 'node:fs';
import path from 'node:path';
import type {LoadContext, Plugin} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type {PluginOptions as SitemapPluginOptions} from '@docusaurus/plugin-sitemap';
import docSeoJson from './seo/docSeo.json';

export const SITE_NAME = 'QueryFlux';
export const DEFAULT_OG_IMAGE = docSeoJson.DEFAULT_OG_IMAGE;
export const DOC_SEO = docSeoJson.DOC_SEO;

type DocsVersionsConfig = NonNullable<
  NonNullable<Preset.Options['docs']>['versions']
>;

/** UTC calendar date (YYYY-MM-DD) — used as sitemap lastmod on each build/deploy. */
export function sitemapLastModDate(): string {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Reads versions.json and returns Docusaurus docs version metadata.
 *
 * SEO rule (automatic on every build):
 * - "Next" (current / docs/) → noIndex (draft, not for search)
 * - Latest release (versions.json[0], served at /docs/) → indexable
 * - All older snapshots → noIndex + unmaintained banner
 */
export function buildDocsVersionConfig(): DocsVersionsConfig {
  const versionsPath = path.join(process.cwd(), 'versions.json');
  const released = JSON.parse(
    fs.readFileSync(versionsPath, 'utf8'),
  ) as string[];
  const older = released.slice(1);

  const versions: DocsVersionsConfig = {
    current: {
      label: 'Next',
      path: 'next',
      banner: 'unreleased',
      noIndex: true,
    },
  };

  for (const version of older) {
    versions[version] = {
      noIndex: true,
      banner: 'unmaintained',
    };
  }

  return versions;
}

/** Paths that are thin / utility and should not compete in search. */
function shouldOmitFromSitemap(pathname: string): boolean {
  if (pathname.startsWith('/docs/next')) {
    return true;
  }
  // Older doc versions are noIndex; omit if they still appear.
  if (/^\/docs\/\d+\.\d+(\.\d+)?(\/|$)/.test(pathname)) {
    return true;
  }
  if (
    pathname === '/blog/archive' ||
    pathname === '/blog/authors' ||
    pathname === '/blog/tags' ||
    pathname.startsWith('/blog/tags/')
  ) {
    return true;
  }
  return false;
}

function sitemapPriority(pathname: string): number {
  if (pathname === '/') {
    return 1.0;
  }
  if (pathname === '/docs/intro' || pathname === '/docs/getting-started') {
    return 0.9;
  }
  if (pathname === '/blog') {
    return 0.85;
  }
  // Individual posts: /blog/<slug> (not /blog itself)
  if (pathname.startsWith('/blog/') && !pathname.slice('/blog/'.length).includes('/')) {
    return 0.85;
  }
  if (
    pathname.startsWith('/docs/quickstart/') ||
    pathname === '/docs/configuration' ||
    pathname === '/docs/architecture/overview' ||
    pathname === '/docs/architecture/routing-and-clusters' ||
    pathname === '/docs/architecture/guardrails' ||
    pathname === '/docs/access-control/overview' ||
    pathname === '/docs/agentic/overview' ||
    pathname === '/docs/quickstart/ai-agent-mcp'
  ) {
    return 0.8;
  }
  if (pathname === '/community') {
    return 0.6;
  }
  if (pathname.startsWith('/docs/')) {
    return 0.7;
  }
  return 0.5;
}

/** Sitemap tuned for SEO; lastmod refreshes on every build (schedule deploy daily at midnight UTC). */
export function buildSitemapConfig(): Partial<SitemapPluginOptions> {
  const lastmod = sitemapLastModDate();

  return {
    lastmod: 'date',
    changefreq: 'weekly',
    priority: 0.5,
    createSitemapItems: async (params) => {
      const items = await params.defaultCreateSitemapItems(params);
      return items
        .map((item) => {
          let pathname: string;
          try {
            pathname = new URL(item.url).pathname.replace(/\/$/, '') || '/';
          } catch {
            pathname = item.url.replace(/\/$/, '') || '/';
          }
          return {item, pathname};
        })
        .filter(({pathname}) => !shouldOmitFromSitemap(pathname))
        .map(({item, pathname}) => ({
          ...item,
          lastmod,
          changefreq: 'weekly' as const,
          priority: sitemapPriority(pathname),
        }));
    },
  };
}

/** Writes robots.txt at build time so Sitemap: always matches config url/baseUrl. */
export function robotsTxtPlugin(_context: LoadContext): Plugin {
  return {
    name: 'queryflux-robots-txt',
    async postBuild({outDir, siteConfig}) {
      const base = siteConfig.baseUrl.replace(/\/$/, '');
      const sitemapUrl = `${siteConfig.url}${base}/sitemap.xml`;
      const contents = `# Generated at build time — do not edit build/robots.txt by hand.
# https://docusaurus.io/docs/seo#robots-file
User-agent: *
Allow: /
# Unreleased "Next" docs (also noIndex)
Disallow: /docs/next
Disallow: /docs/next/
# Thin blog utility pages (also omitted from sitemap)
Disallow: /blog/tags
Disallow: /blog/tags/
Disallow: /blog/archive
Disallow: /blog/authors

Sitemap: ${sitemapUrl}
`;
      fs.writeFileSync(path.join(outDir, 'robots.txt'), contents, 'utf8');
    },
  };
}
