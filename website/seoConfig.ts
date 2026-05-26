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

/** Sitemap tuned for SEO; lastmod refreshes on every build (schedule deploy daily at midnight UTC). */
export function buildSitemapConfig(): Partial<SitemapPluginOptions> {
  const lastmod = sitemapLastModDate();

  return {
    lastmod: 'date',
    changefreq: 'weekly',
    priority: 0.5,
    createSitemapItems: async (params) => {
      const items = await params.defaultCreateSitemapItems(params);
      return items.map((item) => {
        let pathname: string;
        try {
          pathname = new URL(item.url).pathname.replace(/\/$/, '') || '/';
        } catch {
          pathname = item.url.replace(/\/$/, '') || '/';
        }
        let priority = 0.7;
        if (pathname === '/') {
          priority = 1.0;
        } else if (pathname === '/community') {
          priority = 0.6;
        } else if (pathname === '/docs/intro') {
          priority = 0.9;
        }
        return {
          ...item,
          lastmod,
          changefreq: 'weekly' as const,
          priority,
        };
      });
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
Disallow:

Sitemap: ${sitemapUrl}
`;
      fs.writeFileSync(path.join(outDir, 'robots.txt'), contents, 'utf8');
    },
  };
}
