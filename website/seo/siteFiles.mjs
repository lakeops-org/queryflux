/**
 * robots.txt and llms.txt shared by the dev static copies and the production build.
 * Run: node seo/siteFiles.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';

const websiteDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
export const SITE_URL = 'https://queryflux.dev';

const DOC_SEO = JSON.parse(
  fs.readFileSync(path.join(websiteDir, 'seo/docSeo.json'), 'utf8'),
).DOC_SEO;

/** Longest matching rule wins; Allow wins a tie. Mirrors Google's robots.txt spec. */
export function robotsAllows(pathname, robotsTxt) {
  const rules = [];
  for (const line of robotsTxt.split('\n')) {
    const allow = line.match(/^Allow:\s*(\S+)\s*$/);
    const disallow = line.match(/^Disallow:\s*(\S+)\s*$/);
    if (allow) rules.push({allow: true, path: allow[1]});
    if (disallow) rules.push({allow: false, path: disallow[1]});
  }
  let best = null;
  for (const rule of rules) {
    if (!ruleMatches(pathname, rule.path)) continue;
    if (
      !best ||
      rule.path.length > best.path.length ||
      (rule.path.length === best.path.length && rule.allow && !best.allow)
    ) {
      best = rule;
    }
  }
  return best ? best.allow : true;
}

function ruleMatches(pathname, rulePath) {
  let pattern = rulePath;
  let anchored = false;
  if (pattern.endsWith('$')) {
    anchored = true;
    pattern = pattern.slice(0, -1);
  }
  if (pattern.endsWith('*')) {
    pattern = pattern.slice(0, -1);
  }
  if (!pattern.includes('*')) {
    return anchored ? pathname === pattern : pathname.startsWith(pattern);
  }
  const parts = pattern.split('*');
  let pos = 0;
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part === '') continue;
    if (i === 0) {
      if (!pathname.startsWith(part)) return false;
      pos = part.length;
      continue;
    }
    const idx = pathname.indexOf(part, pos);
    if (idx < 0) return false;
    pos = idx + part.length;
  }
  return anchored ? pos === pathname.length : true;
}

export function buildRobotsTxt(siteUrl = SITE_URL) {
  const sitemapUrl = `${siteUrl.replace(/\/$/, '')}/sitemap.xml`;
  return `# Generated at build time — do not edit build/robots.txt by hand.
# https://docusaurus.io/docs/seo#robots-file
# Latest docs are /docs/. Drafts are /docs/next/ and carry noindex.
# Allow lines are longer than the draft Disallow, so indexable docs stay crawlable.
# GitHub Pages also serves each page at a .html URL; those duplicates are disallowed.
# LLM index: ${siteUrl.replace(/\/$/, '')}/llms.txt

User-agent: *
Allow: /llms.txt$
Allow: /docs/architecture/frontends/overview$
Allow: /docs/architecture/query-translation$
Allow: /docs/architecture/auth-authz-design$
Allow: /docs/
Allow: /blog/
Allow: /
Disallow: /docs/next$
Disallow: /docs/next/
Disallow: /blog/tags$
Disallow: /blog/tags/
Disallow: /blog/archive$
Disallow: /blog/archive/
Disallow: /blog/authors$
Disallow: /blog/authors/
Disallow: /index.html$
Disallow: /community.html$
Disallow: /docs/*.html$
Disallow: /blog/*.html$

Sitemap: ${sitemapUrl}
`;
}

function parseFrontmatter(content) {
  if (!content.startsWith('---\n')) return {};
  const end = content.indexOf('\n---\n', 4);
  if (end === -1) return {};
  const fm = {};
  let key = null;
  for (const line of content.slice(4, end).split('\n')) {
    const m = line.match(/^([\w_-]+):\s*(.*)$/);
    if (m && !line.startsWith(' ')) {
      key = m[1];
      fm[key] = m[2].replace(/^["']|["']$/g, '');
      continue;
    }
    if (key && line.startsWith('  - ')) {
      if (!Array.isArray(fm[key])) fm[key] = [];
      fm[key].push(line.slice(4).replace(/^["']|["']$/g, ''));
    }
  }
  return fm;
}

function walkMarkdown(dir) {
  if (!fs.existsSync(dir)) return [];
  const out = [];
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walkMarkdown(full));
    else if (entry.name.endsWith('.md')) out.push(full);
  }
  return out;
}

function sectionFor(id) {
  if (id.startsWith('architecture/frontends/')) return 'Frontends';
  if (id.startsWith('architecture/adding-support/')) return 'Extending QueryFlux';
  if (id.startsWith('architecture/')) return 'Architecture';
  if (id.startsWith('quickstart/')) return 'Quickstarts';
  if (id.startsWith('installation/')) return 'Install';
  if (id.startsWith('access-control/')) return 'Access control';
  if (id.startsWith('agentic/')) return 'Agents';
  if (id.startsWith('operations/')) return 'Operations';
  return 'Reference';
}

const SECTION_ORDER = [
  'Start here',
  'Quickstarts',
  'Install',
  'Agents',
  'Access control',
  'Architecture',
  'Frontends',
  'Extending QueryFlux',
  'Operations',
  'Reference',
];

const START_HERE = new Set([
  'intro',
  'getting-started',
  'configuration',
  'studio',
  'authentication',
]);

export function buildLlmsTxt(siteUrl = SITE_URL) {
  const origin = siteUrl.replace(/\/$/, '');
  const versions = JSON.parse(
    fs.readFileSync(path.join(websiteDir, 'versions.json'), 'utf8'),
  );
  const latest = versions[0];
  const docsRoot = path.join(websiteDir, 'versioned_docs', `version-${latest}`);
  const groups = new Map(SECTION_ORDER.map((name) => [name, []]));

  for (const file of walkMarkdown(docsRoot)) {
    const id = path.relative(docsRoot, file).replace(/\.md$/, '').replace(/\\/g, '/');
    const fm = parseFrontmatter(fs.readFileSync(file, 'utf8'));
    if (fm.unlisted === 'true' || fm.draft === 'true') continue;
    const seo = DOC_SEO[id] || {};
    const title = fm.title || seo.title || id;
    const description = (fm.description || seo.description || '').replace(/\s+/g, ' ');
    const section = START_HERE.has(id) ? 'Start here' : sectionFor(id);
    groups.get(section).push({
      title,
      description,
      url: `${origin}/docs/${id}`,
    });
  }

  const posts = fs
    .readdirSync(path.join(websiteDir, 'blog'))
    .filter((name) => name.endsWith('.md'))
    .map((name) => {
      const fm = parseFrontmatter(
        fs.readFileSync(path.join(websiteDir, 'blog', name), 'utf8'),
      );
      const slug =
        fm.slug || name.replace(/^\d{4}-\d{2}-\d{2}-/, '').replace(/\.md$/, '');
      return {
        date: name.slice(0, 10),
        title: fm.title || slug,
        description: (fm.description || '').replace(/\s+/g, ' '),
        url: `${origin}/blog/${slug}`,
      };
    })
    .sort((a, b) => (a.date < b.date ? 1 : -1));

  const lines = [
    '# QueryFlux',
    '',
    '> Open-source SQL proxy and router. One endpoint for tools and AI agents across Trino, DuckDB, StarRocks, ClickHouse, Snowflake, and more — with routing, dialect translation, guardrails, access control, and a record of every query.',
    '',
    'QueryFlux sits in front of engines you already run. Clients keep their drivers (Trino HTTP, PostgreSQL wire, MySQL wire, Snowflake HTTP, Arrow Flight SQL, MCP). QueryFlux chooses the engine, translates the dialect, applies guardrails and row filters, and records the query. The canonical site is https://queryflux.dev/ — use that host, not www.',
    '',
    `- [Home](${origin}/): product overview`,
    `- [Blog](${origin}/blog): essays and how-to posts`,
    `- [Community](${origin}/community): Slack and GitHub Discussions`,
    `- [Source](https://github.com/lakeops-org/queryflux): Apache-2.0 repository`,
    '',
  ];

  for (const section of SECTION_ORDER) {
    const items = groups.get(section);
    if (!items?.length) continue;
    items.sort((a, b) => a.title.localeCompare(b.title));
    lines.push(`## ${section}`, '');
    for (const item of items) {
      lines.push(
        item.description
          ? `- [${item.title}](${item.url}): ${item.description}`
          : `- [${item.title}](${item.url})`,
      );
    }
    lines.push('');
  }

  lines.push('## Blog', '');
  for (const post of posts) {
    lines.push(
      post.description
        ? `- [${post.title}](${post.url}): ${post.description}`
        : `- [${post.title}](${post.url})`,
    );
  }
  lines.push(
    '',
    '## Optional',
    '',
    '- Draft and older doc snapshots are noindex. Prefer the `/docs/` links above.',
    '- Thin blog indexes (`/blog/tags`, `/blog/archive`, `/blog/authors`) are not useful as citations.',
    '',
  );
  return lines.join('\n');
}

const isMain =
  process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const staticDir = path.join(websiteDir, 'static');
  fs.writeFileSync(path.join(staticDir, 'robots.txt'), buildRobotsTxt(), 'utf8');
  fs.writeFileSync(path.join(staticDir, 'llms.txt'), buildLlmsTxt(), 'utf8');
  console.log('Wrote static/robots.txt and static/llms.txt');
}
