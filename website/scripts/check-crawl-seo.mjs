#!/usr/bin/env node
/**
 * Checks robots.txt lets crawlers reach every page, including noindex ones, and that
 * llms.txt links the indexable URLs. Page-level checks are in check-build-seo.mjs.
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {buildLlmsTxt, buildRobotsTxt, robotsAllows} from '../seo/siteFiles.mjs';

const websiteDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const robots = buildRobotsTxt();
const llms = buildLlmsTxt();
const staticRobots = fs.readFileSync(path.join(websiteDir, 'static/robots.txt'), 'utf8');
const staticLlms = fs.readFileSync(path.join(websiteDir, 'static/llms.txt'), 'utf8');

let errors = 0;
function fail(msg) {
  console.error(`✗ ${msg}`);
  errors++;
}
function ok(msg) {
  console.log(`✓ ${msg}`);
}

if (staticRobots !== robots) {
  fail('static/robots.txt is stale — run npm run seo:files');
} else {
  ok('static/robots.txt matches generator');
}
if (staticLlms !== llms) {
  fail('static/llms.txt is stale — run npm run seo:files');
} else {
  ok('static/llms.txt matches generator');
}

// Noindex pages must stay crawlable: Google reports a blocked page as
// "Blocked by robots.txt" and never reads its noindex tag.
const crawlable = [
  '/',
  '/llms.txt',
  '/docs/getting-started',
  '/docs/architecture/frontends/overview',
  '/blog/why-queryflux',
  '/community',
  '/docs/next/intro',
  '/docs/next/architecture/query-translation',
  '/docs/0.1.2/architecture/frontends/overview',
  '/blog/tags/guides',
  '/blog/archive',
  '/blog/authors',
];

for (const pathname of crawlable) {
  if (!robotsAllows(pathname, robots)) fail(`robots blocks ${pathname}`);
  else ok(`crawlable ${pathname}`);
}

for (const pathname of [
  '/docs/architecture/frontends/overview',
  '/docs/architecture/query-translation',
  '/docs/architecture/auth-authz-design',
  '/blog/why-queryflux',
]) {
  const url = `https://queryflux.dev${pathname}`;
  if (!llms.includes(url)) fail(`llms.txt missing ${url}`);
  else ok(`llms ${url}`);
}
if (llms.includes('/docs/next/')) fail('llms.txt links draft /docs/next/ pages');
else ok('llms.txt does not link drafts');

if (!llms.startsWith('# QueryFlux\n')) fail('llms.txt missing H1');
if (!robots.includes('Sitemap: https://queryflux.dev/sitemap.xml')) {
  fail('robots.txt missing sitemap');
}

if (errors) {
  console.error(`\n${errors} crawl SEO issue(s)`);
  process.exit(1);
}
console.log('\nCrawl SEO checks passed.');
