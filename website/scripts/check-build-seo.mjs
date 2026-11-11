#!/usr/bin/env node
/**
 * Checks every page in build/ for search and social metadata, and checks that
 * sitemap.xml, robots.txt, and llms.txt agree with each page's robots meta.
 * Run after `npm run build`:
 *   npm run seo:check-build
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {robotsAllows} from '../seo/siteFiles.mjs';

const websiteDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const buildDir = path.join(websiteDir, 'build');
const ORIGIN = 'https://queryflux.dev';
const MAX_TITLE = 70;
const MIN_DESC = 70;
const MAX_DESC = 160;

if (!fs.existsSync(path.join(buildDir, 'sitemap.xml'))) {
  console.error('✗ build/sitemap.xml missing — run npm run build first');
  process.exit(1);
}

const problems = [];
const fail = (msg) => problems.push(msg);

function walkHtml(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walkHtml(full));
    else if (entry.name.endsWith('.html')) out.push(full);
  }
  return out;
}

function pagePath(file) {
  const rel = '/' + path.relative(buildDir, file).replace(/\\/g, '/');
  if (rel === '/index.html') return '/';
  if (rel.endsWith('/index.html')) return rel.slice(0, -'/index.html'.length);
  return rel.replace(/\.html$/, '');
}

function decode(s) {
  return s
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function head(html) {
  const end = html.indexOf('</head>');
  return end === -1 ? html : html.slice(0, end);
}

function attr(tag, name) {
  const m = tag.match(new RegExp(`\\s${name}="([^"]*)"`));
  return m ? decode(m[1]) : undefined;
}

function metaValues(headHtml, key, name) {
  return [...headHtml.matchAll(/<meta\s[^>]*>/g)]
    .map((m) => m[0])
    .filter((tag) => attr(tag, key) === name)
    .map((tag) => attr(tag, 'content') ?? '');
}

function linkHrefs(headHtml, rel) {
  return [...headHtml.matchAll(/<link\s[^>]*>/g)]
    .map((m) => m[0])
    .filter((tag) => attr(tag, 'rel') === rel)
    .map((tag) => attr(tag, 'href') ?? '');
}

/** Width and height from a PNG or JPEG header. */
function imageSize(file) {
  const buf = fs.readFileSync(file);
  if (buf.readUInt32BE(0) === 0x89504e47) {
    return {width: buf.readUInt32BE(16), height: buf.readUInt32BE(20)};
  }
  if (buf[0] === 0xff && buf[1] === 0xd8) {
    let i = 2;
    while (i < buf.length) {
      const marker = buf[i + 1];
      const len = buf.readUInt16BE(i + 2);
      if (marker >= 0xc0 && marker <= 0xcf && ![0xc4, 0xc8, 0xcc].includes(marker)) {
        return {width: buf.readUInt16BE(i + 7), height: buf.readUInt16BE(i + 5)};
      }
      i += 2 + len;
    }
  }
  return null;
}

function localFile(url) {
  if (!url.startsWith(`${ORIGIN}/`)) return null;
  return path.join(buildDir, decodeURI(new URL(url).pathname));
}

const pages = [];
for (const file of walkHtml(buildDir)) {
  const pathname = pagePath(file);
  if (pathname === '/404') continue;
  const html = fs.readFileSync(file, 'utf8');
  const h = head(html);
  const robots = metaValues(h, 'name', 'robots');
  pages.push({
    pathname,
    url: pathname === '/' ? `${ORIGIN}/` : `${ORIGIN}${pathname}`,
    titles: [...h.matchAll(/<title[^>]*>([^<]*)<\/title>/g)].map((m) => decode(m[1])),
    canonicals: linkHrefs(h, 'canonical'),
    icons: linkHrefs(h, 'icon'),
    robots,
    googlebot: metaValues(h, 'name', 'googlebot'),
    noindex: robots.some((r) => /\bnoindex\b/i.test(r)),
    descriptions: metaValues(h, 'name', 'description'),
    og: {
      title: metaValues(h, 'property', 'og:title'),
      description: metaValues(h, 'property', 'og:description'),
      url: metaValues(h, 'property', 'og:url'),
      image: metaValues(h, 'property', 'og:image'),
      width: metaValues(h, 'property', 'og:image:width'),
      height: metaValues(h, 'property', 'og:image:height'),
    },
    twitter: {
      card: metaValues(h, 'name', 'twitter:card'),
      image: metaValues(h, 'name', 'twitter:image'),
    },
    h1Count: (html.match(/<h1[\s>]/g) || []).length,
  });
}

const sitemapXml = fs.readFileSync(path.join(buildDir, 'sitemap.xml'), 'utf8');
const sitemap = new Map(
  [...sitemapXml.matchAll(/<url>([\s\S]*?)<\/url>/g)].map((m) => [
    (m[1].match(/<loc>([^<]*)<\/loc>/) || [])[1],
    (m[1].match(/<lastmod>([^<]*)<\/lastmod>/) || [])[1],
  ]),
);
const robotsTxt = fs.readFileSync(path.join(buildDir, 'robots.txt'), 'utf8');
const llmsTxt = fs.readFileSync(path.join(buildDir, 'llms.txt'), 'utf8');
const llmsLinks = [...llmsTxt.matchAll(/\]\((https:\/\/queryflux\.dev[^)]*)\)/g)].map(
  (m) => m[1],
);
const byUrl = new Map(pages.map((p) => [p.url, p]));
const indexable = pages.filter((p) => !p.noindex);

for (const p of pages) {
  const where = p.pathname;
  if (p.titles.length !== 1 || !p.titles[0]) fail(`${where}: expected one non-empty <title>`);
  if (p.canonicals.length !== 1) fail(`${where}: expected one canonical, got ${p.canonicals.length}`);
  for (const c of p.canonicals) {
    if (!c.startsWith(`${ORIGIN}/`)) fail(`${where}: canonical not on ${ORIGIN}: ${c}`);
    if (c.endsWith('.html')) fail(`${where}: canonical ends in .html: ${c}`);
    if (c !== `${ORIGIN}/` && c.endsWith('/')) fail(`${where}: canonical has trailing slash: ${c}`);
  }
  if (p.robots.length > 1) fail(`${where}: ${p.robots.length} robots meta tags`);
  if (p.googlebot.length) fail(`${where}: googlebot meta can contradict robots meta`);
  if (p.descriptions.length !== 1) fail(`${where}: expected one meta description, got ${p.descriptions.length}`);
  if (p.icons.length !== 1) fail(`${where}: expected one favicon link, got ${p.icons.length}`);
  if (!robotsAllows(p.pathname, robotsTxt)) {
    fail(`${where}: blocked by robots.txt, so Google cannot read its robots meta`);
  }

  if (p.noindex) {
    if (sitemap.has(p.url)) fail(`${where}: noindex page listed in sitemap`);
    if (llmsLinks.includes(p.url)) fail(`${where}: noindex page linked from llms.txt`);
    continue;
  }

  const title = p.titles[0] ?? '';
  const description = p.descriptions[0] ?? '';
  if (p.canonicals[0] !== p.url) fail(`${where}: canonical ${p.canonicals[0]} is not the page URL`);
  if (!sitemap.has(p.url)) fail(`${where}: indexable page missing from sitemap`);
  if (p.h1Count !== 1) fail(`${where}: expected one <h1>, got ${p.h1Count}`);
  if (title.length > MAX_TITLE) fail(`${where}: title ${title.length} chars (max ${MAX_TITLE})`);
  if (description.length < MIN_DESC || description.length > MAX_DESC) {
    fail(`${where}: description ${description.length} chars (want ${MIN_DESC}–${MAX_DESC})`);
  }
  for (const [key, values] of Object.entries({
    'og:title': p.og.title,
    'og:description': p.og.description,
    'og:url': p.og.url,
    'og:image': p.og.image,
    'twitter:card': p.twitter.card,
    'twitter:image': p.twitter.image,
  })) {
    if (values.length !== 1 || !values[0]) fail(`${where}: expected one ${key}, got ${values.length}`);
  }
  if (p.og.url[0] && p.og.url[0] !== p.url) fail(`${where}: og:url ${p.og.url[0]} is not the page URL`);
  for (const image of [...p.og.image, ...p.twitter.image]) {
    const file = localFile(image);
    if (!file) fail(`${where}: social image not an absolute ${ORIGIN} URL: ${image}`);
    else if (!fs.existsSync(file)) fail(`${where}: social image missing from build: ${image}`);
  }
  const ogFile = p.og.image[0] && localFile(p.og.image[0]);
  if (ogFile && fs.existsSync(ogFile)) {
    const size = imageSize(ogFile);
    const declared = {width: Number(p.og.width[0]), height: Number(p.og.height[0])};
    if (!size) fail(`${where}: cannot read og:image size: ${p.og.image[0]}`);
    else if (p.og.width.length !== 1 || p.og.height.length !== 1) {
      fail(`${where}: og:image width/height should each appear once`);
    } else if (size.width !== declared.width || size.height !== declared.height) {
      fail(
        `${where}: og:image is ${size.width}×${size.height} but declares ${declared.width}×${declared.height}`,
      );
    } else if (size.width < 1200) {
      fail(`${where}: og:image is ${size.width}px wide (want ≥1200 for large cards)`);
    }
  }
}

for (const field of ['titles', 'descriptions']) {
  const seen = new Map();
  for (const p of indexable) {
    const value = p[field][0];
    if (!value) continue;
    if (seen.has(value)) fail(`duplicate ${field.slice(0, -1)} on ${seen.get(value)} and ${p.pathname}`);
    else seen.set(value, p.pathname);
  }
}

const today = new Date().toISOString().slice(0, 10);
for (const [loc, lastmod] of sitemap) {
  const p = byUrl.get(loc);
  if (!p) {
    fail(`sitemap: ${loc} has no built page`);
    continue;
  }
  if (p.noindex) fail(`sitemap: ${loc} is noindex`);
  const isContent = p.pathname.startsWith('/docs/') || /^\/blog\/[^/]+$/.test(p.pathname);
  if (isContent && !lastmod) fail(`sitemap: ${loc} has no lastmod`);
  if (lastmod && (Number.isNaN(Date.parse(lastmod)) || lastmod.slice(0, 10) > today)) {
    fail(`sitemap: ${loc} has invalid lastmod ${lastmod}`);
  }
}

for (const link of llmsLinks) {
  const p = byUrl.get(link);
  if (!p) fail(`llms.txt: ${link} has no built page`);
}
for (const p of indexable) {
  const isContent = p.pathname.startsWith('/docs/') || /^\/blog\/[^/]+$/.test(p.pathname);
  if (isContent && !llmsLinks.includes(p.url)) fail(`llms.txt: missing ${p.url}`);
}
if (!robotsTxt.includes(`Sitemap: ${ORIGIN}/sitemap.xml`)) fail('robots.txt: missing Sitemap line');

const icons = new Set(pages.flatMap((p) => p.icons));
for (const icon of icons) {
  const file = path.join(buildDir, decodeURI(icon.replace(ORIGIN, '')));
  const size = fs.existsSync(file) ? imageSize(file) : null;
  if (!size) fail(`favicon ${icon}: missing or unreadable`);
  else if (size.width !== size.height || size.width < 48) {
    fail(`favicon ${icon}: ${size.width}×${size.height}; Google needs a square icon of at least 48px`);
  }
}

const noindexCount = pages.length - indexable.length;
if (problems.length) {
  for (const msg of problems) console.error(`✗ ${msg}`);
  console.error(`\n${problems.length} SEO issue(s) across ${pages.length} pages`);
  process.exit(1);
}
console.log(
  `✓ ${pages.length} pages: ${indexable.length} indexable (all in sitemap and llms.txt), ` +
    `${noindexCount} noindex (crawlable, not in sitemap). ${sitemap.size} sitemap URLs.`,
);
