#!/usr/bin/env node
/**
 * Validates blog posts have SEO frontmatter and that each slug appears in the sitemap.
 * Run after `npm run build`:
 *   npm run seo:check-blog
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const websiteDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const blogDir = path.join(websiteDir, 'blog');
const sitemapPath = path.join(websiteDir, 'build', 'sitemap.xml');
const staticImgDir = path.join(websiteDir, 'static');

const MAX_TITLE = 60; // without " | QueryFlux"
const MAX_DESC = 165;
const MIN_DESC = 70;

function parseFrontmatter(content) {
  if (!content.startsWith('---\n')) {
    return null;
  }
  const end = content.indexOf('\n---\n', 4);
  if (end === -1) {
    return null;
  }
  const raw = content.slice(4, end);
  const fm = {};
  const lines = raw.split('\n');
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const m = line.match(/^([\w_-]+):\s*(.*)$/);
    if (!m) {
      i++;
      continue;
    }
    const [, key, value] = m;
    if (value === '' || value === '|' || value === '>') {
      const list = [];
      i++;
      while (i < lines.length && /^\s+-\s+/.test(lines[i])) {
        list.push(lines[i].replace(/^\s+-\s+/, '').replace(/^["']|["']$/g, ''));
        i++;
      }
      fm[key] = list.length ? list : '';
      continue;
    }
    fm[key] = value.replace(/^["']|["']$/g, '');
    i++;
  }
  return fm;
}

const posts = fs
  .readdirSync(blogDir)
  .filter((f) => f.endsWith('.md'))
  .map((f) => {
    const content = fs.readFileSync(path.join(blogDir, f), 'utf8');
    return {file: f, fm: parseFrontmatter(content)};
  });

let errors = 0;
function fail(msg) {
  console.error(`✗ ${msg}`);
  errors++;
}
function ok(msg) {
  console.log(`✓ ${msg}`);
}

if (!posts.length) {
  fail('No blog posts found');
}

const slugs = [];
for (const {file, fm} of posts) {
  if (!fm) {
    fail(`${file}: missing frontmatter`);
    continue;
  }
  const slug = fm.slug || file.replace(/^\d{4}-\d{2}-\d{2}-/, '').replace(/\.md$/, '');
  slugs.push(slug);

  if (!fm.title) {
    fail(`${file}: missing title`);
  } else if (fm.title.length > MAX_TITLE) {
    fail(`${file}: title ${fm.title.length} chars (max ${MAX_TITLE} before site name): ${fm.title}`);
  } else {
    ok(`${file}: title (${fm.title.length} chars)`);
  }

  if (!fm.description) {
    fail(`${file}: missing description`);
  } else if (fm.description.length > MAX_DESC || fm.description.length < MIN_DESC) {
    fail(
      `${file}: description ${fm.description.length} chars (want ${MIN_DESC}–${MAX_DESC})`,
    );
  } else {
    ok(`${file}: description (${fm.description.length} chars)`);
  }

  if (!fm.image) {
    fail(`${file}: missing image (OG / BlogPosting)`);
  } else {
    const imgPath = path.join(staticImgDir, fm.image);
    if (!fs.existsSync(imgPath)) {
      fail(`${file}: image not found at static/${fm.image}`);
    } else {
      ok(`${file}: image ${fm.image}`);
    }
  }

  if (!Array.isArray(fm.keywords) || fm.keywords.length < 3) {
    fail(`${file}: need keywords list (≥3)`);
  } else {
    ok(`${file}: keywords (${fm.keywords.length})`);
  }
}

if (fs.existsSync(sitemapPath)) {
  const xml = fs.readFileSync(sitemapPath, 'utf8');
  for (const slug of slugs) {
    const loc = `https://queryflux.dev/blog/${slug}`;
    if (!xml.includes(`<loc>${loc}</loc>`)) {
      fail(`sitemap missing ${loc} — run npm run build`);
    } else {
      ok(`sitemap: ${loc}`);
    }
  }
  if (xml.includes('/blog/tags') || xml.includes('/blog/archive')) {
    fail('sitemap still lists thin blog utility pages');
  }
} else {
  console.warn('⚠ build/sitemap.xml missing — skip sitemap checks (run npm run build)');
}

if (errors) {
  console.error(`\n${errors} blog SEO issue(s)`);
  process.exit(1);
}
console.log(`\nAll ${posts.length} blog posts pass SEO checks.`);
