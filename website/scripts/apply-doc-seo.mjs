#!/usr/bin/env node
/**
 * Syncs title, description, and image frontmatter in doc markdown files
 * from seo/docSeo.json. Run after adding or renaming docs:
 *   npm run seo:apply-doc-meta
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const websiteDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const docsDir = path.join(websiteDir, 'docs');
const versions = JSON.parse(
  fs.readFileSync(path.join(websiteDir, 'versions.json'), 'utf8'),
);
const latestReleased = versions[0];

const docRoots = [
  docsDir,
  path.join(websiteDir, 'versioned_docs', `version-${latestReleased}`),
];

const {DOC_SEO, DEFAULT_OG_IMAGE} = JSON.parse(
  fs.readFileSync(path.join(websiteDir, 'seo/docSeo.json'), 'utf8'),
);

function docIdFromPath(filePath, root) {
  return path.relative(root, filePath).replace(/\.md$/, '').replace(/\\/g, '/');
}

function parseFrontmatter(content) {
  if (!content.startsWith('---\n')) {
    return {frontmatter: {}, body: content};
  }
  const end = content.indexOf('\n---\n', 4);
  if (end === -1) {
    return {frontmatter: {}, body: content};
  }
  const raw = content.slice(4, end);
  const body = content.slice(end + 5);
  const frontmatter = {};
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
    if (value === '') {
      const listItems = [];
      const blockLines = [];
      i++;
      while (i < lines.length) {
        const next = lines[i];
        if (next.match(/^[\w_-]+:/)) break;
        if (next.startsWith('  - ')) {
          if (blockLines.length) {
            frontmatter[key] = blockLines.join('\n').trim();
            blockLines.length = 0;
          }
          listItems.push(next.slice(4));
        } else if (next.startsWith('  ')) {
          if (listItems.length) break;
          blockLines.push(next.slice(2));
        } else {
          break;
        }
        i++;
      }
      if (listItems.length) {
        frontmatter[key] = listItems;
      } else if (blockLines.length) {
        frontmatter[key] = blockLines.join('\n').trim();
      } else {
        frontmatter[key] = '';
      }
      continue;
    }
    frontmatter[key] = value.replace(/^["']|["']$/g, '');
    i++;
  }
  return {frontmatter, body};
}

function yamlScalar(value) {
  if (typeof value !== 'string') return String(value);
  if (/[:#\n]|^\s/.test(value) || value.includes('"')) {
    return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
  }
  return value;
}

function serializeFrontmatter(fm) {
  const lines = ['---'];
  const order = [
    'sidebar_position',
    'sidebar_label',
    'title',
    'description',
    'image',
    'keywords',
  ];
  const written = new Set();
  for (const key of order) {
    if (fm[key] === undefined) continue;
    written.add(key);
    if (key === 'keywords' && Array.isArray(fm[key])) {
      lines.push(`${key}:`);
      for (const kw of fm[key]) {
        lines.push(`  - ${kw}`);
      }
    } else {
      lines.push(`${key}: ${yamlScalar(String(fm[key]))}`);
    }
  }
  for (const [key, value] of Object.entries(fm)) {
    if (written.has(key)) continue;
    lines.push(`${key}: ${yamlScalar(String(value))}`);
  }
  lines.push('---', '');
  return lines.join('\n');
}

function walk(dir) {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, {withFileTypes: true});
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...walk(full));
    else if (entry.name.endsWith('.md')) files.push(full);
  }
  return files;
}

let updated = 0;
for (const root of docRoots) {
  const label = path.relative(websiteDir, root);
  for (const file of walk(root)) {
    const id = docIdFromPath(file, root);
    const seo = DOC_SEO[id];
    if (!seo) {
      console.warn(`No SEO entry for ${label}/${id}`);
      continue;
    }
    const content = fs.readFileSync(file, 'utf8');
    const {frontmatter, body} = parseFrontmatter(content);

    const next = {...frontmatter};
    next.title = seo.title;
    next.description = seo.description;
    next.image = seo.image ?? DEFAULT_OG_IMAGE;

    const out = serializeFrontmatter(next) + body.replace(/^\n/, '');
    if (out !== content) {
      fs.writeFileSync(file, out, 'utf8');
      updated++;
      console.log(`Updated ${label}/${id}`);
    }
  }
}

console.log(`Done. ${updated} file(s) updated.`);
