#!/usr/bin/env node
/**
 * Renders monospace config snippets as dark-themed SVG "editor window" images.
 * Usage: node scripts/render-blog-config-svg.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(__dirname, '../static/img/blog');

const COLORS = {
  bg: '#0c0a12',
  panel: '#12101c',
  border: 'rgba(255,255,255,0.1)',
  muted: '#71717a',
  key: '#c4b5fd',
  value: '#a3e635',
  punct: '#e4e4e7',
  comment: '#52525b',
  keyword: '#f472b6',
  fn: '#67e8f9',
  string: '#a3e635',
  default: '#d4d4d8',
};

function esc(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function yamlLine(line) {
  const trimmed = line.trimStart();
  const indent = line.length - trimmed.length;
  const parts = [];
  let x = 48 + indent * 7;

  if (trimmed.startsWith('#')) {
    parts.push({x, text: line, fill: COLORS.comment});
    return parts;
  }

  const commentIdx = trimmed.indexOf(' #');
  const main = commentIdx >= 0 ? trimmed.slice(0, commentIdx) : trimmed;
  const trailComment = commentIdx >= 0 ? trimmed.slice(commentIdx) : '';

  const colon = main.indexOf(':');
  if (colon > 0 && !main.trimStart().startsWith('-')) {
    const key = main.slice(0, colon + 1);
    const rest = main.slice(colon + 1);
    parts.push({x, text: ' '.repeat(indent) + key, fill: COLORS.key});
    x += (indent + key.length) * 7.1;
    if (rest.length) {
      const val = rest.trim();
      const fill =
        val === 'true' || val === 'false' || /^\d+$/.test(val)
          ? COLORS.keyword
          : val.startsWith('"') || val.startsWith("'")
            ? COLORS.string
            : COLORS.default;
      parts.push({x, text: rest, fill});
    }
  } else {
    parts.push({x, text: line, fill: COLORS.default});
  }

  if (trailComment) {
    x += (main.length - indent) * 7.1;
    parts.push({x, text: trailComment, fill: COLORS.comment});
  }

  return parts;
}

function pyLine(line) {
  const parts = [];
  let x = 48;
  const trimmed = line.trim();
  if (!trimmed) {
    return [{x, text: '', fill: COLORS.default}];
  }
  if (trimmed.startsWith('#')) {
    return [{x, text: line, fill: COLORS.comment}];
  }
  if (trimmed.startsWith('def ')) {
    return [{x, text: line, fill: COLORS.fn}];
  }
  if (trimmed.includes('return ')) {
    const idx = line.indexOf('return ');
    parts.push({x, text: line.slice(0, idx + 7), fill: COLORS.keyword});
    x += (idx + 7) * 7.1;
    parts.push({x, text: line.slice(idx + 7), fill: COLORS.string});
    return parts;
  }
  return [{x, text: line, fill: COLORS.default}];
}

function bashLine(line) {
  if (line.startsWith('$ ') || line.startsWith('> ')) {
    return [
      {x: 48, text: line.slice(0, 2), fill: COLORS.keyword},
      {x: 48 + 14, text: line.slice(2), fill: COLORS.default},
    ];
  }
  return [{x: 48, text: line, fill: COLORS.default}];
}

function renderWindow({filename, title, lines, lineHeight = 20, lang = 'yaml'}) {
  const lineFn = lang === 'python' ? pyLine : lang === 'bash' ? bashLine : yamlLine;
  const headerH = 44;
  const padB = 24;
  const width = lang === 'bash' ? 820 : 760;
  const height = headerH + lines.length * lineHeight + padB;

  const textEls = lines
    .map((line, i) => {
      const y = headerH + 18 + i * lineHeight;
      const segments = lineFn(line);
      return segments
        .map(
          (seg) =>
            `<text x="${seg.x}" y="${y}" fill="${seg.fill}" font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" font-size="13">${esc(seg.text)}</text>`,
        )
        .join('\n');
    })
    .join('\n');

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(title)}">
  <defs>
    <linearGradient id="glow" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#7c3aed" stop-opacity="0.35"/>
      <stop offset="100%" stop-color="#ff7619" stop-opacity="0.12"/>
    </linearGradient>
  </defs>
  <rect width="100%" height="100%" rx="14" fill="${COLORS.bg}"/>
  <rect x="1" y="1" width="${width - 2}" height="${height - 2}" rx="13" fill="none" stroke="url(#glow)" stroke-width="1"/>
  <rect x="0" y="0" width="${width}" height="${headerH}" rx="14" fill="${COLORS.panel}"/>
  <rect x="0" y="${headerH - 8}" width="${width}" height="8" fill="${COLORS.panel}"/>
  <circle cx="20" cy="22" r="5" fill="#fb7185"/>
  <circle cx="38" cy="22" r="5" fill="#fbbf24"/>
  <circle cx="56" cy="22" r="5" fill="#a3e635"/>
  <text x="80" y="27" fill="${COLORS.muted}" font-family="ui-monospace, Menlo, monospace" font-size="12">${esc(filename)}</text>
  ${textEls}
</svg>`;
}

const agentsConfig = `auth:
  provider: oidc
  required: true
  oidc:
    issuer: https://idp.example.com/realms/queryflux
    jwksUri: https://idp.example.com/realms/queryflux/protocol/openid-connect/certs
    audience: queryflux
    groupsClaim: groups

queryflux:
  frontends:
    mcp:
      enabled: true
      port: 8811

clusters:
  trino-1:
    engine: trino
    endpoint: https://trino.internal:8443
    auth:                  # QueryFlux service credential
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:             # identity each query runs as
      type: impersonate

authorization:
  provider: none

clusterGroups:
  analysts:
    maxRunningQueries: 40
    members: [trino-1]
    authorization:
      allowGroups: [analysts]   # blocks MCP engine_hint bypass
  agents:
    maxRunningQueries: 8
    members: [trino-1]
    authorization:
      allowGroups: [agents]

routers:
  - type: protocolBased
    mcp: agents
routingFallback: analysts

guardrails:
  groups:
    agents:
      - kind: built_in
        name: read_only
      - kind: built_in
        name: row_limit
        max_rows: 5000
      - kind: built_in
        name: require_predicate
        applies_to: ["fct_*", "events.*"]`.split('\n');

const routingYaml = `routers:
  - type: queryRegex
    rules:
      - regex: "FROM local\\\\..*"
        targetGroup: duckdb-local
  - type: protocolBased
    trinoHttp: trino-default`.split('\n');

const routingPy = `def route(query: str, ctx: dict) -> str | None:
    # Small, local queries -> DuckDB
    if "local." in query and "JOIN" not in query.upper():
        return "duckdb-local"
    return None`.split('\n');

const mcpBash = `$ claude mcp add --transport http queryflux \\
    http://localhost:8811/mcp \\
    --header "Authorization: Bearer $QUERYFLUX_MCP_TOKEN"`.split('\n');

fs.mkdirSync(outDir, {recursive: true});

fs.writeFileSync(
  path.join(outDir, 'agents-mcp-config.svg'),
  renderWindow({
    filename: 'config.yaml',
    title: 'QueryFlux agent MCP configuration',
    lines: agentsConfig,
  }),
);

fs.writeFileSync(
  path.join(outDir, 'routing-rules-yaml.svg'),
  renderWindow({
    filename: 'routing.yaml',
    title: 'QueryFlux routing rules YAML',
    lines: routingYaml,
  }),
);

fs.writeFileSync(
  path.join(outDir, 'routing-rules-python.svg'),
  renderWindow({
    filename: 'router.py',
    title: 'QueryFlux custom Python router',
    lines: routingPy,
    lang: 'python',
  }),
);

fs.writeFileSync(
  path.join(outDir, 'mcp-connect.svg'),
  renderWindow({
    filename: 'terminal',
    title: 'Connect Claude to QueryFlux MCP',
    lines: mcpBash,
    lang: 'bash',
    lineHeight: 24,
  }),
);

console.log('Wrote config SVGs to', outDir);
