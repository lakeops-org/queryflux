import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * Layout inspired by clear doc hubs (e.g. ProxySQL): Overview → Guides → Reference → deep dives.
 */
const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Overview',
    },
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      items: ['getting-started', 'studio', 'authentication'],
    },
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: ['operations/multi-replica'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: ['configuration'],
    },
    {
      type: 'category',
      label: 'Agentic AI',
      collapsed: false,
      items: [
        'agentic/overview',
        'agentic/agent-context',
        'agentic/session-replay',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      collapsed: true,
      items: [
        'architecture/overview',
        'architecture/motivation-and-goals',
        'architecture/system-map',
        'architecture/query-translation',
        'architecture/routing-and-clusters',
        'architecture/cluster-variants-and-health',
        'architecture/caching',
        'architecture/query-tags',
        'architecture/query-params',
        'architecture/guardrails',
        'architecture/observability',
        'architecture/adding-engine-support',
        'architecture/auth-authz-design',
        'architecture/embedding',
      ],
    },
    {
      type: 'category',
      label: 'Frontends',
      collapsed: true,
      items: [
        'architecture/frontends/overview',
        'architecture/frontends/trino-http',
        'architecture/frontends/postgres-wire',
        'architecture/frontends/mysql-wire',
        'architecture/frontends/flight-sql',
        'architecture/frontends/snowflake',
        'architecture/frontends/mcp',
      ],
    },
    {
      type: 'category',
      label: 'Extending QueryFlux',
      collapsed: true,
      items: [
        'architecture/adding-support/overview',
        'architecture/adding-support/frontend',
        'architecture/adding-support/backend',
      ],
    },
    {
      type: 'category',
      label: 'Project',
      collapsed: true,
      items: [
        'development',
        'contribute',
        'benchmarks',
        'project-structure',
        'roadmap',
      ],
    },
    {
      type: 'link',
      label: 'Routing AI Agents',
      href: 'https://github.com/lakeops-org/queryflux/discussions/51',
    },
  ],
};

export default sidebars;
