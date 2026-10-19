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
      label: 'Quickstarts',
      collapsed: true,
      link: {type: 'doc', id: 'getting-started'},
      items: [
        'quickstart/first-query',
        'quickstart/local-duckdb',
        'quickstart/multi-engine',
        'quickstart/observability',
        'quickstart/ai-agent-mcp',
        'quickstart/oidc',
      ],
    },
    {
      type: 'category',
      label: 'Installation & Deployment',
      collapsed: true,
      link: {type: 'doc', id: 'installation/overview'},
      items: [
        'installation/binary',
        'installation/docker',
        'installation/kubernetes',
        'operations/adbc-drivers',
        {
          type: 'category',
          label: 'Scaling out',
          collapsed: true,
          items: ['operations/multi-replica', 'operations/worker-mode', 'operations/worker-sidecar'],
        },
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      collapsed: true,
      items: ['studio', 'authentication'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: true,
      items: ['configuration'],
    },
    {
      type: 'category',
      label: 'Agentic AI',
      collapsed: true,
      items: [
        'agentic/overview',
        'agentic/agent-context',
        'agentic/session-replay',
      ],
    },
    {
      type: 'category',
      label: 'Access Control',
      collapsed: true,
      items: [
        'access-control/overview',
        'access-control/opa',
        'access-control/cerbos',
        {
          type: 'category',
          label: 'Use cases',
          collapsed: true,
          items: [
            'access-control/customer-api-row-filters',
            'access-control/internal-analysts',
            'access-control/support-on-behalf-of',
          ],
        },
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
        'architecture/catalog-integration',
        'architecture/routing-and-clusters',
        'architecture/cluster-variants-and-health',
        'architecture/worker-mode',
        'architecture/capacity-and-throughput',
        'architecture/caching',
        'architecture/query-tags',
        'architecture/query-params',
        'architecture/guardrails',
        'architecture/observability',
        'architecture/adding-engine-support',
        'architecture/auth-authz-design',
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
