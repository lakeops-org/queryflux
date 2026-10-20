import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

type Pillar = {
  title: string;
  body: ReactNode;
  basis: string;
  to: string;
};

const PILLARS: Pillar[] = [
  {
    title: 'Cost-aware routing',
    body: 'Steer CPU-heavy joins to compute-priced clusters and selective cold scans to pay-per-scan engines. Benchmarks show up to 56% lower total workload cost.',
    basis: 'Router chain · Python routers',
    to: '/docs/architecture/routing-and-clusters',
  },
  {
    title: 'SLA protection',
    body: 'Cap running queries per cluster group. When a group is full, queries queue at the proxy or spill to a fallback group instead of hammering the backend.',
    basis: 'Cluster groups · Admission queue',
    to: '/docs/architecture/routing-and-clusters',
  },
  {
    title: 'Dialect translation',
    body: 'Clients write the SQL they know. QueryFlux rewrites it with sqlglot when the target engine speaks a different dialect, in strict or best-effort mode.',
    basis: 'sqlglot · Translation policy',
    to: '/docs/architecture/query-translation',
  },
  {
    title: 'Transparent migration',
    body: 'Weighted load balancing inside a group lets you ramp a new engine from 10% to 100% of traffic with zero client changes and side-by-side latency in Studio.',
    basis: 'Weighted strategies · Query history',
    to: '/docs/architecture/routing-and-clusters',
  },
  {
    title: 'Identity & access control',
    body: 'Authenticate clients with OIDC or LDAP, choose passthrough or impersonation per backend, and enforce row filters and column masks through OPA.',
    basis: 'Auth providers · OPA policies',
    to: '/docs/access-control/overview',
  },
  {
    title: 'Built for operators',
    body: 'Rust core with ~0.35 ms p50 overhead, Prometheus metrics, Postgres-backed state, an admin API, and a Studio UI for clusters, routing, and history.',
    basis: 'Tokio · Prometheus · Studio',
    to: '/docs/studio',
  },
];

export default function HomePillars(): ReactNode {
  return (
    <section className={clsx(styles.section, styles.sectionAlt)} aria-labelledby="home-pillars">
      <div className="container">
        <SectionHeader
          id="home-pillars"
          eyebrow="Capabilities"
          title="Six reasons teams put QueryFlux in front"
          lead="A smart tier between clients and engines — the pattern ProxySQL made standard for OLTP fleets, applied to multi-engine analytics and the lakehouse."
        />
        <div className={styles.pillarGrid}>
          {PILLARS.map((p, i) => (
            <Link key={p.title} to={p.to} className={styles.pillarCard}>
              <div className={styles.pillarTop}>
                <span className={styles.pillarIndex}>{String(i + 1).padStart(2, '0')}</span>
                <span className={styles.pillarArrow} aria-hidden>
                  →
                </span>
              </div>
              <Heading as="h3" className={styles.pillarTitle}>
                {p.title}
              </Heading>
              <p className={styles.pillarBody}>{p.body}</p>
              <div className={styles.pillarBasis}>
                <span>Under the hood</span>
                {p.basis}
              </div>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}
