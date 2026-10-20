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
    title: 'Route',
    body: 'Send each query to the engine that should run it, by protocol, user, tags, SQL pattern or a Python rule. Translate the dialect on the way, and move traffic between engines by weight with no client changes.',
    basis: 'Router chain · sqlglot · weighted groups',
    to: '/docs/architecture/routing-and-clusters',
  },
  {
    title: 'Contain',
    body: 'Cap running queries per engine across every replica and worker, and queue the rest. Block writes, cap rows, require a WHERE clause on chosen tables, and apply row filters and column masks from OPA or Cerbos.',
    basis: 'Capacity limits · guardrails · access control',
    to: '/docs/architecture/guardrails',
  },
  {
    title: 'Understand',
    body: 'See every query with its user or agent, the route it took, its timings and its guardrail decisions. Prometheus metrics, Grafana dashboards, and Studio for history, routing and replay.',
    basis: 'Query history · Prometheus · Studio',
    to: '/docs/architecture/observability',
  },
  {
    title: 'Open',
    body: 'Any engine, any protocol, in your environment. Apache-2.0, written in Rust, about 0.35 ms of p50 overhead. Your clients keep their drivers; your engines stay replaceable.',
    basis: 'Apache-2.0 · Rust · self-hosted',
    to: '/docs/getting-started',
  },
];

export default function HomePillars(): ReactNode {
  return (
    <section className={clsx(styles.section, styles.sectionAlt)} aria-labelledby="home-pillars">
      <div className="container">
        <SectionHeader
          id="home-pillars"
          eyebrow="Capabilities"
          title="Route, contain, understand — over engines you already run"
          lead="One control point between your clients, your agents and your engines: it decides where each query runs, what it may do, and keeps the record."
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
