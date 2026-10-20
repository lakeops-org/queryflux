import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

// ponytail: worker mode and fleet-wide limits ship in 0.5.0; their pages exist only in the
// unreleased docs until the 0.5.0 snapshot is cut. Drop the `/next` prefix after that.
type Card = {
  title: string;
  tag: string;
  body: ReactNode;
  to: string;
};

const CARDS: Card[] = [
  {
    title: 'Agents query as the person they work for',
    tag: 'passthrough · impersonation · token exchange',
    body: (
      <>
        No shared warehouse password for every agent. QueryFlux can pass the caller’s own
        credential through, run the query as that user on Trino or ClickHouse, or exchange their
        token for one the engine accepts. The engine’s own permissions and audit log still apply.
      </>
    ),
    to: '/docs/authentication',
  },
  {
    title: 'Move off an engine without touching a client',
    tag: 'Snowflake wire · dialect translation',
    body: (
      <>
        Snowflake clients connect to QueryFlux as if it were Snowflake, and their queries can run
        on Trino, StarRocks or DuckDB instead. The connection string stays the same; the engine
        behind it is your choice.
      </>
    ),
    to: '/docs/architecture/frontends/snowflake',
  },
  {
    title: 'One limit across the whole fleet',
    tag: 'capacity quotas · fair queueing',
    body: (
      <>
        A cluster’s concurrency limit holds across every replica and worker. Queries that have to
        wait are served first come, first served: in our capacity benchmark, waits dropped from
        3–14 s to about 0.1 s at p99.
      </>
    ),
    to: '/docs/next/architecture/capacity-and-throughput',
  },
  {
    title: 'A worker next to every app',
    tag: 'worker mode · Helm',
    body: (
      <>
        Run one proxy, a set of replicas, or a QueryFlux worker beside each application, as a
        sidecar or one per node. Workers take config, limits and history from a control plane
        and never touch its database.
      </>
    ),
    to: '/docs/next/operations/worker-mode',
  },
];

export default function HomeProduction(): ReactNode {
  return (
    <section className={clsx(styles.section, styles.sectionAlt)} aria-labelledby="home-production">
      <div className="container">
        <SectionHeader
          id="home-production"
          eyebrow="In production"
          title="Built to sit in front of everything"
          lead="Identity, migrations, fleet-wide limits and deployment: the parts that decide whether a proxy survives contact with real traffic."
          center
        />
        <div className={styles.useCaseGrid}>
          {CARDS.map((card, i) => (
            <Link key={card.title} to={card.to} className={styles.useCaseCard}>
              <div className={styles.useCaseTop}>
                <span className={styles.pillarIndex}>{String(i + 1).padStart(2, '0')}</span>
                <span className={styles.useCaseTag}>{card.tag}</span>
              </div>
              <Heading as="h3" className={styles.pillarTitle}>
                {card.title}
              </Heading>
              <p className={styles.pillarBody}>{card.body}</p>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}
