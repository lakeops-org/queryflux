import type {ReactNode} from 'react';
import Heading from '@theme/Heading';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

type UseCase = {
  title: string;
  tag: string;
  body: ReactNode;
};

const USE_CASES: UseCase[] = [
  {
    title: 'Multi-engine data platform',
    tag: 'protocolBased · queryRegex',
    body: (
      <>
        BI tools connect over MySQL wire and land on StarRocks, scheduled jobs stay on Trino,
        and ad-hoc <code>SELECT *</code> exploration is matched by regex and sent to Athena —
        no new connection strings for your teams.
      </>
    ),
  },
  {
    title: 'Cost-aware workload dispatch',
    tag: 'pythonScript',
    body: (
      <>
        A Python router inspects every query and steers CPU-heavy joins and windows to
        compute-priced Trino while scan-heavy Iceberg reads go to scan-priced Athena. Encode the
        cost model once; every client inherits it.
      </>
    ),
  },
  {
    title: 'Dashboard SLA protection',
    tag: 'maxRunningQueries · fallback',
    body: (
      <>
        Cap the StarRocks group with <code>maxRunningQueries</code> so dashboards always have
        headroom. When it is full, ad-hoc queries queue at the proxy or spill to a Trino
        fallback — and Grafana shows queue depth live.
      </>
    ),
  },
  {
    title: 'Transparent engine migration',
    tag: 'weighted load balancing',
    body: (
      <>
        Run Trino and StarRocks in one group and ramp StarRocks from 10% to 100% with zero
        client changes. Studio query history lines up per-engine latency until you are ready to
        flip the weights.
      </>
    ),
  },
];

export default function HomeUseCases(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="home-use-cases">
      <div className="container">
        <SectionHeader
          id="home-use-cases"
          eyebrow="Use cases"
          title="What teams route through QueryFlux"
          lead="Routing, capacity limits, and load balancing — one proxy, every client, no separate stack per engine."
          center
        />
        <div className={styles.useCaseGrid}>
          {USE_CASES.map((u, i) => (
            <article key={u.title} className={styles.useCaseCard}>
              <div className={styles.useCaseTop}>
                <span className={styles.pillarIndex}>{String(i + 1).padStart(2, '0')}</span>
                <span className={styles.useCaseTag}>{u.tag}</span>
              </div>
              <Heading as="h3" className={styles.pillarTitle}>
                {u.title}
              </Heading>
              <p className={styles.pillarBody}>{u.body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}
