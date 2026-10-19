import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

const BEFORE = [
  {title: 'N × M driver configs', sub: 'Every client wired to every engine — connection strings drift per team.'},
  {title: 'Wrong engine, wrong bill', sub: 'CPU-heavy joins land on scan-priced engines and the other way round.'},
  {title: 'Noisy neighbours', sub: 'A batch ETL job and a live dashboard fight over the same cluster.'},
  {title: 'Dialect rewrites', sub: 'Moving a workload means rewriting SQL across every tool that sends it.'},
  {title: 'Fragmented visibility', sub: 'One UI per engine, no shared view of queues, latency, or cost.'},
];

const AFTER = [
  'One endpoint, native protocols — clients keep the drivers they already have',
  'Rule-based routing by protocol, header, tag, regex, or a Python function',
  'Per-group concurrency caps with proxy-side queueing and fallback groups',
  'sqlglot dialect translation when client and engine disagree',
  'Prometheus metrics, query history, and Studio across every engine',
];

export default function HomeComparison(): ReactNode {
  return (
    <section className={clsx(styles.section, styles.sectionAlt)} aria-labelledby="home-compare">
      <div className="container">
        <SectionHeader
          id="home-compare"
          eyebrow="The multi-engine tax"
          title="Every new engine multiplies the glue code"
          lead="Most data platforms already run more than one engine. Without a routing tier, each one adds drivers, conventions, and dashboards that someone has to keep in sync."
          center
        />
        <div className={styles.taxGrid}>
          <div className={styles.taxColumn}>
            <span className={clsx(styles.taxLabel, styles.taxLabelBefore)}>Without QueryFlux</span>
            <ol className={styles.taxStack}>
              {BEFORE.map((item, i) => (
                <li key={item.title} className={styles.taxStackItem}>
                  <span className={styles.taxIndex}>{String(i + 1).padStart(2, '0')}</span>
                  <div>
                    <div className={styles.taxStackTitle}>{item.title}</div>
                    <div className={styles.taxStackSub}>{item.sub}</div>
                  </div>
                </li>
              ))}
            </ol>
          </div>
          <div className={styles.taxArrow} aria-hidden>
            <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
              <path d="M5 12h14M13 6l6 6-6 6" />
            </svg>
          </div>
          <div className={styles.taxColumn}>
            <span className={clsx(styles.taxLabel, styles.taxLabelAfter)}>With QueryFlux</span>
            <div className={styles.taxAfterCard}>
              <div className={styles.taxAfterHeader}>
                <Heading as="h3" className={styles.taxAfterTitle}>
                  A control plane in front of your engines
                </Heading>
                <p className={styles.taxAfterSub}>
                  Topology becomes config, not code. Add an engine, change a rule, or swap a
                  backend without touching a single client.
                </p>
              </div>
              <ul className={styles.taxAfterList}>
                {AFTER.map((line) => (
                  <li key={line}>
                    <span className={styles.check} aria-hidden>
                      ✓
                    </span>
                    {line}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
