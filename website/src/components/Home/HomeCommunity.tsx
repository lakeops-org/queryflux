import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

const START = [
  {title: 'Getting started', body: 'Docker Compose stacks with Trino, Studio, and Postgres — up in one command.', to: '/docs/getting-started'},
  {title: 'Configuration', body: 'The full YAML reference: frontends, cluster groups, routers, persistence.', to: '/docs/configuration'},
  {title: 'Architecture', body: 'Frontends, router, translation, admission, and observability end to end.', to: '/docs/architecture/overview'},
];

const COMMUNITY = [
  {title: 'GitHub', body: 'Star the repo, browse the code, and file issues.', href: 'https://github.com/lakeops-org/queryflux', cta: 'View repository'},
  {title: 'Community', body: 'Join Slack and GitHub Discussions to ask questions and share setups.', to: '/community', cta: 'Join the conversation'},
  {title: 'Contribute', body: 'Engines, routers, docs — read the guide and pick an issue.', to: '/docs/contribute', cta: 'Start contributing'},
];

export default function HomeCommunity(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="home-start">
      <div className="container">
        <SectionHeader
          id="home-start"
          eyebrow="Start building"
          title="From zero to routed queries in minutes"
          lead="Pick a path through the docs, or come say hi — QueryFlux is built in the open."
        />
        <div className={styles.startGrid}>
          {START.map((item, i) => (
            <Link key={item.title} to={item.to} className={styles.startCard}>
              <span className={styles.startIndex}>{String(i + 1).padStart(2, '0')}</span>
              <div className={styles.startTitle}>{item.title}</div>
              <p className={styles.startBody}>{item.body}</p>
            </Link>
          ))}
        </div>

        <div className={styles.statsRow}>
          <div className={styles.statCard}>
            <div className={styles.statValue}>Apache 2.0</div>
            <div className={styles.statLabel}>Free to use, modify, and embed</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statValue}>Rust</div>
            <div className={styles.statLabel}>Memory-safe proxy on an async Tokio core</div>
          </div>
        </div>

        <div className={styles.communityGrid}>
          {COMMUNITY.map((item) => (
            <Link
              key={item.title}
              className={clsx(styles.communityCard)}
              {...(item.href ? {href: item.href} : {to: item.to})}>
              <div className={styles.communityTitle}>{item.title}</div>
              <p className={styles.communityBody}>{item.body}</p>
              <span className={styles.communityArrow}>{item.cta} →</span>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}
