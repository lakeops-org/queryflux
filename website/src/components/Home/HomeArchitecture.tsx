import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

type Layer = {
  title: string;
  caption: string;
  items: string[];
  highlight?: boolean;
};

const LAYERS: Layer[] = [
  {
    title: 'Clients',
    caption: 'Unchanged drivers & tools',
    items: ['BI dashboards', 'dbt / Airflow', 'Notebooks', 'AI agents (MCP)'],
  },
  {
    title: 'Frontends',
    caption: 'Native wire protocols',
    items: ['Trino HTTP', 'PostgreSQL', 'MySQL', 'Snowflake', 'Flight SQL'],
  },
  {
    title: 'Router',
    caption: 'First matching rule wins',
    items: ['protocol', 'header', 'tags', 'queryRegex', 'pythonScript', 'compound'],
    highlight: true,
  },
  {
    title: 'Translate & admit',
    caption: 'Per cluster group',
    items: ['sqlglot dialects', 'Concurrency caps', 'Queue & fallback', 'Access control'],
    highlight: true,
  },
  {
    title: 'Engines',
    caption: 'Load-balanced members',
    items: ['Trino', 'StarRocks', 'DuckDB', 'Athena', 'ADBC'],
  },
  {
    title: 'Observe',
    caption: 'One pane of glass',
    items: ['Prometheus', 'Query history', 'Studio', 'Admin API'],
  },
];

export default function HomeArchitecture(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="home-arch">
      <div className="container">
        <SectionHeader
          id="home-arch"
          eyebrow="Architecture"
          title={
            <>
              How a query flows through <span className={styles.accent}>QueryFlux</span>
            </>
          }
          lead={
            <>
              Every query passes through the same pipeline, whichever protocol it arrived
              on. <Link to="/docs/architecture/overview">Read the system overview →</Link>
            </>
          }
          center
        />
        <ol className={styles.archFlow}>
          {LAYERS.map((layer, i) => (
            <li key={layer.title} className={clsx(styles.archLayer, layer.highlight && styles.archLayerHighlight)}>
              <div className={styles.archHead}>
                <span className={styles.archIndex}>{String(i + 1).padStart(2, '0')}</span>
                <div>
                  <div className={styles.archTitle}>{layer.title}</div>
                  <div className={styles.archCaption}>{layer.caption}</div>
                </div>
              </div>
              <ul className={styles.archItems}>
                {layer.items.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
