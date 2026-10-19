import type {ReactNode} from 'react';
import styles from './styles.module.css';

const FRONTENDS = ['Trino HTTP', 'PostgreSQL wire', 'MySQL wire', 'Snowflake HTTP', 'Snowflake SQL API', 'Arrow Flight SQL', 'MCP'];
const ENGINES = ['Trino', 'DuckDB', 'StarRocks', 'Athena', 'ClickHouse', 'MySQL', 'Snowflake (ADBC)', 'Databricks (ADBC)', 'BigQuery (ADBC)', 'Redshift (ADBC)'];

function Row({label, items, reverse}: {label: string; items: string[]; reverse?: boolean}): ReactNode {
  // Items are rendered twice so the CSS marquee can loop seamlessly at -50%.
  return (
    <div className={styles.stripRow}>
      <span className={styles.stripLabel}>{label}</span>
      <div className={styles.marquee}>
        <ul className={reverse ? styles.marqueeTrackReverse : styles.marqueeTrack}>
          {[...items, ...items].map((item, i) => (
            <li key={i} className={styles.chip} aria-hidden={i >= items.length}>
              {item}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default function EngineStrip(): ReactNode {
  return (
    <section className={styles.strip} aria-label="Supported protocols and engines">
      <div className="container">
        <Row label="Clients speak" items={FRONTENDS} />
        <Row label="Queries land on" items={ENGINES} reverse />
      </div>
    </section>
  );
}
