import type {ReactNode} from 'react';
import styles from './styles.module.css';

const CLIENTS = ['Trino CLI', 'psql', 'mysql', 'Flight SQL', 'BI tools'];
const ENGINES = ['Trino', 'DuckDB', 'StarRocks', 'Snowflake', 'Athena'];

/**
 * Hub-and-spoke diagram: clients on the left, QueryFlux as the routing hub,
 * engines on the right. Pure CSS/SVG — no raster asset to go stale or 404.
 */
export default function HomepageHeroVisual(): ReactNode {
  return (
    <div className={styles.visual} aria-hidden="true">
      <div className={styles.column}>
        {CLIENTS.map((label) => (
          <span key={label} className={styles.chip}>
            {label}
          </span>
        ))}
      </div>

      <svg className={styles.wires} viewBox="0 0 200 100" preserveAspectRatio="none">
        <defs>
          <linearGradient id="qfWireIn" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#7c3aed" stopOpacity="0.05" />
            <stop offset="100%" stopColor="#7c3aed" stopOpacity="0.55" />
          </linearGradient>
          <linearGradient id="qfWireOut" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#ff7619" stopOpacity="0.55" />
            <stop offset="100%" stopColor="#ff7619" stopOpacity="0.05" />
          </linearGradient>
        </defs>
        {[10, 30, 50, 70, 90].map((y) => (
          <path
            key={`in-${y}`}
            d={`M0,${y} C40,${y} 60,50 100,50`}
            stroke="url(#qfWireIn)"
            strokeWidth="1"
            fill="none"
          />
        ))}
        {[10, 30, 50, 70, 90].map((y) => (
          <path
            key={`out-${y}`}
            d={`M100,50 C140,50 160,${y} 200,${y}`}
            stroke="url(#qfWireOut)"
            strokeWidth="1"
            fill="none"
          />
        ))}
      </svg>

      <div className={styles.hub}>
        <span className={styles.hubGlow} />
        <span className={styles.hubMark}>QF</span>
      </div>

      <div className={styles.column}>
        {ENGINES.map((label) => (
          <span key={label} className={styles.chip}>
            {label}
          </span>
        ))}
      </div>
    </div>
  );
}
