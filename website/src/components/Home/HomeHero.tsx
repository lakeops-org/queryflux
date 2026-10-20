import {useRef, useState, type KeyboardEvent, type ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type Tab = {
  id: string;
  label: string;
  lines: ReactNode[];
};

// Hand-highlighted so the hero renders identically with or without JS, and so
// we can keep colors on-brand without pulling Prism into the landing page.
const k = (s: string) => <span className={styles.tokKey}>{s}</span>;
const v = (s: string) => <span className={styles.tokVal}>{s}</span>;
const c = (s: string) => <span className={styles.tokComment}>{s}</span>;
const p = (s: string) => <span className={styles.tokPrompt}>{s}</span>;

const TABS: Tab[] = [
  {
    id: 'config',
    label: 'config.yaml',
    lines: [
      <>{k('clusterGroups')}:</>,
      <>  {k('starrocks-bi')}: {'{'} {k('maxRunningQueries')}: {v('50')}, {k('members')}: [{v('sr-1')}] {'}'}</>,
      <>  {k('trino-etl')}:    {'{'} {k('maxRunningQueries')}: {v('20')}, {k('members')}: [{v('trino-1')}] {'}'}</>,
      <>  {k('duckdb-adhoc')}: {'{'} {k('maxRunningQueries')}: {v('4')}, {k('members')}: [{v('duck-1')}] {'}'}</>,
      <> </>,
      <>{k('routers')}:</>,
      <>  {c('# BI tools connect over MySQL wire')}</>,
      <>  - {k('type')}: {v('protocolBased')}</>,
      <>    {k('mysqlWire')}: {v('starrocks-bi')}</>,
      <>  {c('# batch jobs tag themselves')}</>,
      <>  - {k('type')}: {v('tags')}</>,
      <>    {k('rules')}:</>,
      <>      - {k('tags')}: {'{'} {k('workload')}: {v('etl')} {'}'}</>,
      <>        {k('targetGroup')}: {v('trino-etl')}</>,
      <> </>,
      <>{k('routingFallback')}: {v('duckdb-adhoc')}</>,
    ],
  },
  {
    id: 'clients',
    label: 'clients',
    lines: [
      <>{c('# Same endpoint, whatever driver you already have')}</>,
      <>{p('$')} trino --server http://queryflux:8080</>,
      <>{p('$')} psql -h queryflux -p 5432</>,
      <>{p('$')} mysql -h queryflux -P 3306</>,
      <> </>,
      <>{c('# QueryFlux picks the engine and translates the dialect')}</>,
      <>{p('trino>')} SELECT date_trunc({v("'day'")}, ts), count(*)</>,
      <>{'      '}FROM events GROUP BY 1;</>,
      <>{c('-- no rule matched → duckdb-adhoc · translated trino → duckdb')}</>,
    ],
  },
  {
    id: 'run',
    label: 'quickstart',
    lines: [
      <>{p('$')} git clone https://github.com/lakeops-org/queryflux</>,
      <>{p('$')} cd queryflux/examples/minimal-trino</>,
      <>{p('$')} docker compose up -d --wait</>,
      <> </>,
      <>{c('# SQL via QueryFlux   http://localhost:8080')}</>,
      <>{c('# Studio              http://localhost:3000')}</>,
      <>{c('# Admin API           http://localhost:9000')}</>,
    ],
  },
];

function CodeWindow(): ReactNode {
  const [active, setActive] = useState(TABS[0].id);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const activeIndex = Math.max(0, TABS.findIndex((t) => t.id === active));
  const tab = TABS[activeIndex];

  // WAI-ARIA tabs pattern: arrows move between tabs, Home/End jump to the ends.
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const last = TABS.length - 1;
    const next = {
      ArrowRight: activeIndex === last ? 0 : activeIndex + 1,
      ArrowLeft: activeIndex === 0 ? last : activeIndex - 1,
      Home: 0,
      End: last,
    }[event.key];
    if (next === undefined) {
      return;
    }
    event.preventDefault();
    setActive(TABS[next].id);
    tabRefs.current[next]?.focus();
  };

  return (
    <div className={styles.codeWindow}>
      <div className={styles.codeHeader}>
        <span className={styles.codeDots} aria-hidden>
          <i />
          <i />
          <i />
        </span>
        <div
          className={styles.codeTabs}
          role="tablist"
          aria-label="QueryFlux examples"
          onKeyDown={onKeyDown}>
          {TABS.map((t, i) => (
            <button
              key={t.id}
              ref={(el) => {
                tabRefs.current[i] = el;
              }}
              id={`hero-tab-${t.id}`}
              type="button"
              role="tab"
              aria-selected={t.id === active}
              aria-controls={`hero-panel-${t.id}`}
              tabIndex={t.id === active ? 0 : -1}
              className={clsx(styles.codeTab, t.id === active && styles.codeTabActive)}
              onClick={() => setActive(t.id)}>
              {t.label}
            </button>
          ))}
        </div>
      </div>
      <pre
        className={styles.codeBody}
        role="tabpanel"
        id={`hero-panel-${tab.id}`}
        aria-labelledby={`hero-tab-${tab.id}`}
        tabIndex={0}>
        {tab.lines.map((line, i) => (
          <div key={i} className={styles.codeLine}>
            <span className={styles.codeLineNo} aria-hidden>
              {i + 1}
            </span>
            <code>{line}</code>
          </div>
        ))}
      </pre>
    </div>
  );
}

export default function HomeHero(): ReactNode {
  return (
    <header className={styles.hero}>
      <div className={styles.heroGrid} aria-hidden />
      <div className={clsx('container', styles.heroInner)}>
        <div className={styles.heroCopy}>
          <span className={styles.heroEyebrow}>
            <span className={styles.pulse} aria-hidden />
            Open source · Apache 2.0 · Written in Rust
          </span>
          <Heading as="h1" className={styles.heroTitle}>
            One SQL endpoint.<br /><span className={styles.accent}>Every engine.</span>
          </Heading>
          <p className={styles.heroSubtitle}>
            QueryFlux is a protocol-aware SQL proxy. Clients connect over Trino HTTP,
            PostgreSQL, MySQL, Snowflake, or Arrow Flight — QueryFlux routes each query
            to the right engine, translates the dialect, and queues work before your
            clusters fall over.
          </p>
          <div className={styles.heroCtas}>
            <Link className={clsx('button button--lg', styles.ctaPrimary)} to="/docs/getting-started">
              Get started →
            </Link>
            <Link
              className={clsx('button button--lg', styles.ctaGhost)}
              href="https://github.com/lakeops-org/queryflux">
              <svg width="18" height="18" viewBox="0 0 98 96" aria-hidden>
                <path
                  fill="currentColor"
                  d="M48.854 0C21.839 0 0 22 0 49.217c0 21.756 13.993 40.172 33.405 46.69 2.427.49 3.316-1.059 3.316-2.362 0-1.141-.08-5.052-.08-9.127-13.59 2.934-16.42-5.867-16.42-5.867-2.184-5.704-5.42-7.17-5.42-7.17-4.448-3.015.324-3.015.324-3.015 4.934.326 7.523 5.052 7.523 5.052 4.367 7.496 11.404 5.378 14.235 4.074.404-3.178 1.699-5.378 3.074-6.6-10.839-1.195-22.179-5.378-22.179-24.057 0-5.378 1.939-9.778 5.014-13.173-.503-1.196-2.184-6.02.478-12.518 0 0 4.075-1.302 13.406 4.994 4.002-1.079 8.29-1.619 12.548-1.619 4.259 0 8.546.54 12.548 1.619 9.318-6.296 13.393-4.994 13.393-4.994 2.662 6.498 1.003 11.322.478 12.518 3.08 3.395 5.014 7.795 5.014 13.173 0 18.795-11.354 22.848-22.194 24.043 1.741 1.508 3.302 4.407 3.302 8.927 0 6.434-.057 11.621-.057 13.173 0 1.304.869 2.852 3.316 2.367 19.394-6.518 33.382-24.934 33.382-46.69C97.708 22 75.788 0 48.854 0z"
                />
              </svg>
              View on GitHub
            </Link>
          </div>
          <dl className={styles.heroFacts}>
            <div>
              <dt>~0.35 ms</dt>
              <dd>p50 proxy overhead</dd>
            </div>
            <div>
              <dt>up to 56%</dt>
              <dd>workload cost cut</dd>
            </div>
            <div>
              <dt>6 routers</dt>
              <dd>protocol → Python</dd>
            </div>
          </dl>
        </div>
        <div className={styles.heroCodeColumn}>
          <CodeWindow />
        </div>
      </div>
    </header>
  );
}
