import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

type Faq = {
  q: string;
  a: ReactNode;
};

const FAQS: Faq[] = [
  {
    q: 'Do I have to move my data?',
    a: 'No. QueryFlux sits in front of the engines you already run and forwards queries to them. Your data stays where it is.',
  },
  {
    q: 'Which engines and clients does it work with?',
    a: (
      <>
        Clients connect over Trino HTTP, PostgreSQL, MySQL, Snowflake, Arrow Flight SQL or MCP.
        Queries run on Trino, DuckDB, StarRocks, ClickHouse, Athena and MySQL, and through ADBC
        on Snowflake, BigQuery, Databricks, Redshift, PostgreSQL and more.
      </>
    ),
  },
  {
    q: 'How does it decide which engine runs a query?',
    a: (
      <>
        Routing rules you write, checked in order: by protocol, user, tags, a SQL pattern, or a
        Python function. The first match picks a cluster group, and the group spreads queries
        across its members. See <Link to="/docs/architecture/routing-and-clusters">routing</Link>.
      </>
    ),
  },
  {
    q: 'How do I stop an agent from doing damage?',
    a: (
      <>
        Turn on guardrails (read-only, row limits, a required WHERE clause on chosen tables, or
        your own Python or webhook checks) and access policies with row filters and column
        masks. They run in the query path for every protocol. Route agents to their own group
        with its own concurrency limit, so they can't crowd out other work. See <Link to="/docs/architecture/guardrails">guardrails</Link>.
      </>
    ),
  },
  {
    q: 'Can I see what an agent did?',
    a: (
      <>
        Yes. Every query records the agent, conversation, step and tool call, and Studio replays
        a conversation step by step. See <Link to="/docs/agentic/overview">agentic AI</Link>.
      </>
    ),
  },
  {
    q: 'Where does it run?',
    a: 'In your environment. QueryFlux is Apache-2.0 open source: run it with Docker or Helm, as one proxy, a set of replicas, or workers next to your applications.',
  },
  {
    q: 'How much latency does it add?',
    a: (
      <>
        About 0.35 ms at p50 on the proxy path, measured with the benchmark in the repo. See{' '}
        <Link to="/docs/benchmarks">benchmarks</Link>.
      </>
    ),
  },
];

export default function HomeFaq(): ReactNode {
  return (
    <section className={styles.section} aria-labelledby="home-faq">
      <div className="container">
        <SectionHeader id="home-faq" eyebrow="FAQ" title="Questions teams ask first" center />
        <div className={styles.faqList}>
          {FAQS.map((f) => (
            <details key={f.q} className={styles.faqItem}>
              <summary className={styles.faqQuestion}>{f.q}</summary>
              <p className={styles.faqAnswer}>{f.a}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}
