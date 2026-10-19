import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import PageSeo from '@site/src/components/PageSeo';
// Refreshed from the `benchmark-data` branch by deploy-website.yml on every build; the
// committed copies are a fallback so local and PR builds work without it.
import latest from '@site/src/data/benchmarks/proxy-latest.json';
import history from '@site/src/data/benchmarks/proxy-history.json';
import styles from './benchmarks.module.css';

type Run = {commit: string; date: string; runner?: string; metrics: Record<string, number>};

const REPO_URL = 'https://github.com/lakeops-org/queryflux';
const CODSPEED_URL = 'https://codspeed.io/lakeops-org/queryflux';
const ENGINES = ['Trino', 'StarRocks'] as const;
const HISTORY_ROWS = 15;

const DESCRIPTION =
  'QueryFlux proxy overhead measured on every merge to main — latency through QueryFlux vs direct to a mock Trino / StarRocks backend.';

function metric(run: Run, engine: string, name: string): number | undefined {
  return run.metrics[`${engine} — ${name} (ms)`];
}

function fmt(ms: number | undefined): string {
  if (ms === undefined) return '—';
  return ms < 1 ? `${Math.round(ms * 1000)} µs` : `${ms.toFixed(2)} ms`;
}

function CommitLink({sha}: {sha: string}): ReactNode {
  return (
    <Link href={`${REPO_URL}/commit/${sha}`}>
      <code>{sha.slice(0, 7)}</code>
    </Link>
  );
}

function LatestTable({run}: {run: Run}): ReactNode {
  return (
    <table className={styles.table}>
      <thead>
        <tr>
          <th>Engine path</th>
          <th>Direct p50</th>
          <th>Via QueryFlux p50</th>
          <th>p95</th>
          <th>p99</th>
          <th>Overhead p50</th>
        </tr>
      </thead>
      <tbody>
        {ENGINES.map((e) => (
          <tr key={e}>
            <td>{e}</td>
            <td>{fmt(metric(run, e, 'Direct p50'))}</td>
            <td>{fmt(metric(run, e, 'Via QueryFlux p50'))}</td>
            <td>{fmt(metric(run, e, 'Via QueryFlux p95'))}</td>
            <td>{fmt(metric(run, e, 'Via QueryFlux p99'))}</td>
            <td>
              <strong>{fmt(metric(run, e, 'Overhead p50'))}</strong>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function HistoryTable({runs}: {runs: Run[]}): ReactNode {
  const recent = [...runs].reverse().slice(0, HISTORY_ROWS);
  return (
    <table className={styles.table}>
      <thead>
        <tr>
          <th>Commit</th>
          <th>Date</th>
          {ENGINES.map((e) => (
            <th key={e}>{e} overhead p50</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {recent.map((r) => (
          <tr key={r.commit}>
            <td>
              <CommitLink sha={r.commit} />
            </td>
            <td>{r.date.slice(0, 10)}</td>
            {ENGINES.map((e) => (
              <td key={e}>{fmt(metric(r, e, 'Overhead p50'))}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function Benchmarks(): ReactNode {
  const run = latest as Run;
  return (
    <Layout title="Benchmarks" description={DESCRIPTION}>
      <PageSeo title="Benchmarks" description={DESCRIPTION} pathname="/benchmarks" />
      <main className="container margin-vert--xl">
        <Heading as="h1">Benchmarks</Heading>
        <p className={styles.lead}>
          Every merge to <code>main</code> that touches QueryFlux is benchmarked in CI and checked
          against recent history. Pull requests get hot-path micro-benchmarks, plus an A/B
          against their base when a maintainer requests one.
        </p>

        <Heading as="h2">Proxy overhead</Heading>
        <p>
          Latest run: <CommitLink sha={run.commit} /> · {run.date.slice(0, 10)}. Client latency
          for <code>SELECT 1</code> sent directly to an in-process mock backend vs the same request
          through QueryFlux (Trino HTTP frontend). Mock backends isolate what QueryFlux adds — no
          warehouse I/O or planning is included. Medians of 6 rounds × 2,000 queries on{' '}
          {run.runner ?? 'a GitHub Actions ubuntu-latest runner'}.
        </p>
        <LatestTable run={run} />

        <Heading as="h3">Recent merges</Heading>
        <HistoryTable runs={history as Run[]} />

        <Heading as="h2">Hot-path micro-benchmarks</Heading>
        <p>
          Fingerprinting, routing, guardrails and SQL translation are benchmarked per function with
          CodSpeed, which counts CPU instructions instead of timing them — results are stable to
          under 1% on shared CI runners. History and per-PR reports are on the{' '}
          <Link href={CODSPEED_URL}>CodSpeed dashboard</Link>.
        </p>

        <p>
          How the numbers are produced, and how to run them locally:{' '}
          <Link to="/docs/next/benchmarks">Benchmarks methodology</Link>.
        </p>
      </main>
    </Layout>
  );
}
