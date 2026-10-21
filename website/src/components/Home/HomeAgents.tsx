import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import HomeAgentFlow from './HomeAgentFlow';
import SectionHeader from './SectionHeader';
import styles from './styles.module.css';

type Card = {
  title: string;
  tag: string;
  body: ReactNode;
};

const CARDS: Card[] = [
  {
    title: 'One MCP endpoint',
    tag: 'MCP · streamable HTTP',
    body: (
      <>
        Point Claude Code, Cursor or Claude Desktop at one URL. Agents get tools to run
        queries, list schemas, describe tables, read the engine plan, and check or cancel
        what they started.
      </>
    ),
  },
  {
    title: 'Every query has an owner',
    tag: 'agent context · session replay',
    body: (
      <>
        Each query records the agent, conversation, step and tool call behind it. Replay a
        conversation step by step in Studio and see exactly what the agent ran, and what was
        blocked.
      </>
    ),
  },
  {
    title: 'Guardrails in the query path',
    tag: 'read-only · row limits · Python · webhooks',
    body: (
      <>
        Block writes, cap result sizes, require a WHERE clause on the tables you choose, or call
        your own Python or webhook check. Guardrails run in QueryFlux itself, so they hold whether an agent
        connects over MCP or a SQL driver.
      </>
    ),
  },
  {
    title: 'Policies agents can’t talk their way around',
    tag: 'OPA · Cerbos',
    body: (
      <>
        Row filters and column masks from OPA or Cerbos apply to agents like any other user.
        Route agents to their own group with its own concurrency limit, so a busy agent queues
        instead of starving your dashboards.
      </>
    ),
  },
];

export default function HomeAgents(): ReactNode {
  return (
    <section className={styles.sectionAlt} aria-labelledby="home-agents">
      <div className="container">
        <SectionHeader
          id="home-agents"
          eyebrow="Built for agents"
          title="Follow a query through QueryFlux"
          lead={
            <>
              From an agent question to a Studio trail: guardrails, dialect translate, routing,
              cache, then the engine that should run it.{' '}
              <Link to="/docs/quickstart/ai-agent-mcp">Connect an agent →</Link>
            </>
          }
          center
        />
        <HomeAgentFlow />
        <div className={clsx(styles.useCaseGrid, styles.flowFollowup)}>
          {CARDS.map((card, i) => (
            <article key={card.title} className={styles.useCaseCard}>
              <div className={styles.useCaseTop}>
                <span className={styles.pillarIndex}>{String(i + 1).padStart(2, '0')}</span>
                <span className={styles.useCaseTag}>{card.tag}</span>
              </div>
              <Heading as="h3" className={styles.pillarTitle}>
                {card.title}
              </Heading>
              <p className={styles.pillarBody}>{card.body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}
