import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import PageSeo from '@site/src/components/PageSeo';
import styles from './community.module.css';

/** Slack workspace invite (Queryflux / query-flux). */
const SLACK_WORKSPACE_URL =
  'https://join.slack.com/t/query-flux/shared_invite/zt-3v7qedxj9-o8ElCLGK0UXT8xBU0_bD8w';

const GITHUB_DISCUSSIONS_URL = 'https://github.com/lakeops-org/queryflux/discussions';

const COMMUNITY_DESCRIPTION =
  'Join the QueryFlux community — ask questions on Slack or GitHub Discussions, share what you are building, and help shape the project.';

const SlackGlyph = (): ReactNode => (
  <svg className={styles.slackIcon} viewBox="0 0 24 24" aria-hidden="true">
    <path
      fill="currentColor"
      d="M5.042 15.165a2.528 2.528 0 0 1-2.52 2.523A2.528 2.528 0 0 1 0 15.165a2.527 2.527 0 0 1 2.522-2.52h2.52v2.52zM6.313 15.165a2.527 2.527 0 0 1 2.521-2.52 2.527 2.527 0 0 1 2.521 2.52v6.313A2.528 2.528 0 0 1 8.834 24a2.528 2.528 0 0 1-2.521-2.522v-6.313zM8.834 5.042a2.528 2.528 0 0 1-2.521-2.52A2.528 2.528 0 0 1 8.834 0a2.528 2.528 0 0 1 2.521 2.522v2.52H8.834V5.042zm0 1.313a2.528 2.528 0 0 1 2.521 2.521 2.528 2.528 0 0 1-2.521 2.521H2.522A2.528 2.528 0 0 1 0 8.876a2.528 2.528 0 0 1 2.522-2.521h6.312zm10.123 2.521a2.528 2.528 0 0 1 2.522-2.521A2.528 2.528 0 0 1 24 8.876a2.528 2.528 0 0 1-2.522 2.521h-2.522V8.876zm-1.313 0a2.528 2.528 0 0 1-2.521 2.521 2.527 2.527 0 0 1-2.521-2.521V2.522A2.528 2.528 0 0 1 15.165 0a2.528 2.528 0 0 1 2.521 2.522v6.354zm-2.521 10.123a2.528 2.528 0 0 1 2.521 2.522A2.528 2.528 0 0 1 15.165 24a2.527 2.527 0 0 1-2.521-2.522v-2.522h2.521zm0-1.313a2.527 2.527 0 0 1-2.521-2.521 2.527 2.527 0 0 1 2.521-2.521h6.313A2.528 2.528 0 0 1 24 15.165a2.528 2.528 0 0 1-2.522 2.521h-6.313z"
    />
  </svg>
);

const GitHubGlyph = (): ReactNode => (
  <svg className={styles.githubIcon} viewBox="0 0 24 24" aria-hidden="true">
    <path
      fill="currentColor"
      d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"
    />
  </svg>
);

const COMMUNITY_PAGE_TITLE = 'Community';

export default function Community(): ReactNode {
  return (
    <Layout title={COMMUNITY_PAGE_TITLE} description={COMMUNITY_DESCRIPTION}>
      <PageSeo
        title={COMMUNITY_PAGE_TITLE}
        description={COMMUNITY_DESCRIPTION}
        pathname="/community"
      />
      <div className={styles.page}>
        <header className={styles.hero}>
          <div className={styles.heroGlow} aria-hidden />
          <div className={clsx('container', styles.heroInner)}>
            <Heading as="h1" className={styles.title}>
              Community
            </Heading>
            <p className={styles.lead}>
              Have a question? Just ask. The fastest way to get unstuck is to jump into Slack or
              start a GitHub Discussion — the community and maintainers are happy to help.
            </p>
            <div className={styles.ctaRow}>
              <Link
                className={clsx('button button--lg', styles.slackButton)}
                href={SLACK_WORKSPACE_URL}
                target="_blank"
                rel="noopener noreferrer">
                <SlackGlyph />
                Join Slack
              </Link>
              <Link
                className="button button--lg button--primary"
                href={GITHUB_DISCUSSIONS_URL}
                target="_blank"
                rel="noopener noreferrer">
                GitHub Discussions
              </Link>
              <Link
                className={clsx('button button--lg', styles.githubButton)}
                href="https://github.com/lakeops-org/queryflux"
                target="_blank"
                rel="noopener noreferrer">
                <GitHubGlyph />
                GitHub
              </Link>
            </div>
          </div>
        </header>

        <main className={styles.main}>
          <div className="container">
            <h2 className={styles.sectionTitle}>How to get involved</h2>
            <p className={styles.sectionSub}>
              No question is too small. Reach out in Slack for real-time chat or start a GitHub
              Discussion if you want a longer thread — both are watched by maintainers and community
              members.
            </p>

            <div className={styles.cardGrid}>
              <article className={styles.card}>
                <div className={styles.cardAccent} aria-hidden />
                <div className={clsx(styles.cardIcon, styles.cardIconSlack)} aria-hidden="true">
                  <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M5.042 15.165a2.528 2.528 0 0 1-2.52 2.523A2.528 2.528 0 0 1 0 15.165a2.527 2.527 0 0 1 2.522-2.52h2.52v2.52zM6.313 15.165a2.527 2.527 0 0 1 2.521-2.52 2.527 2.527 0 0 1 2.521 2.52v6.313A2.528 2.528 0 0 1 8.834 24a2.528 2.528 0 0 1-2.521-2.522v-6.313zM8.834 5.042a2.528 2.528 0 0 1-2.521-2.52A2.528 2.528 0 0 1 8.834 0a2.528 2.528 0 0 1 2.521 2.522v2.52H8.834V5.042zm0 1.313a2.528 2.528 0 0 1 2.521 2.521 2.528 2.528 0 0 1-2.521 2.521H2.522A2.528 2.528 0 0 1 0 8.876a2.528 2.528 0 0 1 2.522-2.521h6.312zm10.123 2.521a2.528 2.528 0 0 1 2.522-2.521A2.528 2.528 0 0 1 24 8.876a2.528 2.528 0 0 1-2.522 2.521h-2.522V8.876zm-1.313 0a2.528 2.528 0 0 1-2.521 2.521 2.527 2.527 0 0 1-2.521-2.521V2.522A2.528 2.528 0 0 1 15.165 0a2.528 2.528 0 0 1 2.521 2.522v6.354zm-2.521 10.123a2.528 2.528 0 0 1 2.521 2.522A2.528 2.528 0 0 1 15.165 24a2.527 2.527 0 0 1-2.521-2.522v-2.522h2.521zm0-1.313a2.527 2.527 0 0 1-2.521-2.521 2.527 2.527 0 0 1 2.521-2.521h6.313A2.528 2.528 0 0 1 24 15.165a2.528 2.528 0 0 1-2.522 2.521h-6.313z" />
                  </svg>
                </div>
                <Heading as="h3" className={styles.cardTitle}>
                  Slack — Queryflux workspace
                </Heading>
                <p className={styles.cardText}>
                  The fastest way to get help. Ask a question, share what you are building, or just
                  say hello. Maintainers and community members are active here.
                </p>
                <ul className={styles.bullets}>
                  <li>Real-time answers from the community</li>
                  <li>Share feedback and early ideas</li>
                </ul>
                <div className={styles.cardActions}>
                  <Link
                    className={clsx('button button--primary', styles.slackButton)}
                    href={SLACK_WORKSPACE_URL}
                    target="_blank"
                    rel="noopener noreferrer">
                    <SlackGlyph />
                    Join Slack
                  </Link>
                </div>
              </article>

              <article className={styles.card}>
                <div className={styles.cardAccent} aria-hidden />
                <div className={clsx(styles.cardIcon, styles.cardIconDiscussions)} aria-hidden="true">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75">
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                  </svg>
                </div>
                <Heading as="h3" className={styles.cardTitle}>
                  GitHub Discussions
                </Heading>
                <p className={styles.cardText}>
                  Great for longer questions, feature ideas, or anything you want to stay searchable.
                  Start a thread and the community will weigh in.
                </p>
                <ul className={styles.bullets}>
                  <li>Q&A, ideas, and design feedback</li>
                  <li>Indexed and searchable for future visitors</li>
                </ul>
                <div className={styles.cardActions}>
                  <Link
                    className="button button--primary"
                    href={GITHUB_DISCUSSIONS_URL}
                    target="_blank"
                    rel="noopener noreferrer">
                    Open Discussions
                  </Link>
                </div>
              </article>
            </div>

            <article className={clsx(styles.card, styles.wideCard)}>
              <div className={styles.cardAccent} aria-hidden />
              <div className={clsx(styles.cardIcon, styles.cardIconContribute)} aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75">
                  <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22" />
                </svg>
              </div>
              <Heading as="h3" className={styles.cardTitle}>
                Contribute
              </Heading>
              <p className={styles.cardText}>
                We welcome issues, design discussion, and pull requests. Star the repo, read the
                contribute guide, and open an issue when you are unsure where to start.
              </p>
              <div className={styles.cardActions}>
                <Link
                  className="button button--primary"
                  href="https://github.com/lakeops-org/queryflux"
                  target="_blank"
                  rel="noopener noreferrer">
                  GitHub repository
                </Link>
                <Link className="button button--secondary" to="/docs/contribute">
                  Contribute guide
                </Link>
                <Link
                  className={clsx('button', styles.githubButton)}
                  href="https://github.com/lakeops-org/queryflux/issues"
                  target="_blank"
                  rel="noopener noreferrer">
                  <GitHubGlyph />
                  Issues
                </Link>
              </div>
            </article>

            <p className={styles.docsFootnote}>
              Looking for setup guides or configuration reference?{' '}
              <Link to="/docs/intro">Browse the documentation →</Link>
            </p>
          </div>
        </main>
      </div>
    </Layout>
  );
}
