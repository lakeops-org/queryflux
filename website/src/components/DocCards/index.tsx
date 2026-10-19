import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import {useActiveVersion} from '@docusaurus/plugin-content-docs/client';
import styles from './styles.module.css';

type Card = {
  title: string;
  /** Doc path written against the latest version, e.g. `/docs/installation/overview`. */
  to: string;
  children: ReactNode;
};

/** Numbered link cards for doc landing pages (MDX: <DocCards><DocCard …/></DocCards>). */
export function DocCards({children}: {children: ReactNode}): ReactNode {
  return <div className={styles.grid}>{children}</div>;
}

/**
 * `to` is passed straight to the router, so Docusaurus does not rewrite it per
 * version the way it does Markdown links. Re-root `/docs/...` onto the active
 * version (`/docs/next/...`, `/docs/0.3.0/...`) so cards stay in the reader's version.
 */
function useVersionedPath(to: string): string {
  const version = useActiveVersion(undefined);
  if (!version || !to.startsWith('/docs/')) {
    return to;
  }
  return `${version.path}${to.slice('/docs'.length)}`;
}

export function DocCard({title, to, children}: Card): ReactNode {
  const href = useVersionedPath(to);
  return (
    <Link to={href} className={styles.card}>
      <span className={styles.title}>
        {title}
        <span className={styles.arrow} aria-hidden>
          →
        </span>
      </span>
      <span className={styles.body}>{children}</span>
    </Link>
  );
}
