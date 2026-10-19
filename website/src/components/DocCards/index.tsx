import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import styles from './styles.module.css';

type Card = {
  title: string;
  to: string;
  children: ReactNode;
};

/** Numbered link cards for doc landing pages (MDX: <DocCards><DocCard …/></DocCards>). */
export function DocCards({children}: {children: ReactNode}): ReactNode {
  return <div className={styles.grid}>{children}</div>;
}

export function DocCard({title, to, children}: Card): ReactNode {
  return (
    <Link to={to} className={styles.card}>
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
