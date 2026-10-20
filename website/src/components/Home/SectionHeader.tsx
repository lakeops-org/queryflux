import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type Props = {
  id: string;
  eyebrow: string;
  title: ReactNode;
  lead?: ReactNode;
  center?: boolean;
};

export default function SectionHeader({id, eyebrow, title, lead, center}: Props): ReactNode {
  return (
    <div className={clsx(styles.sectionHeader, center && styles.sectionHeaderCenter)}>
      <span className={styles.eyebrow}>{eyebrow}</span>
      <Heading as="h2" id={id} className={styles.sectionTitle}>
        {title}
      </Heading>
      {lead && <p className={styles.sectionLead}>{lead}</p>}
    </div>
  );
}
