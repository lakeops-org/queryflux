import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';

/** Keeps a page out of search while letting crawlers follow its links. Also drops it from the sitemap. */
export default function NoIndex(): ReactNode {
  return (
    <Head>
      <meta name="robots" content="noindex, follow" />
    </Head>
  );
}
