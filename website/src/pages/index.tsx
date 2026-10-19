import type {ReactNode} from 'react';
import Layout from '@theme/Layout';

import HomeHero from '@site/src/components/Home/HomeHero';
import EngineStrip from '@site/src/components/Home/EngineStrip';
import HomeComparison from '@site/src/components/Home/HomeComparison';
import HomeArchitecture from '@site/src/components/Home/HomeArchitecture';
import HomePillars from '@site/src/components/Home/HomePillars';
import HomeUseCases from '@site/src/components/Home/HomeUseCases';
import HomeCommunity from '@site/src/components/Home/HomeCommunity';
import PageSeo from '@site/src/components/PageSeo';
import styles from '@site/src/components/Home/styles.module.css';

/** Homepage meta description; keep concise for search-result snippets (~155 chars / ~1000px). */
const HOME_DESCRIPTION =
  'Multi-engine query routing proxy for SQL clients: route Trino, PostgreSQL, MySQL, and Flight queries to the best backend engine with translation, queueing, and observability.';

/**
 * Page title before Docusaurus appends ` | QueryFlux` (~12 chars). Keep the *final*
 * string ≤ ~60 chars for Twitter / strict OG validators.
 */
const HOME_PAGE_TITLE = 'Multi-engine SQL query routing proxy';

export default function Home(): ReactNode {
  return (
    <Layout title={HOME_PAGE_TITLE} description={HOME_DESCRIPTION}>
      <PageSeo
        title={HOME_PAGE_TITLE}
        description={HOME_DESCRIPTION}
        pathname="/"
        image="img/queryflux-hero-banner.png"
      />
      <div className={styles.home}>
        <HomeHero />
        <main>
          <EngineStrip />
          <HomeComparison />
          <HomeArchitecture />
          <HomePillars />
          <HomeUseCases />
          <HomeCommunity />
        </main>
      </div>
    </Layout>
  );
}
