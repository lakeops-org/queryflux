import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';

export const DEFAULT_OG_IMAGE = 'img/queryflux-hero-banner.png';

type PageSeoProps = {
  /** Short page title (Layout appends ` | QueryFlux`). */
  title: string;
  description: string;
  /** Pathname only, e.g. `/community`. */
  pathname: string;
  /** Static asset path; defaults to homepage hero banner. */
  image?: string;
};

/** Explicit Open Graph / Twitter tags for custom pages (homepage, community). */
export default function PageSeo({
  title,
  description,
  pathname,
  image = DEFAULT_OG_IMAGE,
}: PageSeoProps): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const imagePath = useBaseUrl(`/${image.replace(/^\//, '')}`);
  const imageUrl = `${siteConfig.url}${imagePath}`;
  const pageUrl = `${siteConfig.url}${pathname}`;
  const fullTitle = `${title} | ${siteConfig.title}`;

  return (
    <Head>
      <meta property="og:url" content={pageUrl} />
      <meta property="og:title" content={fullTitle} />
      <meta property="og:description" content={description} />
      <meta property="og:image" content={imageUrl} />
      <meta name="twitter:url" content={pageUrl} />
      <meta name="twitter:title" content={fullTitle} />
      <meta name="twitter:description" content={description} />
      <meta name="twitter:image" content={imageUrl} />
      <link rel="canonical" href={pageUrl} />
    </Head>
  );
}
