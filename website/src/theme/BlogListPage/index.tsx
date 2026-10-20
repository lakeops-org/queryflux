import {useMemo, useState, type ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import {HtmlClassNameProvider, PageMetadata, ThemeClassNames} from '@docusaurus/theme-common';
import SearchMetadata from '@theme/SearchMetadata';
import BlogListPageStructuredData from '@theme/BlogListPage/StructuredData';
import type {Props} from '@theme/BlogListPage';
import styles from './styles.module.css';

type Post = Props['items'][number]['content']['metadata'];

const ALL = 'All posts';

function category(post: Post): string {
  return post.tags[0]?.label ?? 'Posts';
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

function Byline({post}: {post: Post}): ReactNode {
  const names = post.authors.map((a) => a.name).filter(Boolean).join(', ');
  return (
    <p className={styles.byline}>
      {names && <span>{names}</span>}
      {names && <span aria-hidden> · </span>}
      <time dateTime={post.date}>{formatDate(post.date)}</time>
    </p>
  );
}

// The post's `image` front matter is its cover; posts without one get a generated cover
// (category and wordmark on a gradient).
function Cover({post, index, large}: {post: Post; index: number; large?: boolean}): ReactNode {
  const image = post.frontMatter.image as string | undefined;
  const src = useBaseUrl(image ? `/${image.replace(/^\//, '')}` : '/');
  if (image) {
    return (
      <div className={clsx(styles.cover, styles.coverImage, large && styles.coverLarge)} aria-hidden>
        <img src={src} alt="" loading={large ? 'eager' : 'lazy'} />
      </div>
    );
  }
  return (
    <div className={clsx(styles.cover, large && styles.coverLarge)} data-variant={index % 4} aria-hidden>
      <span className={styles.coverCategory}>{category(post)}</span>
      <span className={styles.coverMark}>
        Query<span>Flux</span>
      </span>
    </div>
  );
}

function Card({post, index}: {post: Post; index: number}): ReactNode {
  return (
    <Link to={post.permalink} className={styles.card}>
      <Cover post={post} index={index} />
      <div className={styles.cardBody}>
        <h3 className={styles.cardTitle}>{post.title}</h3>
        <Byline post={post} />
        {post.description && <p className={styles.description}>{post.description}</p>}
        <span className={styles.chip}>{category(post)}</span>
      </div>
    </Link>
  );
}

export default function BlogListPage(props: Props): ReactNode {
  const {metadata, items} = props;
  const posts = items.map((i) => i.content.metadata);
  const [active, setActive] = useState(ALL);

  const counts = useMemo(() => {
    const c = new Map<string, number>();
    posts.forEach((p) => c.set(category(p), (c.get(category(p)) ?? 0) + 1));
    // Array.from, not spread: the site's loose transpile turns [...map] into [iterator].
    return Array.from(c.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [posts]);

  const [featured, ...rest] = posts;
  const shown = active === ALL ? rest : posts.filter((p) => category(p) === active);

  return (
    <HtmlClassNameProvider
      className={clsx(ThemeClassNames.wrapper.blogPages, ThemeClassNames.page.blogListPage)}>
      <PageMetadata title={metadata.blogTitle} description={metadata.blogDescription} />
      <SearchMetadata tag="blog_posts_list" />
      <BlogListPageStructuredData {...props} />
      <Layout>
        <main className={styles.page}>
          <div className={styles.inner}>
            <header className={styles.header}>
              <h1 className={styles.title}>
                The QueryFlux Blog<span>.</span>
              </h1>
              <p className={styles.subtitle}>{metadata.blogDescription}</p>
            </header>

            {featured && (
              <Link to={featured.permalink} className={styles.featured}>
                <Cover post={featured} index={0} large />
                <div className={styles.featuredBody}>
                  <h2 className={styles.featuredTitle}>{featured.title}</h2>
                  <Byline post={featured} />
                  {featured.description && (
                    <p className={styles.description}>{featured.description}</p>
                  )}
                  <span className={styles.chip}>{category(featured)}</span>
                </div>
              </Link>
            )}

            <div className={styles.filters} role="group" aria-label="Filter posts by category">
              {[[ALL, posts.length] as const, ...counts].map(([name, n]) => (
                <button
                  key={name}
                  type="button"
                  aria-pressed={active === name}
                  className={clsx(styles.filter, active === name && styles.filterActive)}
                  onClick={() => setActive(name)}>
                  {name}
                  <span className={styles.filterCount}>{n}</span>
                </button>
              ))}
            </div>

            <p className={styles.count}>
              {shown.length} {shown.length === 1 ? 'article' : 'articles'}
            </p>
            <div className={styles.grid}>
              {shown.map((post) => (
                <Card key={post.permalink} post={post} index={posts.indexOf(post)} />
              ))}
            </div>
          </div>
        </main>
      </Layout>
    </HtmlClassNameProvider>
  );
}
