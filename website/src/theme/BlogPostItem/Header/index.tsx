import type {ReactNode} from 'react';
import Header from '@theme-original/BlogPostItem/Header';
import type HeaderType from '@theme/BlogPostItem/Header';
import type {WrapperProps} from '@docusaurus/types';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import useBaseUrl from '@docusaurus/useBaseUrl';

type Props = WrapperProps<typeof HeaderType>;

/** The post's `image` front matter, shown as a hero under the byline on the post page. */
export default function HeaderWrapper(props: Props): ReactNode {
  const {frontMatter, assets, isBlogPostPage} = useBlogPost();
  const image = (assets.image ?? frontMatter.image) as string | undefined;
  const src = useBaseUrl(image && !/^https?:/.test(image) ? `/${image.replace(/^\//, '')}` : image ?? '/');
  return (
    <>
      <Header {...props} />
      {isBlogPostPage && image && <img className="blog-post-hero" src={src} alt="" />}
    </>
  );
}
