import type {ReactNode} from 'react';
import BlogTagsPostsPage from '@theme-original/BlogTagsPostsPage';
import type BlogTagsPostsPageType from '@theme/BlogTagsPostsPage';
import type {WrapperProps} from '@docusaurus/types';
import NoIndex from '@site/src/components/NoIndex';

type Props = WrapperProps<typeof BlogTagsPostsPageType>;

export default function BlogTagsPostsPageWrapper(props: Props): ReactNode {
  return (
    <>
      <NoIndex />
      <BlogTagsPostsPage {...props} />
    </>
  );
}
