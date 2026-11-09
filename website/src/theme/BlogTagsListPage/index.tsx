import type {ReactNode} from 'react';
import BlogTagsListPage from '@theme-original/BlogTagsListPage';
import type BlogTagsListPageType from '@theme/BlogTagsListPage';
import type {WrapperProps} from '@docusaurus/types';
import NoIndex from '@site/src/components/NoIndex';

type Props = WrapperProps<typeof BlogTagsListPageType>;

export default function BlogTagsListPageWrapper(props: Props): ReactNode {
  return (
    <>
      <NoIndex />
      <BlogTagsListPage {...props} />
    </>
  );
}
