import type {ReactNode} from 'react';
import BlogAuthorsListPage from '@theme-original/Blog/Pages/BlogAuthorsListPage';
import type BlogAuthorsListPageType from '@theme/Blog/Pages/BlogAuthorsListPage';
import type {WrapperProps} from '@docusaurus/types';
import NoIndex from '@site/src/components/NoIndex';

type Props = WrapperProps<typeof BlogAuthorsListPageType>;

export default function BlogAuthorsListPageWrapper(props: Props): ReactNode {
  return (
    <>
      <NoIndex />
      <BlogAuthorsListPage {...props} />
    </>
  );
}
