import type {ReactNode} from 'react';
import BlogArchivePage from '@theme-original/BlogArchivePage';
import type BlogArchivePageType from '@theme/BlogArchivePage';
import type {WrapperProps} from '@docusaurus/types';
import NoIndex from '@site/src/components/NoIndex';

type Props = WrapperProps<typeof BlogArchivePageType>;

export default function BlogArchivePageWrapper(props: Props): ReactNode {
  return (
    <>
      <NoIndex />
      <BlogArchivePage {...props} />
    </>
  );
}
