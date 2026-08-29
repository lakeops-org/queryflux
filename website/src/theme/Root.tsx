import React from 'react';
import Root from '@theme-original/Root';
import OpenPanelInit from '../components/OpenPanelInit';

/** Wrap the site so OpenPanel initializes once on the client (SPA route tracking). */
export default function RootWrapper(
  props: React.ComponentProps<typeof Root>,
): React.ReactElement {
  return (
    <>
      <OpenPanelInit />
      <Root {...props} />
    </>
  );
}
