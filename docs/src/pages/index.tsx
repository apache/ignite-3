import {Redirect} from '@docusaurus/router';
import useBaseUrl from '@docusaurus/useBaseUrl';
import React from 'react';

/**
 * Root page that redirects to the latest documentation version.
 * This ensures that accessing /docs/ignite3/ redirects to /docs/ignite3/3.1.0/
 */
export default function Home(): React.ReactElement {
  const targetUrl = useBaseUrl('/3.1.0/');
  return <Redirect to={targetUrl} />;
}
