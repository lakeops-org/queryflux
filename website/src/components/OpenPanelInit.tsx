import {useEffect, useRef} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {OpenPanel} from '@openpanel/web';
import {
  OPENPANEL_DEV_ORIGINS,
  OPENPANEL_TRACKING_OPTIONS,
  type OpenPanelCustomFields,
} from '../../analytics/openpanel';

/** Client-only OpenPanel init — see https://openpanel.dev/guides/react-analytics */
export default function OpenPanelInit(): null {
  const {siteConfig} = useDocusaurusContext();
  const customFields = siteConfig.customFields as OpenPanelCustomFields;
  const started = useRef(false);

  useEffect(() => {
    const clientId = customFields.openPanelClientId?.trim();
    if (customFields.openPanelDisabled || !clientId || started.current) {
      return;
    }
    started.current = true;

    const isLocalDev = OPENPANEL_DEV_ORIGINS.includes(
      window.location.origin as typeof OPENPANEL_DEV_ORIGINS[number],
    );

    new OpenPanel({
      clientId,
      ...OPENPANEL_TRACKING_OPTIONS,
      // Surface failed ingest in the console during local dev (CORS allowlist, ad blockers).
      debug: isLocalDev,
    });

    if (isLocalDev) {
      console.info(
        '[OpenPanel] Local dev: add %s to your project CORS allowlist in the OpenPanel dashboard (Settings). Browser tracking uses clientId only — not clientSecret.',
        window.location.origin,
      );
    }
  }, [customFields.openPanelClientId, customFields.openPanelDisabled]);

  return null;
}
