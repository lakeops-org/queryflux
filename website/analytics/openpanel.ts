/** Browser-safe OpenPanel defaults (no process.env — that runs only in docusaurus.config.ts). */
export const DEFAULT_OPENPANEL_CLIENT_ID = 'f88d1483-36bc-4a52-8333-7f5865e78d16';

export const OPENPANEL_TRACKING_OPTIONS = {
  trackScreenViews: true,
  trackOutgoingLinks: true,
  trackAttributes: true,
} as const;

export type OpenPanelCustomFields = {
  openPanelClientId?: string;
  openPanelDisabled?: boolean;
};

/**
 * OpenPanel cloud enforces a per-project CORS allowlist for browser tracking.
 * Only `clientId` is sent from the browser — never `clientSecret` ([auth docs](https://openpanel.dev/docs/api/authentication)).
 * Add every dev origin here: OpenPanel dashboard → Project → Settings (domains / CORS).
 */
export const OPENPANEL_DEV_ORIGINS = [
  'http://localhost:3000',
  'http://127.0.0.1:3000',
] as const;
