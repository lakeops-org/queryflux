/**
 * Browser-safe OpenPanel options (no process.env — that runs only in docusaurus.config.ts).
 *
 * `clientId` is a public project id (same class as a GA measurement ID). It must be injected
 * at build/start via OPENPANEL_CLIENT_ID — never commit it, and never put clientSecret here.
 * Abuse control is the OpenPanel CORS/domain allowlist, not hiding the id.
 */
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
