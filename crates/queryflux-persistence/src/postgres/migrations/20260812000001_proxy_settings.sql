-- Generic key/value settings used by ProxySettingsStore for keys that are
-- not backed by a dedicated table (e.g. admin_credentials).
CREATE TABLE IF NOT EXISTS proxy_settings (
    key         TEXT PRIMARY KEY,
    value       JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
