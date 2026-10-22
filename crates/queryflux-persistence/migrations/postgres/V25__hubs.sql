-- Hub enrollment (#255): the sites a control plane serves through hubs, one-time
-- enrollment tokens (stored as SHA-256), and each enrolled hub's public key.
CREATE TABLE IF NOT EXISTS hub_sites (
    name        TEXT PRIMARY KEY,
    -- JSON array of cluster names this site gets; NULL: every cluster.
    clusters    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hub_enrollment_tokens (
    token_sha256 TEXT PRIMARY KEY,
    site         TEXT NOT NULL REFERENCES hub_sites(name) ON DELETE CASCADE,
    expires_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hub_keys (
    id          TEXT PRIMARY KEY,
    site        TEXT NOT NULL REFERENCES hub_sites(name) ON DELETE CASCADE,
    public_key  TEXT NOT NULL,
    host        TEXT,
    enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
