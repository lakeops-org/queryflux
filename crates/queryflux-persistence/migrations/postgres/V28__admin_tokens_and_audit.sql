-- Admin API bearer tokens (stored as SHA-256) and the admin API's audit log.
CREATE TABLE admin_tokens (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    token_hash   TEXT NOT NULL UNIQUE,
    scopes       TEXT[] NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ
);

CREATE TABLE admin_audit (
    id        BIGSERIAL PRIMARY KEY,
    at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    principal TEXT NOT NULL,
    actor     TEXT,
    method    TEXT NOT NULL,
    path      TEXT NOT NULL,
    status    SMALLINT NOT NULL
);
