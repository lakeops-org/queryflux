-- Guardrails configuration table.
-- One row per scope: kind = 'global' or a cluster group name.
-- guards is a JSONB array of guard spec objects.
CREATE TABLE IF NOT EXISTS guardrails (
    kind       TEXT        PRIMARY KEY,
    guards     JSONB       NOT NULL DEFAULT '[]',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
