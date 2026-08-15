-- Phase 1A: agent identity + conversation tracking fields on query_records
ALTER TABLE query_records
    ADD COLUMN IF NOT EXISTS agent_id          TEXT,
    ADD COLUMN IF NOT EXISTS conversation_id   TEXT,
    ADD COLUMN IF NOT EXISTS step_index        INTEGER,
    ADD COLUMN IF NOT EXISTS tool_call_id      TEXT,
    ADD COLUMN IF NOT EXISTS query_intent      TEXT,
    ADD COLUMN IF NOT EXISTS guard_actions     JSONB    NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS was_guard_blocked BOOLEAN  NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_query_records_agent_id
    ON query_records (agent_id)
    WHERE agent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_query_records_conversation_id
    ON query_records (conversation_id)
    WHERE conversation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_query_records_was_guard_blocked
    ON query_records (was_guard_blocked)
    WHERE was_guard_blocked = TRUE;
