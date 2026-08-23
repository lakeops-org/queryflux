-- Add 'guard' as a valid user_scripts kind.
-- Guard scripts are managed from the Guardrails page; groups select them by name.
ALTER TABLE user_scripts
    DROP CONSTRAINT IF EXISTS user_scripts_kind_check;

ALTER TABLE user_scripts
    ADD CONSTRAINT user_scripts_kind_check
    CHECK (kind IN ('translation_fixup', 'routing', 'guard'));
