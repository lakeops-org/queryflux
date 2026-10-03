-- NULL means the translation outcome was not recorded (including historical rows).
ALTER TABLE query_records ADD COLUMN translation JSONB;
