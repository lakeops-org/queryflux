-- Site agents (#255): an agent registers as kind 'siteAgent', and relays the workers at
-- its site, which are stored as '<site>/<name>' with parent = the site.
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'worker';
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS parent TEXT;
