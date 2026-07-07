-- Seed data for the Vortara DLQ/replay demo.
-- Run against the demo Postgres database.

CREATE TABLE IF NOT EXISTS leads (
    id             TEXT PRIMARY KEY,
    email          TEXT NOT NULL,
    first_name     TEXT,
    last_name      TEXT,
    company        TEXT,
    title          TEXT,
    lead_score     INT,
    lifecycle_stage TEXT,
    last_activity_at TIMESTAMPTZ DEFAULT now(),
    created_at     TIMESTAMPTZ DEFAULT now()
);

TRUNCATE leads;

INSERT INTO leads (id, email, first_name, last_name, company, title, lead_score, lifecycle_stage)
VALUES
    ('lead_001', 'alice@acme.com',   'Alice', 'Smith',  'Acme Corp',    'VP Eng',      88, 'mql'),
    ('lead_002', 'bob@corp.io',      'Bob',   'Jones',  'Corp Inc',     'Head of Data', 75, 'mql'),
    ('lead_003', 'carol@startup.io', 'Carol', 'Wu',     'Startup Labs', 'CEO',          92, 'sql');
