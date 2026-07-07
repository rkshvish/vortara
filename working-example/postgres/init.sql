CREATE SCHEMA IF NOT EXISTS b2b_saas;

CREATE TABLE IF NOT EXISTS b2b_saas.usage_sessions (
    session_id TEXT PRIMARY KEY,
    user_email TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    revenue NUMERIC(12,2) NOT NULL DEFAULT 0
);

INSERT INTO b2b_saas.usage_sessions (session_id, user_email, session_start, status, revenue)
VALUES
  ('sess_001', 'alice@example.com', '2026-07-01T10:00:00Z', 'won', 12000.00),
  ('sess_002', 'bob@example.com',   '2026-07-01T11:15:00Z', 'lost',  2500.00),
  ('sess_003', 'cara@example.com',  '2026-07-01T12:30:00Z', 'won',   9800.00)
ON CONFLICT (session_id) DO NOTHING;

INSERT INTO b2b_saas.usage_sessions (session_id, user_email, session_start, status, revenue)
SELECT
  format('sess_%05s', gs),
  format('user%05s@example.com', gs),
  TIMESTAMPTZ '2026-07-01 00:00:00Z' + (gs || ' minutes')::interval,
  CASE WHEN gs % 3 = 0 THEN 'won'
       WHEN gs % 3 = 1 THEN 'lost'
       ELSE 'pending'
  END,
  (gs % 2500) + 100.00
FROM generate_series(4, 10003) AS gs
ON CONFLICT (session_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS leads (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  first_name TEXT,
  last_name TEXT,
  company TEXT,
  title TEXT,
  lead_score INT,
  lifecycle_stage TEXT,
  last_activity_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ
);

INSERT INTO leads (
  id, email, first_name, last_name, company, title,
  lead_score, lifecycle_stage, last_activity_at, created_at
)
VALUES
  ('lead_001', 'alice@example.com', 'Alice', 'Rao', 'Acme', 'VP Sales',
   82, 'mql', TIMESTAMPTZ '2026-07-07T09:00:00Z', TIMESTAMPTZ '2026-06-07T09:00:00Z'),
  ('lead_002', 'bob@example.com', 'Bob', 'Shah', 'BetaCorp', 'Founder',
   45, 'trial', TIMESTAMPTZ '2026-07-07T09:05:00Z', TIMESTAMPTZ '2026-06-17T09:00:00Z'),
  ('lead_003', 'cara@example.com', 'Cara', 'Iyer', 'Gamma', 'RevOps',
   91, 'sql', TIMESTAMPTZ '2026-07-07T09:10:00Z', TIMESTAMPTZ '2026-06-27T09:00:00Z')
ON CONFLICT (id) DO UPDATE SET
  email = EXCLUDED.email,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  company = EXCLUDED.company,
  title = EXCLUDED.title,
  lead_score = EXCLUDED.lead_score,
  lifecycle_stage = EXCLUDED.lifecycle_stage,
  last_activity_at = EXCLUDED.last_activity_at,
  created_at = EXCLUDED.created_at;
