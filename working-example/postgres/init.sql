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
