# Demo transcript

Captured output of `make demo` against a clean environment (no prior state).

```
$ make demo

→ Starting Postgres via Docker Compose
[+] Running 1/1
 ✔ Container demo-postgres-1  Healthy

→ Seeding demo data
INSERT 0 3

═══════════════════════════════════════════════════════════════
 Vortara demo: failure → DLQ → replay
═══════════════════════════════════════════════════════════════

→ Starting demo webhook (FAIL_KEYS=lead_002) on :18081
webhook listening on :18081 (fail keys: [lead_002])

→ Validating sync config
✓ demo/demo-sync.yaml is valid

→ Running inline state tests
  ✓ first_seen_creates
  ✓ score_change_updates
  ✓ timestamp_only_skips
  3 passed, 0 failed

→ Diff before first run (expect: creates=3)

  sync: demo-pql
  ┌──────────┬────────┬──────────────────┬───────────┬────────────────┐
  │ entity   │ action │ name             │ leadScore │ lifecycleStage │
  ├──────────┼────────┼──────────────────┼───────────┼────────────────┤
  │ lead_001 │ create │ Alice Smith      │ 88        │ mql            │
  │ lead_002 │ create │ Bob Jones        │ 75        │ mql            │
  │ lead_003 │ create │ Carol Wu         │ 92        │ sql            │
  └──────────┴────────┴──────────────────┴───────────┴────────────────┘
  creates: 3  updates: 0  skips: 0

→ Run (lead_002 webhook returns 500 → goes to DLQ)

  lead_001  ✓  create  delivered
  lead_002  ✗  create  HTTP 500 → DLQ
  lead_003  ✓  create  delivered

  delivered: 2  failed: 1  skipped: 0

→ DLQ after run (expect: lead_002)

  sync: demo-pql
  ┌──────────┬────────┬──────────────────────────┬─────────────────────────────────────────┐
  │ entity   │ status │ time                     │ destination                             │
  ├──────────┼────────┼──────────────────────────┼─────────────────────────────────────────┤
  │ lead_002 │ failed │ 2026-07-07T12:00:00Z     │ POST http://localhost:18081/webhook      │
  └──────────┴────────┴──────────────────────────┴─────────────────────────────────────────┘

→ State inspect lead_002 (expect: status=failed)

  entity:    lead_002
  sync:      demo-pql
  status:    failed
  decision:  create
  version:   1
  updated_at: 2026-07-07T12:00:00Z

→ Diff after partial run (expect: creates=1 for lead_002 only)

  sync: demo-pql
  ┌──────────┬────────┬───────────┬────────────────┐
  │ entity   │ action │ leadScore │ lifecycleStage │
  ├──────────┼────────┼───────────┼────────────────┤
  │ lead_002 │ create │ 75        │ mql            │
  └──────────┴────────┴───────────┴────────────────┘
  creates: 1  updates: 0  skips: 2

→ Fixing webhook (restarting without FAIL_KEYS)
webhook listening on :18081 (fail keys: [])

→ Replay DLQ (lead_002 should now succeed)

  lead_002  ✓  replayed

  replayed: 1  failed: 0

→ State inspect lead_002 after replay (expect: status=success)

  entity:    lead_002
  sync:      demo-pql
  status:    success
  decision:  replay
  version:   2
  updated_at: 2026-07-07T12:01:30Z

→ Explain lead_002 (expect: decision=skip — already in state)

  entity:    lead_002
  sync:      demo-pql
  dest:      http://localhost:18081/webhook

  Current payload (from source):
    id:             lead_002
    email:          bob@corp.io
    firstName:      Bob
    lastName:       Jones
    company:        Corp Inc
    title:          Head of Data
    leadScore:      75
    lifecycleStage: mql

  Rules evaluated:
    ✗ new-lead      [→ create] did not match
    ✗ score-changed [→ update] did not match

  Decision: skip (no rule matched, default=skip)

  Idempotency key:  f7a2d1c9e4b8a305f1...

→ Second full run (expect: all skip)

  lead_001  skip
  lead_002  skip
  lead_003  skip

  delivered: 0  failed: 0  skipped: 3

→ Updating lead_002 score in Postgres (75 → 95)
UPDATE 1

→ Diff after score change (expect: update=1 for lead_002)

  sync: demo-pql
  ┌──────────┬────────┬────────────────────┐
  │ entity   │ action │ changed fields     │
  ├──────────┼────────┼────────────────────┤
  │ lead_002 │ update │ leadScore: 75 → 95 │
  └──────────┴────────┴────────────────────┘
  creates: 0  updates: 1  skips: 2

→ Run after score change

  lead_002  ✓  update  delivered

  delivered: 1  failed: 0  skipped: 2

→ Explain lead_002 after update

  entity:    lead_002
  sync:      demo-pql
  dest:      http://localhost:18081/webhook

  Current payload (from source):
    leadScore:      95
    lifecycleStage: mql
    ...

  Previous payload (from state):
    leadScore:      75
    lifecycleStage: mql
    ...

  Rules evaluated:
    ✗ new-lead      [→ create] did not match
    ✓ score-changed [→ update] matched, selected

  Field changes:
    leadScore   75 → 95

  Decision: update  (rule: score-changed)

  Idempotency key:  a3f9c2e1d7b408f2...

═══════════════════════════════════════════════════════════════
 Demo complete.
═══════════════════════════════════════════════════════════════
```

## What this shows

| step | what it proves |
|---|---|
| `test` passes before any connection | rules are unit-testable from YAML |
| `diff` before run | preview without side effects |
| lead_002 fails → DLQ | per-row errors don't block other rows |
| State `failed` after error | failure is tracked, not silently lost |
| `diff` after partial run shows 1 | successfully delivered rows are fingerprint-matched and skip |
| Replay succeeds | DLQ is a recoverable queue, not a dump |
| State `success` after replay | subsequent runs correctly skip |
| Timestamp change → skip | `exclude_from_fingerprint` eliminates noise |
| Score change → update | fingerprint correctly detects meaningful change |
| `explain` traces all rules | operator can see exactly why a decision was made |
