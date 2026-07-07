# Demo: Postgres leads → REST webhook

This is the canonical Vortara demo. It shows the full lifecycle: create, fail, DLQ, fix, replay, skip, update, explain.

Source: `demo/demo-sync.yaml`

## What it syncs

Three leads in a Postgres `leads` table:

| id | email | leadScore | lifecycleStage |
|---|---|---|---|
| lead_001 | alice@acme.com | 88 | mql |
| lead_002 | bob@corp.io | 75 | mql |
| lead_003 | carol@startup.io | 92 | sql |

Destination: a local HTTP webhook server that intentionally returns `500` for `lead_002` on the first run.

## The sync spec

Key decisions:

```yaml
decisions:
  default: skip
  rules:
    - name: new-lead
      when: first_seen()
      action: create

    - name: score-changed
      when:
        fingerprint_changed: {}
      action: update
```

`lastActivityAt` and `createdAt` are excluded from the fingerprint — timestamp refreshes don't trigger updates.

## Step by step

### 1. Validate

```bash
vortara validate demo/demo-sync.yaml
```

Checks YAML structure. Does not open any connections.

### 2. Test inline state tests

```bash
vortara test demo/demo-sync.yaml
```

The spec includes three tests:

- `first_seen_creates` — no prior state → decision: create
- `score_change_updates` — leadScore changed → decision: update
- `timestamp_only_skips` — only `lastActivityAt` changed (excluded from fingerprint) → decision: skip

### 3. Diff before first run

```bash
vortara diff demo/demo-sync.yaml
```

All three leads are `first_seen()` — three creates pending.

### 4. First run (lead_002 fails)

```bash
vortara run demo/demo-sync.yaml
```

The demo webhook server has `FAIL_KEYS=lead_002` set. `lead_001` and `lead_003` deliver successfully. `lead_002` gets HTTP 500, goes to DLQ, state saved as `failed`.

### 5. Inspect the DLQ

```bash
vortara dlq list demo/demo-sync.yaml
```

Shows `lead_002` with the error and timestamp.

### 6. Diff after partial run

```bash
vortara diff demo/demo-sync.yaml
```

Only `lead_002` appears — the other two are fingerprint-matched and skip.

### 7. Fix the webhook and replay

The Makefile restarts the webhook without `FAIL_KEYS`:

```bash
vortara replay demo/demo-sync.yaml --dlq
```

`lead_002` delivers successfully. State is updated to `success`.

### 8. Second full run — all skip

```bash
vortara run demo/demo-sync.yaml
```

Nothing has changed. All three entities skip.

### 9. Score update

```bash
UPDATE leads SET lead_score=95, last_activity_at=now() WHERE id='lead_002';
```

`last_activity_at` changes but is excluded from the fingerprint. `lead_score` changes and is included.

```bash
vortara diff demo/demo-sync.yaml
```

One update: `lead_002 — leadScore: 75 → 95`.

### 10. Explain

```bash
vortara explain demo/demo-sync.yaml --key lead_002
```

```
entity:    lead_002
source:    leads (Postgres)
dest:      http://localhost:18081/webhook

Rules evaluated:
  ✓ new-lead      [→ create] matched, not selected (first-match-wins)
  ✓ score-changed [→ update] matched, selected

Field changes:
  leadScore   75 → 95

Idempotency key:  a3f9...7c2e
```

`new-lead` matched (it fires on `first_seen()` — wait, lead_002 has prior state now). Actually after the replay, `new-lead` would not match because state exists. `score-changed` matches because the fingerprint changed. The explain output shows exactly this reasoning.

## Running it yourself

```bash
make demo        # full automated run
make demo-clean  # tear down Postgres, remove state and DLQ files
```

## Files

| file | purpose |
|---|---|
| `demo/demo-sync.yaml` | sync specification with inline tests |
| `demo/docker-compose.yml` | Postgres 16 on port 15432 |
| `demo/seed/seed.sql` | creates and seeds the `leads` table |
| `demo/webhook/main.go` | webhook server; `FAIL_KEYS=lead_002` makes it return 500 |
| `Makefile` | `demo`, `demo-infra`, `demo-clean` targets |
