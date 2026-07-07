# Vortara

> **Status: early MVP.** Vortara is ready for local demos and design-partner feedback, not production CRM workloads yet.

Vortara is a programmable-state Reverse ETL engine.

It lets you define *when* records create, update, skip, fail, replay, or trigger actions — and explain every decision before mutating production systems.

Vortara runs as a Go CLI and keeps sync state in your own infrastructure, starting with SQLite for local and self-hosted workflows.

```
make demo
```

---

## What that one command does

The demo runs a complete failure → DLQ → replay cycle against a local Postgres database and a real HTTP webhook:

```
→ Validating sync config
→ Running inline state tests                          4 passed
→ Diff before first run (expect: creates=3)

  lead_001  create   Alice Smith  leadScore=88
  lead_002  create   Bob Jones    leadScore=75
  lead_003  create   Carol Wu     leadScore=92

→ Run (lead_002 webhook returns 500 → goes to DLQ)

  lead_001  ✓ delivered
  lead_002  ✗ failed (HTTP 500) → written to DLQ
  lead_003  ✓ delivered

→ DLQ after run

  lead_002  failed  2026-07-07T12:00:00Z  POST http://localhost:18081/webhook

→ State inspect lead_002 (expect: status=failed)

  status:    failed
  decision:  create

→ Diff after partial run (expect: creates=1 for lead_002 only)

  lead_002  create   Bob Jones  leadScore=75

→ Fixing webhook (restarting without FAIL_KEYS)
→ Replay DLQ (lead_002 should now succeed)

  lead_002  ✓ replayed

→ State inspect lead_002 after replay (expect: status=success)

  status:    success
  decision:  replay

→ Second full run (expect: all skip)

  lead_001  skip
  lead_002  skip
  lead_003  skip

→ Updating lead_002 score in Postgres (75 → 95)
→ Diff after score change (expect: update=1 for lead_002)

  lead_002  update  leadScore: 75 → 95

→ Run after score change

  lead_002  ✓ delivered

→ Explain lead_002 after update

  entity:    lead_002
  source:    leads (Postgres)
  dest:      http://localhost:18081/webhook

  Rules evaluated:
    ✗ new-lead       [→ create] did not match
    ✗ became-sql     [→ update] did not match
    ✓ score-changed  [→ update] matched, selected

  Field changes:
    leadScore   75 → 95

  Idempotency key:  a3f9...7c2e  (stable for this exact payload)
```

The entire cycle — validate, test, diff, run, DLQ, replay, diff again, explain — in a single command against your own infrastructure.

---

## Prerequisites

```bash
# Go 1.22+
go version

# Docker (for demo Postgres)
docker info
```

Docker is required for the demo Postgres instance. A local `psql` client is only needed if you want to inspect the demo database manually.

## Install

```bash
go install github.com/rkshvish/vortara/cmd/vortara@latest
```

## Run the demo

```bash
git clone https://github.com/rkshvish/vortara
cd vortara
make demo
```

Clean up when done:

```bash
make demo-clean
```

---

## How it works

A sync spec has four sections:

**Source** — a SQL query (Postgres today) with an `entity_key` column that identifies each row.

**Mapping** — renames and controls which fields are included in the fingerprint.

**Decisions** — rules that evaluate the current row against remembered state. First match wins. Default is `skip`.

**Destination** — where to deliver the payload (`restapi` today; HubSpot and Salesforce are planned).

```yaml
sync:
  name: pql-to-webhook

  source:
    type: postgres
    url: "${DATABASE_URL}"
    query: SELECT id, email, lead_score, lifecycle_stage FROM leads
    entity_key: id

  mapping:
    - source: lead_score
      dest: leadScore
    - source: lifecycle_stage
      dest: lifecycleStage
    - source: last_activity_at
      dest: lastActivityAt
      exclude_from_fingerprint: true   # timestamp noise won't trigger updates

  state:
    backend: sqlite
    path: ./state/pql.db

  decisions:
    default: skip
    rules:
      - name: new-lead
        when: first_seen()
        action: create

      - name: became-sql
        when:
          transitioned:
            field: lifecycleStage
            from: mql
            to: sql
        action: update

      - name: score-changed
        when:
          fingerprint_changed: {}
        action: update

  destination:
    type: restapi
    url: "${WEBHOOK_URL}"
    method: POST

  errors:
    dlq:
      path: ./dlq/pql.dlq.jsonl
      on_status: [500, 502, 503]
```

---

## Why Vortara?

Most Reverse ETL systems hide state internally. Vortara makes state a product surface:

- `diff` shows what would change before delivery.
- `explain` shows why one entity will create, update, skip, or replay.
- Inline state tests let you test activation rules in CI without a live source.
- DLQ + replay gives failed rows a recovery path instead of silent loss.
- Safety limits stop large accidental mutations before any row is delivered.

---

## CLI reference

```bash
vortara validate sync.yaml          # parse and validate config — no connections opened
vortara test sync.yaml              # run inline state tests (no live source needed)
vortara diff sync.yaml              # show what would change without delivering
vortara run sync.yaml               # deliver all pending decisions
vortara explain sync.yaml --key ID  # explain the decision for one entity
vortara dlq list sync.yaml          # show failed deliveries
vortara replay sync.yaml --dlq      # re-deliver failed rows from the DLQ
vortara state inspect sync.yaml ID  # show remembered state for one entity
```

---

## Inline state tests

Tests live in the YAML. No external fixtures needed.

```yaml
tests:
  - name: first_seen_creates
    previous: null
    current:
      id: lead_001
      email: alice@example.com
      leadScore: 82
      lifecycleStage: mql
    expect:
      decision: create
      triggered_rules: [new-lead]

  - name: timestamp_only_change_skips
    previous:
      leadScore: 82
      lifecycleStage: mql
      lastActivityAt: "2026-07-01T10:00:00Z"
    current:
      leadScore: 82
      lifecycleStage: mql
      lastActivityAt: "2026-07-02T10:00:00Z"
    expect:
      decision: skip
```

Run with: `vortara test sync.yaml`

---

## DLQ + replay

When a destination returns a retryable error (configurable status codes), the row is written to a JSONL dead-letter file instead of being silently dropped. State is saved as `failed`.

```bash
vortara dlq list sync.yaml
vortara replay sync.yaml --dlq
```

On successful replay, state is updated to `success`. The next `run` will skip that entity until its data changes again.

---

## Safety

```yaml
safety:
  max_creates_per_run: 500
  max_updates_per_run: 1000
```

A run that would exceed the limit is aborted before any delivery. Use `vortara diff` to preview before running.

---

## Docs

- [Architecture](docs/architecture.md)
- [State model](docs/state-model.md)
- [Decision rules](docs/decision-rules.md)
- [DLQ and replay](docs/dlq-replay.md)
- [Demo walkthrough](docs/examples/rest-webhook-demo.md)
- [Demo transcript](docs/demo-transcript.md)

---

## Roadmap

- [ ] HubSpot contacts destination
- [ ] Postgres state backend (for multi-instance deployments)
- [ ] Approval gate (`vortara approve`)
- [ ] Delivery history command
- [ ] State export/import
- [ ] GitHub Actions dry-run comment
- [ ] Prometheus metrics endpoint
- [ ] Salesforce destination

Tracked as [GitHub issues](https://github.com/rkshvish/vortara/issues).

---

## License

Apache 2.0
