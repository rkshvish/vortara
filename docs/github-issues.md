# GitHub issues to create

Open these at https://github.com/rkshvish/vortara/issues/new

---

## 1. HubSpot contacts destination

Implement a HubSpot destination for upserting contacts via the HubSpot Contacts API v3.

**Scope**
- Batch upsert contacts by email (or external ID)
- Map Vortara field names to HubSpot property names via the `mapping:` block
- Auth: `type: bearer, token: ${HUBSPOT_API_KEY}`
- Handle rate limits (429) → DLQ
- Handle partial batch failures

**Config shape**
```yaml
destination:
  type: hubspot
  object: contacts
  match_on: [email]
  auth:
    type: bearer
    token: "${HUBSPOT_API_KEY}"
```

---

## 2. Postgres state backend

Add a Postgres-backed state store for multi-instance deployments.

SQLite works for single-process use. When running Vortara in multiple pods or workers against the same sync, each instance needs to read/write shared state without conflicts.

**Scope**
- `state.backend: postgres` config option
- Connection pool with configurable max connections
- Row-level locking on upsert to prevent concurrent writes for the same entity
- Schema migration on startup (same columns as SQLite backend)

---

## 3. Approval gate (`vortara approve`)

Add an approval flow that requires a human sign-off before a batch of creates/updates is delivered.

**Flow**
1. `vortara diff sync.yaml` produces a diff with a snapshot hash
2. Operator reviews and runs `vortara approve sync.yaml --hash <hash>`
3. `vortara run sync.yaml` checks for a valid unexpired approval before delivering
4. Approval expires after a configurable TTL (default: 1 hour)

**Use case**: high-value syncs where a bad run could spam thousands of contacts.

---

## 4. Delivery history command

Add a `vortara history sync.yaml --key <entity_key>` command that shows the delivery history for a single entity.

**Output should show:**
- Timestamp, action, status (success/failed/skip), fingerprint version, rule that triggered

**Implementation note:** requires a `delivery_history` table in the state store with FK to `entity_state`.

---

## 5. State export/import

Add `vortara state export sync.yaml > state.json` and `vortara state import sync.yaml < state.json`.

**Use cases:**
- Migrate state between SQLite and Postgres backends
- Back up state before a risky run
- Seed state for testing without running a full sync

---

## 6. GitHub Actions dry-run comment

Publish a GitHub Action that runs `vortara diff` on a PR and posts the output as a PR comment.

**Workflow**
```yaml
- uses: rkshvish/vortara-action@v1
  with:
    sync: sync.yaml
    mode: diff
    comment: true
```

Comment format: summary table (creates/updates/skips) + expandable field-level diff.

---

## 7. Prometheus metrics endpoint

Expose a `/metrics` endpoint in `vortara start` (scheduler mode) with standard counters:

- `vortara_rows_delivered_total{sync, action}` — total successful deliveries
- `vortara_rows_failed_total{sync}` — total failed deliveries (DLQ)
- `vortara_rows_skipped_total{sync}` — total skips
- `vortara_run_duration_seconds{sync}` — run duration histogram
- `vortara_dlq_depth{sync}` — current DLQ record count

**Note:** Prometheus metrics were prototyped in an earlier version. Wire the existing `internal/metrics` package (if it exists) to these label names.
