# State model

## What state is

For each entity (identified by `entity_key`), Vortara remembers:

| field | type | description |
|---|---|---|
| `current_fingerprint` | string | SHA-256 of the normalized, canonical payload |
| `current_payload` | JSON | the last successfully delivered payload |
| `last_status` | string | `success`, `failed`, or `skip` |
| `last_decision` | string | `create`, `update`, `skip`, `replay`, or `delete` |
| `version` | int | increments on every state write |
| `updated_at` | datetime | when this state was last written |
| `remembered_state` | JSON | values saved by `remember:` blocks in rules |

State is scoped to `(sync_name, entity_key)`. Running two different syncs against the same database does not share state.

## When state is written

| event | state written |
|---|---|
| Successful delivery | `last_status=success`, fingerprint and payload updated, version incremented |
| Failed delivery (HTTP error) | `last_status=failed`, fingerprint and payload from current row, version incremented |
| Skip (no rule matched) | state is **not** written — previous state is preserved |
| Replay success | `last_status=success`, `last_decision=replay`, version incremented |
| Dry-run | state is **never** written (all writes are gated by `dryRun` flag) |

## How state drives decisions

The `first_seen()` condition checks whether any state exists for the entity. If there is no prior state, the rule matches.

The `fingerprint_changed()` condition computes the current fingerprint and compares it to `current_fingerprint` in state. If they differ, the rule matches.

The `transitioned(field, from, to)` condition looks up the value of `field` in `current_payload` (the last delivered payload) and compares it to the value in the current row.

The `once: true` flag on a rule checks `remembered_state` — if the rule name appears there, the rule is skipped regardless of whether its condition would match.

## The `remember:` block

Rules can write values to `remembered_state`:

```yaml
rules:
  - name: became-sql
    when:
      transitioned:
        field: lifecycleStage
        from: mql
        to: sql
    action: update
    remember:
      became_sql_at: now()
```

After this rule fires, `remembered_state` contains `{"became_sql_at": "2026-07-07T..."}`. This persists across runs and is available to condition evaluators.

## Inspecting state

```bash
vortara state inspect sync.yaml lead_002
```

Output:

```
entity:     lead_002
sync:       demo-pql
status:     success
decision:   replay
version:    3
updated_at: 2026-07-07T12:34:56Z

payload:
  id:             lead_002
  email:          bob@corp.io
  firstName:      Bob
  leadScore:      95
  lifecycleStage: mql
```

## SQLite schema

```sql
CREATE TABLE IF NOT EXISTS entity_state (
    sync_name           TEXT NOT NULL,
    entity_key          TEXT NOT NULL,
    current_fingerprint TEXT,
    current_payload     TEXT,
    last_status         TEXT,
    last_decision       TEXT,
    version             INTEGER DEFAULT 0,
    updated_at          DATETIME,
    remembered_state    TEXT,
    PRIMARY KEY (sync_name, entity_key)
);
```
