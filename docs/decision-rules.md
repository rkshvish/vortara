# Decision rules

## Structure

```yaml
decisions:
  default: skip          # action when no rule matches (skip | create | update | delete)
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
      once: true          # fire at most once per entity
      remember:
        became_sql_at: now()

    - name: score-changed
      when:
        fingerprint_changed: {}
      action: update
```

## Evaluation order

Rules are evaluated top to bottom. **First match wins** — the first rule whose `when:` condition evaluates to true sets the delivery action. Subsequent matching rules are recorded in the trace with `Matched=true, Winner=false`.

All rules are always evaluated (no short-circuit) so that `explain` can show the full trace.

## Conditions

### `first_seen()`

Matches when there is no previous state for this entity. True on the very first run for a given `entity_key`.

```yaml
when: first_seen()
```

### `fingerprint_changed`

Matches when the current fingerprint differs from the stored fingerprint. Use this to trigger on any data change (excluding fields marked `exclude_from_fingerprint`).

```yaml
when:
  fingerprint_changed: {}
```

### `transitioned`

Matches when a specific field has changed from one value to another between the last delivered payload and the current row.

```yaml
when:
  transitioned:
    field: lifecycleStage
    from: mql
    to: sql
```

Both `from` and `to` are required. Partial transitions (e.g. "any value → sql") are not yet supported.

## Rule flags

### `once: true`

The rule fires at most once per entity, regardless of how many times the condition would match in future runs. The fired state is recorded in `remembered_state`.

```yaml
- name: became-sql
  when:
    transitioned:
      field: lifecycleStage
      from: mql
      to: sql
  action: update
  once: true
```

### `remember:`

Write named values to `remembered_state` when the rule fires. Available values:

| expression | value stored |
|---|---|
| `now()` | current UTC RFC3339 timestamp |

```yaml
remember:
  became_sql_at: now()
  onboarding_status: completed
```

## The `explain` command

```bash
vortara explain sync.yaml --key lead_002
```

Shows how each rule evaluated for a specific entity, including rules that matched but lost to first-match-wins:

```
Rules evaluated:
  ✓ new-lead      [→ create] matched, not selected (first-match-wins)
  ✓ score-changed [→ update] matched, selected
  ✗ became-sql    [→ update] did not match
```

## Inline tests

Test rule behavior directly in the YAML without a live source:

```yaml
tests:
  - name: lifecycle_transition_to_sql
    previous:
      lifecycleStage: mql
      leadScore: 91
    current:
      lifecycleStage: sql
      leadScore: 91
    expect:
      decision: update
      triggered_rules:
        - became-sql
      changed_fields:
        - lifecycleStage
```

`triggered_rules` — the rules that matched AND won (i.e. `Winner=true`).
`changed_fields` — fields that must appear in the diff (must-contain, not exhaustive).

Run with: `vortara test sync.yaml`
