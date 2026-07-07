# Architecture

## Overview

Vortara evaluates a decision model over three inputs — the current source row, the previously remembered state for that entity, and a set of user-defined rules — and produces a delivery action: `create`, `update`, `skip`, or `delete`.

```
Source (Postgres query)
        │
        ▼
   Entity rows  ──────────────────────────────────────┐
        │                                             │
        ▼                                             ▼
   Mapping / Normalization               State store (SQLite)
   (field renames, fingerprint           (remembered payload,
    exclusions, timestamp normalize)      fingerprint, status)
        │                                             │
        └──────────────┬──────────────────────────────┘
                       ▼
              Decision engine
              (evaluate rules in order,
               first match wins,
               default = skip)
                       │
              ┌────────┴──────────┐
              ▼                   ▼
           deliver             skip / DLQ
        (REST API etc.)
              │
              ▼
         Save state
         (fingerprint, payload,
          status, version, timestamp)
```

## Key concepts

### Entity key

Every row has an `entity_key` — a stable identifier (e.g. `id`) that Vortara uses to correlate current source data with previously remembered state. This is the primary key of the state store.

### Fingerprint

Before comparing a row to its previous state, Vortara:

1. Applies the mapping (field renames).
2. Normalizes timestamps to UTC RFC3339 strings (so `time.Time` and `"2026-07-01T10:00:00Z"` are equal).
3. Serializes the result to canonical JSON (sorted keys, no whitespace).
4. Computes SHA-256 of that canonical JSON.

Fields marked `exclude_from_fingerprint: true` are stripped before hashing. This means a timestamp-only change (e.g. `lastActivityAt` refreshing every poll) does not produce an update.

### Decision trace

`decision.Trace()` evaluates **all** rules — it does not short-circuit on first match. Each rule gets a `RuleTrace`:

| field | meaning |
|---|---|
| `Matched` | the rule's `when:` condition evaluated to true |
| `Winner` | this was the first matching rule — its action is used |
| `FiredBefore` | rule has `once: true` and has already fired for this entity |
| `Action` | the action this rule would take |
| `Reason` | human-readable explanation of why it matched or didn't |

`explain` uses this to show all rules with clear status: `matched, selected` / `matched, not selected (first-match-wins)` / `did not match`.

### Two-phase delivery

`runOnce` uses a two-phase approach to prevent partial delivery on safety violations:

1. **Phase 1 — evaluate**: iterate all source rows, run decisions, buffer all `pendingDelivery` structs.
2. **Phase 2 — deliver**: check safety limits (max_creates, max_updates) against buffered counts, then deliver and save state.

If safety limits are exceeded, the entire run is aborted before any row is delivered.

### Delivery key (idempotency)

Each delivery gets a deterministic key:

```
sha256(syncName + ":" + destName + ":" + entityKey + ":" + action + ":" + fingerprint)
```

This key is stable for the same logical operation (same entity, same content, same action). If a destination has already seen this key, it can deduplicate. If the entity's data changes, the fingerprint changes and the key changes — triggering a new delivery.

### Dry-run isolation

With `--dry-run`, the engine evaluates and delivers to an in-memory destination. All `SaveEntityState` and `MarkRuleFired` calls are gated by `e.dryRun` — the real state store is never written.

## State store

SQLite by default. Schema is managed by the engine on first run.

```
entity_state
  sync_name       TEXT
  entity_key      TEXT
  current_fingerprint  TEXT
  current_payload      TEXT (JSON)
  last_status     TEXT  (success | failed | skip)
  last_decision   TEXT  (create | update | skip | replay)
  version         INT
  updated_at      DATETIME
  remembered_state TEXT (JSON)  -- from rule `remember:` blocks
```

A Postgres backend is planned for multi-instance deployments.

## Error model

Errors at two levels:

- **Connection/load error** (`loadErr`): the source or destination could not be reached. The entire run fails; no state is written.
- **Per-row error** (`res.Errors`): a single entity failed to deliver (e.g. HTTP 500). State is saved as `failed`. If DLQ is configured, the row is written to the JSONL file. The run continues with remaining entities.

## Packages

```
cmd/vortara/cmd/         CLI commands (run, diff, explain, test, replay, dlq, state, validate)
internal/engine/         runOnce, delivery loop, dry-run, safety, DLQ write
internal/decision/       Trace(), RuleTrace, condition evaluators
internal/fingerprint/    Of(), NormalizePayload(), writeCanonical()
internal/diff/           Compute(), FieldChange
pkg/config/sync/         Config structs, YAML unmarshalling
pkg/registry/            Source/destination type registry
pkg/source/              Source interface, Postgres implementation
pkg/destination/         Destination interface, REST API implementation
pkg/state/               EntityState struct, SQLite backend
pkg/dlq/                 DLQ writer, JSONL format, record struct
```
