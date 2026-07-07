# DLQ and replay

## What the DLQ is

The dead-letter queue (DLQ) is a JSONL file that receives rows that failed to deliver. Each line is a JSON record containing the original payload, the sync name, entity key, destination, and error details.

Vortara writes to the DLQ instead of silently dropping rows or halting the run. A failed row does not block other rows from delivering.

## Configuration

```yaml
errors:
  on_error: skip              # continue run after per-row errors
  dlq:
    path: ./dlq/pql.dlq.jsonl
    on_status: [500, 502, 503] # HTTP status codes that trigger DLQ write
```

When a delivery returns one of the listed status codes:

1. The row is written to the DLQ file.
2. State is saved as `last_status=failed`.
3. The run continues with the next entity.

## Inspecting the DLQ

```bash
vortara dlq list sync.yaml
```

Output:

```
entity       status  time                  destination
lead_002     failed  2026-07-07T12:00:00Z  POST http://localhost:18081/webhook
```

## Replaying

```bash
vortara replay sync.yaml --dlq
```

For each record in the DLQ:

1. Load the current row from the source (to get the latest data).
2. Re-deliver to the destination.
3. On success: save state as `last_status=success`, `last_decision=replay`, increment version.
4. On failure: leave the DLQ record in place (or append a new failure).

After a successful replay, the next `run` will see `last_status=success` and apply normal decision logic — if nothing has changed since the replay, the entity will skip.

## State after replay

```bash
vortara state inspect sync.yaml lead_002
```

```
status:    success
decision:  replay
version:   2
```

## Idempotency

Each replay delivery uses a deterministic key based on the sync name, entity key, action, and current fingerprint. If the destination has already processed this exact delivery (same content), it can deduplicate using the key.

## DLQ file format

```json
{"sync_name":"demo-pql","entity_key":"lead_002","destination":"restapi","action":"create","payload":{"id":"lead_002","email":"bob@corp.io","leadScore":75},"error":"HTTP 500: Internal Server Error","failed_at":"2026-07-07T12:00:00Z","delivery_key":"a3f9...7c2e"}
```

## Relationship to `diff`

After a partial run (some rows failed):

```bash
vortara diff sync.yaml
```

Shows only the entities that still have pending work — successfully delivered entities are fingerprint-matched and shown as `skip`. Failed entities appear as `create` or `update` (the operation that failed).

This makes it easy to confirm the scope of a replay before running it.
