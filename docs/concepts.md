# Concepts

## Three modes

**Batch** extracts rows on a schedule. The source receives a lower bound watermark and a fixed upper bound for the run. Each run processes one bounded time window.

**Streaming** processes events as they arrive from Kafka or webhook POST requests. Offsets or acknowledgements are only committed after downstream delivery succeeds.

**Both** runs batch and streaming together: keep a batch `source:` and add a streaming `also:` block. Use this when you want a scheduled backfill path and a real-time path at the same time.

## Watermark

The watermark is not “the newest timestamp seen in the data.” It is “the end of the last completed extraction window.”

For a batch run, Vortara computes `interval_end` once at run start and extracts:

```text
Run 1: updated_at > 0001-01-01T00:00:00Z AND updated_at <= 2024-01-01T10:00:00Z
Run 1 saves watermark: 2024-01-01T10:00:00Z

Run 2: updated_at > 2024-01-01T10:00:00Z AND updated_at <= 2024-01-01T10:15:00Z
```

This matters because rows that arrive during a long extraction are picked up by the next run, not lost or duplicated.

Append-only tables with an auto-increment key can use the integer column
directly (`watermark: id`) — a keyset cursor with no late-arrival risk for
inserts. Tables without a timestamp column can use `watermark: none` — a full
snapshot every run with no cursor saved. Pair it with `replace` or `merge`.

Reset the watermark for a full re-sync:

```bash
vortara watermark reset pipeline.yaml
```

Or reset and run in one step:

```bash
vortara run pipeline.yaml --full-refresh
```

## Strategy

How rows are written to a destination:

| Strategy | Behavior | Requires `match_on` |
|---|---|---|
| `merge` | upsert existing rows, default strategy | yes |
| `append` | insert only, duplicates allowed | no |
| `replace` | truncate target, then insert | no |
| `delete+insert` | delete matching rows, then insert | yes |

The destination decides the write mechanics. The strategy decides the write pattern.

## Idempotency

Each row has a generated row ID. Destinations check the delivery log in SQLite before writing. If a row was already delivered to the same pipeline and destination, it is skipped.

That makes rerunning a batch pipeline safe when the strategy uses delivery checks. `append` intentionally skips that check because duplicate inserts are part of the strategy.

## State backends

SQLite (default) keeps all state in one local file — right for a single
instance. `backend: postgres` stores the same tables
(`vortara_watermarks`, `vortara_run_log`, ...) in a shared database so
multiple instances, or instances without durable local disk, see the
same watermarks and delivery log.

## Error handling

`settings.on_error` decides what happens when a row fails delivery:

- `skip` (default) — count the error, keep going, run is marked failed
- `retry` — up to 3 delivery attempts with backoff, then treated as failed
- `dlq` — failed rows are appended to a JSONL dead-letter file and the run
  stays successful; re-deliver later with `vortara dlq replay`

## Dry run

`vortara run --dry-run` still extracts, transforms, and routes rows. It replaces real destinations with a stdout destination so you can inspect the final payload without writing anything.

## Custom query mode

Postgres batch sources can use a raw SQL query instead of `table` plus `watermark`. The query can reference:

- `{{watermark}}`
- `{{interval_end}}`
- `{{pipeline}}`

In custom query mode, `exclude` is ignored because the query controls the selected columns directly.
