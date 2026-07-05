# Postgres Destination

Writes rows to a Postgres table. Supports all four strategies with
bulk-optimized writes.

## Config

```yaml
destinations:
  - type: postgres
    url: postgres://user:pass@localhost:5432/dest?sslmode=disable
    table: orders_copy             # or schema-qualified
    match_on: [id]
    strategy: merge                # merge | append | replace | delete+insert
```

## Strategies and mechanics

| Strategy | Mechanics |
|---|---|
| `merge` | multi-row `INSERT ... ON CONFLICT DO UPDATE` chunks (up to 1000 rows / statement), per-row fallback on chunk failure |
| `append` | COPY protocol above the copy threshold, multi-row INSERT below |
| `replace` | staging table + atomic swap: rows COPY into a per-run staging table, then one transaction truncates the target and swaps the snapshot in at run end — a failed run leaves the target untouched |
| `delete+insert` | delete matching keys, then insert |
| `scd2` | type-2 history: changed rows close the current version and insert a new one; `_scd_valid_from/_scd_valid_to/_scd_is_current` columns added automatically |

## Notes

- Chunks split automatically when a `match_on` value repeats within a
  batch (Postgres cannot update the same row twice in one statement).
- Multi-column `match_on` uses the per-row path.
- `merge` and `delete+insert` check the delivery log per row, making
  re-runs idempotent; `append`/`replace` intentionally skip that check.
- `options.copy_threshold` tunes when COPY kicks in.
