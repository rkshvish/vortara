# MySQL Destination

Writes rows to a MySQL table. Supports all four strategies with
multi-row batched writes.

## Config

```yaml
destinations:
  - type: mysql
    url: mysql://user:pass@localhost:3306/dest    # or Go DSN form
    table: orders_copy
    match_on: [id]
    strategy: merge
```

## Strategies

| Strategy | Mechanics |
|---|---|
| `merge` | multi-row `INSERT ... ON DUPLICATE KEY UPDATE` (500 rows/statement) |
| `append` | multi-row `INSERT` |
| `replace` | `TRUNCATE` once per run, then `INSERT` |
| `delete+insert` | `DELETE` matching keys, then `INSERT` |

## Notes

- `merge` relies on MySQL's ON DUPLICATE KEY semantics: the `match_on`
  columns must be covered by a PRIMARY KEY or UNIQUE index on the target
  table.
- A failed chunk falls back to per-row writes so bad rows are isolated.
- Map/array values are marshaled to JSON text for JSON columns.
- `merge` and `delete+insert` use the delivery log for per-row
  idempotency; `append`/`replace` intentionally skip it.
