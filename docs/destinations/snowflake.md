# Snowflake Destination

Writes rows to a Snowflake table. Supports all four strategies.

## Config

```yaml
destinations:
  - type: snowflake
    url: snowflake://user:pass@account/analytics/marts?warehouse=WH&role=LOADER
    table: dim_accounts            # or schema-qualified: marts.dim_accounts
    match_on: [account_id]
    strategy: merge                # merge | append | replace | delete+insert
```

## Strategies

| Strategy | Mechanics |
|---|---|
| `merge` | `MERGE INTO ... USING (SELECT ...)` upsert per row |
| `append` | plain `INSERT` |
| `replace` | `TRUNCATE` once per run, then `INSERT` |
| `delete+insert` | `DELETE` matching keys, then `INSERT` |

## Notes

- Identifiers are uppercased and quoted, matching Snowflake's default
  case-folding for tables created with unquoted DDL.
- Map/array values are marshaled to JSON text for VARIANT/OBJECT columns.
- `merge` and `delete+insert` require `match_on` and use the delivery
  log for per-row idempotency.
