# Snowflake Source

Reads rows from Snowflake in batch mode using watermark-based incremental extraction.

## Config

```yaml
source:
  type: snowflake
  url: snowflake://user:pass@account/database/schema?warehouse=WH&role=LOADER
  table: DIM_ACCOUNTS           # or schema-qualified: MARTS.DIM_ACCOUNTS
  watermark: UPDATED_AT
  batch_size: 1000
```

Schema defaults to `PUBLIC` when the URL path has only a database.

## Notes

- Identifiers follow Snowflake case-folding: unquoted DDL creates
  uppercase names, and the connector introspects accordingly.
- Every session sets `QUERY_TAG = 'vortara'` for cost attribution.
- Watermark window: `WHERE wm > ? AND wm <= ?`, pagination via
  LIMIT/OFFSET ordered by the watermark column.
