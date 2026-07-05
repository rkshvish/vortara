# Redshift Source

Reads rows from Amazon Redshift in batch mode. Redshift speaks the
PostgreSQL wire protocol, so this connector shares the Postgres source's
introspection, watermark windows, and pagination.

## Config

```yaml
source:
  type: redshift
  url: redshift://user:pass@cluster.abc123.us-east-1.redshift.amazonaws.com:5439/analytics
  table: dim_accounts           # or query: "SELECT ..."
  watermark: updated_at
  batch_size: 5000
```

The `redshift://` scheme is normalized to `postgres://` internally;
a plain `postgres://` URL pointing at a cluster works too.

## Notes

- Default Redshift port is 5439; clusters require SSL (add
  `?sslmode=require` if your cluster enforces it).
- Redshift does not enforce primary keys, so parallel PK-range
  extraction typically falls back to sequential pagination.
- `SUPER` columns arrive as text; there is no JSONB in Redshift.
