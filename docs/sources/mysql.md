# MySQL Source

Reads rows from MySQL in batch mode using watermark-based incremental extraction.

## Config

```yaml
source:
  type: mysql
  url: mysql://user:pass@localhost:3306/app    # or Go DSN: user:pass@tcp(host:3306)/app
  table: orders                 # or query: "SELECT ..." (not both)
  watermark: updated_at         # default: updated_at
  exclude: [password_hash]
```

## Watermark behavior

```text
WHERE watermark > ? AND watermark <= ?   -- (last watermark, interval_end]
```

First run (zero watermark) has no lower bound. `watermark: none` skips the
window entirely — a full snapshot every run (for tables without a timestamp
column); pair it with `merge` or `replace`. An integer watermark column
(e.g. an auto-increment id) switches to keyset-cursor extraction
automatically. A missing or unsupported watermark column fails at
extraction with a clear error. Custom `query:` mode may
use one `?` placeholder (interval end) or two (watermark, interval end).

## Notes

- Schema and primary keys are introspected from `information_schema`;
  identifiers are backtick-quoted.
- `parseTime=true` is forced so DATETIME/TIMESTAMP columns arrive as
  `time.Time`. JSON columns arrive as JSON text, DECIMAL as numeric
  strings, NULL as nil.
- Composite primary keys produce `k1=v1,k2=v2` row keys.
