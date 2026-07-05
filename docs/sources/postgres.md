# Postgres Source

Reads rows from Postgres in batch mode using watermark-based incremental extraction.

## Config

```yaml
source:
  type: postgres
  url: postgres://user:pass@localhost:5432/app?sslmode=disable
  table: deals                 # or query: "SELECT ..." (not both)
  watermark: updated_at        # default: updated_at
  exclude: [ssn]               # columns to drop at extraction
  batch_size: 1000             # pagination size
  parallelism: 4               # parallel PK-range extraction
```

## Watermark behavior

Table mode queries a bounded window fixed at run start:

```text
WHERE watermark > $1 AND watermark <= $2   -- (last watermark, interval_end]
```

First run (zero watermark) has no lower bound. `watermark: none` skips the
window entirely — a full snapshot every run (for tables without a timestamp
column); pair it with `merge` or `replace`. An integer watermark column
(e.g. an auto-increment id) switches to keyset-cursor extraction
automatically. A missing or unsupported watermark column fails at
extraction with a clear error. Custom `query:` mode can
reference `{{watermark}}`, `{{interval_end}}`, and `{{pipeline}}`;
`exclude` is ignored there.

## Parallel extraction

With `parallelism > 1` and a numeric primary key, the PK range is split
into chunks scanned concurrently. Falls back to sequential pagination
otherwise.

## Type mapping

Schema is introspected from `information_schema`. JSON/JSONB arrive as
JSON text, uuid as canonical strings, numerics as float64, timestamps as
`time.Time` (UTC), NULL as nil.
