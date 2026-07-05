# Getting Started

## Install

```bash
go install github.com/rakesh/vortaraos/cmd/vortara@latest
```

Verify the binary is installed:

```bash
vortara --help
```

## Your first pipeline

Create `pipeline.yaml`:

```yaml
name: my-first-pipeline

source:
  type: postgres
  url: postgres://user:pass@localhost:5432/mydb
  table: orders
  watermark: updated_at
  batch_size: 1000

destinations:
  - type: postgres
    url: postgres://user:pass@localhost:5432/destdb
    table: orders_copy
    match_on: [id]
    strategy: merge

settings:
  state:
    backend: sqlite
    path: ./vortara/state.db
```

No `cron:` means the pipeline runs once and exits — perfect for trying
things out. Add `cron: "*/15 * * * *"` later and use `vortara start`.

Validate and test connections:

```bash
vortara validate pipeline.yaml
vortara test pipeline.yaml
```

Run once:

```bash
vortara run pipeline.yaml
```

Check the last run:

```bash
vortara status pipeline.yaml
```

Start the scheduler (requires `cron:`):

```bash
vortara start pipeline.yaml
```

## Useful first commands

Full refresh from the beginning:

```bash
vortara run pipeline.yaml --full-refresh
```

Backfill from an explicit date:

```bash
vortara run pipeline.yaml --since 2026-01-01
```

Dry run without writing to destinations:

```bash
vortara run pipeline.yaml --dry-run
```

## Next steps

- Add `transform:` steps (filter, rename, add, drop, mask)
- Route rows conditionally with `when:` on each destination
- Set `settings.on_error: dlq` and use `vortara dlq show|replay`
- See the [YAML Reference](yaml-reference.md) for every option
