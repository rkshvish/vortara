# CLI Reference

## `vortara validate <file>`

Validate a pipeline config file.

```bash
vortara validate pipeline.yaml
```

## `vortara test <file>`

Test every configured connection without moving data.

```bash
vortara test pipeline.yaml
```

The command attempts to connect to:

- the batch source, if configured
- the streaming source, if configured
- every destination

## `vortara run <file>`

Run one batch pipeline execution.

```bash
vortara run pipeline.yaml
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `--once` | `true` | accepted for compatibility, run once and exit |
| `--full-refresh` | `false` | reset the batch watermark before the run |
| `--dry-run` | `false` | replace destinations with stdout output |
| `--since DATE` | none | backfill: set the watermark to an explicit start date (RFC3339 or YYYY-MM-DD) for this run |

Examples:

```bash
vortara run pipeline.yaml --full-refresh
vortara run pipeline.yaml --dry-run
vortara run pipeline.yaml --since 2026-01-01
```

`--full-refresh` and `--since` are mutually exclusive.

## `vortara dlq show|replay <file>`

Inspect or re-deliver dead-lettered rows (`settings.on_error: dlq`).

```bash
vortara dlq show pipeline.yaml            # list dead-lettered rows
vortara dlq replay pipeline.yaml          # re-deliver; successes are removed from the file
vortara dlq replay pipeline.yaml --file custom.dlq.jsonl
```

`replay` sends rows straight to the destinations (transforms are not
re-applied — DLQ records hold post-transform data) and rewrites the file
with only the rows that failed again. Exit code is non-zero if any rows
remain.

## `vortara start <file>`

Start the scheduler and/or streaming loop and block until shutdown.

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `--api-port` | `0` (disabled) | serve the control-plane API on 127.0.0.1: `/ping` (liveness), `/ready` (readiness — 503 after a failed run), `/health` (JSON status), `/metrics` (Prometheus format), `/version` |

The daemon ends in exactly three ways: SIGINT/SIGTERM (graceful — in-flight
rows drain, the watermark is saved, streaming offsets commit only for acked
events), a fatal error (failure alert fires, non-zero exit), or never.
`settings.limits.max_runtime` bounds each scheduled run, not the daemon.

```bash
vortara start pipeline.yaml
```

Batch mode starts the scheduler.
Streaming mode starts the streaming source loop.
Both mode does both.

## `vortara status <file>`

Show the last recorded run.

```bash
vortara status pipeline.yaml
```

## `vortara history <file>`

Show recent run history.

```bash
vortara history pipeline.yaml
vortara history pipeline.yaml --limit 20
```

## `vortara watermark get <file>`

Show the current batch watermark.

```bash
vortara watermark get pipeline.yaml
```

## `vortara watermark reset <file>`

Reset the batch watermark to zero.

```bash
vortara watermark reset pipeline.yaml
```

## `vortara offset get <file>`

Show committed streaming offsets.

```bash
vortara offset get pipeline.yaml
```

## `vortara offset reset <file>`

Reset committed streaming offsets to zero for discovered partitions.

```bash
vortara offset reset pipeline.yaml
```
