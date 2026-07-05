# Postgres CDC Source

Streams row changes from PostgreSQL logical replication (pgoutput) —
log-based change data capture instead of polling. Inserts, updates, and
deletes arrive as events within seconds of commit.

## Config

```yaml
source:
  type: postgres_cdc
  url: ${POSTGRES_URL}
  table: deals
  slot: vortara_deals          # optional; created if missing (permanent)
  publication: vortara_pub     # optional; created if missing
```

## Requirements

- `wal_level = logical` on the server
- A role with `REPLICATION` (or superuser) to create the slot/publication

## Event shape

Each change is a row whose `Data` holds the column values (as text) plus
`_op`: `insert` | `update` | `delete`. `PrimaryKey` comes from the
replica identity columns; `Watermark` is the commit timestamp. Route or
filter on the operation:

```yaml
transform:
  - filter: "_op != 'delete'"
```

## Delivery semantics

Rows are buffered per transaction and emitted at commit, stamped with
the transaction end LSN. The engine acks after downstream delivery; the
confirmed LSN is flushed on the standby status update (every 5s), so
acked changes are never re-sent — even across restarts. Unacked changes
redeliver on reconnect (at-least-once; merge strategies dedupe via the
delivery log).

## Caveats

- The permanent slot retains WAL while Vortara is down — monitor
  `pg_replication_slots` lag and drop the slot if you decommission the
  pipeline.
- Unchanged TOAST values arrive as NULL on updates (Postgres does not
  include them in the WAL); use `REPLICA IDENTITY FULL` on the table if
  you need every column on every update.
- Values are text-encoded (no type introspection yet).
