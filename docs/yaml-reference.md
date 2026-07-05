# YAML Reference

A Vortara pipeline is a single YAML file with seven top-level keys:

```yaml
name: deals-sync            # required
source: {...}               # required
also: {...}                 # optional second (streaming) source
transform: [...]            # optional
destinations: [...]         # required, at least one
cron: "*/15 * * * *"        # optional — omit for a one-shot run
settings: {...}             # optional
alerts: {...}               # optional
```

Environment variables are interpolated anywhere with `${VAR_NAME}`.

## `name`

Pipeline identifier. Used for state (watermarks, run history, delivery log).

## `source`

One source per pipeline. The `type` decides which other keys apply.

### Batch sources (`postgres`, `mysql`, `redshift`, `snowflake`, `bigquery`, `restapi`)

```yaml
source:
  type: postgres
  url: ${POSTGRES_URL}          # connection string
  table: deals                  # OR query: "SELECT ..." (not both)
  watermark: updated_at         # default: updated_at; "none" = full snapshot every run
  exclude: [ssn, password_hash] # columns to drop at extraction
  batch_size: 1000              # rows per page (default 1000)
  parallelism: 4                # parallel range extraction (postgres)
```

| Type | URL form |
|---|---|
| `postgres` | `postgres://user:pass@host:5432/db?sslmode=disable` |
| `mysql` | `mysql://user:pass@host:3306/db` or Go DSN `user:pass@tcp(host:3306)/db` |
| `redshift` | `redshift://user:pass@cluster.region.redshift.amazonaws.com:5439/db` |
| `snowflake` | `snowflake://user:pass@account/db/schema?warehouse=WH&role=R` |
| `bigquery` | uses `project`, `dataset`, `credentials_file` / `credentials_json` keys |
| `restapi` | endpoint URL; polls `GET ?since=&until=` |

#### Watermark modes

- **Timestamp column** (default `updated_at`): incremental — each run extracts
  `(last watermark, run start]`.
- **Integer column** (`watermark: id`): keyset cursor for append-only tables —
  each run extracts `id > last id` in order, saving the highest delivered id.
  Detected automatically from the column type. No late-arrival risk for
  inserts. Supported for postgres, mysql, and redshift.
- **`watermark: none`**: full snapshot every run, for tables without any
  cursor column (lookup/reference tables). No cursor is saved. Pair with
  `merge`, `replace`, or `delete+insert` — validation rejects `append`, which
  would duplicate rows every run. Supported for postgres, mysql, and redshift.

`vortara run --since` accepts an integer for numeric-cursor pipelines
(re-extract ids greater than the value) or a date for timestamp pipelines.

### Streaming sources (`kafka`, `webhook`, `postgres_cdc`)

```yaml
source:
  type: kafka
  brokers: ["${KAFKA_BROKER}"]
  topic: deals-events
  group_id: vortara-deals
  dedup:
    window: 5m
    key: event_id
  flush:
    interval: 2s
    records: 500
```

```yaml
source:
  type: webhook
  path: /hooks/deals       # HTTP path to listen on
  port: 8090
  secret: ${HMAC_SECRET}   # HMAC signature verification
```

```yaml
source:
  type: postgres_cdc       # log-based change data capture (wal_level=logical)
  url: ${POSTGRES_URL}
  table: deals
  slot: vortara_deals      # optional; replication slot, created if missing
  publication: vortara_pub # optional; created if missing
```

## `also`

Run a streaming source alongside a batch source in the same pipeline.
Same keys as a streaming `source:` block.

```yaml
source: { type: postgres, url: ${PG}, table: deals }
also:   { type: kafka, brokers: ["${KAFKA}"], topic: deal-events, group_id: g1 }
```

## `transform`

Steps applied in order, per row, in a worker pool:

```yaml
transform:
  - filter: "status == 'won' AND revenue > 1000"   # AND/OR/NOT, contains(), startsWith()
  - rename: { deal_name: Name, deal_value: Amount }
  - add:    { synced_at: "{{ now() }}", full_name: "{{ first }} {{ last }}" }
  - drop:   [internal_notes]
  - mask:   [email, ssn]                            # PII redaction
  - trim:   ["*"]                                   # trim whitespace ("*" = all string fields)
  - flatten: "_"                                    # nested JSON → flat keys (user.email → user_email)
```

`add` values are templates: a whole-string placeholder (`"{{ now() }}"`)
keeps the value's type; mixed text concatenates. `flatten` is the
last-mile step for webhook/CDC payloads whose nested JSON needs flat
columns — heavier reshaping belongs upstream in SQL or dbt.

## `destinations`

A list. Each destination optionally has a `when:` condition — rows are routed
to every destination whose condition matches (no `when:` = always).

```yaml
destinations:
  - type: salesforce
    object: Opportunity
    match_on: [deal_id]
    strategy: merge
    when: "tier == 'enterprise'"
    rate_limit: "100/10s"
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}

  - type: slack
    webhook: ${SLACK_WEBHOOK}
    message: "🎉 Deal won: {{ row.Name }} — ${{ row.Amount }}"
```

| Type | Required keys | Strategies |
|---|---|---|
| `postgres` | `url`, `table` | merge, append, replace, delete+insert, scd2 |
| `mysql` | `url`, `table` | merge, append, replace, delete+insert |
| `snowflake` | `url`, `table` | merge, append, replace, delete+insert |
| `restapi` | `url` | append (default) |
| `salesforce` | `url`, `object`, `auth` | merge |
| `hubspot` | `object`, `auth` | merge |
| `slack` | `webhook`, `message` | append (default) |
| `googlesheets` | `spreadsheet_id`, credentials | append (default) |

`strategy` defaults to `merge` (`append` for restapi/slack/googlesheets). `merge` and
`delete+insert` require `match_on`. `scd2` (postgres only) keeps type-2 history: changed rows close the current
version (`_scd_valid_to`, `_scd_is_current=false`) and insert a new one;
unchanged rows are untouched. The three history columns are added to the
target automatically. On postgres, `replace` is atomic: rows load
into a per-run staging table and swap into the target in one transaction at run
end, so a failed run never leaves the target truncated or partial.

### `auth`

`type`: `bearer` | `api_key` | `basic` | `oauth2`

```yaml
auth: { type: bearer, token: ${TOKEN} }
auth: { type: api_key, key: X-Api-Key, value: ${KEY}, in_header: true }
auth: { type: basic, username: u, password: ${P} }
auth: { type: oauth2, client_id: ..., client_secret: ..., token_url: ..., scopes: [...] }
```

## `cron`

Standard 5-field cron. Omit for a one-shot run (`vortara run`).

```yaml
cron: "0 * * * *"   # hourly
```

## `settings`

```yaml
settings:
  state:
    backend: sqlite            # sqlite (default) | postgres | memory
    path: ./vortara/state.db   # sqlite only
    connection: ${STATE_PG}    # postgres only — shared state for multi-instance deployments
    key_prefix: vortara        # postgres table prefix (vortara_watermarks, ...)
    delivered_ttl: 720h        # prune delivery-log entries older than this after successful runs
  log:
    level: info                # debug | info | warn | error
    format: text               # text | json
  limits:
    max_runtime: 30m           # cap one run; next run resumes from the saved watermark
    max_rows: 1000000          # cap one run's extraction; resume works the same way
  on_error: skip               # skip (default) | retry | dlq
  dlq_path: ./deals.dlq.jsonl  # dead-letter file (default: <name>.dlq.jsonl)
  concurrency:
    workers: 8                 # transform workers (default: NumCPU)
    batch_size: 1000           # channel buffer
```

`limits` bound a single run, not the pipeline: a capped run saves the
watermark of the rows it delivered and is marked `timeout`; the next run
continues from exactly that point. `delivered_ttl` keeps the sqlite state
bounded — pruning is safe because the watermark guarantees rows older than
the extraction horizon are never re-checked (a `--full-refresh` re-delivers
pruned rows, which merge strategies absorb harmlessly).

With `on_error: retry`, each failed row is retried up to 2 more times with
linear backoff (500ms, 1s) before being treated as failed.

With `on_error: dlq`, rows that fail delivery are appended as JSON lines to
the dead-letter file and the run continues (and stays `success`). Dead-lettered
rows are NOT retried on subsequent runs — re-deliver them with `vortara dlq replay`. In
streaming mode the failed event is acked so the stream keeps advancing.

## `alerts`

```yaml
alerts:
  on_failure:
    webhook_url: ${ALERT_WEBHOOK}
```

On run failure Vortara POSTs:

```json
{"pipeline": "deals-sync", "status": "failed", "error": "...", "failed_at": "2026-07-03T12:00:00Z"}
```
