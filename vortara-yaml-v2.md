# Vortara YAML Reference v2

One pipeline per file. Seven top-level keys.

---

## Complete example

```yaml
name: deals-to-salesforce-and-slack

source:
  type: postgres
  url: ${POSTGRES_URL}
  table: deals
  watermark: updated_at
  exclude: [internal_notes, ssn]
  batch_size: 1000
  parallelism: 4

also:
  type: kafka
  brokers: [${KAFKA_BROKER}]
  topic: deal-events
  group_id: vortara-deals
  flush:
    interval: 5s
    records: 100
  dedup:
    window: 5m
    key: event_id

transform:
  - filter: "status == 'won' AND revenue > 10000"
  - rename:
      deal_name: Name
      revenue:   Amount
      deal_id:   ExternalId__c
  - add:
      synced_at: "now()"
      tier: "if revenue > 100000 then 'enterprise' else 'smb'"
  - drop: [raw_payload]
  - mask: [credit_card]

destinations:
  - type: salesforce
    url: ${SF_INSTANCE_URL}
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}
    object: Opportunity
    match_on: [ExternalId__c]
    strategy: merge
    rate_limit: 100/10s

  - type: slack
    webhook: ${SLACK_WEBHOOK}
    message: "🏆 Deal won: {{Name}} — ${{Amount}} ({{tier}})"
    when: "tier == 'enterprise'"

cron: "*/15 * * * *"

settings:
  state:
    backend: sqlite
    path: ./vortara/state.db
  log:
    level: info
    format: text
  limits:
    max_runtime: 2h
    max_rows: 1000000
  on_error: skip
  concurrency:
    workers: 8
    batch_size: 1000
```

---

## name

```yaml
name: deals-to-salesforce
```

Pipeline identifier. Used in logs, state store, and CLI commands.

---

## source

Exactly one source per pipeline. `type` determines the connector.

### postgres

```yaml
source:
  type: postgres
  url: ${POSTGRES_URL}            # required
  table: deals                    # required (or query:)
  watermark: updated_at           # required for batch
  exclude: [ssn, internal_notes]  # optional — never enter pipeline
  batch_size: 1000                # optional — default 1000
  parallelism: 4                  # optional — parallel PK range scans
  auth:                           # optional — for auth-required Postgres
    type: basic
    username: ${PG_USER}
    password: ${PG_PASS}

# Custom SQL (aggregations, joins)
source:
  type: postgres
  url: ${POSTGRES_URL}
  query: |
    SELECT account_id, SUM(revenue) AS arr,
           MAX(updated_at) AS updated_at
    FROM deals
    WHERE updated_at > {{watermark}}
      AND updated_at <= {{interval_end}}
    GROUP BY account_id
  watermark: updated_at
```

Placeholders: `{{watermark}}`, `{{interval_end}}`, `{{pipeline}}`

### snowflake

```yaml
source:
  type: snowflake
  url: ${SNOWFLAKE_DSN}           # snowflake://user:pass@account/db/schema?warehouse=WH
  table: DIM_ACCOUNTS             # uppercased automatically
  watermark: UPDATED_AT
  batch_size: 1000
```

### bigquery

```yaml
source:
  type: bigquery
  project: ${BQ_PROJECT}
  dataset: analytics
  table: fct_accounts
  watermark: updated_at
  credentials_file: /path/to/sa.json   # optional — uses ADC if omitted
  credentials_json: ${BQ_CREDS_JSON}   # optional — base64 JSON
```

### restapi

```yaml
source:
  type: restapi
  url: ${API_URL}
  watermark: updated_at
  auth:
    type: bearer
    token: ${API_TOKEN}
```

### kafka (streaming)

```yaml
source:
  type: kafka
  brokers: [${KAFKA_BROKER}]
  topic: deals-events
  group_id: vortara-deals
  flush:
    interval: 5s      # flush every 5 seconds
    records: 100      # or every 100 events, whichever first
  dedup:
    window: 5m        # drop duplicates seen within 5 minutes
    key: event_id     # deduplicate on this field (default: row ID)
```

### webhook (streaming)

```yaml
source:
  type: webhook
  path: /ingest/deals
  port: 8080
  secret: ${WEBHOOK_SECRET}    # optional — HMAC verification
  flush:
    interval: 5s
    records: 100
```

---

## also (optional)

Streaming source alongside batch. Batch handles history on schedule.
Streaming handles real-time continuously.

```yaml
also:
  type: kafka
  brokers: [${KAFKA_BROKER}]
  topic: deal-events
  group_id: vortara-deals
  flush:
    interval: 5s
    records: 100
  dedup:
    window: 5m
    key: event_id
```

Only `kafka` and `webhook` are valid in `also:`.

---

## transform (optional)

Ordered processors. Applied to every row in sequence.
Row dropped if `filter` condition is false.

```yaml
transform:
  - filter: "status == 'won' AND revenue > 10000"
  - rename:
      old_field: new_field
  - add:
      new_field: "expression"
  - drop: [field1, field2]
  - mask: [field1, field2]
```

### filter

```yaml
- filter: "status == 'won'"
- filter: "status == 'won' AND revenue > 10000"
- filter: "status != 'lost' OR revenue > 50000"
- filter: "NOT status == 'draft'"
- filter: "contains(email, '@company.com')"
- filter: "startsWith(country, 'US')"
```

Operators: `==` `!=` `>` `<` `>=` `<=` `AND` `OR` `NOT`
Functions: `contains(field, str)` `startsWith(field, str)`

### rename

```yaml
- rename:
    deal_name: Name
    deal_value: Amount
    deal_id: ExternalId__c
```

### add

```yaml
- add:
    synced_at: "now()"
    tier: "if revenue > 100000 then 'enterprise' else 'smb'"
    literal_field: "'fixed string value'"
    copy_field: "other_field"
```

Expressions: `now()`, `if X then Y else Z`, field references, string literals (single-quoted)

### drop

```yaml
- drop: [internal_notes, raw_json, debug_field]
```

### mask

```yaml
- mask: [ssn, credit_card, phone, email]
```

Replaces field value with `***`.

---

## destinations

List of destinations. All run per row unless `when:` is set.
One failure does not block others.

### salesforce

```yaml
destinations:
  - type: salesforce
    url: ${SF_INSTANCE_URL}
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}
    object: Opportunity           # Salesforce object API name
    match_on: [ExternalId__c]     # external ID field for upsert
    strategy: merge               # merge|append|replace|delete+insert
    rate_limit: 100/10s           # optional
    when: "tier == 'enterprise'"  # optional routing condition
```

Bulk API v2 used automatically for batches > 200 rows.

### hubspot

```yaml
  - type: hubspot
    auth:
      type: bearer
      token: ${HUBSPOT_TOKEN}
    object: contacts              # contacts|companies|deals|tickets
    match_on: [email]             # HubSpot idProperty
    strategy: merge
    rate_limit: 100/10s
    when: "tier == 'smb'"
```

### slack

```yaml
  - type: slack
    webhook: ${SLACK_WEBHOOK}
    message: "Deal won: {{Name}} — ${{Amount}}"
    rate_limit: 1/1s
    when: "tier == 'enterprise'"
```

Template: `{{field_name}}` interpolates row data.

### postgres

```yaml
  - type: postgres
    url: ${POSTGRES_URL}
    table: deals_copy
    match_on: [id]
    strategy: merge
    auth:                          # optional
      type: basic
      username: ${PG_USER}
      password: ${PG_PASS}
```

### snowflake

```yaml
  - type: snowflake
    url: ${SNOWFLAKE_DSN}
    table: SF_OPPORTUNITIES
    match_on: [deal_id]
    strategy: merge
```

### restapi

```yaml
  - type: restapi
    url: ${API_URL}
    method: POST                   # POST|PUT|PATCH
    auth:
      type: api_key
      key: X-API-Key
      value: ${API_KEY}
      in_header: true
    strategy: merge
```

### googlesheets

```yaml
  - type: googlesheets
    credentials_file: /path/to/sa.json
    spreadsheet_id: ${SHEET_ID}
    sheet: Sheet1
    strategy: append
```

---

## auth types

```yaml
# OAuth2 client credentials
auth:
  type: oauth2
  client_id: ${CLIENT_ID}
  client_secret: ${CLIENT_SECRET}
  token_url: ${TOKEN_URL}
  scopes: [api]                  # optional

# Bearer token
auth:
  type: bearer
  token: ${TOKEN}

# API Key
auth:
  type: api_key
  key: X-API-Key                 # header or param name
  value: ${API_KEY}
  in_header: true                # false = query parameter

# Basic auth
auth:
  type: basic
  username: ${USER}
  password: ${PASS}
```

---

## strategy

| Value | Behavior | Requires match_on |
|---|---|---|
| merge | INSERT + UPDATE on conflict (default) | yes |
| append | INSERT only, duplicates allowed | no |
| replace | Truncate then INSERT | no |
| delete+insert | DELETE matching rows then INSERT | yes |

---

## cron

Standard cron expression. Five fields.

```yaml
cron: "*/15 * * * *"    # every 15 minutes
cron: "0 * * * *"       # every hour
cron: "0 9 * * 1-5"     # 9am weekdays
cron: "0 0 * * *"       # daily midnight
cron: "0 0 1 * *"       # monthly
```

Streaming pipelines (`source.type: kafka` or `webhook`) ignore `cron`.

---

## settings

All fields optional. Sane defaults for everything.

```yaml
settings:
  state:
    backend: sqlite              # sqlite|postgres|redis|memory
    path: ./vortara/state.db    # sqlite only
    connection: ${STATE_URL}    # postgres or redis URL
    delivered_ttl: 24h          # redis: TTL on delivery log
    key_prefix: vortara         # redis: key prefix

  log:
    level: info                  # debug|info|warn|error
    format: text                 # text|json

  limits:
    max_runtime: 2h              # stop run after duration
    max_rows: 1000000            # stop after N rows extracted

  on_error: skip                 # skip|retry|dlq

  concurrency:
    workers: 8                   # transform goroutines (default: NumCPU)
    batch_size: 1000             # rows per batch (default: adaptive)
```

---

## Environment variables

All string values support `${VAR_NAME}` substitution.
Resolved at startup from environment.

```yaml
url: ${POSTGRES_URL}
token: ${HUBSPOT_TOKEN}
```

---

## CLI usage

```bash
vortara validate pipeline.yaml     # validate config
vortara test pipeline.yaml         # test all connections
vortara run pipeline.yaml          # run batch once
vortara run pipeline.yaml --dry-run        # show what would sync
vortara run pipeline.yaml --full-refresh   # ignore watermark
vortara start pipeline.yaml        # start scheduler + streaming
vortara status pipeline.yaml       # last run stats
vortara history pipeline.yaml      # run history
vortara watermark get pipeline.yaml
vortara watermark reset pipeline.yaml
```
