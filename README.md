# Vortara — Open-Source Reverse ETL

Sync data from your warehouse or database to the tools your team actually uses.
Salesforce, HubSpot, Klaviyo, Intercom, Pipedrive, Zendesk, Mixpanel, Slack, and more — from a single YAML file.

> **Reverse ETL** means your data warehouse is the source of truth, and Vortara keeps your operational tools in sync with it — automatically, incrementally, and idempotently.

## Why Vortara

| | Vortara | Census / Hightouch |
|---|---|---|
| Pricing | Free, self-hosted | $500–$2000+/mo |
| Deployment | Single binary, Docker, or K8s | SaaS only |
| Sources | Postgres, MySQL, Redshift, Snowflake, BigQuery, REST API | Same |
| Destinations | 13 built-in (see below) | 200+ |
| Streaming | Kafka, Webhook, Postgres CDC | No |
| dbt support | Point at the model table | Yes |

## Destinations

| Destination | Use case |
|---|---|
| **Salesforce** | Sync deals, contacts, accounts via Bulk API v2 |
| **HubSpot** | Upsert contacts, companies, deals via batch API |
| **Klaviyo** | Sync customer profiles and properties for email/SMS |
| **Intercom** | Upsert contacts and companies for customer messaging |
| **Pipedrive** | Sync persons, deals, and organizations to CRM |
| **Zendesk** | Create or update users and tickets |
| **Mixpanel** | Sync user profiles and properties to product analytics |
| **Slack** | Post row-level notifications to channels |
| **Google Sheets** | Append or sync rows to a spreadsheet |
| **Postgres** | Merge, replace, SCD2, or append to any table |
| **MySQL** | Merge, replace, or append to any table |
| **Snowflake** | Merge or replace in your warehouse |
| **REST API** | Push to any HTTP endpoint |

## Sources

Postgres · MySQL · Redshift · Snowflake · BigQuery · REST API · Kafka · Webhook · Postgres CDC

## Install

```bash
go install github.com/rkshvish/vortara/cmd/vortara@latest
```

Or with Docker:

```bash
docker run --rm -v $(pwd):/pipelines ghcr.io/rkshvish/vortara run /pipelines/pipeline.yaml
```

## Quick start

```bash
vortara validate pipeline.yaml     # check config
vortara test pipeline.yaml         # test connections
vortara run pipeline.yaml          # run once
vortara run pipeline.yaml --since 2026-01-01   # backfill from a date
vortara start pipeline.yaml        # run on cron schedule
vortara status pipeline.yaml       # last run stats + row counts
vortara dlq replay pipeline.yaml   # re-deliver failed rows
```

## Example: Snowflake → Salesforce

Sync your `customer_health` dbt model to Salesforce Opportunities every 15 minutes:

```yaml
name: customer-health-sync

source:
  type: snowflake
  url: ${SNOWFLAKE_URL}
  table: analytics.customer_health   # your dbt model
  watermark: updated_at

transform:
  - filter: "health_score < 70"
  - rename: { account_id: AccountId__c, health_score: Health_Score__c }

destinations:
  - type: salesforce
    url: ${SALESFORCE_INSTANCE_URL}
    object: Opportunity
    match_on: [AccountId__c]
    strategy: merge
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}

cron: "*/15 * * * *"

settings:
  on_error: dlq

alerts:
  on_failure:
    webhook_url: ${ALERT_WEBHOOK}
```

## Example: Postgres → Klaviyo + Intercom

Fan out one source to multiple destinations in a single pipeline:

```yaml
name: user-attributes-sync

source:
  type: postgres
  url: ${POSTGRES_URL}
  table: users
  watermark: updated_at

transform:
  - mask: [password_hash, ssn]

destinations:
  - type: klaviyo
    auth: { type: bearer, token: ${KLAVIYO_API_KEY} }
    match_on: [email]

  - type: intercom
    auth: { type: bearer, token: ${INTERCOM_TOKEN} }
    options: { object: contacts }
    match_on: [external_id]
    when: "plan != 'free'"

cron: "0 * * * *"
```

## How it works

1. **Extract** — pulls only rows newer than the last watermark (incremental by default). Supports timestamp columns, integer cursors, and full snapshots.
2. **Transform** — filter, rename, mask, add computed fields, trim whitespace, flatten nested JSON. Runs in a worker pool.
3. **Route** — `when:` conditions on each destination let one pipeline fan out selectively.
4. **Load** — per-row delivery log prevents duplicates across retries and restarts. Failed rows go to a dead-letter file for replay.

Memory stays bounded by `batch_size` regardless of table size.

## Reliability features

- **Incremental watermarks** — each run picks up from where the last one succeeded
- **Per-row idempotency** — delivery log deduplicates across retries, restarts, and replays
- **Dead-letter queue** — failed rows written to `.dlq.jsonl`; replay with `vortara dlq replay`
- **Circuit breaker** — stops hammering a destination when it's down
- **Rate limiting** — configurable per-destination request limits
- **Atomic replace** — Postgres `replace` strategy uses staging tables; a failed run never leaves the target truncated
- **SCD2** — Postgres destination supports type-2 slowly changing dimensions out of the box
- **Max runtime / max rows** — cap a run and resume exactly from the watermark on the next cycle
- **Failure alerts** — webhook notification on run failure

## Syncing dbt models

dbt models are ordinary warehouse tables. Point the source at the relation and schedule after your dbt run:

```yaml
source:
  type: snowflake
  url: ${SNOWFLAKE_URL}
  table: analytics.customer_health
  watermark: updated_at
```

## Docs

- [Getting Started](docs/getting-started.md)
- [YAML Reference](docs/yaml-reference.md)
- [CLI Reference](docs/cli.md)
- [Concepts](docs/concepts.md)
- [Source Connectors](docs/sources/postgres.md)
- [Destination Connectors](docs/destinations/postgres.md)

## Compared to alternatives

- **Census / Hightouch** — managed SaaS, $500+/mo. Vortara is self-hosted and free.
- **Airbyte** — focused on EL (warehouse ingestion), not reverse ETL. Different direction.
- **dbt + custom scripts** — dbt transforms, you still need something to push the result. That's Vortara.
- **Fivetran** — inbound only (source → warehouse). Vortara goes the other direction.

## License

Apache 2.0
