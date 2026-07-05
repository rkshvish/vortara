# Vortara

Move data between sources and destinations.
Batch, streaming, and reverse ETL in a single binary.

## What it does

- **Reverse ETL** — sync warehouse/DB data to Salesforce (Bulk API v2), HubSpot (batch upsert), Slack, and REST APIs, with incremental watermarks and per-row idempotency
- **Batch** — watermark-based incremental extraction from Postgres, MySQL, Redshift, Snowflake, BigQuery, and REST APIs
- **Streaming** — Kafka consumers, webhook receivers, and Postgres CDC (logical replication) with ack/nack delivery

Same engine, same YAML, same CLI for all three.

| Sources | Destinations |
|---|---|
| Postgres, MySQL, Redshift, Snowflake, BigQuery, REST API, Kafka, Webhook, Postgres CDC | Salesforce, HubSpot, Slack, Google Sheets, Postgres, MySQL, Snowflake, REST API |

## Install

```bash
go install github.com/rkshvish/vortaraos/cmd/vortara@latest
```

## Quick start

```bash
vortara validate pipeline.yaml     # check the config
vortara test pipeline.yaml         # test all connections
vortara run pipeline.yaml          # run once
vortara run pipeline.yaml --since 2026-01-01   # backfill
vortara start pipeline.yaml        # run on the cron schedule
vortara status pipeline.yaml       # last run stats
vortara dlq replay pipeline.yaml   # re-deliver dead-lettered rows
```

## Pipeline config

```yaml
name: deals-sync

source:
  type: postgres
  url: ${POSTGRES_URL}
  table: deals
  watermark: updated_at

transform:
  - filter: "status == 'won' AND revenue > 1000"
  - rename: { deal_name: Name, deal_value: Amount }
  - mask: [email]

destinations:
  - type: salesforce
    url: ${SALESFORCE_INSTANCE_URL}
    object: Opportunity
    match_on: [deal_id]
    strategy: merge
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}

  - type: slack
    webhook: ${SLACK_WEBHOOK}
    message: "🎉 Deal won: {{ row.Name }} — ${{ row.Amount }}"
    when: "revenue > 100000"

cron: "*/15 * * * *"

settings:
  state:
    backend: sqlite
    path: ./vortara/state.db
  on_error: dlq          # skip | retry | dlq

alerts:
  on_failure:
    webhook_url: ${ALERT_WEBHOOK}
```

Rows are extracted incrementally (only `updated_at` newer than the last
successful run), transformed in a worker pool, routed by `when:` conditions,
and loaded in batches — Postgres uses COPY/multi-row upserts, Salesforce the
Bulk API, HubSpot batch upserts. Memory stays bounded by `batch_size`
regardless of table size.

### Syncing dbt models

dbt models materialize as ordinary warehouse tables, so any model syncs
directly — point the source at the model's relation:

```yaml
source:
  type: snowflake
  url: ${SNOWFLAKE_URL}
  table: analytics.customer_health   # your dbt model
  watermark: updated_at              # or watermark: none for full snapshots
```

Schedule the pipeline after your dbt run completes and the freshest model
lands in your CRM every cycle.

## Docs

- [Getting Started](docs/getting-started.md)
- [Concepts](docs/concepts.md)
- [YAML Reference](docs/yaml-reference.md)
- [CLI Reference](docs/cli.md)
- [Source Connectors](docs/sources/postgres.md)
- [Destination Connectors](docs/destinations/postgres.md)
