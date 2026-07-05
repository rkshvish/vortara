# Salesforce Destination

Upserts rows into Salesforce. Small batches use REST upsert; large
batches use the Bulk API v2 with parallel sub-batches.

## Config

```yaml
destinations:
  - type: salesforce
    url: ${SALESFORCE_INSTANCE_URL}
    object: Opportunity
    match_on: [deal_id]            # external ID field
    strategy: merge
    rate_limit: "100/10s"
    auth:
      type: oauth2
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      token_url: ${SF_TOKEN_URL}
```

## Mechanics

- Below the bulk threshold (default 200 rows) each row is a REST
  `PATCH /sobjects/{object}/{externalId}/{value}` upsert.
- At or above the threshold, rows go through Bulk API v2 ingest jobs
  (CSV upload), split into parallel sub-batches.
- Retry/backoff on 429/5xx, circuit breaker on consecutive failures,
  delivery-log idempotency per row.
