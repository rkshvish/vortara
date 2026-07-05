# HubSpot Destination

Upserts rows into HubSpot CRM using the batch upsert API
(`/crm/v3/objects/{object}/batch/upsert`), 100 rows per request.

## Config

```yaml
destinations:
  - type: hubspot
    object: contacts               # contacts | companies | deals | ...
    match_on: [email]              # idProperty for the upsert
    strategy: merge
    auth:
      type: bearer
      token: ${HUBSPOT_TOKEN}      # private app token
    rate_limit: "100/10s"
```

## Mechanics

- Rows are chunked into batches of 100; multiple batches dispatch in
  parallel.
- Multi-status responses are parsed per row — failed rows surface as row
  errors while the rest of the batch succeeds.
- Retry/backoff on 429/5xx, circuit breaker, delivery-log idempotency.
- All non-match-on row fields are sent as string properties.
