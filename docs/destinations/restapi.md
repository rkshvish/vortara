# REST API Destination

Delivers one HTTP request per row to any REST endpoint — the escape
hatch for tools without a dedicated connector.

## Config

```yaml
destinations:
  - type: restapi
    url: https://api.example.com/webhook
    method: POST                   # default POST
    headers:
      Content-Type: application/json
    auth:
      type: api_key
      key: X-Api-Key
      value: ${API_KEY}
    rate_limit: "50/1s"
```

## Mechanics

- Each row is serialized as a JSON object and sent with an
  `X-Idempotency-Key` header (the row ID) so receivers can dedupe.
- Requests fan out across parallel workers (default 3).
- Retry with backoff on 429/5xx (configurable via `retry_attempts`,
  `backoff_on`, `drop_on`), circuit breaker, delivery-log idempotency.
- Default strategy is `append`.
