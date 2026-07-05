# REST API Source

Polls a REST endpoint in batch mode using the watermark window as query
parameters.

## Config

```yaml
source:
  type: restapi
  url: https://api.example.com/v1/orders
  watermark: updated_at
  auth:
    type: bearer
    token: ${API_TOKEN}
```

## Request shape

Each run issues `GET url?since=<watermark>&until=<interval_end>`
(RFC3339). The response may be a bare JSON array or an envelope object
containing one; each element becomes a row.

## Notes

- The watermark is read from the `watermark` field of each item when
  present.
- Bearer, API-key, basic, and OAuth2 auth are supported via `auth:`.
