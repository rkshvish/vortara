# Webhook Source

Runs an HTTP server and turns inbound POST requests into rows in
streaming mode. The HTTP response is tied to delivery: `200` after the
row is acked, `500` on nack — so senders can retry safely.

## Config

```yaml
source:
  type: webhook
  path: /hooks/deals
  port: 8090
  secret: ${HMAC_SECRET}    # verify X-Signature HMAC of the body
```

## Behavior

- JSON bodies become row data; the receive time is the watermark.
- With `secret` set, requests with a missing or invalid HMAC signature
  are rejected with `401`.
- Responses: `200` after downstream delivery, `500` when delivery fails
  (sender should retry).
