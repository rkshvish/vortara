# Kafka Source

Consumes events from Kafka in streaming mode. Offsets are committed only
after the engine acks a row, which happens after downstream delivery
succeeds — at-least-once end to end.

## Config

```yaml
source:
  type: kafka
  brokers: ["${KAFKA_BROKER}"]
  topic: deals-events
  group_id: vortara-deals
  dedup:
    window: 5m          # drop duplicate events inside this window
    key: event_id       # dedup key field (default: message key)
  flush:
    interval: 2s
    records: 500
```

## Payload handling

Message values are parsed as JSON objects into row data; non-JSON
payloads become a single `value` field. The event timestamp is used as
the row watermark.

## Delivery semantics

- `Ack` commits the offset; `Nack` leaves it uncommitted so the event
  redelivers.
- Combine with `settings.on_error: dlq` to capture poison messages
  without stalling the partition.
