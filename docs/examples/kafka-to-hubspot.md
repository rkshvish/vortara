# Kafka to HubSpot

This is a streaming pipeline example using Kafka as the source and HubSpot as the destination.

## Example config

```yaml
pipeline:
  name: kafka-to-hubspot
  mode: streaming
  streaming:
    type: kafka
    broker: localhost:9092
    group_id: vortara-hubspot
    topic: contact-events
  route:
    - destination: hubspot
      condition: always
  destinations:
    hubspot:
      type: hubspot
      match_on: email
      options:
        object: contacts
      auth:
        type: bearer
        token: ${HUBSPOT_TOKEN}
      rate_limit:
        requests: 100
        period: 10s
  state:
    backend: sqlite
    path: ./vortara/state.db
```

## How streaming differs from batch

Batch mode reads a bounded time window and saves a watermark after the run completes.

Streaming mode does not use watermarks. Kafka messages are fetched continuously, and offsets are committed only after delivery succeeds. If the process dies before `Ack`, Kafka redelivers the message on restart.

## Run it

Validate:

```bash
vortara validate pipeline.yaml
```

Test connections:

```bash
vortara test pipeline.yaml
```

Start the streaming loop:

```bash
vortara start pipeline.yaml
```

## How to verify delivery

Check that:

- the consumer group is reading the topic
- HubSpot objects appear in the target object type
- offsets are moving forward

Show committed offsets:

```bash
vortara offset get pipeline.yaml
```

If you need to replay from the beginning for currently known partitions:

```bash
vortara offset reset pipeline.yaml
```
