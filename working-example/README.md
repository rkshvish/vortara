# Working Example

This folder contains a local end-to-end demo:

- Postgres on `localhost:15433`
- a webhook sink on `http://localhost:18081/webhook`
- a ready-to-run pipeline config at `pipeline.yaml`

## Quick start

```bash
make setup
./vortara run working-example/pipeline.yaml
```

## What it does

The demo seeds a small `b2b_saas.usage_sessions` table in Postgres and sends
rows to the webhook service as JSON.
