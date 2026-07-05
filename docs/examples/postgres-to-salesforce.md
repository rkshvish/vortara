# Postgres to Salesforce

This example uses [examples/postgres-to-salesforce.yaml](/Users/rakesh_1/opensource/vortaraos/examples/postgres-to-salesforce.yaml).

## What it does

- reads rows from the Postgres `deals` table
- filters to rows where `status == 'won'`
- enriches each row with `tier: enterprise`
- upserts the result into Salesforce `Opportunity`

## Prerequisites

Set these environment variables:

```bash
export POSTGRES_URL='postgres://user:pass@localhost:5432/app?sslmode=disable'
export SALESFORCE_INSTANCE_URL='https://myorg.my.salesforce.com'
export SF_CLIENT_ID='...'
export SF_CLIENT_SECRET='...'
export SF_TOKEN_URL='https://myorg.my.salesforce.com/services/oauth2/token'
```

## Step by step

Validate the file:

```bash
vortara validate examples/postgres-to-salesforce.yaml
```

Test the connections:

```bash
vortara test examples/postgres-to-salesforce.yaml
```

Inspect the transformed payload without writing to Salesforce:

```bash
vortara run examples/postgres-to-salesforce.yaml --dry-run
```

Run the real sync:

```bash
vortara run examples/postgres-to-salesforce.yaml
```

If you need a full re-sync from the start:

```bash
vortara run examples/postgres-to-salesforce.yaml --full-refresh
```

## Troubleshooting

`source connect failed`
The Postgres connection string is wrong, or the database is unreachable.

`salesforce destination: url is required`
`SALESFORCE_INSTANCE_URL` did not resolve, or the destination block is missing `url`.

`oauth2 token server returned ...`
Check `SF_TOKEN_URL`, client ID, and client secret.

`destination "salesforce" connect failed`
The object name or auth config is invalid, or the destination cannot initialize its HTTP dependencies.
