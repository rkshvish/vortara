# BigQuery Source

Reads rows from BigQuery in batch mode using watermark-based incremental extraction.

## Config

```yaml
source:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  table: events                 # or query: "SELECT ..."
  watermark: updated_at
  credentials_file: /secrets/sa.json    # or credentials_json: ${SA_JSON}
```

Without explicit credentials, Application Default Credentials are used.

## Notes

- Extraction uses the incremental filter
  `WHERE wm > @watermark AND wm <= @interval_end`.
- The service account needs `bigquery.jobs.create` plus read access to
  the dataset.
