# Google Sheets Destination

Appends rows to a Google Sheet using the Sheets API v4 with a service account.

```yaml
destinations:
  - type: googlesheets
    spreadsheet_id: 1AbC...XyZ        # from the sheet URL
    sheet: Deals                       # tab name, default Sheet1
    columns: "name, amount, synced_at" # optional explicit column order
    credentials_file: /secrets/sa.json # or credentials_json: ${SA_JSON}
```

Notes:

- Strategy is always `append`; each engine batch becomes one
  `values.append` API call (`INSERT_ROWS`).
- Without `columns`, cells are written in alphabetical column order.
- Share the spreadsheet with the service account email, or the API
  returns 403.
- Rows are checked against the delivery log first, so re-running a
  pipeline does not duplicate rows.
