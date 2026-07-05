# Slack Destination

Posts one message per row to a Slack incoming webhook.

## Config

```yaml
destinations:
  - type: slack
    webhook: ${SLACK_WEBHOOK}
    message: "🎉 Deal won: {{ row.Name }} — ${{ row.Amount }}"
    when: "revenue > 100000"       # optional routing condition
    rate_limit: "1/1s"             # recommended — Slack allows ~1 msg/s
```

## Message templates

`{{ row.field }}` placeholders are replaced with row values; missing
fields render as empty strings.

## Notes

- Strategy is always `append`; one webhook POST per row.
- Rows are checked against the delivery log first, so re-running a
  pipeline does not repost messages.
- Use a `when:` condition to avoid flooding a channel with every row.
