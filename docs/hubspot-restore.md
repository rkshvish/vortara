# Restoring Archived HubSpot Contacts

When Vortara archives a HubSpot contact (because the source record was deleted and
`on_missing_from_source.action: delete` is configured), HubSpot soft-deletes the
record. It is **not permanently destroyed** — you can restore it from the HubSpot
portal or via API.

---

## What "archive" means in HubSpot

HubSpot uses the word *archive* for soft-deletion. Archived contacts:

- Are removed from active contact lists and views.
- No longer show up in default searches or workflows.
- Retain all properties and history.
- Can be fully restored at any time.

---

## Restoring from the HubSpot portal (UI)

1. Go to **Contacts → Contacts** in your HubSpot portal.
2. Click the **Views** dropdown in the top-left and select **Archived contacts**.
   (If you don't see this view, add it: click **Add view → Archived contacts**.)
3. Find the contact by name or email using the search bar.
4. Open the contact record.
5. Click the **Restore** button in the top banner.

The contact is immediately active again in all lists and workflows.

---

## Restoring via the HubSpot API

```bash
# POST to /crm/v3/objects/contacts/{id}/restore
curl -X POST "https://api.hubapi.com/crm/v3/objects/contacts/{CONTACT_ID}/restore" \
  -H "Authorization: Bearer YOUR_PRIVATE_APP_TOKEN"
```

A successful restore returns `HTTP 204 No Content`.

---

## Finding the HubSpot contact ID

The HubSpot contact ID is stored in Vortara state. To look it up:

```bash
# vortara explain shows Dest ID for a known entity key
vortara explain your-sync.yaml --key <entity-key>
```

The `Dest ID:` line shows the HubSpot contact ID (e.g. `515388171988`).

Alternatively, query the state database directly:

```bash
sqlite3 ./state/pql-to-hubspot.db \
  "SELECT entity_key, destination_id FROM entity_state WHERE last_decision='delete';"
```

---

## Preventing accidental archives

Vortara requires **explicit opt-in** before sending any archive request:

```yaml
on_missing_from_source:
  action: delete
  after_missing_runs: 1
  allow_destructive_actions: true   # required — omit to block all archives
```

Without `allow_destructive_actions: true`, Vortara logs a warning and skips the
archive, leaving the entity state unchanged until you deliberately enable it.

Use `max_deletes_per_run` to cap the blast radius:

```yaml
safety:
  max_creates_per_run: 25
  max_updates_per_run: 100
  max_deletes_per_run: 10           # never archive more than 10 per run
```

---

## Re-syncing a restored contact

After restoring a contact in HubSpot, re-insert the source record in your warehouse.
On the next run, Vortara will see it as `first_seen()` and create a new record (the
old `destination_id` is stale after restore). If you want Vortara to patch the
existing restored contact instead of creating a duplicate, clear the stored state
for that entity first:

```bash
sqlite3 ./state/pql-to-hubspot.db \
  "DELETE FROM entity_state WHERE entity_key='<entity-key>';"
```

Then re-insert the source row and run. Vortara will search HubSpot by email, find
the restored contact, and patch it — no duplicate created.
