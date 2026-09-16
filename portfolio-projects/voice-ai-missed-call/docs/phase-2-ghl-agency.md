# Phase 2 — GoHighLevel Agency White-Label

Goal: plug the missed-call Voice AI system into GHL so agencies can resell it.

## What agencies get

- Branded missed-call text-back within 30 seconds
- AI voice callback that books into GHL calendar
- Contact + pipeline automation in their sub-account

## Integration points

| Event | GHL action |
|-------|------------|
| Missed call | Create/update contact, add tag `missed-call` |
| SMS reply | Trigger workflow, notify assigned user |
| Booking completed | Create appointment, move opportunity stage |

## White-label pitch (from playbook)

> "Hey [Name], I built a custom Voice AI missed-call system that plugs directly into GHL to automatically call back and book missed leads. I'm looking for 2–3 agencies to white-label this under their own brand. I handle the tech, you keep the markup. Want a 60-second video showing how it works?"

## Needs from you

- GHL agency or sub-account with API access
- Webhook URLs configured in GHL workflows
- Pricing model (per-location vs per-agency)
