# n8n Workflow — Missed Call SMS (Template)

Import or rebuild this workflow once Twilio credentials are in `.env`.

## Trigger

- **Webhook** node — POST from Twilio Status Callback
- Filter: `CallStatus` = `no-answer` or `busy` or `failed`

## Steps

1. **Webhook** — receive Twilio payload (`From`, `To`, `CallSid`)
2. **IF** — only continue on missed/unanswered statuses
3. **Twilio** — Send SMS to `From` number:
   - Body: use template from `prompts/booking-script.md` (SMS section)
4. **HTTP Request** (optional) — POST to GHL API to create contact
5. **Respond to Webhook** — 200 OK

## Twilio status callback setup

In Twilio Console → Phone Number → Voice & Fax:

- Status Callback URL: your n8n webhook URL
- Status Callback Events: `completed`

## Notes

- Keep SMS under 160 characters when possible.
- Respect TCPA / opt-out rules for production client deployments.
- Test with your own phone before filming the demo.
