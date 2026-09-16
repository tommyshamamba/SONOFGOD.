# Phase 1 — Demo Foundation

Goal: a working missed-call → SMS demo you can film in 60 seconds.

## Checklist

- [ ] Buy or port a dedicated demo phone number (Twilio recommended)
- [ ] Create a Vapi assistant using `prompts/booking-script.md`
- [ ] Configure missed-call webhook → SMS within 30 seconds
- [ ] Test: call number, hang up, confirm SMS on your phone
- [ ] Record Loom: setup (15s) → live demo (30s) → CTA (15s)

## Missed-call SMS flow (conceptual)

1. Inbound call hits Twilio number.
2. If unanswered within ~15–20 seconds, Twilio fires a status callback.
3. Webhook (n8n, Make, or small server) sends SMS via Twilio REST API.
4. Optional: log the lead to GHL contact.

## Credentials needed

See `.env.example`. Minimum for Phase 1:

- Twilio Account SID, Auth Token, Phone Number
- Vapi API key (for Voice AI on answered calls)

## Demo video script

1. **Setup (15s):** Show phone number + automation timeline in your dashboard.
2. **Live demo (30s):** Dial from your mobile, let it ring, hang up. Show SMS arriving.
3. **CTA (15s):** "I build this for agencies and local businesses — DM me for a white-label demo."
