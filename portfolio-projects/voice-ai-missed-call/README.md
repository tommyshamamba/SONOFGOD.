# Voice AI Missed-Call Platform

Starter repo for a white-label Voice AI missed-call system and outbound sales toolkit.

## What this is

This project scaffolds the five phases from the outbound playbook:

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Demo foundation (phone number, booking script, auto SMS) | Templates ready — needs telephony API keys |
| 2 | GHL agency white-label integration | Architecture documented — needs GHL sub-account |
| 3 | Local home services lead scraping | Script template — needs target city |
| 4 | Startup freelance applications | Templates ready |
| 5 | Daily outbound metrics | Tracker included |

## Recommended build order

1. **Phase 1 first** — Live demo is the proof asset every other phase depends on.
2. **Phase 5 in parallel** — Daily metrics tracker costs nothing and keeps outreach honest.
3. **Phase 2** — GHL integration once the demo works.
4. **Phases 3 & 4** — Outreach tooling once you can show the 60-second Loom.

## Architecture (default stack: GHL + Vapi + Twilio)

```
Missed call on business number
        │
        ▼
   Twilio (telephony)
        │
        ├──► Vapi Voice AI agent (booking script)
        │
        └──► Webhook → n8n/Make workflow
                    │
                    ├── SMS within 30s (Twilio)
                    ├── Log lead in GoHighLevel CRM
                    └── Trigger callback / calendar booking
```

Swap components based on your stack choice during setup.

## Quick start

1. Copy `.env.example` to `.env` and fill in credentials.
2. Review `prompts/booking-script.md` and customize for your niche.
3. Log daily outreach in `metrics/daily-log.csv` or run `scripts/log-metrics.py`.
4. Wire telephony webhooks once API keys are ready (Phase 1).

## Folder structure

```
voice-ai-missed-call/
├── config/           # Environment and stack configuration
├── prompts/          # Voice AI agent scripts
├── workflows/        # n8n / Make workflow notes and exports
├── scripts/          # Lead scraping and metrics helpers
├── metrics/          # Daily outbound tracking
└── docs/             # Phase-specific setup guides
```

## What still requires your accounts

- **Twilio** — phone number + SMS
- **Vapi** (or Bland/Retell) — Voice AI agent
- **GoHighLevel** — CRM, workflows, white-label delivery
- **OpenAI** (or similar) — LLM for agent if not bundled with Vapi
- **Google Maps / Places API** — lead scraping (Phase 3)

Do not commit real API keys. Use `.env` locally only.

## Next steps

Answer the setup questions in the project README discussion, then wire Phase 1 telephony with your chosen stack.
