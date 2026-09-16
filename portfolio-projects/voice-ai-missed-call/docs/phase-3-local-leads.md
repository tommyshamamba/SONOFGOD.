# Phase 3 — Local Home Services Lead List

Goal: build a call list of 50 local businesses (3.5–4.5 stars) for cold outreach.

## Target profile

- Categories: plumbers, dentists, HVAC, electricians, roofers
- Rating: 3.5 to 4.5 stars (enough traffic, likely operational gaps)
- Need: business name, phone, address, rating, owner name if listed

## Manual process (no API key)

1. Google Maps: `"plumbers in [CITY]"` or `"dentists near [CITY]"`
2. Open each listing, copy details into `metrics/leads.csv`
3. Prioritize businesses with no website chat widget or obvious after-hours gap

## Script template (when API key available)

Run `scripts/scrape-leads.py` after setting `GOOGLE_MAPS_API_KEY`, `TARGET_CITY`, and `TARGET_STATE` in `.env`.

## Cold call script (from playbook)

> "Hi, is the owner available? No worries, quick question: Do you know how many customer calls your front desk misses when you're on jobs or after hours? I build a simple AI assistant that answers those missed calls instantly so you don't lose the job to a competitor. Can I text you a 1-minute video of how it works?"

## Daily target

15 direct calls per day (Phase 5 metrics).
