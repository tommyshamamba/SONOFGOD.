#!/usr/bin/env python3
"""
Google Places lead scraper stub (Phase 3).

Requires GOOGLE_MAPS_API_KEY, TARGET_CITY, TARGET_STATE in .env or environment.
Install: pip install googlemaps python-dotenv
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    import googlemaps
except ImportError:
    print("Install dependencies: pip install googlemaps python-dotenv")
    raise SystemExit(1)

load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
CITY = os.getenv("TARGET_CITY")
STATE = os.getenv("TARGET_STATE")
OUTPUT = Path(__file__).resolve().parent.parent / "metrics" / "leads.csv"

QUERIES = ["plumber", "dentist", "HVAC contractor", "electrician"]
MIN_RATING = 3.5
MAX_RATING = 4.5


def main() -> None:
    if not all([API_KEY, CITY, STATE]):
        print("Set GOOGLE_MAPS_API_KEY, TARGET_CITY, and TARGET_STATE in .env first.")
        raise SystemExit(1)

    gmaps = googlemaps.Client(key=API_KEY)
    rows: list[dict] = []

    for query in QUERIES:
        result = gmaps.places(
            query=f"{query} in {CITY}, {STATE}",
            type="establishment",
        )
        for place in result.get("results", []):
            rating = place.get("rating")
            if rating is None or not (MIN_RATING <= rating <= MAX_RATING):
                continue
            detail = gmaps.place(place["place_id"], fields=["name", "formatted_phone_number", "formatted_address", "rating", "user_ratings_total", "website"])
            info = detail.get("result", {})
            rows.append({
                "name": info.get("name", ""),
                "phone": info.get("formatted_phone_number", ""),
                "address": info.get("formatted_address", ""),
                "rating": info.get("rating", ""),
                "reviews": info.get("user_ratings_total", ""),
                "category": query,
                "website": info.get("website", ""),
                "notes": "",
            })

    fieldnames = ["name", "phone", "address", "rating", "reviews", "category", "website", "notes"]
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} leads to {OUTPUT}")


if __name__ == "__main__":
    main()
