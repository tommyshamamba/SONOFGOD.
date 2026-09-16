#!/usr/bin/env python3
"""Simple daily outbound metrics logger for Phase 5."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

TARGETS = {
    "agency_dms": 20,
    "cold_calls": 15,
    "job_applications": 2,
}

CSV_PATH = Path(__file__).resolve().parent.parent / "metrics" / "daily-log.csv"
FIELDS = ["date", "agency_dms", "cold_calls", "job_applications", "notes"]


def prompt_int(label: str, target: int) -> int:
    while True:
        raw = input(f"{label} (target: {target}/day): ").strip()
        try:
            return max(0, int(raw))
        except ValueError:
            print("Enter a whole number.")


def main() -> None:
    today = date.today().isoformat()
    print(f"\nDaily outbound log — {today}\n")

    row = {
        "date": today,
        "agency_dms": prompt_int("GHL agency DMs", TARGETS["agency_dms"]),
        "cold_calls": prompt_int("Local cold calls", TARGETS["cold_calls"]),
        "job_applications": prompt_int("Startup applications", TARGETS["job_applications"]),
        "notes": input("Notes (optional): ").strip(),
    }

    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_header = not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0

    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print("\n--- Summary ---")
    for key, target in TARGETS.items():
        actual = row[key]
        status = "✓" if actual >= target else "✗"
        print(f"  {status} {key.replace('_', ' ')}: {actual}/{target}")

    print(f"\nSaved to {CSV_PATH}")


if __name__ == "__main__":
    main()
