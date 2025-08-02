#!/usr/bin/env python3
"""Utility script to run all scraping scripts in order.

This script is intended for use with a scheduler (e.g., cron)
to collect new tournaments, matches and players each night.
"""

from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parent
SCRIPTS = [
    "scrape_tournament_urls.py",
    "scrape_matches.py",
    "scrape_players.py",
]

def run_script(script_name: str) -> None:
    subprocess.run([sys.executable, str(BASE_DIR / script_name)], check=True)

def main() -> None:
    for script in SCRIPTS:
        run_script(script)

if __name__ == "__main__":
    main()
