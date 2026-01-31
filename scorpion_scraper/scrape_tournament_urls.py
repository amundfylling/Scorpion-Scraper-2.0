import requests
from bs4 import BeautifulSoup
import argparse
import logging
import time
import csv
from typing import List, Dict, Optional, Set
from pathlib import Path

try:
    from . import utils
except ImportError:
    import utils

# Configuration
BASE_URL = "https://th.sportscorpion.com/eng/tournament/archive/?page="
TOURNAMENT_BASE = "https://th.sportscorpion.com"
MAX_PAGES = 5 # since the script runs daily, it only needs to go through the newest tournaments
# Resolve paths relative to the project root so scripts work from any CWD
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "tournament_data.csv"

utils.setup_logging()
# Global session with retries
SESSION = utils.get_retry_session()

def fetch_page(url: str) -> Optional[requests.Response]:
    try:
        response = SESSION.get(url, timeout=15)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logging.warning(f"Request failed for {url}: {e}")
        return None

def parse_tournaments_from_overview(soup: BeautifulSoup) -> List[Dict[str, str]]:
    tournaments = []
    table = soup.find('table', {'class': 'sTable'}) or soup.find('table')
    if not table:
        return tournaments
    tbody = table.find('tbody') or table
    rows = tbody.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue
        link = cols[0].find('a')
        if link and link.get('href') and '/eng/tournament/id/' in link.get('href'):
            href = link.get('href')
            # Extract ID from URL
            try:
                id_part = href.split('/eng/tournament/id/')[1].split('/')[0]
            except Exception:
                continue
            name = link.text.strip()
            tournaments.append({
                'ID': id_part,
                'Name': name,
                'DetailURL': TOURNAMENT_BASE + href
            })
    return tournaments

def get_tournament_type(detail_url: str) -> str:
    response = fetch_page(detail_url)
    if not response:
        return ''
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table', {'class': 'iTable'})
    th_texts = []
    for table in tables:
        tbody = table.find('tbody') or table
        rows = tbody.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th:
                th_texts.append(th.text.strip())
            if th and td and th.text.strip() == 'Tournament type':
                return td.text.strip()
    # Debug: print HTML and all th texts if type not found
    print(f"\n--- DEBUG: HTML content of tournament detail page {detail_url} (first 10000 chars) ---\n")
    print(response.text[:10000])
    print("\n--- DEBUG: <th> texts found on page ---\n")
    print(th_texts)
    print("\n--- END DEBUG ---\n")
    return ''

def read_existing_ids(filename: Path) -> Set[str]:
    ids: Set[str] = set()
    if not filename.exists():
        return ids
    with filename.open('r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ids.add(row['ID'])
    return ids

def append_tournaments_to_csv(filename: Path, tournaments: List[Dict[str, str]]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = filename.exists()
    with filename.open('a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['ID', 'Name', 'Type'])
        if not file_exists:
            writer.writeheader()
        for t in tournaments:
            writer.writerow(t)

def main():
    parser = argparse.ArgumentParser(description="Scrape tournament URLs.")
    parser.add_argument("--full", action="store_true", help="Run a full scrape (all pages). Default is incremental (5 pages).")
    args = parser.parse_args()

    # Pass 1: Collect all tournaments from overview pages
    all_tournaments = []
    
    # Determine page range
    if args.full:
        logging.info("Starting FULL scrape of tournament URLs.")
        page_range = range(1, 10000) # Effectively infinite for this context
    else:
        logging.info(f"Starting INCREMENTAL scrape of tournament URLs (max {MAX_PAGES} pages).")
        page_range = range(1, MAX_PAGES + 1)

    for page_num in page_range:
        page_url = BASE_URL + str(page_num)
        response = fetch_page(page_url)
        if not response:
            logging.info(f"Stopping: No response for page {page_num}.")
            break
        soup = BeautifulSoup(response.text, 'html.parser')
        tournaments = parse_tournaments_from_overview(soup)
        if not tournaments:
            logging.info(f"Stopping: No tournaments found on page {page_num}.")
            break
        all_tournaments.extend(tournaments)
        logging.info(f"Parsed page {page_num} with {len(tournaments)} tournaments.")
    logging.info(f"Collected {len(all_tournaments)} tournaments from overview pages.")

    # Pass 2: Only fetch type for tournaments not in CSV
    # If full scrape, we might want to re-check types?
    # The requirement says "overwrite the current table" for matches, but for URLs it implies fetching all.
    # However, for robustness, if we already have the type, we might blindly trust it unless "overwrite" is strictly required here too.
    # Let's keep logic: if ID exists, skip. If full scrape, maybe we should NOT skip?
    # The user said "overwrite the current table" in the context of "full scrape".
    # For tournament_urls.py, "incremental" means append new ones. "Full" usually means "ensure everything is there".
    # If we want to strictly overwrite, we should clear the CSV or ignore existing.
    
    # Current implementation reads existing IDs to know what to skip.
    # If full scrape, we should probably re-verify types OR just ensure we have them all.
    # Since checking type requires a request per tournament, re-checking ALL 6000 tournaments is expensive.
    # I will assume "Full" here mainly applies to *finding* potentially missed tournaments from older pages.
    # But for matches, it explicitly says "overwrite".
    
    existing_ids = read_existing_ids(OUTPUT_FILE)
    new_tournaments = []
    
    for t in all_tournaments:
        if t['ID'] in existing_ids:
            continue  # Skip already collected
        t_type = get_tournament_type(t['DetailURL'])
        new_tournaments.append({
            'ID': t['ID'],
            'Name': t['Name'],
            'Type': t_type
        })
        logging.info(f"Parsed tournament: ID={t['ID']}, Name={t['Name']}, Type={t_type}")
    if new_tournaments:
        append_tournaments_to_csv(OUTPUT_FILE, new_tournaments)
        logging.info(f"Appended {len(new_tournaments)} new tournaments to {OUTPUT_FILE}.")
    else:
        logging.info("No new tournaments found.")

if __name__ == "__main__":
    main() 
