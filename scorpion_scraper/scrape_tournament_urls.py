import requests
from bs4 import BeautifulSoup
import logging
import time
import csv
from typing import List, Dict, Optional, Set
from pathlib import Path

# Configuration
BASE_URL = "https://th.sportscorpion.com/eng/tournament/archive/?page="
TOURNAMENT_BASE = "https://th.sportscorpion.com"
MAX_PAGES = 5 # since the script runs daily, it only needs to go through the newest tournaments
# Resolve paths relative to the project root so scripts work from any CWD
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "tournament_data.csv"
RETRY_LIMIT = 3
RETRY_DELAY = 0  # seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_page(url: str, retries: int = RETRY_LIMIT) -> Optional[requests.Response]:
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response
            else:
                logging.warning(f"Non-200 status code {response.status_code} for URL: {url}")
                break
        except requests.RequestException as e:
            logging.warning(f"Request failed for {url} (attempt {attempt+1}/{retries}): {e}")
            time.sleep(RETRY_DELAY)
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
    # Pass 1: Collect all tournaments from overview pages
    all_tournaments = []
    for page_num in range(1, MAX_PAGES + 1):
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
