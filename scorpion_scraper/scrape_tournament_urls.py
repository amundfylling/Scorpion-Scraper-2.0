import requests
from bs4 import BeautifulSoup
import argparse
import logging
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path

try:
    from . import utils
    from . import tournament_metadata
except ImportError:
    import utils
    import tournament_metadata

# Configuration
BASE_URL = "https://th.sportscorpion.com/eng/tournament/archive/?page="
TOURNAMENT_BASE = "https://th.sportscorpion.com"
MAX_PAGES = 5 # since the script runs daily, it only needs to go through the newest tournaments
# Resolve paths relative to the project root so scripts work from any CWD
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "tournament_data.csv"
METADATA_FILE = DATA_DIR / "tournament_metadata.csv"
STAGE_FILE = DATA_DIR / "tournament_stages.csv"

utils.setup_logging()

def fetch_page(url: str, session: requests.Session) -> Optional[requests.Response]:
    try:
        return utils.get_with_status(session, url)
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

def get_tournament_details(tournament: Dict[str, str]) -> Optional[Tuple[Dict[str, str], Dict[str, str], List[Dict[str, str]]]]:
    with utils.get_retry_session() as session:
        response = fetch_page(tournament['DetailURL'], session)
        if not response:
            return None
    soup = BeautifulSoup(response.text, 'lxml')
    metadata_row = tournament_metadata.parse_tournament_metadata(
        soup,
        tournament['DetailURL'],
        fallback_id=tournament['ID'],
        fallback_name=tournament['Name'],
    )
    stage_rows = tournament_metadata.parse_tournament_stages(
        soup,
        metadata_row['TournamentID'],
        TOURNAMENT_BASE,
    )
    return tournament_metadata.catalog_row_from_metadata(metadata_row), metadata_row, stage_rows

def get_tournament_type(detail_url: str) -> str:
    details = get_tournament_details({
        'ID': tournament_metadata.extract_tournament_id(detail_url),
        'Name': '',
        'DetailURL': detail_url,
    })
    if not details:
        return ''
    catalog_row, _, _ = details
    return catalog_row.get('Type', '')

def read_existing_ids(filename: Path) -> Set[str]:
    return {row['ID'] for row in read_existing_tournaments(filename) if row.get('ID')}

def read_existing_tournaments(filename: Path) -> List[Dict[str, str]]:
    if not filename.exists():
        return []
    with filename.open('r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [dict(row) for row in reader]

def append_tournaments_to_csv(filename: Path, tournaments: List[Dict[str, str]]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = filename.exists()
    with filename.open('a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['ID', 'Name', 'Type'], lineterminator='\n')
        if not file_exists:
            writer.writeheader()
        for t in tournaments:
            writer.writerow(t)

def write_tournaments_to_csv(filename: Path, tournaments: List[Dict[str, str]]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with filename.open('w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['ID', 'Name', 'Type'], lineterminator='\n')
        writer.writeheader()
        for t in tournaments:
            writer.writerow({
                'ID': t.get('ID', ''),
                'Name': t.get('Name', ''),
                'Type': t.get('Type', ''),
            })

def main():
    parser = argparse.ArgumentParser(description="Scrape tournament URLs.")
    parser.add_argument("--full", action="store_true", help="Run a full scrape (all pages). Default is incremental (5 pages).")
    parser.add_argument("--pages", type=int, default=MAX_PAGES, help="Number of archive pages to scan in incremental mode.")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers for tournament detail pages.")
    parser.add_argument("--refresh-existing", action="store_true", help="Refresh type/name for tournaments already present in the CSV.")
    args = parser.parse_args()

    # Pass 1: Collect all tournaments from overview pages
    all_tournaments = []
    
    # Determine page range
    if args.full:
        logging.info("Starting FULL scrape of tournament URLs.")
        page_range = range(1, 10000) # Effectively infinite for this context
    else:
        logging.info(f"Starting INCREMENTAL scrape of tournament URLs (max {args.pages} pages).")
        page_range = range(1, args.pages + 1)

    with utils.get_retry_session() as session:
        for page_num in page_range:
            page_url = BASE_URL + str(page_num)
            response = fetch_page(page_url, session)
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

    existing_ids = read_existing_ids(OUTPUT_FILE)
    tournaments_to_parse = [
        t for t in all_tournaments
        if args.refresh_existing or t['ID'] not in existing_ids
    ]
    new_tournaments = []
    new_metadata_rows = []
    new_stage_rows = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_to_tournament = {
            executor.submit(get_tournament_details, t): t
            for t in tournaments_to_parse
        }
        for future in as_completed(future_to_tournament):
            t = future_to_tournament[future]
            try:
                details = future.result()
            except Exception as exc:
                logging.warning("Failed parsing tournament %s: %s", t['ID'], exc)
                details = None
            if not details:
                continue
            catalog_row, metadata_row, stage_rows = details
            new_tournaments.append(catalog_row)
            new_metadata_rows.append(metadata_row)
            new_stage_rows.extend(stage_rows)
            logging.info(
                "Parsed tournament: ID=%s, Name=%s, Type=%s",
                catalog_row['ID'],
                catalog_row['Name'],
                catalog_row['Type'],
            )
    if new_tournaments:
        if args.refresh_existing:
            parsed_by_id = {t['ID']: t for t in new_tournaments}
            merged_tournaments = []
            for existing in read_existing_tournaments(OUTPUT_FILE):
                replacement = parsed_by_id.pop(existing.get('ID'), None)
                merged_tournaments.append(replacement or existing)
            merged_tournaments.extend(parsed_by_id.values())
            write_tournaments_to_csv(OUTPUT_FILE, merged_tournaments)
            logging.info(f"Refreshed {len(new_tournaments)} tournaments in {OUTPUT_FILE}.")
        else:
            append_tournaments_to_csv(OUTPUT_FILE, new_tournaments)
            logging.info(f"Appended {len(new_tournaments)} new tournaments to {OUTPUT_FILE}.")
        tournament_metadata.upsert_metadata_csv(METADATA_FILE, new_metadata_rows)
        logging.info(f"Updated {METADATA_FILE} with {len(new_metadata_rows)} metadata rows.")
        tournament_metadata.upsert_stage_csv(STAGE_FILE, new_stage_rows)
        logging.info(f"Updated {STAGE_FILE} with {len(new_stage_rows)} stage rows.")
    else:
        logging.info("No new tournaments found.")

if __name__ == "__main__":
    main() 
