import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import argparse
import re

try:
    from . import utils
    from . import scrape_matches
except ImportError:
    import utils
    import scrape_matches

# Resolve paths relative to the project root so scripts work from any CWD
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BASE_URL = "https://th.sportscorpion.com"
PLAYER_COLUMNS = ['PlayerID', 'Name', 'RankingID', 'Country', 'City', 'DateOfBirth', 'Sex']

# The site renders profile pages server-side and slows down sharply under
# parallel load: throughput peaks at a handful of concurrent requests, and more
# workers only make each page slower (and trigger 503s/timeouts). Profiles of
# active players can take well over 20s to render, so give them a longer read
# timeout instead of abandoning the render and starting it again on retry.
DEFAULT_WORKERS = 5
PLAYER_PAGE_TIMEOUT = (10, 90)

def get_thread_session() -> requests.Session:
    return utils.get_thread_session()

def fetch_page(session, url: str) -> BeautifulSoup:
    """Fetch the page content and return a BeautifulSoup object"""
    response = utils.get_with_status(session, url)
    return BeautifulSoup(response.text, 'lxml')

def parse_player_page(player_id: int, text) -> Dict[str, Any]:
    """
    Parse a player profile page into a player row.

    Uses lxml directly (via the same helpers as the match parser) instead of
    BeautifulSoup: profile pages of active players are large because they list
    every tournament played, and building a BeautifulSoup tree for them cost
    far more CPU than the handful of fields read here.
    """
    root = scrape_matches.parse_html(text)

    # Extract player name
    name_element = next((h1 for h1 in root.iter('h1') if h1.get('id') == 'header'), None)
    name = scrape_matches.element_text(name_element).split(' - ')[-1] if name_element is not None else ''

    # Extract information from the table
    table = next((t for t in root.iter('table') if 'iTable' in (t.get('class') or '').split()), None)

    if table is not None:
        data = {}
        for row in table.iter('tr'):
            cells = [cell for cell in row.iter('th', 'td') if cell is not row]
            if len(cells) == 2:
                key = scrape_matches.element_text(cells[0]).strip()
                value = scrape_matches.element_text(cells[1]).strip()
                data[key] = value

        # Extract specific data fields
        ranking_match = re.search(r'ID\s*(\d+)', data.get('World ranking', ''))
        ranking_id = ranking_match.group(1) if ranking_match else ''
        country = data.get('Country', '')
        city = data.get('City', '')
        date_of_birth = data.get('Date of birth', '')
        sex = data.get('Sex', '')
    else:
        ranking_id = country = city = date_of_birth = sex = ''

    return {
        'PlayerID': player_id,
        'Name': name,
        'RankingID': ranking_id,
        'Country': country,
        'City': city,
        'DateOfBirth': date_of_birth,
        'Sex': sex
    }

def process_player(player_id: int) -> Optional[Dict[str, Any]]:
    """
    Process a single player by ID and return player information.
    
    Args:
        player_id: Player ID to scrape
        
    Returns:
        Dictionary containing player information
    """
    try:
        session = get_thread_session()
        link = f"{BASE_URL}/eng/user/id/{player_id}/"
        response = utils.get_with_status(session, link, timeout=PLAYER_PAGE_TIMEOUT)
        return parse_player_page(player_id, response.text)
    except requests.RequestException as e:
        logging.warning(f"Request failed for player {player_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error processing player {player_id}: {e}")
        return None

def get_unique_player_ids(parquet_file_path: Path) -> List[int]:
    """
    Extract unique player IDs from the matches data.
    
    Args:
        parquet_file_path: Path to the parquet file containing match data
        
    Returns:
        List of unique player IDs
    """
    if not parquet_file_path.exists():
        logging.error(f"File {parquet_file_path} not found!")
        return []
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file_path)
    
    # Extract unique player IDs from both Player1ID and Player2ID columns
    player1_ids = df['Player1ID'].dropna().unique()
    player2_ids = df['Player2ID'].dropna().unique()
    
    # Combine and get unique IDs
    all_player_ids = set(player1_ids) | set(player2_ids)
    
    # Convert to list and sort
    unique_player_ids = sorted(list(all_player_ids))
    
    logging.info(f"Found {len(unique_player_ids)} unique player IDs")
    return unique_player_ids

def scrape_all_players(
    player_ids: List[int],
    output_file: Path = DATA_DIR / "players_data.csv",
    max_workers: int = DEFAULT_WORKERS,
    refresh_existing: bool = False,
) -> pd.DataFrame:
    """
    Scrape information for all players in parallel.
    
    Args:
        player_ids: List of player IDs to scrape
        output_file: Output CSV file path
        
    Returns:
        DataFrame containing all player information
    """
    # Check if output file exists and load existing data
    existing_players: set[int] = set()
    existing_df = pd.DataFrame()
    if output_file.exists():
        existing_df = pd.read_csv(output_file)
        if refresh_existing:
            existing_players = set()
        else:
            name_missing = existing_df['Name'].isna() | existing_df['Name'].astype(str).str.strip().eq('')
            existing_players = set(existing_df.loc[~name_missing, 'PlayerID'].astype(int))
            missing_count = int(name_missing.sum())
            if missing_count:
                logging.info(f"Will retry {missing_count} existing players with missing names")
        logging.info(f"Found {len(existing_players)} existing players in {output_file}")
    
    # Filter out already scraped players
    new_player_ids = sorted({int(pid) for pid in player_ids if int(pid) not in existing_players})
    logging.info(f"Need to scrape {len(new_player_ids)} new players")
    
    if not new_player_ids:
        logging.info("No new players to scrape!")
        return existing_df
    
    # List to store player data
    players_data = []

    with tqdm(total=len(new_player_ids), desc="Processing Players", unit="player") as progress_bar:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            future_to_id = {executor.submit(process_player, player_id): player_id for player_id in new_player_ids}

            for future in as_completed(future_to_id):
                result = future.result()
                if result is not None:
                    players_data.append(result)
                progress_bar.update(1)
    
    # Create DataFrame from new data
    new_players_df = pd.DataFrame(players_data, columns=PLAYER_COLUMNS)

    if new_players_df.empty and output_file.exists():
        logging.warning("No player requests succeeded; leaving existing output unchanged.")
        return existing_df
    
    # Combine with existing data if it exists
    if output_file.exists():
        combined_df = pd.concat([existing_df, new_players_df], ignore_index=True)
        # Remove duplicates based on PlayerID
        combined_df.drop_duplicates(subset=['PlayerID'], keep='last', inplace=True)
        combined_df.reset_index(drop=True, inplace=True)
    else:
        combined_df = new_players_df
    
    # Save the combined DataFrame
    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logging.info(f"Saved {len(combined_df)} players to {output_file}")
    
    return combined_df

def main():
    """Main execution function"""
    utils.setup_logging()
    parser = argparse.ArgumentParser(description="Scrape player profile data from player IDs in scraped matches.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers for player profile pages.")
    parser.add_argument("--refresh-existing", action="store_true", help="Re-scrape all players, not just new or blank rows.")
    args = parser.parse_args()

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # File paths
    matches_file = DATA_DIR / "scraped_matches.parquet"
    players_file = DATA_DIR / "players_data.csv"
    
    logging.info("Extracting unique player IDs from matches data...")
    unique_player_ids = get_unique_player_ids(matches_file)
    
    if not unique_player_ids:
        logging.info("No player IDs found!")
        return
    
    logging.info(f"Scraping information for {len(unique_player_ids)} unique players...")
    players_df = scrape_all_players(
        unique_player_ids,
        players_file,
        max_workers=args.workers,
        refresh_existing=args.refresh_existing,
    )
    
    logging.info("Player scraping completed!")
    logging.info(f"Total players in database: {len(players_df)}")
    
    # Display some statistics
    if not players_df.empty:
        stats_df = players_df.replace('', pd.NA)
        logging.info("\nPlayer Statistics:")
        logging.info(f"Players with country info: {stats_df['Country'].notna().sum()}")
        logging.info(f"Players with city info: {stats_df['City'].notna().sum()}")
        logging.info(f"Players with birth date: {stats_df['DateOfBirth'].notna().sum()}")
        logging.info(f"Players with sex info: {stats_df['Sex'].notna().sum()}")

if __name__ == "__main__":
    main()
