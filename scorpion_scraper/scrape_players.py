import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Dict, Any
from pathlib import Path

# Resolve paths relative to the project root so scripts work from any CWD
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BASE_URL = "https://th.sportscorpion.com"

def fetch_page(session, url: str) -> BeautifulSoup:
    """Fetch the page content and return a BeautifulSoup object"""
    response = session.get(url)
    return BeautifulSoup(response.text, 'lxml')

def process_player(session, player_id: int) -> Dict[str, Any]:
    """
    Process a single player by ID and return player information.
    
    Args:
        session: requests.Session object
        player_id: Player ID to scrape
        
    Returns:
        Dictionary containing player information
    """
    try:
        link = f"{BASE_URL}/eng/user/id/{player_id}/"
        response = session.get(link)
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Extract player name
        name_element = soup.find('h1', id='header')
        name = name_element.text.split(' - ')[-1] if name_element else ''
        
        # Extract information from the table
        table = soup.find('table', class_='iTable')
        
        if table:
            rows = table.find_all('tr')
            data = {}
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    data[key] = value
            
            # Extract specific data fields
            ranking_id = data.get('World ranking', '').split('ID ')[-1].strip(')') if 'ID' in data.get('World ranking', '') else ''
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
    except Exception as e:
        print(f"Error processing player {player_id}: {e}")
        return {
            'PlayerID': player_id,
            'Name': '',
            'RankingID': '',
            'Country': '',
            'City': '',
            'DateOfBirth': '',
            'Sex': ''
        }

def get_unique_player_ids(parquet_file_path: Path) -> List[int]:
    """
    Extract unique player IDs from the matches data.
    
    Args:
        parquet_file_path: Path to the parquet file containing match data
        
    Returns:
        List of unique player IDs
    """
    if not parquet_file_path.exists():
        print(f"File {parquet_file_path} not found!")
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
    
    print(f"Found {len(unique_player_ids)} unique player IDs")
    return unique_player_ids

def scrape_all_players(player_ids: List[int], output_file: Path = DATA_DIR / "players_data.csv") -> pd.DataFrame:
    """
    Scrape information for all players in parallel.
    
    Args:
        player_ids: List of player IDs to scrape
        output_file: Output CSV file path
        
    Returns:
        DataFrame containing all player information
    """
    # Check if output file exists and load existing data
    existing_players = set()
    if output_file.exists():
        existing_df = pd.read_csv(output_file)
        existing_players = set(existing_df['PlayerID'].astype(int))
        print(f"Found {len(existing_players)} existing players in {output_file}")
    
    # Filter out already scraped players
    new_player_ids = [pid for pid in player_ids if pid not in existing_players]
    print(f"Need to scrape {len(new_player_ids)} new players")
    
    if not new_player_ids:
        print("No new players to scrape!")
        return existing_df if os.path.exists(output_file) else pd.DataFrame()
    
    # Set up session with headers
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # List to store player data
    players_data = []
    
    # Set up the progress bar
    progress_bar = tqdm(total=len(new_player_ids), desc="Processing Players", unit="player")
    
    # Using ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        with requests.Session() as session:
            session.headers.update(headers)
            future_to_id = {executor.submit(process_player, session, player_id): player_id for player_id in new_player_ids}
            
            for future in as_completed(future_to_id):
                result = future.result()
                players_data.append(result)
                progress_bar.update(1)
    
    # Close the progress bar
    progress_bar.close()
    
    # Create DataFrame from new data
    new_players_df = pd.DataFrame(players_data)
    
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
    print(f"Saved {len(combined_df)} players to {output_file}")
    
    return combined_df

def main():
    """Main execution function"""
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # File paths
    matches_file = DATA_DIR / "scraped_matches.parquet"
    players_file = DATA_DIR / "players_data.csv"
    
    print("Extracting unique player IDs from matches data...")
    unique_player_ids = get_unique_player_ids(matches_file)
    
    if not unique_player_ids:
        print("No player IDs found!")
        return
    
    print(f"Scraping information for {len(unique_player_ids)} unique players...")
    players_df = scrape_all_players(unique_player_ids, players_file)
    
    print("Player scraping completed!")
    print(f"Total players in database: {len(players_df)}")
    
    # Display some statistics
    if not players_df.empty:
        print("\nPlayer Statistics:")
        print(f"Players with country info: {players_df['Country'].notna().sum()}")
        print(f"Players with city info: {players_df['City'].notna().sum()}")
        print(f"Players with birth date: {players_df['DateOfBirth'].notna().sum()}")
        print(f"Players with sex info: {players_df['Sex'].notna().sum()}")

if __name__ == "__main__":
    main()