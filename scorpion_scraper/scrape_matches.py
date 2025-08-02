import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Tuple
from pathlib import Path

# Resolve paths relative to the project root so scripts work from any CWD
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BASE_URL = "https://th.sportscorpion.com"

# Mapping of playoff stage names to fraction values
PLAYOFF_STAGE_MAP = {
    "1/64 final": 1/64,
    "1/32 final": 1/32,
    "1/16 final": 1/16,
    "1/8 final": 1/8,
    "Quarterfinal": 1/4,
    "Semi-final": 1/2,
    "Final": 1,
    "Match for the third place": 0.9
}

def fetch_page(session, url: str) -> BeautifulSoup:
    # Fetch the page content and return a BeautifulSoup object
    response = session.get(url)
    return BeautifulSoup(response.text, 'lxml')

def clean_score_text(score_text: str) -> str:
    """
    Clean the raw score text by removing OT, W.O, etc.
    """
    # A single chain of replaces is slightly more efficient than repeated calls
    # Also more readable and consistent
    return (score_text
            .replace('(OT)', '')
            .replace('(W.O)', '')
            .replace('\xa0', '')
            .replace('*', '')
            .replace('\n', ''))

def get_playoff_stage_fraction(stage_name: str) -> float:
    # Convert the playoff stage name into a numeric fraction, e.g. 'Quarterfinal' -> 0.25
    stage_name = stage_name.strip().lower()
    for key, val in PLAYOFF_STAGE_MAP.items():
        if key.lower() in stage_name:
            return val
    # If not found in map, default to None (unknown stage)
    return None

def extract_name_and_id(a_tag) -> Tuple[str, str]:
    """
    Given an <a> tag for a player, return (player_name, player_id).
    If there's no <a> tag or it's missing an ID, returns (None, None).
    """
    if not a_tag:
        return None, None
    
    player_name = a_tag.text.strip()
    href = a_tag.get('href', '')
    match_id = re.search(r'/user/id/(\d+)/', href)
    player_id = match_id.group(1) if match_id else None
    
    return player_name, player_id

def get_match_info(session, url: str) -> List[Tuple[str, str, str, str, str, int, int, str, str, float, int]]:
    """
    Fetch match information from a given stage page.
    
    Returns a list of tuples:
    (
        URL,
        Player1Name,
        Player1ID,
        Player2Name,
        Player2ID,
        GoalsPlayer1,
        GoalsPlayer2,
        Overtime,
        Stage,            # 'Playoff' or 'Round-Robin'
        RoundNumber,      # numeric fraction for playoff stage or round # for RR
        PlayoffGameNumber # int or None
    )
    """
    soup = fetch_page(session, url)
    
    # Remove the 'saved-matches' section to avoid duplicates
    saved_matches_div = soup.find('div', class_='saved-matches')
    if saved_matches_div:
        saved_matches_div.decompose()
    
    match_info = []

    # Check if the page is for the playoff stage
    is_playoff = len(soup.select('tr.series-container')) > 0

    if is_playoff:
        # For playoff stages, matches are organized by subheaders (Quarterfinal, Semi-final, etc.)
        subheaders = soup.select('div.subheader')
        for subheader in subheaders:
            stage_name = subheader.get_text(strip=True)
            playoff_fraction = get_playoff_stage_fraction(stage_name)

            # Find the .gr_match blocks after this subheader until next subheader
            next_siblings = subheader.find_all_next('div', class_='gr_match')
            for block in next_siblings:
                # If block belongs to another subheader, stop processing further blocks
                next_sub = block.find_previous_sibling('div', class_='subheader')
                if next_sub and next_sub != subheader:
                    break

                series = block.select('tr.series-container')
                for serie in series:
                    # Each player is in 'td[class^="ma_name"] a'
                    # We want the first link for player1, second link for player2
                    players = serie.select('td[class^="ma_name"] a')
                    if len(players) < 2:
                        continue
                    player1_name, player1_id = extract_name_and_id(players[0])
                    player2_name, player2_id = extract_name_and_id(players[1])
                    
                    # Each 'td[class^="ma_result_"]' corresponds to one game in the series
                    scores = serie.select('td[class^="ma_result_"]')
                    # Ignore the last score which is the total series score
                    for game_number, score in enumerate(scores[:-1], start=1):
                        if ':' in score.text:
                            score_cleaned = clean_score_text(score.text)
                            try:
                                goals_player_1, goals_player_2 = map(int, score_cleaned.split(':'))
                                overtime = 'Yes' if '(OT)' in score.text else 'No'
                                match_info.append(
                                    (
                                        url,
                                        player1_name,
                                        player1_id,
                                        player2_name,
                                        player2_id,
                                        goals_player_1,
                                        goals_player_2,
                                        overtime,
                                        'Playoff',
                                        playoff_fraction,
                                        game_number
                                    )
                                )
                            except ValueError:
                                continue

    else:
        # Scrape round-robin matches
        match_tables = soup.select('table.grTable')
        for table in match_tables:
            header = table.select_one('th:-soup-contains("Tour")')
            if header:
                round_text = header.get_text(strip=True)
                round_match = re.search(r'(\d+)\s*Tour', round_text)
                round_number = float(round_match.group(1)) if round_match else None
            else:
                round_number = None

            rows = table.select('tr[id^="match"]')
            for row in rows:
                # Player 1 link
                player1_a = row.select_one('td.ma_name1 a')
                player1_name, player1_id = extract_name_and_id(player1_a)
                # If no <a>, fallback to the raw text
                if not player1_name:
                    player1_name = row.select_one('td.ma_name1').text.strip()

                # Player 2 link
                player2_a = row.select_one('td.ma_name2 a')
                player2_name, player2_id = extract_name_and_id(player2_a)
                # If no <a>, fallback to the raw text
                if not player2_name:
                    player2_name = row.select_one('td.ma_name2').text.strip()

                score = row.select_one('td[class^="ma_result_"]')
                if score and ':' in score.text:
                    score_cleaned = (
                        score.text.replace('(OT)', '')
                                  .replace('(W.O)', '')
                                  .replace('\xa0', '')
                                  .replace('*', '')
                                  .replace('\n', '')
                    )
                    try:
                        goals_player_1, goals_player_2 = map(int, score_cleaned.split(':'))
                        overtime = 'Yes' if '(OT)' in score.text else 'No'
                        # For round-robin, we don't have a playoff game number, so set None
                        match_info.append(
                            (
                                url,
                                player1_name,
                                player1_id,
                                player2_name,
                                player2_id,
                                goals_player_1,
                                goals_player_2,
                                overtime,
                                'Round-Robin',
                                round_number,
                                None  # <-- None for round-robin
                            )
                        )
                    except ValueError:
                        print(f"Unable to parse score '{score_cleaned}' from match {url}")

    return match_info

def get_tournament_matches(tournament_urls: List[str], existing_stage_ids: set[str]) -> pd.DataFrame:
    all_matches = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    def fetch_tournament_data(url):
        with requests.Session() as session:
            session.headers.update(headers)
            tournament_id = url.split('/')[-2]
            tournament_url = f"{BASE_URL}/eng/tournament/id/{tournament_id}/"
            tournament_soup = fetch_page(session, tournament_url)

            # Check if tournament is a team tournament, skip if yes
            tournament_type_element = tournament_soup.select_one("th:-soup-contains('Tournament type') + td")
            tournament_type = tournament_type_element.text.strip() if tournament_type_element else 'Unknown'
            if tournament_type.lower() == 'team':
                return []

            # Extract tournament name and date
            tournament_name_element = tournament_soup.select_one("h1#header")
            tournament_name = tournament_name_element.text.strip() if tournament_name_element else 'Unknown'

            date_element = tournament_soup.select_one("th:-soup-contains('Date of the tournament') + td")
            date = date_element.text.strip() if date_element else 'Unknown'

            # Extract the stages and their sequences
            stage_rows = tournament_soup.select('table.stages-table tr')
            stage_data = []
            for row in stage_rows:
                seq_cell = row.select_one('td.stage-gr')
                if seq_cell:
                    stage_sequence = seq_cell.get_text(strip=True)
                    sched_link = row.select_one('a:-soup-contains("Schedule and results")')
                    if sched_link:
                        stage_url = f"{BASE_URL}{sched_link['href']}?print"
                        stage_id = stage_url.split('/')[-3]
                        stage_data.append((stage_id, stage_url, stage_sequence))

            stage_matches = []
            for stage_id, stage_url, stage_sequence in stage_data:
                # Skip stage if already in existing_stage_ids
                if stage_id in existing_stage_ids:
                    continue
                matches = get_match_info(session, stage_url)
                for match in matches:
                    # match = (
                    #   url,
                    #   Player1Name, Player1ID,
                    #   Player2Name, Player2ID,
                    #   GoalsPlayer1, GoalsPlayer2,
                    #   Overtime, Stage, RoundNumber,
                    #   PlayoffGameNumber
                    # )
                    stage_matches.append((
                        int(stage_id) if stage_id.isdigit() else None,  # Ensure StageID is numeric
                        match[1],  # Player1Name
                        int(match[2]) if match[2] and match[2].isdigit() else None,  # Player1ID
                        match[3],  # Player2Name
                        int(match[4]) if match[4] and match[4].isdigit() else None,  # Player2ID
                        match[5],  # GoalsPlayer1
                        match[6],  # GoalsPlayer2
                        match[7],  # Overtime
                        match[8],  # Stage
                        match[9],  # RoroundNumber
                        match[10],  # PlayoffGameNumber
                        date,
                        tournament_name,
                        int(tournament_id),  # Ensure TournamentID is numeric
                        int(stage_sequence) if stage_sequence.isdigit() else None  # Ensure StageSequence is numeric
                    ))
            return stage_matches

    processed_tournaments = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(fetch_tournament_data, url): url for url in tournament_urls}
        for future in tqdm(as_completed(future_to_url), total=len(future_to_url),
                           desc="Processing tournaments", unit="tournament"):
            url = future_to_url[future]
            try:
                matches = future.result()
                all_matches.extend(matches)
                processed_tournaments += 1
                tqdm.write(f"\rProcessed tournaments: {processed_tournaments}", end='')
            except Exception as exc:
                print(f'{url} generated an exception: {exc}')

    # Build DataFrame
    df = pd.DataFrame(
        all_matches, 
        columns=[
            'StageID',
            'Player1',
            'Player1ID',
            'Player2',
            'Player2ID',
            'GoalsPlayer1',
            'GoalsPlayer2',
            'Overtime',
            'Stage',
            'RoundNumber',
            'PlayoffGameNumber',
            'Date',
            'TournamentName',
            'TournamentID',
            'StageSequence'
        ]
    )

    # Convert columns to match the schema
    df['StageID'] = pd.to_numeric(df['StageID'], errors='coerce', downcast='integer')
    df['Player1ID'] = pd.to_numeric(df['Player1ID'], errors='coerce', downcast='integer')
    df['Player2ID'] = pd.to_numeric(df['Player2ID'], errors='coerce', downcast='integer')
    df['TournamentID'] = pd.to_numeric(df['TournamentID'], errors='coerce', downcast='integer')
    df['StageSequence'] = pd.to_numeric(df['StageSequence'], errors='coerce', downcast='integer')

    # Format 'Date' to string for Parquet compatibility
    df['Date'] = df['Date'].apply(
        lambda x: pd.to_datetime(x, format='%d.%m.%Y', errors='coerce').strftime('%Y-%m-%d') if pd.notnull(x) else None
    )

    # Sort data
    df.sort_values(by=["Date", "StageSequence", "RoundNumber", "PlayoffGameNumber"], 
                   inplace=True, na_position='last')

    # Remove playoff draws
    df = df[~((df['Stage'] == 'Playoff') & (df['GoalsPlayer1'] == df['GoalsPlayer2']))]
    df.reset_index(drop=True, inplace=True)

    return df

# Read tournament data from CSV and filter for Individual tournaments
def get_individual_tournament_urls(csv_file_path: Path) -> List[str]:
    """
    Read tournament data from CSV file and return URLs for Individual tournaments only.
    """
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Filter for Individual tournaments only
    individual_tournaments = df[df['Type'] == 'Individual']
    
    # Convert tournament IDs to URLs
    tournament_urls = []
    for tournament_id in individual_tournaments['ID']:
        url = f"{BASE_URL}/eng/tournament/id/{tournament_id}/"
        tournament_urls.append(url)
    
    print(f"Found {len(individual_tournaments)} Individual tournaments out of {len(df)} total tournaments")
    return tournament_urls

# Main execution
if __name__ == "__main__":
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get tournament URLs from CSV file
    csv_file_path = DATA_DIR / "tournament_data.csv"
    tournament_urls = get_individual_tournament_urls(csv_file_path)

    # Parquet output file
    output_file = DATA_DIR / "scraped_matches.parquet"

    # Determine already scraped TournamentIDs
    if output_file.exists():
        existing_df = pd.read_parquet(output_file)
        scraped_ids = set(existing_df["TournamentID"].dropna().astype(str).unique())
    else:
        scraped_ids = set()

    # Filter tournament URLs to only those not already scraped
    def extract_id_from_url(url):
        return url.rstrip("/").split("/")[-1]
    tournament_urls_to_scrape = [url for url in tournament_urls if extract_id_from_url(url) not in scraped_ids]

    num_skipped = len(tournament_urls) - len(tournament_urls_to_scrape)
    print(f"Skipping {num_skipped} tournaments already scraped out of {len(tournament_urls)} total. {len(tournament_urls_to_scrape)} left to scrape.")

    print(f"Scraping {len(tournament_urls_to_scrape)} tournaments (after filtering and limiting)")

    if tournament_urls_to_scrape:
        df = get_tournament_matches(tournament_urls_to_scrape, existing_stage_ids=set())
        print(f"Total matches scraped: {len(df)}")
        print(f"DataFrame shape: {df.shape}")

        # Append to Parquet (or create new)
        if output_file.exists():
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # Drop duplicates based on TournamentID, StageID, Player1ID, Player2ID, GoalsPlayer1, GoalsPlayer2, Date
            combined_df.drop_duplicates(subset=["TournamentID", "StageID", "Player1ID", "Player2ID", "GoalsPlayer1", "GoalsPlayer2", "Date"], inplace=True)
            combined_df.to_parquet(output_file, index=False)
        else:
            df.to_parquet(output_file, index=False)
        print(f"Matches saved to {output_file}")
    else:
        print("No new tournaments to scrape.") 