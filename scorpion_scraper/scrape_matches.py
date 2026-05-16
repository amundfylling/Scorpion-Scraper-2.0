import pandas as pd
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Any, Callable, Dict, List, Optional, Tuple
from pathlib import Path
import logging
import argparse

try:
    from . import utils
    from . import tournament_metadata
except ImportError:
    import utils
    import tournament_metadata

# Resolve paths relative to the project root so scripts work from any CWD
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BASE_URL = "https://th.sportscorpion.com"

MATCH_COLUMNS = [
    'StageID',
    'MatchID',
    'Player1',
    'Player1ID',
    'Player2',
    'Player2ID',
    'GoalsPlayer1',
    'GoalsPlayer2',
    'Overtime',
    'Walkover',
    'Stage',
    'RoundNumber',
    'PlayoffGameNumber',
    'Date',
    'TournamentName',
    'TournamentID',
    'StageSequence',
    'TournamentType',
    'TeamMatchID',
    'Team1',
    'Team1ID',
    'Team2',
    'Team2ID',
    'TeamGameNumber',
]

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
    response = utils.get_with_status(session, url)
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

def extract_team_name_and_id(a_tag) -> Tuple[str, str]:
    if not a_tag:
        return None, None

    team_name = a_tag.text.strip()
    href = a_tag.get('href', '')
    match_id = re.search(r'/team/id/(\d+)/', href)
    team_id = match_id.group(1) if match_id else None

    return team_name, team_id

def extract_match_id(raw_id: Optional[str]) -> Optional[str]:
    if not raw_id:
        return None
    match = re.search(r'(\d+)', raw_id)
    return match.group(1) if match else None

def safe_int(value) -> Optional[int]:
    if value is None:
        return None
    value = str(value)
    return int(value) if value.isdigit() else None

def parse_score_cell(score) -> Optional[Tuple[int, int, str, str]]:
    if not score or ':' not in score.text:
        return None
    score_text = score.text
    score_cleaned = clean_score_text(score_text)
    try:
        goals_player_1, goals_player_2 = map(int, score_cleaned.split(':'))
    except ValueError:
        return None
    overtime = 'Yes' if '(OT)' in score_text else 'No'
    walkover = 'Yes' if '(W.O)' in score_text else 'No'
    return goals_player_1, goals_player_2, overtime, walkover

def normalize_output_date(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if not value:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    parsed = pd.to_datetime(value, errors='coerce', dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.strftime('%Y-%m-%d')

def get_match_info(session, url: str) -> List[Dict[str, Any]]:
    """
    Fetch match information from a given stage page.
    """
    soup = fetch_page(session, url)
    return parse_individual_stage_matches(soup, url)

def parse_individual_stage_matches(soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
    """
    Parse played individual matches from a stage schedule page.
    """
    
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
                    
                    # Game cells have a data-match-id; the final series-total cell does not.
                    # Filtering this way avoids dropping real games when markup varies.
                    game_cells = [
                        td for td in serie.select('td[class^="ma_result_"]')
                        if td.get('data-match-id')
                    ]
                    for game_number, score in enumerate(game_cells, start=1):
                        parsed_score = parse_score_cell(score)
                        if not parsed_score:
                            continue
                        goals_player_1, goals_player_2, overtime, walkover = parsed_score
                        match_info.append({
                            'SourceURL': url,
                            'MatchID': extract_match_id(score.get('data-match-id')),
                            'Player1': player1_name,
                            'Player1ID': player1_id,
                            'Player2': player2_name,
                            'Player2ID': player2_id,
                            'GoalsPlayer1': goals_player_1,
                            'GoalsPlayer2': goals_player_2,
                            'Overtime': overtime,
                            'Walkover': walkover,
                            'Stage': 'Playoff',
                            'RoundNumber': playoff_fraction,
                            'PlayoffGameNumber': game_number,
                        })

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
                player1_cell = row.select_one('td.ma_name1')
                if not player1_name and player1_cell:
                    player1_name = player1_cell.text.strip()

                # Player 2 link
                player2_a = row.select_one('td.ma_name2 a')
                player2_name, player2_id = extract_name_and_id(player2_a)
                # If no <a>, fallback to the raw text
                player2_cell = row.select_one('td.ma_name2')
                if not player2_name and player2_cell:
                    player2_name = player2_cell.text.strip()

                score = row.select_one('td[class^="ma_result_"]')
                parsed_score = parse_score_cell(score)
                if not parsed_score:
                    continue
                goals_player_1, goals_player_2, overtime, walkover = parsed_score
                match_info.append({
                    'SourceURL': url,
                    'MatchID': extract_match_id(row.get('id')),
                    'Player1': player1_name,
                    'Player1ID': player1_id,
                    'Player2': player2_name,
                    'Player2ID': player2_id,
                    'GoalsPlayer1': goals_player_1,
                    'GoalsPlayer2': goals_player_2,
                    'Overtime': overtime,
                    'Walkover': walkover,
                    'Stage': 'Round-Robin',
                    'RoundNumber': round_number,
                    'PlayoffGameNumber': None,
                })

    return match_info

def _append_team_child_matches(
    match_info: List[Dict[str, Any]],
    detail_soup_loader: Callable[[str], BeautifulSoup],
    context: Dict[str, Any],
) -> None:
    team_match_id = context.get('TeamMatchID')
    if not team_match_id:
        return
    try:
        detail_soup = detail_soup_loader(str(team_match_id))
    except Exception as exc:
        logging.warning("Failed loading team match detail %s: %s", team_match_id, exc)
        return
    match_info.extend(parse_team_child_matches(detail_soup, context))

def parse_team_child_matches(soup: BeautifulSoup, context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse player-vs-player rows embedded in a team aggregate match detail page.
    """
    child_matches = []
    team_game_number = 0

    for row in soup.select('tr[id^="match"]'):
        player1_a = row.select_one('td.ma_name1 a')
        player2_a = row.select_one('td.ma_name2 a')
        player1_name, player1_id = extract_name_and_id(player1_a)
        player2_name, player2_id = extract_name_and_id(player2_a)
        if not player1_id or not player2_id:
            continue

        score = row.select_one('td[class^="ma_result_"]')
        parsed_score = parse_score_cell(score)
        if not parsed_score:
            continue

        team_game_number += 1
        goals_player_1, goals_player_2, overtime, walkover = parsed_score
        child_matches.append({
            'SourceURL': context.get('SourceURL'),
            'MatchID': extract_match_id(row.get('id')),
            'Player1': player1_name,
            'Player1ID': player1_id,
            'Player2': player2_name,
            'Player2ID': player2_id,
            'GoalsPlayer1': goals_player_1,
            'GoalsPlayer2': goals_player_2,
            'Overtime': overtime,
            'Walkover': walkover,
            'Stage': context.get('Stage'),
            'RoundNumber': context.get('RoundNumber'),
            'PlayoffGameNumber': context.get('PlayoffGameNumber'),
            'TeamMatchID': context.get('TeamMatchID'),
            'Team1': context.get('Team1'),
            'Team1ID': context.get('Team1ID'),
            'Team2': context.get('Team2'),
            'Team2ID': context.get('Team2ID'),
            'TeamGameNumber': team_game_number,
        })

    return child_matches

def parse_team_stage_matches(
    soup: BeautifulSoup,
    url: str,
    detail_soup_loader: Callable[[str], BeautifulSoup],
) -> List[Dict[str, Any]]:
    """
    Parse team tournament schedules and expand each aggregate match into
    played player-vs-player child matches.
    """
    saved_matches_div = soup.find('div', class_='saved-matches')
    if saved_matches_div:
        saved_matches_div.decompose()

    match_info = []
    is_playoff = len(soup.select('tr.series-container')) > 0

    if is_playoff:
        subheaders = soup.select('div.subheader')
        for subheader in subheaders:
            stage_name = subheader.get_text(strip=True)
            playoff_fraction = get_playoff_stage_fraction(stage_name)
            next_siblings = subheader.find_all_next('div', class_='gr_match')

            for block in next_siblings:
                next_sub = block.find_previous_sibling('div', class_='subheader')
                if next_sub and next_sub != subheader:
                    break

                for serie in block.select('tr.series-container'):
                    teams = serie.select('td[class^="ma_name"] a')
                    if len(teams) < 2:
                        continue
                    team1_name, team1_id = extract_team_name_and_id(teams[0])
                    team2_name, team2_id = extract_team_name_and_id(teams[1])

                    game_cells = [
                        td for td in serie.select('td[class^="ma_result_"]')
                        if td.get('data-match-id')
                    ]
                    for game_number, score in enumerate(game_cells, start=1):
                        if not parse_score_cell(score):
                            continue
                        context = {
                            'SourceURL': url,
                            'TeamMatchID': extract_match_id(score.get('data-match-id')),
                            'Team1': team1_name,
                            'Team1ID': team1_id,
                            'Team2': team2_name,
                            'Team2ID': team2_id,
                            'Stage': 'Playoff',
                            'RoundNumber': playoff_fraction,
                            'PlayoffGameNumber': game_number,
                        }
                        _append_team_child_matches(match_info, detail_soup_loader, context)
        return match_info

    for table in soup.select('table.grTable'):
        header = table.select_one('th:-soup-contains("Tour")')
        if header:
            round_text = header.get_text(strip=True)
            round_match = re.search(r'(\d+)\s*Tour', round_text)
            round_number = float(round_match.group(1)) if round_match else None
        else:
            round_number = None

        for row in table.select('tr[id^="match"]'):
            score = row.select_one('td[class^="ma_result_"]')
            if not parse_score_cell(score):
                continue

            team1_a = row.select_one('td.ma_name1 a')
            team2_a = row.select_one('td.ma_name2 a')
            team1_name, team1_id = extract_team_name_and_id(team1_a)
            team2_name, team2_id = extract_team_name_and_id(team2_a)
            if not team1_id or not team2_id:
                continue

            context = {
                'SourceURL': url,
                'TeamMatchID': extract_match_id(row.get('id')),
                'Team1': team1_name,
                'Team1ID': team1_id,
                'Team2': team2_name,
                'Team2ID': team2_id,
                'Stage': 'Round-Robin',
                'RoundNumber': round_number,
                'PlayoffGameNumber': None,
            }
            _append_team_child_matches(match_info, detail_soup_loader, context)

    return match_info

def get_team_match_info(session, url: str) -> List[Dict[str, Any]]:
    soup = fetch_page(session, url)

    def detail_soup_loader(team_match_id: str) -> BeautifulSoup:
        return fetch_page(session, f"{BASE_URL}/eng/match/id/{team_match_id}/")

    return parse_team_stage_matches(soup, url, detail_soup_loader)

def build_output_row(
    match: Dict[str, Any],
    stage_id: str,
    stage_sequence: str,
    tournament_id: str,
    tournament_name: str,
    tournament_date: str,
    tournament_type: str,
) -> Dict[str, Any]:
    return {
        'StageID': safe_int(stage_id),
        'MatchID': safe_int(match.get('MatchID')),
        'Player1': match.get('Player1'),
        'Player1ID': safe_int(match.get('Player1ID')),
        'Player2': match.get('Player2'),
        'Player2ID': safe_int(match.get('Player2ID')),
        'GoalsPlayer1': match.get('GoalsPlayer1'),
        'GoalsPlayer2': match.get('GoalsPlayer2'),
        'Overtime': match.get('Overtime'),
        'Walkover': match.get('Walkover'),
        'Stage': match.get('Stage'),
        'RoundNumber': match.get('RoundNumber'),
        'PlayoffGameNumber': match.get('PlayoffGameNumber'),
        'Date': tournament_date,
        'TournamentName': tournament_name,
        'TournamentID': safe_int(tournament_id),
        'StageSequence': safe_int(stage_sequence),
        'TournamentType': tournament_type,
        'TeamMatchID': safe_int(match.get('TeamMatchID')),
        'Team1': match.get('Team1'),
        'Team1ID': safe_int(match.get('Team1ID')),
        'Team2': match.get('Team2'),
        'Team2ID': safe_int(match.get('Team2ID')),
        'TeamGameNumber': safe_int(match.get('TeamGameNumber')),
    }

def get_tournament_matches(tournament_urls: List[str], existing_stage_ids: set[str], max_workers: int = 10) -> pd.DataFrame:
    all_matches = []

    def fetch_tournament_data(url):
        with utils.get_retry_session() as session:
            tournament_id = tournament_metadata.extract_tournament_id(url)
            tournament_url = f"{BASE_URL}/eng/tournament/id/{tournament_id}/"
            tournament_soup = fetch_page(session, tournament_url)
            metadata_row = tournament_metadata.parse_tournament_metadata(
                tournament_soup,
                tournament_url,
                fallback_id=tournament_id,
            )
            tournament_type = metadata_row.get('Type') or 'Unknown'
            tournament_name = metadata_row.get('Name') or 'Unknown'
            date = metadata_row.get('Date') or ''

            # Extract the stages and their sequences
            stage_rows = tournament_soup.select('table.stages-table tr')
            stage_data = []
            current_stage_sequence = None
            for row in stage_rows:
                seq_cell = row.select_one('td.stage-gr')
                if seq_cell:
                    current_stage_sequence = seq_cell.get_text(strip=True)
                sched_link = row.select_one('a:-soup-contains("Schedule and results")')
                if not sched_link:
                    continue
                stage_url = f"{BASE_URL}{sched_link['href']}?print"
                stage_id = stage_url.split('/')[-3]
                stage_data.append((stage_id, stage_url, current_stage_sequence))

            stage_matches = []
            for stage_id, stage_url, stage_sequence in stage_data:
                # Skip stage if already in existing_stage_ids
                if stage_id in existing_stage_ids:
                    continue
                if tournament_type.lower() == 'team':
                    matches = get_team_match_info(session, stage_url)
                else:
                    matches = get_match_info(session, stage_url)
                for match in matches:
                    stage_matches.append(build_output_row(
                        match,
                        stage_id,
                        stage_sequence,
                        tournament_id,
                        tournament_name,
                        date,
                        tournament_type,
                    ))
            return stage_matches

    processed_tournaments = 0

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
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
                logging.error(f'{url} generated an exception: {exc}')

    df = pd.DataFrame(all_matches, columns=MATCH_COLUMNS)

    # Convert columns to match the schema
    df['StageID'] = pd.to_numeric(df['StageID'], errors='coerce', downcast='integer')
    df['MatchID'] = pd.to_numeric(df['MatchID'], errors='coerce', downcast='integer')
    df['Player1ID'] = pd.to_numeric(df['Player1ID'], errors='coerce', downcast='integer')
    df['Player2ID'] = pd.to_numeric(df['Player2ID'], errors='coerce', downcast='integer')
    df['TournamentID'] = pd.to_numeric(df['TournamentID'], errors='coerce', downcast='integer')
    df['StageSequence'] = pd.to_numeric(df['StageSequence'], errors='coerce', downcast='integer')
    df['TeamMatchID'] = pd.to_numeric(df['TeamMatchID'], errors='coerce', downcast='integer')
    df['Team1ID'] = pd.to_numeric(df['Team1ID'], errors='coerce', downcast='integer')
    df['Team2ID'] = pd.to_numeric(df['Team2ID'], errors='coerce', downcast='integer')
    df['TeamGameNumber'] = pd.to_numeric(df['TeamGameNumber'], errors='coerce', downcast='integer')

    # Format 'Date' to string for Parquet compatibility.
    df['Date'] = df['Date'].apply(normalize_output_date)

    # Sort data
    df.sort_values(by=["Date", "StageSequence", "RoundNumber", "PlayoffGameNumber", "TeamMatchID", "TeamGameNumber", "MatchID"],
                   inplace=True, na_position='last')

    # Remove playoff draws
    df = df[~((df['TournamentType'] != 'Team') & (df['Stage'] == 'Playoff') & (df['GoalsPlayer1'] == df['GoalsPlayer2']))]
    df.reset_index(drop=True, inplace=True)

    return df

LEGACY_DUPLICATE_COLUMNS = [
    "TournamentID",
    "StageID",
    "Player1ID",
    "Player2ID",
    "GoalsPlayer1",
    "GoalsPlayer2",
    "Date",
    "RoundNumber",
    "PlayoffGameNumber",
]

def drop_duplicate_matches(df: pd.DataFrame) -> pd.DataFrame:
    fallback_columns = [
        column for column in LEGACY_DUPLICATE_COLUMNS + ["TournamentType", "TeamMatchID", "TeamGameNumber"]
        if column in df.columns
    ]
    if "MatchID" not in df.columns:
        return df.drop_duplicates(subset=fallback_columns)

    with_match_id = df[df["MatchID"].notna()].drop_duplicates(subset=["MatchID"], keep="last")
    without_match_id = df[df["MatchID"].isna()].drop_duplicates(subset=fallback_columns, keep="last")
    return pd.concat([without_match_id, with_match_id], ignore_index=True)

def get_tournament_urls(csv_file_path: Path) -> List[str]:
    """
    Read tournament data from CSV file and return tournament URLs.
    """
    df = pd.read_csv(csv_file_path)

    tournament_urls = []
    for tournament_id in df['ID']:
        url = f"{BASE_URL}/eng/tournament/id/{tournament_id}/"
        tournament_urls.append(url)

    logging.info(f"Found {len(tournament_urls)} tournaments in {csv_file_path}")
    return tournament_urls

# Main execution
if __name__ == "__main__":
    utils.setup_logging()
    
    parser = argparse.ArgumentParser(description="Scrape matches.")
    parser.add_argument("--full", action="store_true", help="Run a full scrape (all tournaments) and overwrite existing data. Default is incremental.")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers for tournament pages.")
    parser.add_argument(
        "--refresh-recent-days",
        type=int,
        default=14,
        help="In incremental mode, re-scrape tournaments with matches from the last N days and replace their old rows.",
    )
    args = parser.parse_args()

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get tournament URLs from CSV file
    csv_file_path = DATA_DIR / "tournament_data.csv"
    tournament_urls = get_tournament_urls(csv_file_path)

    # Parquet output file
    output_file = DATA_DIR / "scraped_matches.parquet"

    # Determine already scraped TournamentIDs
    scraped_ids = set()
    refresh_ids = set()
    existing_df = pd.DataFrame()
    if args.full:
        logging.info("FULL SCRAPE MODE: Ignoring existing data. Will look at all tournaments.")
    elif output_file.exists():
        existing_df = pd.read_parquet(output_file)
        scraped_ids = set(existing_df["TournamentID"].dropna().astype(str).unique())
        logging.info(f"INCREMENTAL MODE: Found {len(scraped_ids)} already scraped tournaments.")
        if args.refresh_recent_days > 0 and "Date" in existing_df.columns:
            dates = pd.to_datetime(existing_df["Date"], errors="coerce")
            cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=args.refresh_recent_days)
            refresh_ids = set(existing_df.loc[dates >= cutoff, "TournamentID"].dropna().astype(str).unique())
            scraped_ids -= refresh_ids
            logging.info(
                "Will refresh %s recently scraped tournaments from %s onward.",
                len(refresh_ids),
                cutoff.strftime("%Y-%m-%d"),
            )

    # Filter tournament URLs to only those not already scraped
    def extract_id_from_url(url):
        return url.rstrip("/").split("/")[-1]
    
    if args.full:
        tournament_urls_to_scrape = tournament_urls
    else:
        tournament_urls_to_scrape = [url for url in tournament_urls if extract_id_from_url(url) not in scraped_ids]

    num_skipped = len(tournament_urls) - len(tournament_urls_to_scrape)
    logging.info(f"Skipping {num_skipped} tournaments already scraped out of {len(tournament_urls)} total. {len(tournament_urls_to_scrape)} left to scrape.")

    logging.info(f"Scraping {len(tournament_urls_to_scrape)} tournaments (after filtering and limiting)")

    if tournament_urls_to_scrape:
        df = get_tournament_matches(tournament_urls_to_scrape, existing_stage_ids=set(), max_workers=args.workers)
        logging.info(f"Total matches scraped: {len(df)}")
        logging.info(f"DataFrame shape: {df.shape}")

        # Append to Parquet (or create new)
        if output_file.exists() and not args.full:
            # Incremental append
            refreshed_ids = set(df["TournamentID"].dropna().astype(str).unique()) & refresh_ids
            base_df = existing_df
            if refreshed_ids:
                base_df = existing_df[~existing_df["TournamentID"].dropna().astype(str).isin(refreshed_ids)]
                logging.info("Replacing existing rows for %s refreshed tournaments.", len(refreshed_ids))
            combined_df = pd.concat([base_df, df], ignore_index=True)
            combined_df = drop_duplicate_matches(combined_df)
            combined_df.to_parquet(output_file, index=False)
            logging.info(f"Appended matches to {output_file}")
        else:
            # Full overwrite or new file
            df.to_parquet(output_file, index=False)
            logging.info(f"Saved (overwritten) matches to {output_file}")
    else:
        logging.info("No new tournaments to scrape.")
