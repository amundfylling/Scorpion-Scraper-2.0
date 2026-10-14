import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree
from lxml import html as lxml_html
import re
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple
from pathlib import Path
import logging
import argparse
import queue

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
METADATA_FILE = DATA_DIR / "tournament_metadata.csv"
STAGE_FILE = DATA_DIR / "tournament_stages.csv"

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

# Tournament status that means results will no longer change. Anything else
# ("Finished *", "Is being conducted *", "Registration ...") can still change.
FINAL_STATUS = "Finished"

USER_ID_RE = re.compile(r'/user/id/(\d+)/')
TEAM_ID_RE = re.compile(r'/team/id/(\d+)/')
DIGITS_RE = re.compile(r'(\d+)')
TOUR_RE = re.compile(r'(\d+)\s*Tour')
ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

# ---------------------------------------------------------------------------
# HTML helpers
#
# Match pages are parsed with lxml directly instead of BeautifulSoup: building
# and querying a BeautifulSoup tree was the main CPU cost of a scrape. The
# helpers below mirror the BeautifulSoup/soupsieve semantics the parsers used
# before (class token matching, attribute prefix matching, get_text()), so the
# parsed rows are identical.
# ---------------------------------------------------------------------------

def parse_html(text) -> etree._Element:
    """Parse an HTML document (str or bytes) into an lxml tree."""
    try:
        return lxml_html.document_fromstring(text)
    except ValueError:
        # lxml refuses str input that carries an XML encoding declaration.
        if isinstance(text, str):
            return lxml_html.document_fromstring(text.encode('utf-8'))
        raise

def fetch_page(session, url: str) -> etree._Element:
    # Fetch the page content and return a parsed lxml tree
    response = utils.get_with_status(session, url)
    return parse_html(response.text)

def fetch_soup(session, url: str) -> BeautifulSoup:
    # Tournament info pages are parsed by tournament_metadata with BeautifulSoup
    response = utils.get_with_status(session, url)
    return BeautifulSoup(response.text, 'lxml')

def _iter_strings(el) -> Iterator[str]:
    # Text nodes in document order, skipping comments (like BeautifulSoup).
    if el.text and isinstance(el.tag, str):
        yield el.text
    for child in el:
        if isinstance(child.tag, str):
            yield from _iter_strings(child)
        if child.tail:
            yield child.tail

def element_text(el) -> str:
    """Equivalent of BeautifulSoup's ``tag.text``."""
    return ''.join(_iter_strings(el))

def element_text_stripped(el) -> str:
    """Equivalent of BeautifulSoup's ``tag.get_text(strip=True)``."""
    return ''.join(s.strip() for s in _iter_strings(el) if s.strip())

def _has_class(el, name: str) -> bool:
    return name in (el.get('class') or '').split()

def _attr_startswith(el, attr: str, prefix: str) -> bool:
    return (el.get(attr) or '').startswith(prefix)

def _descendants(el, tag: str, predicate: Callable[[Any], bool] = None) -> Iterator[Any]:
    for node in el.iter(tag):
        if node is el:
            continue
        if predicate is None or predicate(node):
            yield node

def _first(iterator: Iterator[Any]):
    return next(iterator, None)

def _has_ancestor(el, predicate: Callable[[Any], bool]) -> bool:
    parent = el.getparent()
    while parent is not None:
        if predicate(parent):
            return True
        parent = parent.getparent()
    return False

def _is_name_cell(el) -> bool:
    return el.tag == 'td' and _attr_startswith(el, 'class', 'ma_name')

def _is_result_cell(el) -> bool:
    return _attr_startswith(el, 'class', 'ma_result_')

def _links_in_class_cell(el, cell_class: str) -> Iterator[Any]:
    # CSS: td.<cell_class> a
    return _descendants(
        el, 'a',
        lambda a: _has_ancestor(a, lambda p: p.tag == 'td' and _has_class(p, cell_class)),
    )

def _links_in_name_cells(el) -> List[Any]:
    # CSS: td[class^="ma_name"] a
    return list(_descendants(el, 'a', lambda a: _has_ancestor(a, _is_name_cell)))

def _match_rows(el) -> Iterator[Any]:
    # CSS: tr[id^="match"]
    return _descendants(el, 'tr', lambda tr: _attr_startswith(tr, 'id', 'match'))

def _first_result_cell(el):
    # CSS: td[class^="ma_result_"]
    return _first(_descendants(el, 'td', _is_result_cell))

def _remove_saved_matches(root) -> None:
    # Remove the 'saved-matches' section to avoid duplicates
    saved_matches_div = _first(_descendants(root, 'div', lambda div: _has_class(div, 'saved-matches')))
    if saved_matches_div is not None:
        saved_matches_div.drop_tree()

def _round_number(table) -> Optional[float]:
    header = _first(_descendants(table, 'th', lambda th: 'Tour' in element_text(th)))
    if header is None:
        return None
    round_match = TOUR_RE.search(element_text_stripped(header))
    return float(round_match.group(1)) if round_match else None

def _iter_playoff_series(root) -> Iterator[Tuple[Optional[float], Any]]:
    """
    Yield (playoff_fraction, series_row) for every series in a playoff stage.

    Matches are organised by subheaders (Quarterfinal, Semi-final, ...). For
    each subheader, the .gr_match blocks after it belong to it until a block
    whose closest previous subheader sibling is a different subheader.
    """
    order = {}
    subheaders = []
    blocks = []
    for index, el in enumerate(root.iter(tag=etree.Element)):
        order[el] = index
        if el.tag == 'div':
            if _has_class(el, 'subheader'):
                subheaders.append(el)
            if _has_class(el, 'gr_match'):
                blocks.append(el)

    for subheader in subheaders:
        stage_name = element_text_stripped(subheader)
        playoff_fraction = get_playoff_stage_fraction(stage_name)
        subheader_order = order[subheader]
        for block in blocks:
            if order[block] <= subheader_order:
                continue
            # If block belongs to another subheader, stop processing further blocks
            next_sub = _first(
                sib for sib in block.itersiblings(preceding=True)
                if sib.tag == 'div' and _has_class(sib, 'subheader')
            )
            if next_sub is not None and next_sub is not subheader:
                break
            for serie in _descendants(block, 'tr', lambda tr: _has_class(tr, 'series-container')):
                yield playoff_fraction, serie

def _is_playoff_page(root) -> bool:
    return _first(_descendants(root, 'tr', lambda tr: _has_class(tr, 'series-container'))) is not None

def _series_game_cells(serie) -> List[Any]:
    # Game cells have a data-match-id; the final series-total cell does not.
    # Filtering this way avoids dropping real games when markup varies.
    return [
        td for td in _descendants(serie, 'td', _is_result_cell)
        if td.get('data-match-id')
    ]

def clean_score_text(score_text: str) -> str:
    """
    Clean the raw score text by removing OT, W.O, etc.
    """
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
    if a_tag is None:
        return None, None

    player_name = element_text(a_tag).strip()
    href = a_tag.get('href', '')
    match_id = USER_ID_RE.search(href)
    player_id = match_id.group(1) if match_id else None

    return player_name, player_id

def extract_team_name_and_id(a_tag) -> Tuple[str, str]:
    if a_tag is None:
        return None, None

    team_name = element_text(a_tag).strip()
    href = a_tag.get('href', '')
    match_id = TEAM_ID_RE.search(href)
    team_id = match_id.group(1) if match_id else None

    return team_name, team_id

def extract_match_id(raw_id: Optional[str]) -> Optional[str]:
    if not raw_id:
        return None
    match = DIGITS_RE.search(raw_id)
    return match.group(1) if match else None

def safe_int(value) -> Optional[int]:
    if value is None:
        return None
    value = str(value)
    return int(value) if value.isdigit() else None

def parse_score_cell(score) -> Optional[Tuple[int, int, str, str]]:
    if score is None:
        return None
    score_text = element_text(score)
    if ':' not in score_text:
        return None
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
    if ISO_DATE_RE.fullmatch(value):
        return value
    parsed = pd.to_datetime(value, errors='coerce', dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.strftime('%Y-%m-%d')

def get_match_info(session, url: str) -> List[Dict[str, Any]]:
    """
    Fetch match information from a given stage page.
    """
    return parse_individual_stage_matches(fetch_page(session, url), url)

def parse_individual_stage_matches(root, url: str) -> List[Dict[str, Any]]:
    """
    Parse played individual matches from a stage schedule page.
    """
    _remove_saved_matches(root)
    match_info = []

    if _is_playoff_page(root):
        for playoff_fraction, serie in _iter_playoff_series(root):
            # We want the first link for player1, second link for player2
            players = _links_in_name_cells(serie)
            if len(players) < 2:
                continue
            player1_name, player1_id = extract_name_and_id(players[0])
            player2_name, player2_id = extract_name_and_id(players[1])

            for game_number, score in enumerate(_series_game_cells(serie), start=1):
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
        return match_info

    # Scrape round-robin matches
    for table in _descendants(root, 'table', lambda t: _has_class(t, 'grTable')):
        round_number = _round_number(table)

        for row in _match_rows(table):
            score = _first_result_cell(row)
            parsed_score = parse_score_cell(score)
            if not parsed_score:
                continue

            player1_name, player1_id = extract_name_and_id(_first(_links_in_class_cell(row, 'ma_name1')))
            if not player1_name:
                # If no <a>, fallback to the raw text
                player1_cell = _first(_descendants(row, 'td', lambda td: _has_class(td, 'ma_name1')))
                if player1_cell is not None:
                    player1_name = element_text(player1_cell).strip()

            player2_name, player2_id = extract_name_and_id(_first(_links_in_class_cell(row, 'ma_name2')))
            if not player2_name:
                player2_cell = _first(_descendants(row, 'td', lambda td: _has_class(td, 'ma_name2')))
                if player2_cell is not None:
                    player2_name = element_text(player2_cell).strip()

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

def parse_team_child_matches(root, context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse player-vs-player rows embedded in a team aggregate match detail page.
    """
    child_matches = []
    team_game_number = 0

    for row in _match_rows(root):
        player1_name, player1_id = extract_name_and_id(_first(_links_in_class_cell(row, 'ma_name1')))
        player2_name, player2_id = extract_name_and_id(_first(_links_in_class_cell(row, 'ma_name2')))
        if not player1_id or not player2_id:
            continue

        parsed_score = parse_score_cell(_first_result_cell(row))
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

def collect_team_match_contexts(root, url: str) -> List[Dict[str, Any]]:
    _remove_saved_matches(root)
    contexts = []

    if _is_playoff_page(root):
        for playoff_fraction, serie in _iter_playoff_series(root):
            teams = _links_in_name_cells(serie)
            if len(teams) < 2:
                continue
            team1_name, team1_id = extract_team_name_and_id(teams[0])
            team2_name, team2_id = extract_team_name_and_id(teams[1])

            for game_number, score in enumerate(_series_game_cells(serie), start=1):
                if not parse_score_cell(score):
                    continue
                contexts.append({
                    'SourceURL': url,
                    'TeamMatchID': extract_match_id(score.get('data-match-id')),
                    'Team1': team1_name,
                    'Team1ID': team1_id,
                    'Team2': team2_name,
                    'Team2ID': team2_id,
                    'Stage': 'Playoff',
                    'RoundNumber': playoff_fraction,
                    'PlayoffGameNumber': game_number,
                })
        return contexts

    for table in _descendants(root, 'table', lambda t: _has_class(t, 'grTable')):
        round_number = _round_number(table)

        for row in _match_rows(table):
            if not parse_score_cell(_first_result_cell(row)):
                continue

            team1_name, team1_id = extract_team_name_and_id(_first(_links_in_class_cell(row, 'ma_name1')))
            team2_name, team2_id = extract_team_name_and_id(_first(_links_in_class_cell(row, 'ma_name2')))
            if not team1_id or not team2_id:
                continue

            contexts.append({
                'SourceURL': url,
                'TeamMatchID': extract_match_id(row.get('id')),
                'Team1': team1_name,
                'Team1ID': team1_id,
                'Team2': team2_name,
                'Team2ID': team2_id,
                'Stage': 'Round-Robin',
                'RoundNumber': round_number,
                'PlayoffGameNumber': None,
            })

    return contexts

def parse_team_stage_matches(
    root,
    url: str,
    detail_loader: Callable[[str], Any],
) -> List[Dict[str, Any]]:
    """
    Parse team tournament schedules and expand each aggregate match into
    played player-vs-player child matches.
    """
    match_info = []
    for context in collect_team_match_contexts(root, url):
        team_match_id = context.get('TeamMatchID')
        if not team_match_id:
            continue
        try:
            detail_root = detail_loader(str(team_match_id))
        except Exception as exc:
            logging.warning("Failed loading team match detail %s: %s", team_match_id, exc)
            continue
        match_info.extend(parse_team_child_matches(detail_root, context))
    return match_info

def parse_team_stage_matches_parallel(
    root,
    url: str,
    detail_loader: Callable[[str], Any],
    detail_workers: int,
) -> List[Dict[str, Any]]:
    contexts = [c for c in collect_team_match_contexts(root, url) if c.get('TeamMatchID')]
    if not contexts:
        return []

    with ThreadPoolExecutor(max_workers=max(1, detail_workers)) as executor:
        futures = [executor.submit(detail_loader, str(c['TeamMatchID'])) for c in contexts]
        match_info = []
        for context, future in zip(contexts, futures):
            try:
                detail_root = future.result()
            except Exception as exc:
                logging.warning("Failed loading team match detail %s: %s", context.get('TeamMatchID'), exc)
                continue
            match_info.extend(parse_team_child_matches(detail_root, context))
    return match_info

def team_match_detail_url(team_match_id: str) -> str:
    return f"{BASE_URL}/eng/match/id/{team_match_id}/"

def get_team_match_info(session, url: str, detail_workers: int = 10) -> List[Dict[str, Any]]:
    root = fetch_page(session, url)

    if detail_workers > 1:
        def detail_loader(team_match_id: str):
            return fetch_page(utils.get_thread_session(), team_match_detail_url(team_match_id))
        return parse_team_stage_matches_parallel(root, url, detail_loader, detail_workers)
    return parse_team_stage_matches(
        root, url, lambda team_match_id: fetch_page(session, team_match_detail_url(team_match_id))
    )

def load_metadata_by_id(filename: Path = METADATA_FILE) -> Dict[str, Dict[str, str]]:
    return {
        row["TournamentID"]: row
        for row in tournament_metadata.read_metadata_csv(filename)
        if row.get("TournamentID")
    }

def load_stages_by_id(filename: Path = STAGE_FILE) -> Dict[str, List[Dict[str, str]]]:
    stages_by_id: Dict[str, List[Dict[str, str]]] = {}
    for row in tournament_metadata.read_stage_csv(filename):
        tournament_id = row.get("TournamentID")
        if not tournament_id:
            continue
        stages_by_id.setdefault(tournament_id, []).append(row)
    return stages_by_id

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

def is_final_status(metadata_row: Optional[Dict[str, str]]) -> bool:
    return bool(metadata_row) and (metadata_row.get('Status') or '').strip() == FINAL_STATUS

def resolve_tournament(
    tournament_id: str,
    metadata_row: Optional[Dict[str, str]],
    stage_rows: List[Dict[str, str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]], bool]:
    """
    Return (metadata_row, stage_rows, fetched_live) for a tournament.

    The cached metadata and stage list are used for finished tournaments. For
    anything that can still change (missing cache, not yet finished) the live
    tournament page is read, so stages added after the tournament was first
    discovered (typically playoffs) and changed dates are picked up.
    """
    if metadata_row and stage_rows and is_final_status(metadata_row):
        return metadata_row, stage_rows, False

    tournament_url = f"{BASE_URL}/eng/tournament/id/{tournament_id}/"
    try:
        soup = fetch_soup(utils.get_thread_session(), tournament_url)
    except Exception as exc:
        if metadata_row and stage_rows:
            logging.warning("Could not refresh tournament %s; using cached metadata: %s", tournament_id, exc)
            return metadata_row, stage_rows, False
        raise

    live_metadata = tournament_metadata.parse_tournament_metadata(soup, tournament_url, fallback_id=tournament_id)
    live_stages = tournament_metadata.parse_tournament_stages(soup, tournament_id, BASE_URL)
    if metadata_row and not (live_metadata.get('Type') or live_metadata.get('Status')):
        # The page did not render tournament info; don't overwrite good cache.
        live_metadata = metadata_row
    if stage_rows and not live_stages:
        live_stages = stage_rows
    return live_metadata, live_stages, True

class _TournamentJob:
    def __init__(self, tournament_id: str, url: str):
        self.tournament_id = tournament_id
        self.url = url
        self.metadata: Dict[str, str] = {}
        self.pending = 0
        self.failed = False
        self.rows: List[Dict[str, Any]] = []

    def add_matches(self, stage_row: Dict[str, str], matches: List[Dict[str, Any]]) -> None:
        metadata = self.metadata
        for match in matches:
            self.rows.append(build_output_row(
                match,
                stage_row.get('StageID', ''),
                stage_row.get('StageSequence', ''),
                self.tournament_id,
                metadata.get('Name') or 'Unknown',
                metadata.get('Date') or '',
                metadata.get('Type') or 'Unknown',
            ))

def _fetch_individual_stage(stage_url: str) -> List[Dict[str, Any]]:
    return get_match_info(utils.get_thread_session(), stage_url)

def _fetch_team_stage(stage_url: str) -> List[Dict[str, Any]]:
    root = fetch_page(utils.get_thread_session(), stage_url)
    return [c for c in collect_team_match_contexts(root, stage_url) if c.get('TeamMatchID')]

def _fetch_team_detail(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = fetch_page(utils.get_thread_session(), team_match_detail_url(str(context['TeamMatchID'])))
    return parse_team_child_matches(root, context)

def get_tournament_matches(
    tournament_urls: List[str],
    existing_stage_ids: Set[str],
    max_workers: int = 10,
    metadata_by_id: Optional[Dict[str, Dict[str, str]]] = None,
    stages_by_id: Optional[Dict[str, List[Dict[str, str]]]] = None,
    team_detail_workers: int = 5,
    live_updates: Optional[Dict[str, Tuple[Dict[str, str], List[Dict[str, str]]]]] = None,
) -> pd.DataFrame:
    """
    Scrape all played matches for the given tournaments.

    Tournament pages, stage pages and team match detail pages all share one
    worker pool, so every worker stays busy regardless of how the work is
    spread across tournaments. ``team_detail_workers`` is kept for backwards
    compatibility and no longer used.

    When ``live_updates`` is given, it receives the metadata and stage rows
    read from live tournament pages, keyed by tournament ID.
    """
    metadata_by_id = metadata_by_id or {}
    stages_by_id = stages_by_id or {}

    jobs: Dict[str, _TournamentJob] = {}
    for url in tournament_urls:
        tournament_id = tournament_metadata.extract_tournament_id(url)
        jobs.setdefault(tournament_id, _TournamentJob(tournament_id, url))

    all_matches: List[Dict[str, Any]] = []
    pending: Dict[Future, Tuple[str, _TournamentJob, Any]] = {}
    completed: "queue.Queue[Future]" = queue.Queue()

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor, \
            tqdm(total=len(jobs), desc="Processing tournaments", unit="tournament") as progress:

        def submit(kind: str, job: _TournamentJob, payload: Any, fn: Callable, *args) -> None:
            future = executor.submit(fn, *args)
            pending[future] = (kind, job, payload)
            job.pending += 1
            future.add_done_callback(completed.put)

        def finish(job: _TournamentJob) -> None:
            job.pending -= 1
            if job.pending:
                return
            if not job.failed:
                all_matches.extend(job.rows)
            job.rows = []
            progress.update(1)

        def fail(job: _TournamentJob, exc: BaseException) -> None:
            if not job.failed:
                logging.error(f'{job.url} generated an exception: {exc}')
            job.failed = True

        for job in jobs.values():
            submit(
                'tournament', job, None, resolve_tournament,
                job.tournament_id,
                metadata_by_id.get(job.tournament_id),
                stages_by_id.get(job.tournament_id, []),
            )

        while pending:
            future = completed.get()
            kind, job, payload = pending.pop(future)
            try:
                result = future.result()
            except Exception as exc:
                if kind == 'detail':
                    logging.warning("Failed loading team match detail %s: %s", payload[1].get('TeamMatchID'), exc)
                else:
                    fail(job, exc)
                finish(job)
                continue

            if job.failed:
                finish(job)
                continue

            if kind == 'tournament':
                metadata_row, stage_rows, fetched_live = result
                job.metadata = metadata_row
                if fetched_live and live_updates is not None:
                    live_updates[job.tournament_id] = (metadata_row, stage_rows)
                is_team = (metadata_row.get('Type') or 'Unknown').lower() == 'team'
                for stage_row in stage_rows:
                    stage_id = stage_row.get('StageID', '')
                    # Skip stage if already in existing_stage_ids
                    if stage_id in existing_stage_ids:
                        continue
                    stage_url = stage_row.get('StageURL') or f"{BASE_URL}/eng/tournament/stage/{stage_id}/matches/?print"
                    if is_team:
                        submit('team_stage', job, stage_row, _fetch_team_stage, stage_url)
                    else:
                        submit('stage', job, stage_row, _fetch_individual_stage, stage_url)
            elif kind == 'stage':
                job.add_matches(payload, result)
            elif kind == 'team_stage':
                for context in result:
                    submit('detail', job, (payload, context), _fetch_team_detail, context)
            elif kind == 'detail':
                job.add_matches(payload[0], result)
            finish(job)

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

def save_live_updates(
    live_updates: Dict[str, Tuple[Dict[str, str], List[Dict[str, str]]]],
    metadata_by_id: Dict[str, Dict[str, str]],
    stages_by_id: Dict[str, List[Dict[str, str]]],
    metadata_file: Path = METADATA_FILE,
    stage_file: Path = STAGE_FILE,
) -> None:
    """
    Write metadata and stage rows read from live tournament pages back to the
    caches, but only where something other than the scrape timestamp changed,
    so unchanged tournaments don't rewrite the CSVs every night.
    """
    def comparable(row: Optional[Dict[str, str]]) -> Dict[str, str]:
        row = row or {}
        return {
            field: row.get(field, '') or ''
            for field in tournament_metadata.METADATA_FIELDS
            if field != 'ScrapedAt'
        }

    def stage_key(rows: List[Dict[str, str]]) -> List[Tuple[str, ...]]:
        return [tuple(row.get(field, '') or '' for field in tournament_metadata.STAGE_FIELDS) for row in rows]

    changed_metadata = []
    changed_stages = []
    for tournament_id, (metadata_row, stage_rows) in live_updates.items():
        if comparable(metadata_row) != comparable(metadata_by_id.get(tournament_id)):
            changed_metadata.append(metadata_row)
        if stage_rows and stage_key(stage_rows) != stage_key(stages_by_id.get(tournament_id, [])):
            changed_stages.extend(stage_rows)

    if changed_metadata:
        tournament_metadata.upsert_metadata_csv(metadata_file, changed_metadata)
        logging.info("Updated cached metadata for %s tournaments.", len(changed_metadata))
    if changed_stages:
        tournament_metadata.upsert_stage_csv(stage_file, changed_stages)
        logging.info(
            "Updated cached stages for %s tournaments.",
            len({row['TournamentID'] for row in changed_stages}),
        )

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

def id_strings(series: pd.Series) -> pd.Series:
    """Tournament IDs as strings ("7979"), regardless of int/float/str dtype."""
    return pd.to_numeric(series, errors='coerce').astype('Int64').astype('string')

def _parse_scraped_at(value: str) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

def is_settled_without_matches(
    metadata_row: Optional[Dict[str, str]],
    cutoff: pd.Timestamp,
    known_before: datetime,
) -> bool:
    """
    True for a tournament that is finished, outside the refresh window and has
    been known long enough to have been scraped on several runs. If such a
    tournament still has no rows, it has no player games to scrape (typically
    team tournaments that only publish aggregate scores), so re-scraping it
    every night only costs time.
    """
    if not is_final_status(metadata_row):
        return False
    date = pd.to_datetime(metadata_row.get('Date'), errors='coerce')
    if pd.isna(date) or date >= cutoff:
        return False
    scraped_at = _parse_scraped_at(metadata_row.get('ScrapedAt'))
    return scraped_at is not None and scraped_at < known_before

# Main execution
if __name__ == "__main__":
    utils.setup_logging()

    parser = argparse.ArgumentParser(description="Scrape matches.")
    parser.add_argument("--full", action="store_true", help="Run a full scrape (all tournaments) and overwrite existing data. Default is incremental.")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers for tournament, stage and team match pages.")
    parser.add_argument("--team-detail-workers", type=int, default=5, help="Deprecated and ignored: team match pages share the --workers pool.")
    parser.add_argument(
        "--refresh-recent-days",
        type=int,
        default=14,
        help="In incremental mode, re-scrape tournaments with matches from the last N days and replace their old rows.",
    )
    parser.add_argument(
        "--refresh-unfinished-days",
        type=int,
        default=60,
        help="In incremental mode, also re-scrape tournaments from the last N days that were not finished when last scraped.",
    )
    parser.add_argument(
        "--recheck-empty",
        action="store_true",
        help="Re-scrape finished tournaments that have never produced any matches (skipped by default).",
    )
    args = parser.parse_args()

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get tournament URLs from CSV file
    csv_file_path = DATA_DIR / "tournament_data.csv"
    tournament_urls = get_tournament_urls(csv_file_path)
    metadata_by_id = load_metadata_by_id(METADATA_FILE)
    stages_by_id = load_stages_by_id(STAGE_FILE)
    cached_stage_count = sum(len(stages) for stages in stages_by_id.values())
    logging.info(
        "Loaded metadata cache for %s tournaments and stage cache with %s stages across %s tournaments.",
        len(metadata_by_id),
        cached_stage_count,
        len(stages_by_id),
    )

    # Parquet output file
    output_file = DATA_DIR / "scraped_matches.parquet"

    # Determine already scraped TournamentIDs
    scraped_ids = set()
    refresh_ids = set()
    existing_df = pd.DataFrame()
    today = pd.Timestamp.today().normalize()
    if args.full:
        logging.info("FULL SCRAPE MODE: Ignoring existing data. Will look at all tournaments.")
    elif output_file.exists():
        existing_df = pd.read_parquet(output_file)
        existing_ids = id_strings(existing_df["TournamentID"])
        scraped_ids = set(existing_ids.dropna().unique())
        logging.info(f"INCREMENTAL MODE: Found {len(scraped_ids)} already scraped tournaments.")
        if args.refresh_recent_days > 0 and "Date" in existing_df.columns:
            dates = pd.to_datetime(existing_df["Date"], errors="coerce")
            cutoff = today - pd.Timedelta(days=args.refresh_recent_days)
            refresh_ids = set(existing_ids[dates >= cutoff].dropna().unique())
            logging.info(
                "Will refresh %s recently scraped tournaments from %s onward.",
                len(refresh_ids),
                cutoff.strftime("%Y-%m-%d"),
            )
        if args.refresh_unfinished_days > 0:
            # Tournaments scraped while still running can gain stages later
            # (e.g. playoffs), and their date may have been moved.
            unfinished_cutoff = today - pd.Timedelta(days=args.refresh_unfinished_days)
            unfinished_ids = {
                tournament_id
                for tournament_id in scraped_ids
                if tournament_id in metadata_by_id
                and not is_final_status(metadata_by_id[tournament_id])
                and pd.to_datetime(metadata_by_id[tournament_id].get('Date'), errors='coerce') >= unfinished_cutoff
            }
            if unfinished_ids - refresh_ids:
                logging.info(
                    "Will refresh %s more tournaments that were unfinished when scraped.",
                    len(unfinished_ids - refresh_ids),
                )
            refresh_ids |= unfinished_ids
        scraped_ids -= refresh_ids

    # Filter tournament URLs to only those not already scraped
    def extract_id_from_url(url):
        return url.rstrip("/").split("/")[-1]

    if args.full:
        tournament_urls_to_scrape = tournament_urls
    else:
        tournament_urls_to_scrape = [url for url in tournament_urls if extract_id_from_url(url) not in scraped_ids]
        if not args.recheck_empty:
            settled_cutoff = today - pd.Timedelta(days=max(args.refresh_recent_days, 14))
            known_before = datetime.now(timezone.utc) - timedelta(days=7)
            settled = [
                url for url in tournament_urls_to_scrape
                if is_settled_without_matches(
                    metadata_by_id.get(extract_id_from_url(url)),
                    settled_cutoff,
                    known_before,
                )
            ]
            if settled:
                settled_set = set(settled)
                tournament_urls_to_scrape = [url for url in tournament_urls_to_scrape if url not in settled_set]
                logging.info(
                    "Skipping %s finished tournaments that have never had player matches (use --recheck-empty to include them).",
                    len(settled),
                )

    num_skipped = len(tournament_urls) - len(tournament_urls_to_scrape)
    logging.info(f"Skipping {num_skipped} tournaments already scraped out of {len(tournament_urls)} total. {len(tournament_urls_to_scrape)} left to scrape.")

    logging.info(f"Scraping {len(tournament_urls_to_scrape)} tournaments (after filtering and limiting)")

    if tournament_urls_to_scrape:
        live_updates: Dict[str, Tuple[Dict[str, str], List[Dict[str, str]]]] = {}
        df = get_tournament_matches(
            tournament_urls_to_scrape,
            existing_stage_ids=set(),
            max_workers=args.workers,
            metadata_by_id=metadata_by_id,
            stages_by_id=stages_by_id,
            live_updates=live_updates,
        )
        logging.info(f"Total matches scraped: {len(df)}")
        logging.info(f"DataFrame shape: {df.shape}")

        # Append to Parquet (or create new)
        if output_file.exists() and not args.full:
            # Incremental append
            refreshed_ids = set(id_strings(df["TournamentID"]).dropna().unique()) & refresh_ids
            base_df = existing_df
            if refreshed_ids:
                base_df = existing_df[~id_strings(existing_df["TournamentID"]).isin(refreshed_ids).fillna(False)]
                logging.info("Replacing existing rows for %s refreshed tournaments.", len(refreshed_ids))
            combined_df = pd.concat([base_df, df], ignore_index=True)
            combined_df = drop_duplicate_matches(combined_df)
            combined_df.to_parquet(output_file, index=False)
            logging.info(f"Appended matches to {output_file}")
        else:
            # Full overwrite or new file
            df.to_parquet(output_file, index=False)
            logging.info(f"Saved (overwritten) matches to {output_file}")

        save_live_updates(live_updates, metadata_by_id, stages_by_id)
    else:
        logging.info("No new tournaments to scrape.")
