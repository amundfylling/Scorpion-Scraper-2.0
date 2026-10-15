import argparse
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from lxml import html as lxml_html
from tqdm import tqdm

try:
    from . import utils
except ImportError:
    import utils

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RANKING_URL = "https://stiga.trefik.cz/ithf/ranking/rankpl.aspx?pl={ranking_id}"
RANKING_COLUMNS = ['year', 'month', 'rank', 'points']

# The ITHF ranking server is a static-ish IIS page: with a reused connection a
# history page takes ~0.2s, and 16 parallel workers fetched all ~3000 ranked
# players in about 30s without errors.
DEFAULT_WORKERS = 16
RANKING_PAGE_TIMEOUT = (10, 60)

_CELL_RE = re.compile(r'(\d+)\.(?:\s*([\d]+))?')


def ranking_url(ranking_id) -> str:
    return RANKING_URL.format(ranking_id=int(ranking_id))


def _text(el) -> str:
    """Equivalent of BeautifulSoup's ``get_text(separator=' ', strip=True)``."""
    return ' '.join(s.strip() for s in el.itertext() if s.strip())


def _child_cells(row) -> list:
    return [cell for cell in row if cell.tag == 'td']


def parse_player_ranking(text) -> pd.DataFrame:
    """
    Parse a ranking history page into a DataFrame.
    Columns: ['year', 'month', 'rank', 'points']

    The page holds one outer row per year; its second cell wraps a nested table
    with one cell per month (``"<rank>.<br><points>"``, ``-`` when unranked, or
    blank for months not published yet). The header row is built the same way,
    so month names are read from the header's nested table.
    """
    if isinstance(text, str):
        text = text.encode('utf-8')
    root = lxml_html.document_fromstring(text)

    labrank = root.find(".//span[@id='LabRank']")
    if labrank is None:
        raise ValueError('Could not find ranking table on the page.')
    outer = labrank.find('table')
    if outer is None:
        raise ValueError('Could not find ranking table on the page.')
    rows = outer.xpath('./tr | ./tbody/tr')
    if not rows:
        raise ValueError('Could not find ranking table on the page.')

    def nested_cells(row) -> list:
        cells = _child_cells(row)
        if len(cells) < 2:
            return []
        nested = cells[1].find('.//table')
        return [] if nested is None else list(nested.iter('td'))

    months = [_text(td).replace('.', '') for td in nested_cells(rows[0])]
    months = [m for m in months if m not in ('', ' ')]

    data: List[Dict] = []
    for row in rows[1:]:
        cells = _child_cells(row)
        if len(cells) < 2:
            continue
        year = _text(cells[0])
        if not year.isdigit():
            continue
        year = int(year)
        for i, cell in enumerate(nested_cells(row)[:len(months)]):
            cell_text = _text(cell)
            rank = points = None
            if cell_text and cell_text != '-':
                match = _CELL_RE.match(cell_text)
                if match:
                    rank = int(match.group(1))
                    points = int(match.group(2)) if match.group(2) else None
            data.append({'year': year, 'month': months[i], 'rank': rank, 'points': points})
    return pd.DataFrame(data)


def scrape_player_ranking(player_url: str, session: Optional[requests.Session] = None) -> pd.DataFrame:
    """
    Scrape the ranking history of a player from the given URL and return as a DataFrame.
    Columns: ['year', 'month', 'rank', 'points']
    """
    session = session or utils.get_thread_session()
    response = utils.get_with_status(session, player_url, timeout=RANKING_PAGE_TIMEOUT)
    return parse_player_ranking(response.content)


def _fetch_one(ranking_id) -> Optional[pd.DataFrame]:
    try:
        df = scrape_player_ranking(ranking_url(ranking_id))
    except requests.RequestException as e:
        logging.warning(f"Request failed for ranking ID {ranking_id}: {e}")
        return None
    except ValueError as e:
        logging.warning(f"No ranking table for ranking ID {ranking_id}: {e}")
        return None
    df.insert(0, 'RankingID', int(ranking_id))
    return df


def scrape_player_rankings(
    ranking_ids: Iterable,
    max_workers: int = DEFAULT_WORKERS,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Scrape the ranking history of many players in parallel.

    Returns the per-player frames of ``scrape_player_ranking`` stacked, with a
    leading ``RankingID`` column. Players whose page fails are skipped.
    """
    ids = sorted({int(rid) for rid in ranking_ids})
    frames = []
    with tqdm(total=len(ids), desc="Ranking history", unit="player", disable=not show_progress) as bar:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = [executor.submit(_fetch_one, rid) for rid in ids]
            for future in as_completed(futures):
                df = future.result()
                if df is not None and not df.empty:
                    frames.append(df)
                bar.update(1)
    if not frames:
        return pd.DataFrame(columns=['RankingID'] + RANKING_COLUMNS)
    return pd.concat(frames, ignore_index=True).sort_values(['RankingID', 'year'], kind='stable', ignore_index=True)


def ranking_ids_from_players(players_file: Path = DATA_DIR / "players_data.csv") -> List[int]:
    players = pd.read_csv(players_file, usecols=['RankingID'])
    return sorted(players['RankingID'].dropna().astype(int).unique().tolist())


def main():
    utils.setup_logging()
    parser = argparse.ArgumentParser(description="Scrape ITHF world ranking history for players.")
    parser.add_argument("ranking_ids", nargs="*", type=int, help="Ranking IDs to scrape (default: all in players_data.csv).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers for ranking pages.")
    parser.add_argument("--output", type=Path, default=None, help="Write the stacked history to this .parquet or .csv file.")
    args = parser.parse_args()

    ids = args.ranking_ids or ranking_ids_from_players()
    df = scrape_player_rankings(ids, max_workers=args.workers)
    if args.output:
        if args.output.suffix == '.parquet':
            df.to_parquet(args.output, index=False)
        else:
            df.to_csv(args.output, index=False)
        logging.info(f"Saved {len(df)} rows for {df['RankingID'].nunique()} players to {args.output}")
    else:
        print(df)


if __name__ == "__main__":
    main()
