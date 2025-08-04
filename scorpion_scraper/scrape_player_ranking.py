import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from tqdm import tqdm


def scrape_player_ranking(player_url: str) -> pd.DataFrame:
    """
    Scrape the ranking history of a player from the given URL and return as a DataFrame.
    Columns: ['year', 'month', 'rank', 'points']
    """
    response = requests.get(player_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the ranking table inside the span with id 'LabRank'
    labrank_span = soup.find('span', id='LabRank')
    if not labrank_span:
        raise ValueError('Could not find ranking table on the page.')

    # The first row contains month names (skip the first <td> which is empty)
    month_row = labrank_span.find('tr')
    month_tds = month_row.find_all('td')
    months = [td.get_text(strip=True).replace('.', '') for td in month_tds[1:]]  # skip first td
    months = [m for m in months if m not in ('', ' ')]

    # All subsequent rows are years with their ranking data
    data = []
    for year_row in labrank_span.find_all('tr')[1:]:
        tds = year_row.find_all('td', recursive=False)
        if len(tds) < 2:
            continue
        year = tds[0].get_text(strip=True)
        if not year.isdigit():
            continue
        year = int(year)
        # The second <td> contains the nested table with monthly data
        nested_table = tds[1].find('table')
        if not nested_table:
            continue
        month_cells = nested_table.find_all('td')
        for i, cell in enumerate(month_cells):
            if i >= len(months):
                break  # Only process as many months as exist in the header
            cell_text = cell.get_text(separator=' ', strip=True)
            if cell_text == '-' or not cell_text:
                rank, points = None, None
            else:
                # Try to extract rank and points (rank is before the dot, points after the dot)
                match = re.match(r'(\d+)\.(?:\s*([\d]+))?', cell_text)
                if match:
                    rank = int(match.group(1))
                    points = int(match.group(2)) if match.group(2) else None
                else:
                    rank, points = None, None
            data.append({
                'year': year,
                'month': months[i],
                'rank': rank,
                'points': points
            })
    df = pd.DataFrame(data)
    return df


def scrape_player_ranking_by_id(ranking_id: int) -> pd.DataFrame:
    """Scrape ranking history for a player given their ranking ID.

    Returns a DataFrame with columns ['RankingID', 'Date', 'Rank', 'Points'].
    """
    url = f"https://stiga.trefik.cz/ithf/ranking/rankpl.aspx?pl={ranking_id}"
    df = scrape_player_ranking(url)
    df["RankingID"] = int(ranking_id)
    df["Date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"],
        format="%Y-%b",
        errors="coerce",
    )
    return df[["RankingID", "Date", "rank", "points"]].rename(
        columns={"rank": "Rank", "points": "Points"}
    )


def scrape_rankings_for_players(players_csv: str, output_csv: str) -> pd.DataFrame:
    """Scrape ranking histories for all players listed in ``players_csv``.

    The input CSV must contain a ``RankingID`` column. The combined ranking
    history is written to ``output_csv`` and also returned as a DataFrame.
    """
    players = pd.read_csv(players_csv)
    ranking_ids = (
        players["RankingID"].dropna().astype(float).astype(int).unique().tolist()
    )

    all_rankings = []
    for rid in tqdm(ranking_ids, desc="Scraping player rankings"):
        try:
            player_df = scrape_player_ranking_by_id(rid)
            all_rankings.append(player_df)
        except Exception as exc:  # noqa: BLE001 - we log and continue
            print(f"Failed to scrape ranking {rid}: {exc}")

    if all_rankings:
        rankings_df = pd.concat(all_rankings, ignore_index=True)
    else:
        rankings_df = pd.DataFrame(columns=["RankingID", "Date", "Rank", "Points"])
    rankings_df.to_csv(output_csv, index=False)
    return rankings_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape player ranking histories and save to CSV."
    )
    parser.add_argument(
        "--players-csv",
        default="data/players_data.csv",
        help="Input CSV containing player information including a RankingID column.",
    )
    parser.add_argument(
        "--output-csv",
        default="data/player_ranking_history.csv",
        help="Destination CSV file for combined ranking history.",
    )
    args = parser.parse_args()

    scrape_rankings_for_players(args.players_csv, args.output_csv)
