import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm.auto import tqdm

# Resolve paths relative to the project root so scripts work from any CWD
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Reference ranking ID (December 2024)
REFERENCE_ID = 417
REFERENCE_YEAR = 2024
REFERENCE_MONTH = 12


def get_latest_ranking_id(reference_id: int = REFERENCE_ID,
                          reference_year: int = REFERENCE_YEAR,
                          reference_month: int = REFERENCE_MONTH) -> int:
    """Calculate the latest ranking ID for the current month."""
    today = datetime.today()
    months_since_reference = ((today.year - reference_year) * 12 +
                              (today.month - reference_month))
    return reference_id + months_since_reference


def fetch_ranking(ranking_id: int) -> list:
    """Fetch ranking data for a specific ranking ID."""
    rankings = []
    url = f"https://stiga.trefik.cz/ithf/ranking/history.aspx?id={ranking_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table", {"border": "1"})
        if not table:
            return rankings

        date_header = soup.find("span", {"id": "LabTitle"}).text
        date_str = date_header.split("as on")[-1].strip()
        ranking_date = datetime.strptime(date_str, "%d.%m.%Y")

        rows = table.find_all("tr")[2:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) != 4:
                continue
            rank = cols[0].get_text(strip=True)
            player_name = cols[1].get_text(strip=True)
            link = cols[1].find("a")
            player_id = int(link["href"].split("=")[-1]) if link else None
            nation = cols[2].img["alt"] if cols[2].find("img") else None
            points_str = cols[3].get_text(strip=True).replace("\u00a0", "").replace(",", "")
            points = int(points_str) if points_str.isdigit() else None
            rankings.append({
                "Rank": int(rank.rstrip(".")),
                "PlayerName": player_name,
                "PlayerID": player_id,
                "Nation": nation,
                "Points": points,
                "Date": ranking_date,
            })
    except Exception as exc:
        print(f"Error fetching data for ID {ranking_id}: {exc}")
    return rankings


def extract_ranking_data_parallel(start_id: int, end_id: int,
                                  max_workers: int = 10) -> pd.DataFrame:
    """Extract ranking data in parallel for a range of IDs."""
    rankings = []
    ranking_ids = list(range(start_id, end_id + 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in tqdm(executor.map(fetch_ranking, ranking_ids),
                           total=len(ranking_ids)):
            rankings.extend(result)
    return pd.DataFrame(rankings)


def main():
    data_file = DATA_DIR / "ranking_history.parquet"

    latest_date = None
    if data_file.exists():
        existing_df = pd.read_parquet(data_file)
        existing_df["Date"] = pd.to_datetime(existing_df["Date"])
        latest_date = existing_df["Date"].max().date()

    if latest_date:
        months_since_reference = ((latest_date.year - REFERENCE_YEAR) * 12 +
                                  (latest_date.month - REFERENCE_MONTH))
        start_id = REFERENCE_ID + months_since_reference + 1
    else:
        start_id = REFERENCE_ID

    end_id = get_latest_ranking_id()
    print(f"Extracting from ID {start_id} to {end_id}")

    if start_id <= end_id:
        ranking_df = extract_ranking_data_parallel(start_id, end_id)
        if data_file.exists():
            combined_df = pd.concat([existing_df, ranking_df], ignore_index=True)
        else:
            combined_df = ranking_df
        combined_df.to_parquet(data_file, engine="pyarrow", compression="zstd")
    else:
        print("No new data to extract.")


if __name__ == "__main__":
    main()
