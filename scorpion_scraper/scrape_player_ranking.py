import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


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


if __name__ == "__main__":
    # Example usage
    url = "https://stiga.trefik.cz/ithf/ranking/rankpl.aspx?pl=655257"
    df = scrape_player_ranking(url)
    print(df.head(20))
    print(df.tail(20))
