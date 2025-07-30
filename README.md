# Scorpion Scraper 2.0

Scorpion Scraper 2.0 is a Python-based web scraping tool designed to collect and process data related to players, matches, and tournaments for Table Hockey. The project scrapes data from relevant web sources and stores it in CSV and Parquet formats for further analysis.

## Features
- Scrapes player, match, and tournament data
- Stores data in CSV and Parquet formats
- Utilizes multithreading for efficient scraping
- Progress bars for long-running tasks

## Requirements
- Python 3.7+
- See `requirements.txt` for Python dependencies

## Installation
1. Clone this repository:
   ```bash
   git clone <repo-url>
   cd "Scorpion Scraper 2.0"
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the desired scraper script from the `scorpion_scraper` directory. For example:

```bash
python scorpion_scraper/scrape_matches.py
python scorpion_scraper/scrape_players.py
python scorpion_scraper/scrape_tournament_urls.py
```

## Data Output
- Scraped data is saved in the `data/` directory as CSV or Parquet files.

## License
MIT License 