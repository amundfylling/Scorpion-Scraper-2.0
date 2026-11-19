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
python3 scorpion_scraper/scrape_matches.py
python scorpion_scraper/scrape_players.py
python scorpion_scraper/scrape_tournament_urls.py
```

Useful options:

```bash
python scorpion_scraper/scrape_tournament_urls.py --pages 10 --workers 8
python scorpion_scraper/scrape_matches.py --workers 12 --refresh-recent-days 30
python scorpion_scraper/scrape_matches.py --recheck-empty
python scorpion_scraper/scrape_players.py --workers 5 --refresh-existing
python scorpion_scraper/scrape_player_ranking.py 655257          # one player's ranking history
python scorpion_scraper/scrape_player_ranking.py --output rankings.parquet  # every RankingID in players_data.csv
```

`scrape_player_ranking.scrape_player_ranking(url)` returns one player's ITHF
ranking history as `year`, `month`, `rank`, `points`;
`scrape_player_rankings(ids)` fetches many players in parallel and adds a
`RankingID` column.

Incremental match scraping re-scrapes tournaments from the last 14 days by
default, then replaces those old tournament rows. This catches late score
updates and tournaments that were scraped before all stages were complete.
Tournaments that were not finished when they were last scraped are also
re-scraped for 60 days (`--refresh-unfinished-days`). For any tournament that is
not marked `Finished`, the live tournament page is read instead of the cached
stage list, so stages added later (such as playoffs) and moved dates are picked
up, and the metadata and stage caches are updated.
Finished tournaments that have never produced player matches (mostly team
tournaments that only publish aggregate scores) are skipped after a week; pass
`--recheck-empty` to scrape them anyway.
Team tournaments are included when SportScorpion exposes player-level games;
the aggregate team score is skipped and only the individual games are saved.
Run `scrape_tournament_urls.py --full --refresh-existing` before a full match
scrape to refresh tournament metadata and stage caches used for faster scraping.

## Round robin standings audit

`scrape_rr_tables.py` scrapes the official standings table ("Detailed tables")
of every round robin stage, and `audit_rr_standings.py` compares it with the
games in `scraped_matches.parquet` to catch missing games, wrong scores and
parsing errors.

```bash
python scorpion_scraper/scrape_rr_tables.py              # incremental: new, recent and newly finished stages
python scorpion_scraper/scrape_rr_tables.py --full       # every stage again
python scorpion_scraper/scrape_rr_tables.py --stage 4696 # one stage
python scorpion_scraper/audit_rr_standings.py            # writes data/rr_audit_mismatches.csv
```

The scraper writes `data/rr_standings.parquet` (one row per stage, group and
player or team), `data/rr_table_games.parquet` (every game in the cross tables,
once per pair) and `data/rr_table_stages.csv` (which stages were scraped and
whether they had a table). Besides the round robin stages in the dataset it
also checks cached stages that have no games at all.

Later stages often include games carried over from an earlier stage, marked
"Transferred result" on the site. The audit counts those against the stage they
were played in, and checks that they exist there. Each row of
`rr_audit_mismatches.csv` is one problem:

| Check | Meaning |
| --- | --- |
| `stage_missing` | The official table has games; the dataset has none for the stage |
| `game_missing` / `game_extra` | A game is only in the official table / only in the dataset |
| `score_mismatch` | Same two players, different score |
| `score_reversed` | Same two players, mirrored score (the site shows some walkovers the other way round on the two pages) |
| `player_id_differs` | The games are stored under a different player ID than the table lists (duplicate accounts on the site) |
| `player_not_in_table` | A player has games in the dataset but is not in the table |
| `totals_mismatch` | G/W/D/L/goals computed from the dataset differ from the official row |
| `points_mismatch` | Totals agree, but points don't follow the stage's points rule (inferred per stage, e.g. 2-1-0 or 3-1-0); usually a penalty or bonus |
| `transferred_not_found` | A transferred game is not in any round robin stage of the tournament |
| `team_match_missing` / `team_match_extra` / `team_stage_missing` | Team stages: a team match in the table has no player games in the dataset, or the reverse |

Both scripts also run at the end of `nightly_scrape.py`.

To execute the scrapers in the required order (tournaments → matches → players),
run the helper script:

```bash
python scorpion_scraper/nightly_scrape.py
```

This script can be scheduled to run automatically each night using `cron`:

```
0 2 * * * /usr/bin/python /path/to/Scorpion-Scraper-2.0/scorpion_scraper/nightly_scrape.py >> /path/to/nightly.log 2>&1
```

## Data Output

- `data/tournament_data.csv` keeps the compatibility tournament catalog with
  `ID`, `Name`, and `Type`.
- `data/tournament_metadata.csv` stores expanded tournament metadata as CSV.
- `data/tournament_stages.csv` stores tournament stage URLs so match scraping
  can skip tournament detail pages when the cache is populated.
- `data/scraped_matches.parquet` stores played matches. Existing columns are
  preserved; newer runs add nullable match IDs, walkover flags, tournament type,
  and team context columns.
- `data/players_data.csv` stores player profile data discovered from match rows.

## License

MIT License
