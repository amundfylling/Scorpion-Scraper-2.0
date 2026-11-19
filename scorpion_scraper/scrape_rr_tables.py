"""
Scrape the official standings table of every round robin stage.

For each stage this reads the "Detailed tables" page
(``/eng/tournament/stage/<StageID>/table/?print``). That page holds, per group,
one row per player (or team) with G/W/D/L/goals/points/place, plus a cross
table with every game score between each pair. Games carried over from an
earlier stage are marked "Transferred result"; they are part of the official
totals but are not listed on the stage's own matches page.

Outputs (in ``data/``):

* ``rr_standings.parquet``  - one row per stage, group and entry
* ``rr_table_games.parquet`` - one row per game in the cross tables, stored once
  per pair (row entry vs. column entry, row number < column number)
* ``rr_table_stages.csv``    - one row per scraped stage, also for stages that
  have no table, so incremental runs know what is done

These are compared with ``scraped_matches.parquet`` by ``audit_rr_standings.py``.
"""
import argparse
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    from . import utils
    from . import tournament_metadata
except ImportError:
    import utils
    import tournament_metadata

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BASE_URL = "https://th.sportscorpion.com"
MATCHES_FILE = DATA_DIR / "scraped_matches.parquet"
METADATA_FILE = DATA_DIR / "tournament_metadata.csv"
STAGE_FILE = DATA_DIR / "tournament_stages.csv"
STANDINGS_FILE = DATA_DIR / "rr_standings.parquet"
TABLE_GAMES_FILE = DATA_DIR / "rr_table_games.parquet"
TABLE_STAGES_FILE = DATA_DIR / "rr_table_stages.csv"

FINAL_STATUS = "Finished"

USER_ID_RE = re.compile(r"/user/id/(\d+)/")
TEAM_ID_RE = re.compile(r"/team/id/(\d+)/")
MATCH_ID_RE = re.compile(r"/match/id/(\d+)/")
SCORE_RE = re.compile(r"(\d+)\s*:\s*(\d+)")
GOALS_RE = re.compile(r"(\d+)\s*-\s*(\d+)")

STANDINGS_COLUMNS = [
    "StageID", "TournamentID", "GroupIndex", "Group", "RowNumber", "Place",
    "EntryType", "EntryID", "Name", "G", "W", "D", "L", "GF", "GA", "Points",
]
TABLE_GAME_COLUMNS = [
    "StageID", "TournamentID", "GroupIndex", "EntryType",
    "Entry1ID", "Entry1", "Entry2ID", "Entry2", "Meeting",
    "Goals1", "Goals2", "Transferred", "Walkover", "Overtime", "MatchID",
]
TABLE_STAGE_COLUMNS = ["StageID", "TournamentID", "HasTable", "Groups", "Entries", "TournamentStatus", "ScrapedAt"]


def table_url(stage_id: Any) -> str:
    return f"{BASE_URL}/eng/tournament/stage/{stage_id}/table/?print"


def _int(text: Optional[str]) -> Optional[int]:
    text = (text or "").strip()
    return int(text) if text.lstrip("-").isdigit() else None


def _text(el) -> str:
    return el.get_text(" ", strip=True).replace("\xa0", " ")


def _entry(name_cell) -> Tuple[str, Optional[int], str]:
    """(entry type, id, name) for a table name cell."""
    name = _text(name_cell)
    link = name_cell.find("a", href=True)
    if link is not None:
        href = link["href"]
        for entry_type, pattern in (("player", USER_ID_RE), ("team", TEAM_ID_RE)):
            match = pattern.search(href)
            if match:
                return entry_type, int(match.group(1)), name
    return "player", None, name


def _game_divs(cell) -> List[Any]:
    # One div per meeting. Walkover cells nest a "(W.O)" div inside the game
    # div, so only direct children count.
    return [div for div in cell.find_all("div", recursive=False)]


def parse_game_div(div) -> Optional[Dict[str, Any]]:
    match = SCORE_RE.search(_text(div))
    if not match:
        return None
    classes = div.get("class") or []
    link = div.find("a", href=MATCH_ID_RE)
    return {
        "Goals1": int(match.group(1)),
        "Goals2": int(match.group(2)),
        "Transferred": div.find(class_="moved") is not None,
        "Walkover": "techdef-cell" in classes or div.find(class_="techdef") is not None,
        "Overtime": "(OT)" in _text(div),
        "MatchID": int(MATCH_ID_RE.search(link["href"]).group(1)) if link is not None else None,
    }


def _group_name(table) -> Optional[str]:
    subheader = table.find_previous(class_="subheader")
    return _text(subheader) if subheader is not None else None


def parse_stage_table(html: str, stage_id: int, tournament_id: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parse a "Detailed tables" page into (standings rows, table game rows).
    A page without a table (playoff stage, stage not started) returns ([], []).
    """
    soup = BeautifulSoup(html, "lxml")
    standings: List[Dict[str, Any]] = []
    games: List[Dict[str, Any]] = []

    for group_index, table in enumerate(soup.find_all("table", class_="resTable")):
        group = _group_name(table)
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [_text(th) for th in rows[0].find_all("th")]
        # Totals columns sit after the cross table columns, so locate them by name.
        try:
            g_col = headers.index("G")
        except ValueError:
            logging.warning("Stage %s: table without a G column, skipped.", stage_id)
            continue

        entries = []
        for tr in rows[1:]:
            cells = tr.find_all("td")
            name_cell = tr.find("td", class_="name")
            if name_cell is None or len(cells) != len(headers):
                continue
            entry_type, entry_id, name = _entry(name_cell)
            values = {header: _text(cell) for header, cell in zip(headers[g_col:], cells[g_col:])}
            goals = GOALS_RE.search(values.get("Gd", ""))
            entry = {
                "StageID": stage_id,
                "TournamentID": tournament_id,
                "GroupIndex": group_index,
                "Group": group,
                "RowNumber": _int(_text(cells[0])),
                "Place": _int(values.get("Place")),
                "EntryType": entry_type,
                "EntryID": entry_id,
                "Name": name,
                "G": _int(values.get("G")),
                "W": _int(values.get("W")),
                "D": _int(values.get("D")),
                "L": _int(values.get("L")),
                "GF": int(goals.group(1)) if goals else None,
                "GA": int(goals.group(2)) if goals else None,
                "Points": _int(values.get("Points")),
            }
            standings.append(entry)
            # Cross table cells: columns 2 .. g_col-1, one per entry in row order.
            entries.append((entry, cells[2:g_col]))

        for row_pos, (entry, cross_cells) in enumerate(entries):
            for col_pos, cell in enumerate(cross_cells):
                # Each game is shown from both sides; keep the upper triangle.
                if col_pos <= row_pos or col_pos >= len(entries):
                    continue
                opponent = entries[col_pos][0]
                for meeting, div in enumerate(_game_divs(cell), start=1):
                    game = parse_game_div(div)
                    if game is None:
                        continue
                    games.append({
                        "StageID": stage_id,
                        "TournamentID": tournament_id,
                        "GroupIndex": group_index,
                        "EntryType": entry["EntryType"],
                        "Entry1ID": entry["EntryID"],
                        "Entry1": entry["Name"],
                        "Entry2ID": opponent["EntryID"],
                        "Entry2": opponent["Name"],
                        "Meeting": meeting,
                        **game,
                    })
    return standings, games


def scrape_stage(stage_id: int, tournament_id: Optional[int]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    response = utils.get_with_status(utils.get_thread_session(), table_url(stage_id), timeout=60)
    return parse_stage_table(response.text, stage_id, tournament_id)


# ---------------------------------------------------------------------------
# Choosing stages
# ---------------------------------------------------------------------------

def candidate_stages(matches: pd.DataFrame, stage_cache: pd.DataFrame) -> pd.DataFrame:
    """
    Stages worth checking: every round robin stage with games in the dataset,
    plus every cached stage with no games at all (to catch stages whose games
    are missing entirely). Returns StageID, TournamentID.
    """
    rr = matches.loc[matches["Stage"] == "Round-Robin", ["StageID", "TournamentID"]].drop_duplicates("StageID")
    known = set(matches["StageID"].dropna().astype(int))
    cache = stage_cache[["StageID", "TournamentID"]].dropna().astype(int)
    without_games = cache[~cache["StageID"].isin(known)].drop_duplicates("StageID")
    out = pd.concat([rr, without_games], ignore_index=True).drop_duplicates("StageID")
    return out.astype({"StageID": int, "TournamentID": int}).reset_index(drop=True)


def stages_to_refresh(
    candidates: pd.DataFrame,
    done: pd.DataFrame,
    metadata: Dict[str, Dict[str, str]],
    recent_days: int,
    today: Optional[pd.Timestamp] = None,
) -> Set[int]:
    """
    Stage IDs to (re)scrape: never scraped, from a tournament dated within
    ``recent_days``, or scraped before its tournament was finished.
    """
    today = today if today is not None else pd.Timestamp.today().normalize()
    cutoff = today - pd.Timedelta(days=recent_days)
    done_status = {} if done.empty else dict(zip(done["StageID"].astype(int), done["TournamentStatus"].fillna("")))
    todo = set()
    for stage_id, tournament_id in zip(candidates["StageID"], candidates["TournamentID"]):
        stage_id = int(stage_id)
        meta = metadata.get(str(tournament_id)) or {}
        status_now = (meta.get("Status") or "").strip()
        if stage_id not in done_status:
            todo.add(stage_id)
            continue
        date = pd.to_datetime(meta.get("Date"), errors="coerce")
        if pd.notna(date) and date >= cutoff:
            todo.add(stage_id)
        elif done_status[stage_id] != FINAL_STATUS and status_now == FINAL_STATUS:
            todo.add(stage_id)
    return todo


def _replace_stages(old: pd.DataFrame, new: pd.DataFrame, stage_ids: Iterable[int], columns: List[str]) -> pd.DataFrame:
    stage_ids = set(stage_ids)
    kept = old[~old["StageID"].isin(stage_ids)] if not old.empty else old
    frames = [frame for frame in (kept, new) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)[columns]


def _typed(df: pd.DataFrame, int_columns: List[str]) -> pd.DataFrame:
    for column in int_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    return df


def run(
    workers: int = 8,
    full: bool = False,
    recent_days: int = 14,
    limit: Optional[int] = None,
    stage_ids: Optional[List[int]] = None,
) -> None:
    matches = pd.read_parquet(MATCHES_FILE, columns=["StageID", "TournamentID", "Stage"])
    stage_cache = pd.read_csv(STAGE_FILE)
    metadata = {row["TournamentID"]: row for row in tournament_metadata.read_metadata_csv(METADATA_FILE)}

    candidates = candidate_stages(matches, stage_cache)
    standings_old = pd.DataFrame() if full or not STANDINGS_FILE.exists() else pd.read_parquet(STANDINGS_FILE)
    games_old = pd.DataFrame() if full or not TABLE_GAMES_FILE.exists() else pd.read_parquet(TABLE_GAMES_FILE)
    done_old = pd.DataFrame() if full or not TABLE_STAGES_FILE.exists() else pd.read_csv(TABLE_STAGES_FILE)

    if stage_ids:
        todo = set(stage_ids)
        lookup = dict(zip(candidates["StageID"], candidates["TournamentID"]))
        missing = todo - set(lookup)
        if missing:
            cache_lookup = dict(zip(stage_cache["StageID"], stage_cache["TournamentID"]))
            lookup.update({sid: cache_lookup.get(sid) for sid in missing})
        jobs = [(sid, lookup.get(sid)) for sid in sorted(todo)]
    else:
        todo = stages_to_refresh(candidates, done_old, metadata, recent_days)
        jobs = [(int(s), int(t)) for s, t in zip(candidates["StageID"], candidates["TournamentID"]) if int(s) in todo]
    if limit:
        jobs = jobs[:limit]
    logging.info("%s candidate stages; scraping %s.", len(candidates), len(jobs))
    if not jobs:
        return

    standings: List[Dict[str, Any]] = []
    games: List[Dict[str, Any]] = []
    done: List[Dict[str, Any]] = []
    failed = 0
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(scrape_stage, sid, tid): (sid, tid) for sid, tid in jobs}
        for future in tqdm(as_completed(futures), total=len(futures), desc="RR tables"):
            sid, tid = futures[future]
            try:
                stage_standings, stage_games = future.result()
            except Exception as exc:
                failed += 1
                logging.warning("Stage %s failed: %s", sid, exc)
                continue
            standings.extend(stage_standings)
            games.extend(stage_games)
            done.append({
                "StageID": sid,
                "TournamentID": tid,
                "HasTable": bool(stage_standings),
                "Groups": len({row["GroupIndex"] for row in stage_standings}),
                "Entries": len(stage_standings),
                "TournamentStatus": (metadata.get(str(tid)) or {}).get("Status", ""),
                "ScrapedAt": now,
            })
    if failed:
        logging.warning("%s stages failed and will be retried next run.", failed)

    scraped_ids = [row["StageID"] for row in done]
    standings_df = _replace_stages(standings_old, pd.DataFrame(standings, columns=STANDINGS_COLUMNS), scraped_ids, STANDINGS_COLUMNS)
    games_df = _replace_stages(games_old, pd.DataFrame(games, columns=TABLE_GAME_COLUMNS), scraped_ids, TABLE_GAME_COLUMNS)
    done_df = _replace_stages(done_old, pd.DataFrame(done, columns=TABLE_STAGE_COLUMNS), scraped_ids, TABLE_STAGE_COLUMNS)

    standings_df = _typed(standings_df, ["StageID", "TournamentID", "GroupIndex", "RowNumber", "Place", "EntryID", "G", "W", "D", "L", "GF", "GA", "Points"])
    games_df = _typed(games_df, ["StageID", "TournamentID", "GroupIndex", "Entry1ID", "Entry2ID", "Meeting", "Goals1", "Goals2", "MatchID"])
    for column in ("Transferred", "Walkover", "Overtime"):
        games_df[column] = games_df[column].astype(bool)
    standings_df.sort_values(["StageID", "GroupIndex", "RowNumber"]).to_parquet(STANDINGS_FILE, index=False)
    games_df.sort_values(["StageID", "GroupIndex", "Entry1ID", "Entry2ID", "Meeting"]).to_parquet(TABLE_GAMES_FILE, index=False)
    done_df.sort_values("StageID").to_csv(TABLE_STAGES_FILE, index=False)
    logging.info(
        "Saved %s standings rows and %s table games for %s stages (%s with a table).",
        len(standings_df), len(games_df), len(done_df), int(done_df["HasTable"].astype(bool).sum()),
    )


if __name__ == "__main__":
    utils.setup_logging()
    parser = argparse.ArgumentParser(description="Scrape official round robin standings tables.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel page downloads.")
    parser.add_argument("--full", action="store_true", help="Re-scrape every stage and overwrite the output files.")
    parser.add_argument("--refresh-recent-days", type=int, default=14, help="Re-scrape stages of tournaments dated within the last N days.")
    parser.add_argument("--limit", type=int, help="Scrape at most N stages (for testing).")
    parser.add_argument("--stage", type=int, action="append", dest="stage_ids", help="Scrape only this StageID (repeatable).")
    args = parser.parse_args()
    run(workers=args.workers, full=args.full, recent_days=args.refresh_recent_days, limit=args.limit, stage_ids=args.stage_ids)
