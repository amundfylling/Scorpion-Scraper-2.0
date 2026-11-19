"""
Compare official round robin standings with the games in scraped_matches.parquet.

Reads the files written by ``scrape_rr_tables.py`` and reports every
disagreement in ``data/rr_audit_mismatches.csv`` (one row per problem), with a
count per check printed at the end.

Checks for individual stages:

* ``stage_missing``      - the official table has games, the dataset has none
* ``game_missing``       - a game in the official cross table is not in the dataset
* ``game_extra``         - a dataset game is not in the official cross table
* ``score_mismatch``     - same pair, different score
* ``player_not_in_table``- a dataset player is not listed in the stage table
* ``totals_mismatch``    - G/W/D/L/GF/GA from dataset games (plus transferred
  games) differ from the official row
* ``points_mismatch``    - totals agree but points don't follow the stage's
  points rule (usually a penalty or bonus in the official table)
* ``transferred_not_found`` - a game marked "Transferred result" is not in any
  other stage of the same tournament in the dataset

Team stages are compared per team match (``team_match_missing`` /
``team_match_extra``), since the dataset only keeps the individual games.
"""
import argparse
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from . import utils
except ImportError:
    import utils

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BASE_URL = "https://th.sportscorpion.com"
MATCHES_FILE = DATA_DIR / "scraped_matches.parquet"
STANDINGS_FILE = DATA_DIR / "rr_standings.parquet"
TABLE_GAMES_FILE = DATA_DIR / "rr_table_games.parquet"
TABLE_STAGES_FILE = DATA_DIR / "rr_table_stages.csv"
OUTPUT_FILE = DATA_DIR / "rr_audit_mismatches.csv"

OUTPUT_COLUMNS = [
    "Check", "StageID", "TournamentID", "TournamentName", "Date", "Group",
    "EntryID", "Name", "OpponentID", "Opponent", "Official", "Dataset", "Detail", "URL",
]
TOTAL_FIELDS = ["G", "W", "D", "L", "GF", "GA"]

# (win, draw, loss) points; earlier entries win ties when several rules fit.
POINT_RULES = [(2, 1, 0), (3, 1, 0), (1, 0, 0), (3, 2, 1), (2, 1, 1), (1, 1, 0), (4, 2, 0), (3, 0, 0), (4, 1, 0), (2, 0, 0)]


def _key(a: Any, b: Any, ga: int, gb: int) -> Tuple[int, int, int, int]:
    """Orientation-free game key: (low id, high id, goals low, goals high)."""
    a, b, ga, gb = int(a), int(b), int(ga), int(gb)
    return (a, b, ga, gb) if a <= b else (b, a, gb, ga)


def infer_point_rule(rows: pd.DataFrame) -> Optional[Tuple[int, int, int]]:
    """The points rule that explains the most official rows of a stage."""
    rows = rows.dropna(subset=["W", "D", "L", "Points"])
    rows = rows[rows["G"] > 0]
    if rows.empty:
        return None
    best, best_hits = None, -1
    for rule in POINT_RULES:
        pw, pd_, pl = rule
        hits = int(((rows["W"] * pw + rows["D"] * pd_ + rows["L"] * pl) == rows["Points"]).sum())
        if hits > best_hits:
            best, best_hits = rule, hits
    return best


def player_totals(games: Iterable[Tuple[int, int, int, int]]) -> Dict[int, Counter]:
    """Per-player G/W/D/L/GF/GA from (id1, id2, goals1, goals2) tuples."""
    totals: Dict[int, Counter] = defaultdict(Counter)
    for p1, p2, g1, g2 in games:
        for player, gf, ga in ((p1, g1, g2), (p2, g2, g1)):
            t = totals[int(player)]
            t["G"] += 1
            t["W"] += gf > ga
            t["D"] += gf == ga
            t["L"] += gf < ga
            t["GF"] += gf
            t["GA"] += ga
    return totals


def diff_games(official: List[Tuple[int, int, int, int]], dataset: List[Tuple[int, int, int, int]]):
    """
    Multiset difference of games keyed per pair. Returns
    (missing_from_dataset, extra_in_dataset, score_mismatches) where a score
    mismatch is a (official_key, dataset_key) pair for the same two players.
    """
    official_left = Counter(official)
    dataset_left = Counter(dataset)
    common = official_left & dataset_left
    official_left -= common
    dataset_left -= common

    by_pair_off: Dict[Tuple[int, int], List] = defaultdict(list)
    by_pair_data: Dict[Tuple[int, int], List] = defaultdict(list)
    for key, n in official_left.items():
        by_pair_off[key[:2]].extend([key] * n)
    for key, n in dataset_left.items():
        by_pair_data[key[:2]].extend([key] * n)

    missing, extra, mismatched = [], [], []
    for pair in set(by_pair_off) | set(by_pair_data):
        off, data = sorted(by_pair_off.get(pair, [])), sorted(by_pair_data.get(pair, []))
        n = min(len(off), len(data))
        mismatched.extend(zip(off[:n], data[:n]))
        missing.extend(off[n:])
        extra.extend(data[n:])
    return missing, extra, mismatched


def _score(key: Tuple[int, int, int, int]) -> str:
    return f"{key[2]}:{key[3]}"


def audit(
    matches: pd.DataFrame,
    standings: pd.DataFrame,
    table_games: pd.DataFrame,
    table_stages: pd.DataFrame,
) -> pd.DataFrame:
    issues: List[Dict[str, Any]] = []
    stage_info = (
        matches.groupby("StageID")[["TournamentID", "TournamentName", "Date"]].first()
        if not matches.empty else pd.DataFrame(columns=["TournamentID", "TournamentName", "Date"])
    )
    tournament_names = matches.groupby("TournamentID")[["TournamentName", "Date"]].first()

    def add(check: str, stage_id: int, tournament_id: Any, **fields) -> None:
        info = stage_info.loc[stage_id] if stage_id in stage_info.index else None
        if info is None and tournament_id in tournament_names.index:
            info = tournament_names.loc[tournament_id]
        issues.append({
            "Check": check,
            "StageID": stage_id,
            "TournamentID": tournament_id,
            "TournamentName": None if info is None else info["TournamentName"],
            "Date": None if info is None else info["Date"],
            "URL": f"{BASE_URL}/eng/tournament/stage/{stage_id}/table/",
            **fields,
        })

    # Dataset games per stage and per tournament.
    stage_games: Dict[int, List[Tuple[int, int, int, int]]] = defaultdict(list)
    tournament_games: Dict[int, Dict[Tuple[int, int, int, int], set]] = defaultdict(lambda: defaultdict(set))
    stage_team_matches: Dict[int, set] = defaultdict(set)
    valid = matches.dropna(subset=["Player1ID", "Player2ID"])
    for sid, tid, p1, p2, g1, g2, tm, stage in zip(
        valid["StageID"], valid["TournamentID"], valid["Player1ID"], valid["Player2ID"],
        valid["GoalsPlayer1"], valid["GoalsPlayer2"], valid["TeamMatchID"], valid["Stage"],
    ):
        key = _key(p1, p2, g1, g2)
        stage_games[int(sid)].append(key)
        if stage == "Round-Robin":
            # Results are only transferred between round robin stages.
            tournament_games[int(tid)][key].add(int(sid))
        if pd.notna(tm):
            stage_team_matches[int(sid)].add(int(tm))
    stage_names = {
        int(sid): dict(zip(group["Player1ID"].astype(int), group["Player1"])) | dict(zip(group["Player2ID"].astype(int), group["Player2"]))
        for sid, group in valid.groupby("StageID")
    }

    games_by_stage = {int(sid): group for sid, group in table_games.groupby("StageID")}
    standings_by_stage = {int(sid): group for sid, group in standings.groupby("StageID")}
    has_table = table_stages[table_stages["HasTable"].astype(bool)]

    for sid, tid in zip(has_table["StageID"].astype(int), has_table["TournamentID"]):
        tid = int(tid) if pd.notna(tid) else None
        rows = standings_by_stage.get(sid, pd.DataFrame(columns=standings.columns))
        tgames = games_by_stage.get(sid, pd.DataFrame(columns=table_games.columns))
        entry_type = rows["EntryType"].iloc[0] if not rows.empty else "player"
        own = tgames[~tgames["Transferred"].astype(bool)] if not tgames.empty else tgames
        moved = tgames[tgames["Transferred"].astype(bool)] if not tgames.empty else tgames
        group_of = dict(zip(rows["EntryID"], rows["Group"]))
        name_of = dict(zip(rows["EntryID"], rows["Name"])) | stage_names.get(sid, {})

        if entry_type == "team":
            official_ids = set(own["MatchID"].dropna().astype(int))
            dataset_ids = stage_team_matches.get(sid, set())
            if not dataset_ids:
                if official_ids:
                    add("team_stage_missing", sid, tid, Official=len(official_ids), Dataset=0,
                        Detail="no player games for any team match in this stage")
                continue
            for _, game in own[own["MatchID"].notna()].iterrows():
                if int(game["MatchID"]) not in dataset_ids:
                    add("team_match_missing", sid, tid, Group=group_of.get(game["Entry1ID"]),
                        EntryID=game["Entry1ID"], Name=game["Entry1"], OpponentID=game["Entry2ID"], Opponent=game["Entry2"],
                        Official=f"{game['Goals1']}:{game['Goals2']}", Detail=f"team match {int(game['MatchID'])}")
            for match_id in sorted(dataset_ids - official_ids):
                add("team_match_extra", sid, tid, Dataset=match_id, Detail=f"team match {match_id} not in the official table")
            continue

        dataset = stage_games.get(sid, [])
        linked = own.dropna(subset=["Entry1ID", "Entry2ID"])
        official = [_key(a, b, x, y) for a, b, x, y in zip(linked["Entry1ID"], linked["Entry2ID"], linked["Goals1"], linked["Goals2"])]

        if official and not dataset:
            add("stage_missing", sid, tid, Official=len(official), Dataset=0, Detail="stage has no games in the dataset")
            continue

        # Transferred games normally live in an earlier stage of the same
        # tournament. Some matches pages list them in this stage instead; a
        # transferred game found only here is accounted for here.
        remaining = Counter(dataset)
        moved_elsewhere = []
        for a, b, x, y in zip(moved["Entry1ID"], moved["Entry2ID"], moved["Goals1"], moved["Goals2"]):
            if pd.isna(a) or pd.isna(b):
                continue
            key = _key(a, b, x, y)
            in_other_stage = tid is not None and bool(tournament_games[tid].get(key, set()) - {sid})
            if not in_other_stage and remaining[key] > 0:
                remaining[key] -= 1
                continue
            moved_elsewhere.append(key)
            if not in_other_stage:
                add("transferred_not_found", sid, tid, Group=group_of.get(key[0]), EntryID=key[0], Name=name_of.get(key[0]),
                    OpponentID=key[1], Opponent=name_of.get(key[1]), Official=_score(key),
                    Detail="transferred result not found in this or another stage of this tournament")

        missing, extra, mismatched = diff_games(official, list(remaining.elements()))
        for key in missing:
            add("game_missing", sid, tid, Group=group_of.get(key[0]), EntryID=key[0], Name=name_of.get(key[0]),
                OpponentID=key[1], Opponent=name_of.get(key[1]), Official=_score(key))
        for key in extra:
            add("game_extra", sid, tid, Group=group_of.get(key[0]), EntryID=key[0], Name=name_of.get(key[0]),
                OpponentID=key[1], Opponent=name_of.get(key[1]), Dataset=_score(key),
                Detail="same player on both sides" if key[0] == key[1] else None)
        for off_key, data_key in mismatched:
            add("score_mismatch", sid, tid, Group=group_of.get(off_key[0]), EntryID=off_key[0], Name=name_of.get(off_key[0]),
                OpponentID=off_key[1], Opponent=name_of.get(off_key[1]), Official=_score(off_key), Dataset=_score(data_key))

        # Player totals: dataset games of this stage + transferred games kept elsewhere.
        totals = player_totals(dataset + moved_elsewhere)
        rule = infer_point_rule(rows)
        listed = set(rows["EntryID"].dropna().astype(int))
        for player in sorted(set(totals) - listed):
            add("player_not_in_table", sid, tid, EntryID=player, Name=name_of.get(player), Dataset=totals[player]["G"],
                Detail="player has games in the dataset but is not in the official table")
        for _, row in rows.iterrows():
            if pd.isna(row["EntryID"]):
                continue
            pid = int(row["EntryID"])
            got = totals.get(pid, Counter())
            official_totals = {f: (None if pd.isna(row[f]) else int(row[f])) for f in TOTAL_FIELDS}
            dataset_totals = {f: int(got[f]) for f in TOTAL_FIELDS}
            if official_totals != dataset_totals:
                fields = [f for f in TOTAL_FIELDS if official_totals[f] != dataset_totals[f]]
                add("totals_mismatch", sid, tid, Group=row["Group"], EntryID=pid, Name=row["Name"],
                    Official=" ".join(f"{f}={official_totals[f]}" for f in fields),
                    Dataset=" ".join(f"{f}={dataset_totals[f]}" for f in fields))
            elif rule is not None and pd.notna(row["Points"]):
                expected = got["W"] * rule[0] + got["D"] * rule[1] + got["L"] * rule[2]
                if expected != int(row["Points"]):
                    add("points_mismatch", sid, tid, Group=row["Group"], EntryID=pid, Name=row["Name"],
                        Official=int(row["Points"]), Dataset=expected,
                        Detail=f"points rule {rule[0]}-{rule[1]}-{rule[2]}")

    return pd.DataFrame(issues, columns=OUTPUT_COLUMNS)


def summarize(issues: pd.DataFrame) -> pd.DataFrame:
    if issues.empty:
        return pd.DataFrame(columns=["Check", "Rows", "Stages", "Tournaments"])
    return (
        issues.groupby("Check")
        .agg(Rows=("Check", "size"), Stages=("StageID", "nunique"), Tournaments=("TournamentID", "nunique"))
        .reset_index()
        .sort_values("Rows", ascending=False)
    )


def main(output: Path = OUTPUT_FILE) -> pd.DataFrame:
    matches = pd.read_parquet(MATCHES_FILE)
    standings = pd.read_parquet(STANDINGS_FILE)
    table_games = pd.read_parquet(TABLE_GAMES_FILE)
    table_stages = pd.read_csv(TABLE_STAGES_FILE)
    issues = audit(matches, standings, table_games, table_stages)
    issues.sort_values(["Date", "StageID", "Check"], na_position="last").to_csv(output, index=False)
    checked = int(table_stages["HasTable"].astype(bool).sum())
    logging.info("Checked %s stages with an official table; %s issues written to %s.", checked, len(issues), output)
    summary = summarize(issues)
    if not summary.empty:
        logging.info("Issues per check:\n%s", summary.to_string(index=False))
    return issues


if __name__ == "__main__":
    utils.setup_logging()
    parser = argparse.ArgumentParser(description="Audit round robin standings against scraped games.")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE, help="CSV file for the mismatches.")
    args = parser.parse_args()
    main(args.output)
