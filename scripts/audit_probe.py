"""
Temporary probe: read th.sportscorpion.com pages for the data-audit questions
and print what the markup shows. Run from GitHub Actions (the site is not
reachable from every network).
"""
import collections
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scorpion_scraper"))
import scrape_matches as sm  # noqa: E402
import utils  # noqa: E402

BASE = "https://th.sportscorpion.com"
DATA = Path(__file__).resolve().parent.parent / "data"
DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{2,4}")


import time
import requests

_SESSION = requests.Session()
_SESSION.headers.update(utils.DEFAULT_HEADERS)


def get(url):
    t = time.time()
    try:
        r = _SESSION.get(url, timeout=30)
        r.raise_for_status()
        return r.text
    finally:
        print(f"    [fetch {time.time() - t:5.1f}s] {url}", flush=True)


def html_of(el, limit=600):
    s = etree.tostring(el, encoding="unicode", with_tail=False)
    s = re.sub(r"\s+", " ", s)
    return s[:limit]


def norm(text):
    return re.sub(r"\d+", "9", re.sub(r"\s+", " ", text or "").strip())


def section(title):
    print("\n" + "=" * 20 + " " + title + " " + "=" * 20, flush=True)


def probe_dates():
    section("END DATES")
    ids = ["1372", "7389", "6388", "5251", "8040", "5894"]
    m = pd.read_csv(DATA / "tournament_metadata.csv", dtype=str)
    d = pd.to_datetime(m.Date, errors="coerce")
    e = pd.to_datetime(m.EndDate, errors="coerce")
    ids += m[e < d].TournamentID.head(3).tolist()
    stages = pd.read_csv(DATA / "tournament_stages.csv", dtype=str)
    for tid in ids:
        url = f"{BASE}/eng/tournament/id/{tid}/"
        try:
            text = get(url)
        except Exception as exc:
            print(tid, "ERR", exc)
            continue
        root = sm.parse_html(text)
        print(f"\n--- tournament {tid}")
        for tr in root.iter("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is not None and td is not None:
                k = sm.element_text_stripped(th)
                if "ate" in k or "egistr" in k or "Status" in k:
                    print("  INFO", k, "=>", sm.element_text(td).strip()[:80])
        flat = re.sub(r"\s+", " ", sm.element_text(root))
        for mt in list(DATE_RE.finditer(flat))[:25]:
            print("  DATE", flat[max(0, mt.start() - 70): mt.end() + 20])
        for row in root.iter("tr"):
            if any(sm._has_class(td, "stage-gr") for td in row.iter("td")) or "stage" in (row.get("class") or ""):
                print("  STAGEROW", html_of(row, 400))
        st = stages[stages.TournamentID == tid]
        for _, srow in st.head(2).iterrows():
            for suffix in ("", "?print"):
                surl = f"{BASE}/eng/tournament/stage/{srow.StageID}/matches/{suffix}"
                try:
                    stext = get(surl)
                except Exception as exc:
                    print("  STAGE ERR", surl, exc)
                    continue
                sflat = re.sub(r"\s+", " ", sm.element_text(sm.parse_html(stext)))
                hits = list(DATE_RE.finditer(sflat))
                print(f"  STAGE {srow.StageID}{suffix}: {len(hits)} date strings")
                for mt in hits[:8]:
                    print("    SDATE", sflat[max(0, mt.start() - 60): mt.end() + 15])
                for mt in re.finditer(r"title=\"[^\"]*\d{2}\.\d{2}\.\d{4}[^\"]*\"", stext):
                    print("    TITLE", mt.group(0)[:150])
                    break


def gap_series():
    df = pd.read_parquet(DATA / "scraped_matches.parquet")
    p = df[(df.Stage == "Playoff") & (df.TournamentType != "Team")].copy()
    p["a"] = p[["Player1ID", "Player2ID"]].min(axis=1)
    p["b"] = p[["Player1ID", "Player2ID"]].max(axis=1)
    g = p.groupby(["StageID", "RoundNumber", "a", "b"], dropna=False).agg(
        mx=("PlayoffGameNumber", "max"),
        n=("PlayoffGameNumber", "size"),
        games=("PlayoffGameNumber", lambda s: sorted(int(x) for x in s)),
        d=("Date", "first"),
    )
    return g[g.mx > g.n].reset_index()


def probe_playoffs():
    section("PLAYOFF GAPS")
    gaps = gap_series()
    print("gap series:", len(gaps), "stages:", gaps.StageID.nunique())
    limit = int(os.environ.get("PROBE_LIMIT", "0") or 0)
    if limit:
        keep = set(gaps[gaps.StageID == 20093].index)
        keep |= set(gaps.sample(n=min(limit, len(gaps)), random_state=1).index)
        gaps = gaps.loc[sorted(keep)]
        print("sampled gap series:", len(gaps))
    pages = {}

    def load(stage_id):
        try:
            return stage_id, get(f"{BASE}/eng/tournament/stage/{stage_id}/matches/?print")
        except Exception as exc:
            return stage_id, exc

    with ThreadPoolExecutor(max_workers=4) as ex:
        for sid, text in ex.map(load, sorted(gaps.StageID.unique())):
            pages[sid] = text

    patterns = collections.Counter()
    examples = collections.defaultdict(list)
    whole_series = []
    failures = 0
    for _, gs in gaps.iterrows():
        text = pages.get(gs.StageID)
        if isinstance(text, Exception) or text is None:
            failures += 1
            continue
        root = sm.parse_html(text)
        sm._remove_saved_matches(root)
        found = False
        for frac, serie in sm._iter_playoff_series(root):
            players = sm._links_in_name_cells(serie)
            if len(players) < 2:
                continue
            ids = {sm.extract_name_and_id(players[0])[1], sm.extract_name_and_id(players[1])[1]}
            if ids != {str(int(gs.a)), str(int(gs.b))}:
                continue
            found = True
            cells = sm._series_game_cells(serie)
            for num, cell in enumerate(cells, start=1):
                if num in gs.games:
                    continue
                key = (cell.get("class"), norm(sm.element_text(cell)), sorted(k for k in cell.attrib if k not in ("class", "data-match-id")).__str__())
                patterns[key] += 1
                if len(examples[key]) < 4:
                    examples[key].append((int(gs.StageID), num, html_of(cell, 300)))
            if len(whole_series) < 6:
                whole_series.append((int(gs.StageID), html_of(serie, 3000)))
            break
        if not found:
            patterns[("SERIES_NOT_FOUND", "", "")] += 1
            if len(examples[("SERIES_NOT_FOUND", "", "")]) < 5:
                examples[("SERIES_NOT_FOUND", "", "")].append((int(gs.StageID), gs.games, ""))
    print("page failures:", failures)
    for key, count in patterns.most_common():
        print(f"  {count:5d}  {key}")
        for ex in examples[key]:
            print("         ", ex)
    for sid, h in whole_series:
        print("SERIES", sid, h)
    # pagination check
    any_text = next((t for t in pages.values() if isinstance(t, str)), "")
    print("pagination markers:", re.findall(r"[?&]page=\d+", any_text)[:5])


def probe_forfeits():
    section("FORFEITS")
    for stage_id, player in ((3877, "3280"), (3882, "3280"), (3877, "2804")):
        text = get(f"{BASE}/eng/tournament/stage/{stage_id}/matches/?print")
        root = sm.parse_html(text)
        sm._remove_saved_matches(root)
        shown = 0
        for row in sm._match_rows(root):
            if f"/user/id/{player}/" not in etree.tostring(row, encoding="unicode"):
                continue
            if shown < 6:
                print(f"  {stage_id}/{player}", html_of(row, 900))
            shown += 1
        print(f"  {stage_id}/{player}: {shown} rows")
    # distribution of result cell markup over a few stages, incl. a known W.O stage
    df = pd.read_parquet(DATA / "scraped_matches.parquet")
    wo_stages = df[df.Walkover == "Yes"].StageID.drop_duplicates().tail(3).tolist()
    for stage_id in [3877, 3882, 20093] + wo_stages:
        text = get(f"{BASE}/eng/tournament/stage/{stage_id}/matches/?print")
        root = sm.parse_html(text)
        sm._remove_saved_matches(root)
        counter = collections.Counter()
        sample = {}
        for cell in root.iter("td"):
            if not sm._is_result_cell(cell):
                continue
            key = (cell.get("class"), norm(sm.element_text(cell)), tuple(sorted(cell.attrib)))
            counter[key] += 1
            sample.setdefault(key, html_of(cell, 300))
        print(f"  stage {stage_id} result cells:")
        for key, c in counter.most_common(25):
            print(f"     {c:4d} {key}  e.g. {sample[key]}")
        for word in ("W.O", "tech", "Tech", "техн", "forfeit", "+:-", "-:+", "+ : -", "- : +", "неяв"):
            if word in text:
                i = text.index(word)
                print(f"     contains {word!r}:", re.sub(r"\s+", " ", text[max(0, i - 200): i + 100]))


if __name__ == "__main__":
    wanted = sys.argv[1:] or ["dates", "forfeits", "playoffs"]
    for fn in [globals()["probe_" + name] for name in wanted]:
        try:
            fn()
        except Exception as exc:
            print("PROBE FAILED", fn.__name__, repr(exc))
