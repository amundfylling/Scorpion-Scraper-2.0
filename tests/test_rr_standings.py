import unittest

import pandas as pd

from scorpion_scraper import audit_rr_standings, scrape_rr_tables


def table_page(rows_html, cross_columns=3, group="Group A"):
    headers = "".join(f"<th>{n}</th>" for n in range(1, cross_columns + 1))
    return f"""
    <div class="subheader">{group}</div>
    <table class="resTable">
      <tr><th> </th><th class="name">Name</th>{headers}
          <th>G</th><th>W</th><th>D</th><th>L</th><th>Gd</th><th>Points</th><th>Place</th></tr>
      {rows_html}
    </table>
    """


INDIVIDUAL_PAGE = table_page("""
  <tr>
    <td class="but">1</td><td class="name"><a href="/eng/user/id/10/">Anna</a></td>
    <td class="ft_sep"> </td>
    <td class="ft_score"><div class="result-win">3 : 1</div><div class="result-draw">2 : 2</div></td>
    <td class="ft_score"><div class="result-win"> 4 : 0<i class="moved" title="Transferred result">*</i></div></td>
    <td>3</td><td>2</td><td>1</td><td>0</td><td>9 - 3 = 6</td><td>5</td><td>1</td>
  </tr>
  <tr>
    <td class="but">2</td><td class="name"><a href="/eng/user/id/20/">Bo</a></td>
    <td class="ft_score"><div class="result-loss">1 : 3</div><div class="result-draw">2 : 2</div></td>
    <td class="ft_sep"> </td>
    <td class="ft_score"><div class="techdef-cell">5 : 0<div class="techdef" title="Walkover">(W.O)</div></div></td>
    <td>3</td><td>1</td><td>1</td><td>1</td><td>8 - 5 = 3</td><td>3</td><td>2</td>
  </tr>
  <tr>
    <td class="but">3</td><td class="name"><a href="/eng/user/id/30/">Cy</a></td>
    <td class="ft_score"><div class="result-loss">0 : 4<i class="moved" title="Transferred result">*</i></div></td>
    <td class="ft_score"><div class="techdef-cell">0 : 5<div class="techdef" title="Walkover">(W.O)</div></div></td>
    <td class="ft_sep"> </td>
    <td>2</td><td>0</td><td>0</td><td>2</td><td>0 - 9 = -9</td><td>0</td><td>3</td>
  </tr>
""")


class TableParserTests(unittest.TestCase):
    def test_standings_and_games_are_parsed(self):
        standings, games = scrape_rr_tables.parse_stage_table(INDIVIDUAL_PAGE, 99, 7)

        self.assertEqual([row["EntryID"] for row in standings], [10, 20, 30])
        anna = standings[0]
        self.assertEqual(anna["Group"], "Group A")
        self.assertEqual((anna["G"], anna["W"], anna["D"], anna["L"]), (3, 2, 1, 0))
        self.assertEqual((anna["GF"], anna["GA"], anna["Points"], anna["Place"]), (9, 3, 5, 1))
        self.assertEqual(standings[2]["GA"], 9)

        # Each game once (upper triangle), one row per meeting.
        self.assertEqual(len(games), 4)
        pairs = [(g["Entry1ID"], g["Entry2ID"], g["Meeting"], g["Goals1"], g["Goals2"]) for g in games]
        self.assertEqual(pairs, [(10, 20, 1, 3, 1), (10, 20, 2, 2, 2), (10, 30, 1, 4, 0), (20, 30, 1, 5, 0)])
        flags = {(g["Entry1ID"], g["Entry2ID"], g["Meeting"]): (g["Transferred"], g["Walkover"]) for g in games}
        self.assertEqual(flags[(10, 30, 1)], (True, False))
        self.assertEqual(flags[(20, 30, 1)], (False, True))
        self.assertEqual(flags[(10, 20, 1)], (False, False))

    def test_empty_cell_and_team_match_link(self):
        page = table_page("""
          <tr><td class="but">1</td><td class="name"><a href="/eng/team/id/5/">Team A</a></td>
              <td class="ft_sep"> </td>
              <td class="ft_score"><div class="result-win"><a href="/eng/match/id/777/">3 : 1</a></div></td>
              <td>1</td><td>1</td><td>0</td><td>0</td><td>3 - 1 = 2</td><td>2</td><td>1</td></tr>
          <tr><td class="but">2</td><td class="name"><a href="/eng/team/id/6/">Team B</a></td>
              <td class="ft_score"><div class="result-loss"><a href="/eng/match/id/777/">1 : 3</a></div></td>
              <td class="ft_sep"> </td>
              <td>1</td><td>0</td><td>0</td><td>1</td><td>1 - 3 = -2</td><td>0</td><td>2</td></tr>
        """, cross_columns=2)
        standings, games = scrape_rr_tables.parse_stage_table(page, 1)
        self.assertEqual(standings[0]["EntryType"], "team")
        self.assertEqual(len(games), 1)
        self.assertEqual(games[0]["MatchID"], 777)

    def test_page_without_table(self):
        self.assertEqual(scrape_rr_tables.parse_stage_table("<html><body>Nothing</body></html>", 1), ([], []))


class StageSelectionTests(unittest.TestCase):
    def test_candidates_include_rr_and_stages_without_games(self):
        matches = pd.DataFrame({
            "StageID": [1, 1, 2],
            "TournamentID": [100, 100, 100],
            "Stage": ["Round-Robin", "Round-Robin", "Playoff"],
        })
        cache = pd.DataFrame({"StageID": [1, 2, 3], "TournamentID": [100, 100, 100]})
        candidates = scrape_rr_tables.candidate_stages(matches, cache)
        self.assertEqual(sorted(candidates["StageID"]), [1, 3])

    def test_refresh_new_recent_and_newly_finished(self):
        candidates = pd.DataFrame({"StageID": [1, 2, 3, 4], "TournamentID": [10, 20, 30, 40]})
        done = pd.DataFrame({"StageID": [1, 2, 3], "TournamentStatus": ["Finished", "Finished", "Is being conducted"]})
        metadata = {
            "10": {"Status": "Finished", "Date": "2020-01-01"},
            "20": {"Status": "Finished", "Date": "2026-11-15"},
            "30": {"Status": "Finished", "Date": "2026-01-01"},
            "40": {"Status": "Finished", "Date": "2020-01-01"},
        }
        todo = scrape_rr_tables.stages_to_refresh(candidates, done, metadata, 14, today=pd.Timestamp("2026-11-19"))
        self.assertEqual(todo, {2, 3, 4})


def matches_frame(rows):
    columns = ["StageID", "TournamentID", "TournamentName", "Date", "Stage", "Player1", "Player1ID",
               "Player2", "Player2ID", "GoalsPlayer1", "GoalsPlayer2", "TeamMatchID"]
    return pd.DataFrame(
        [dict(zip(columns[:4], (sid, 7, "Cup", "2026-01-01")), Stage=stage, Player1=str(p1), Player1ID=p1,
              Player2=str(p2), Player2ID=p2, GoalsPlayer1=g1, GoalsPlayer2=g2, TeamMatchID=None)
         for sid, stage, p1, p2, g1, g2 in rows],
        columns=columns,
    )


class AuditTests(unittest.TestCase):
    def setUp(self):
        standings, games = scrape_rr_tables.parse_stage_table(INDIVIDUAL_PAGE, 99, 7)
        self.standings = pd.DataFrame(standings)
        self.games = pd.DataFrame(games)
        self.stages = pd.DataFrame({"StageID": [99], "TournamentID": [7], "HasTable": [True]})

    def run_audit(self, rows):
        return audit_rr_standings.audit(matches_frame(rows), self.standings, self.games, self.stages)

    def test_matching_stage_has_no_issues(self):
        issues = self.run_audit([
            (99, "Round-Robin", 20, 10, 1, 3),   # stored the other way round
            (99, "Round-Robin", 10, 20, 2, 2),
            (99, "Round-Robin", 20, 30, 5, 0),
            (98, "Round-Robin", 30, 10, 0, 4),   # transferred game, earlier stage
        ])
        self.assertEqual(issues.to_dict("records"), [])

    def test_transferred_game_listed_in_this_stage_is_accepted(self):
        issues = self.run_audit([
            (99, "Round-Robin", 10, 20, 3, 1),
            (99, "Round-Robin", 10, 20, 2, 2),
            (99, "Round-Robin", 20, 30, 5, 0),
            (99, "Round-Robin", 10, 30, 4, 0),
        ])
        self.assertEqual(issues.to_dict("records"), [])

    def test_missing_wrong_extra_and_unfound_transfer(self):
        issues = self.run_audit([
            (99, "Round-Robin", 10, 20, 3, 1),
            (99, "Round-Robin", 20, 30, 4, 0),    # official 5:0
            (99, "Round-Robin", 10, 40, 1, 0),    # not in the table
            (98, "Playoff", 10, 30, 4, 0),        # playoff game is not a transfer source
        ])
        checks = issues["Check"].value_counts().to_dict()
        self.assertEqual(checks.get("game_missing"), 1)       # 2:2 between 10 and 20
        self.assertEqual(checks.get("score_mismatch"), 1)
        self.assertEqual(checks.get("game_extra"), 1)
        self.assertEqual(checks.get("player_not_in_table"), 1)
        self.assertEqual(checks.get("transferred_not_found"), 1)
        self.assertIn("totals_mismatch", checks)
        mismatch = issues[issues["Check"] == "score_mismatch"].iloc[0]
        self.assertEqual((mismatch["Official"], mismatch["Dataset"]), ("5:0", "4:0"))

    def test_stage_without_games_is_one_issue(self):
        issues = self.run_audit([(50, "Round-Robin", 1, 2, 1, 0)])
        self.assertEqual(issues["Check"].tolist(), ["stage_missing"])

    def test_points_rule_is_inferred(self):
        rows = pd.DataFrame({"G": [3, 3, 3], "W": [3, 1, 0], "D": [0, 1, 1], "L": [0, 1, 2], "Points": [9, 4, 1]})
        self.assertEqual(audit_rr_standings.infer_point_rule(rows), (3, 1, 0))


if __name__ == "__main__":
    unittest.main()
