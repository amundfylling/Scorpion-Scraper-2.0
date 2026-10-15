import json
import unittest
from datetime import datetime, timezone

import pandas as pd

from bs4 import BeautifulSoup

from scorpion_scraper import scrape_matches, scrape_player_ranking, scrape_players, tournament_metadata


def soup(html):
    return BeautifulSoup(html, "lxml")


def page(html):
    return scrape_matches.parse_html(html)


class MatchParserTests(unittest.TestCase):
    def test_individual_round_robin_match_row(self):
        html = """
        <table class="grTable">
          <tr><th colspan="5">1 Tour</th></tr>
          <tr id="match2066636">
            <td class="ma_name1"><a href="/eng/user/id/4672/">Pavel Simonov</a></td>
            <td class="ma_name2"><a href="/eng/user/id/14809/">Kirill Iliukhin</a></td>
            <td class="ma_result_b">5&nbsp;:&nbsp;0</td>
          </tr>
          <tr id="match2066638">
            <td class="ma_name1"><a href="/eng/user/id/1/">Rest Player</a></td>
            <td class="ma_name2">Rest</td>
            <td class="ma_result_a"></td>
          </tr>
        </table>
        """

        rows = scrape_matches.parse_individual_stage_matches(page(html), "stage-url")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["MatchID"], "2066636")
        self.assertEqual(rows[0]["Player1ID"], "4672")
        self.assertEqual(rows[0]["Player2ID"], "14809")
        self.assertEqual(rows[0]["GoalsPlayer1"], 5)
        self.assertEqual(rows[0]["Stage"], "Round-Robin")
        self.assertEqual(rows[0]["RoundNumber"], 1.0)

    def test_individual_playoff_series_with_multiple_games(self):
        html = """
        <div class="subheader">Quarterfinal</div>
        <div class="gr_match">
          <table>
            <tr class="series-container">
              <td class="ma_name1"><a href="/eng/user/id/14437/">Alexey Myakishev</a></td>
              <td class="ma_name2"><a href="/eng/user/id/11263/">Roman Bentsa</a></td>
              <td class="ma_result_xb" data-match-id="2066448">1 : 3</td>
              <td class="ma_result_xb" data-match-id="2066449">3 : 4 (OT)</td>
              <td class="ma_result_x0" data-match-id="2066450"></td>
              <td class="ma_result_xb">0 : 2</td>
            </tr>
          </table>
        </div>
        """

        rows = scrape_matches.parse_individual_stage_matches(page(html), "stage-url")

        self.assertEqual(len(rows), 2)
        self.assertEqual([row["MatchID"] for row in rows], ["2066448", "2066449"])
        self.assertEqual([row["PlayoffGameNumber"] for row in rows], [1, 2])
        self.assertEqual(rows[1]["Overtime"], "Yes")
        self.assertEqual(rows[0]["Stage"], "Playoff")

    def test_team_aggregate_expands_child_games(self):
        stage_html = """
        <table class="grTable">
          <tr><th colspan="5">1 Tour</th></tr>
          <tr id="match2064843">
            <td class="ma_name1"><a href="/eng/team/id/1239/">Fjerppen</a></td>
            <td class="ma_name2"><a href="/eng/team/id/1249/">Gangsberg</a></td>
            <td class="ma_result_b">18&nbsp;:&nbsp;8</td>
          </tr>
        </table>
        """
        detail_html = """
        <table class="grTable">
          <tr id="match2064844">
            <td class="ma_name1"><a href="/eng/user/id/1647/">Magnus Klippen</a></td>
            <td class="ma_name2"><a href="/eng/user/id/13043/">Kjartan Moberg</a></td>
            <td class="ma_result_b">5&nbsp;:&nbsp;4</td>
          </tr>
          <tr id="match2064845">
            <td class="ma_name1"><a href="/eng/user/id/3222/">Andreas Fjermestad</a></td>
            <td class="ma_name2"><a href="/eng/user/id/1247/">Trond Ove Gangsøy</a></td>
            <td class="ma_result_b">6&nbsp;:&nbsp;2</td>
          </tr>
        </table>
        """

        rows = scrape_matches.parse_team_stage_matches(
            page(stage_html),
            "stage-url",
            lambda team_match_id: page(detail_html),
        )
        parallel_rows = scrape_matches.parse_team_stage_matches_parallel(
            page(stage_html),
            "stage-url",
            lambda team_match_id: page(detail_html),
            detail_workers=2,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(parallel_rows, rows)
        self.assertEqual([row["MatchID"] for row in rows], ["2064844", "2064845"])
        self.assertEqual({row["TeamMatchID"] for row in rows}, {"2064843"})
        self.assertEqual(rows[0]["Team1"], "Fjerppen")
        self.assertEqual(rows[0]["Team2ID"], "1249")
        self.assertEqual(rows[1]["TeamGameNumber"], 2)

    def test_team_aggregate_without_child_games_is_skipped(self):
        stage_html = """
        <table class="grTable">
          <tr><th colspan="5">1 Tour</th></tr>
          <tr id="match100">
            <td class="ma_name1"><a href="/eng/team/id/1/">Team A</a></td>
            <td class="ma_name2"><a href="/eng/team/id/2/">Team B</a></td>
            <td class="ma_result_b">1&nbsp;:&nbsp;0</td>
          </tr>
        </table>
        """
        detail_html = """
        <table class="grTable">
          <tr id="match101">
            <td class="ma_name1"><a href="/eng/user/id/1/">A</a></td>
            <td class="ma_name2"><a href="/eng/user/id/2/">B</a></td>
            <td class="ma_result_a"></td>
          </tr>
        </table>
        """

        rows = scrape_matches.parse_team_stage_matches(
            page(stage_html),
            "stage-url",
            lambda team_match_id: page(detail_html),
        )

        self.assertEqual(rows, [])

    def test_normalize_output_date_keeps_iso_dates(self):
        self.assertEqual(scrape_matches.normalize_output_date("2026-05-10"), "2026-05-10")
        self.assertEqual(scrape_matches.normalize_output_date("10.05.2026"), "2026-05-10")


class TournamentMetadataParserTests(unittest.TestCase):
    def test_tournament_metadata_known_and_unknown_fields(self):
        html = """
        <h1 id="header">Norwegian Championships 2026 Duo</h1>
        <table class="iTable">
          <tr><th>Tournament type</th><td>Team</td></tr>
          <tr><th>Status</th><td>Finished</td></tr>
          <tr><th>Level of tournament</th><td>6</td></tr>
          <tr><th>Региональный множитель SWR</th><td>0.5</td></tr>
          <tr><th>Country</th><td>Norway</td></tr>
          <tr><th>City</th><td>Kvernaland</td></tr>
          <tr><th>Date of the tournament</th><td>03.05.2026</td></tr>
          <tr><th>Unknown field</th><td>Keep me</td></tr>
        </table>
        """

        row = tournament_metadata.parse_tournament_metadata(
            soup(html),
            "https://th.sportscorpion.com/eng/tournament/id/7979/",
        )

        self.assertEqual(row["TournamentID"], "7979")
        self.assertEqual(row["Name"], "Norwegian Championships 2026 Duo")
        self.assertEqual(row["Type"], "Team")
        self.assertEqual(row["Level"], "6")
        self.assertEqual(row["SWRMultiplier"], "0.5")
        self.assertEqual(row["Date"], "2026-05-03")
        self.assertEqual(json.loads(row["ExtraMetadataJson"]), {"Unknown field": "Keep me"})

    def test_tournament_stage_cache_rows(self):
        html = """
        <table class="stages-table">
          <tr>
            <td class="stage-gr">1</td>
            <td><a href="/eng/tournament/stage/23223/">Participants</a></td>
            <td><a href="/eng/tournament/stage/23223/matches/">Schedule and results</a></td>
          </tr>
          <tr>
            <td class="stage-gr">2</td>
            <td><a href="/eng/tournament/stage/23318/matches/">Schedule and results</a></td>
          </tr>
        </table>
        """

        rows = tournament_metadata.parse_tournament_stages(
            soup(html),
            "7979",
            "https://th.sportscorpion.com",
        )

        self.assertEqual(rows, [
            {
                "TournamentID": "7979",
                "StageID": "23223",
                "StageSequence": "1",
                "StageURL": "https://th.sportscorpion.com/eng/tournament/stage/23223/matches/?print",
            },
            {
                "TournamentID": "7979",
                "StageID": "23318",
                "StageSequence": "2",
                "StageURL": "https://th.sportscorpion.com/eng/tournament/stage/23318/matches/?print",
            },
        ])


class MatchParserEdgeCaseTests(unittest.TestCase):
    def test_playoff_subheaders_assign_their_own_blocks(self):
        html = """
        <div class="subheader">Semi-final</div>
        <div class="gr_match"><table><tr class="series-container">
          <td class="ma_name1"><a href="/eng/user/id/1/">A</a></td>
          <td class="ma_name2"><a href="/eng/user/id/2/">B</a></td>
          <td class="ma_result_xb" data-match-id="11">2 : 1</td>
        </tr></table></div>
        <div class="subheader">Final</div>
        <div class="gr_match"><table><tr class="series-container">
          <td class="ma_name1"><a href="/eng/user/id/1/">A</a></td>
          <td class="ma_name2"><a href="/eng/user/id/3/">C</a></td>
          <td class="ma_result_xb" data-match-id="12">3 : 1</td>
        </tr></table></div>
        """

        rows = scrape_matches.parse_individual_stage_matches(page(html), "stage-url")

        self.assertEqual([(row["MatchID"], row["RoundNumber"]) for row in rows], [("11", 0.5), ("12", 1)])

    def test_saved_matches_section_is_ignored(self):
        html = """
        <div class="saved-matches"><table class="grTable">
          <tr><th>1 Tour</th></tr>
          <tr id="match1"><td class="ma_name1">X</td><td class="ma_name2">Y</td>
          <td class="ma_result_b">1 : 0</td></tr>
        </table></div>
        <table class="grTable">
          <tr><th>2 Tour</th></tr>
          <tr id="match2"><td class="ma_name1"><a href="/eng/user/id/5/">P</a></td>
          <td class="ma_name2"><a href="/eng/user/id/6/">Q</a></td>
          <td class="ma_result_b">3 : 3 (W.O)</td></tr>
        </table>
        """

        rows = scrape_matches.parse_individual_stage_matches(page(html), "stage-url")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["MatchID"], "2")
        self.assertEqual(rows[0]["RoundNumber"], 2.0)
        self.assertEqual(rows[0]["Walkover"], "Yes")


class IncrementalHelperTests(unittest.TestCase):
    def test_id_strings_handles_int_and_float_ids(self):
        self.assertEqual(
            list(scrape_matches.id_strings(pd.Series([7979, 8001.0, None]))),
            ["7979", "8001", pd.NA],
        )

    def test_settled_without_matches_requires_final_old_and_known(self):
        cutoff = pd.Timestamp("2026-10-01")
        known_before = datetime(2026, 10, 7, tzinfo=timezone.utc)
        row = {"Status": "Finished", "Date": "2026-03-01", "ScrapedAt": "2026-03-02T10:00:00+00:00"}

        self.assertTrue(scrape_matches.is_settled_without_matches(row, cutoff, known_before))
        self.assertFalse(scrape_matches.is_settled_without_matches(dict(row, Status="Finished *"), cutoff, known_before))
        self.assertFalse(scrape_matches.is_settled_without_matches(dict(row, Date="2026-10-05"), cutoff, known_before))
        self.assertFalse(scrape_matches.is_settled_without_matches(
            dict(row, ScrapedAt="2026-10-10T10:00:00+00:00"), cutoff, known_before,
        ))
        self.assertFalse(scrape_matches.is_settled_without_matches(None, cutoff, known_before))

    def test_unfinished_tournament_is_read_live(self):
        cached = {"TournamentID": "1", "Status": "Registration is opened", "Type": "Individual"}
        calls = []
        original = scrape_matches.fetch_soup
        live_html = """
        <h1 id="header">Cup</h1>
        <table class="iTable">
          <tr><th>Tournament type</th><td>Individual</td></tr>
          <tr><th>Status</th><td>Finished</td></tr>
          <tr><th>Date of the tournament</th><td>24.09.2026</td></tr>
        </table>
        <table class="stages-table"><tr><td class="stage-gr">1</td>
          <td><a href="/eng/tournament/stage/5/matches/">Schedule and results</a></td></tr></table>
        """
        scrape_matches.fetch_soup = lambda session, url: calls.append(url) or soup(live_html)
        try:
            metadata, stages, live = scrape_matches.resolve_tournament("1", cached, [])
            finished, cached_stages, cached_live = scrape_matches.resolve_tournament(
                "1", dict(metadata), [{"StageID": "5"}],
            )
        finally:
            scrape_matches.fetch_soup = original

        self.assertTrue(live)
        self.assertEqual(metadata["Date"], "2026-09-24")
        self.assertEqual([stage["StageID"] for stage in stages], ["5"])
        self.assertFalse(cached_live)
        self.assertEqual(len(calls), 1)


class PlayerParserTests(unittest.TestCase):
    def test_profile_page_fields(self):
        html = """
        <html><body>
        <h1 id="header">Users - Pavel Simonov</h1>
        <table class="iTable">
          <tr><th>Name, Surname</th><td>Pavel Simonov</td></tr>
          <tr><th>World ranking</th><td>2040
              &mdash; 306 place
            (ID <a href="http://stiga.trefik.cz/ithf/ranking/player.aspx?ID=666217">666217</a>)
          </td></tr>
          <tr><th>Country</th><td>Russia</td></tr>
          <tr><th>City</th><td> Saint Petersburg </td></tr>
          <tr><th>Teams</th><td><a href="/eng/team/id/136/">Russia Juniors</a></td></tr>
          <tr><th>Date of birth</th><td>24.12.2005</td></tr>
          <tr><th>Sex</th><td>Male</td></tr>
        </table>
        </body></html>
        """

        row = scrape_players.parse_player_page(4672, html)

        self.assertEqual(row, {
            "PlayerID": 4672,
            "Name": "Pavel Simonov",
            "RankingID": "666217",
            "Country": "Russia",
            "City": "Saint Petersburg",
            "DateOfBirth": "24.12.2005",
            "Sex": "Male",
        })
        self.assertEqual(list(row), scrape_players.PLAYER_COLUMNS)

    def test_profile_page_without_info_table(self):
        row = scrape_players.parse_player_page(1, "<html><body><p>No profile</p></body></html>")

        self.assertEqual(row, {
            "PlayerID": 1, "Name": "", "RankingID": "", "Country": "",
            "City": "", "DateOfBirth": "", "Sex": "",
        })


class RankingParserTests(unittest.TestCase):
    HTML = """
    <html><body><form>
    <span id="LabRank"><table border="1"><tr><td>&nbsp;</td><td><table><tr>
      <td class="PlayerB">Jan.</td><td class="PlayerB">Feb.</td><td class="PlayerB">March</td>
    </tr></table></td></tr>
    <tr><td class="PlayerB">2010</td><td><table><tr>
      <td class="Player">-</td>
      <td class="Player">2392.<br><a href="history.aspx?id=250">20</a></td>
      <td class="Player">1593.<br><a href="history.aspx?id=251">77</a></td>
    </tr></table></td></tr>
    <tr><td class="PlayerB">2011</td><td><table><tr>
      <td class="Player">990.<br><a href="history.aspx?id=261">398</a></td>
      <td class="Player">&nbsp;</td><td class="Player">&nbsp;</td>
    </tr></table></td></tr>
    </table></span>
    </form></body></html>
    """

    def test_history_table(self):
        df = scrape_player_ranking.parse_player_ranking(self.HTML)

        self.assertEqual(list(df.columns), scrape_player_ranking.RANKING_COLUMNS)
        self.assertEqual(df["year"].tolist(), [2010] * 3 + [2011] * 3)
        # Month labels line up with their own cells (January is the first cell).
        self.assertEqual(df["month"].tolist(), ["Jan", "Feb", "March"] * 2)
        self.assertEqual(df["rank"].tolist()[1:4], [2392, 1593, 990])
        self.assertEqual(df["points"].tolist()[1:4], [20, 77, 398])
        self.assertTrue(df.loc[[0, 4, 5], ["rank", "points"]].isna().all().all())

    def test_page_without_ranking_table(self):
        with self.assertRaises(ValueError):
            scrape_player_ranking.parse_player_ranking("<html><body><p>Unknown player</p></body></html>")


if __name__ == "__main__":
    unittest.main()
