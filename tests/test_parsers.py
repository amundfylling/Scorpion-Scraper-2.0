import json
import unittest

from bs4 import BeautifulSoup

from scorpion_scraper import scrape_matches, tournament_metadata


def soup(html):
    return BeautifulSoup(html, "lxml")


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

        rows = scrape_matches.parse_individual_stage_matches(soup(html), "stage-url")

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

        rows = scrape_matches.parse_individual_stage_matches(soup(html), "stage-url")

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
            soup(stage_html),
            "stage-url",
            lambda team_match_id: soup(detail_html),
        )
        parallel_rows = scrape_matches.parse_team_stage_matches_parallel(
            soup(stage_html),
            "stage-url",
            lambda team_match_id: soup(detail_html),
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
            soup(stage_html),
            "stage-url",
            lambda team_match_id: soup(detail_html),
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


if __name__ == "__main__":
    unittest.main()
