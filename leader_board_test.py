import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import leader_board
from utils import ParseGameEventFn


class LeaderBoardTest(unittest.TestCase):
    SAMPLE_DATA = [
        'user1_team1,team1,18,1447686663000,2015-11-16 15:11:03.921',
        'user1_team1,team1,18,1447690263000,2015-11-16 16:11:03.921',
        'user2_team2,team2,2,1447690263000,2015-11-16 16:11:03.955',
        'user3_team3,team3,8,1447690263000,2015-11-16 16:11:03.955',
        'user4_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
        'user1_team1,team1,14,1447697463000,2015-11-16 18:11:03.955',
    ]

    def create_data(self, p):
        return (p
                | beam.Create(LeaderBoardTest.SAMPLE_DATA)
                | beam.ParDo(ParseGameEventFn())
                | beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])))

    def test_leader_board_teams(self):
        with TestPipeline() as p:
            result = (
                self.create_data(p)
                | leader_board.CalculateTeamScores(
                    team_window_duration=60,
                    allowed_lateness=120))

            assert_that(result, equal_to(
                [('team1', 14), ('team1', 18), ('team1', 18), ('team2', 2), ('team3', 13)]))

    def test_leader_board_users(self):
        with TestPipeline() as p:
            result = (
                self.create_data(p)
                | leader_board.CalculateUserScores(allowed_lateness=120))

            assert_that(result, equal_to([]))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
