import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import hourly_team_score


class HourlyTeamScoreTest(unittest.TestCase):
    SAMPLE_DATA = [
        'user1_team1,team1,18,1447686663000,2015-11-16 15:11:03.921',
        'user1_team1,team1,18,1447690263000,2015-11-16 16:11:03.921',
        'user2_team2,team2,2,1447690263000,2015-11-16 16:11:03.955',
        'user3_team3,team3,8,1447690263000,2015-11-16 16:11:03.955',
        'user4_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
        'user1_team1,team1,14,1447697463000,2015-11-16 18:11:03.955',
    ]

    def test_hourly_team_score(self):
        with TestPipeline() as p:
            result = (p | beam.Create(HourlyTeamScoreTest.SAMPLE_DATA) | hourly_team_score.HourlyTeamScore(
                start_min='2015-11-16-15-20',
                stop_min='2015-11-16-17-20',
                window_duration=60))

            assert_that(result, equal_to(
                [('team1', 18), ('team2', 2), ('team3', 13)]))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
