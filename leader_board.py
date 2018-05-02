from __future__ import absolute_import, print_function

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import trigger
from utils import timestamp2str, ParseGameEventFn, ExtractAndSumScore, TeamScoresDict, WriteToBigQuery


class CalculateTeamScores(beam.PTransform):
    """ Calculates scores for each team within the configured window duration.
    Extract team/score pairs from the event stream, using hour-long windows by default.
    """

    def __init__(self, team_window_duration, allowed_lateness):
        super(CalculateTeamScores, self).__init__()
        self.team_window_duration = team_window_duration * 60
        self.allowed_lateness_sec = allowed_lateness * 60

    def expand(self, pcoll):
        return (pcoll
                | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(
                    beam.window.FixedWindows(self.team_window_duration),
                    trigger=trigger.AfterWatermark(
                        trigger.AfterCount(10), trigger.AfterCount(20)),
                    accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
                | 'ExtractAndSumScore' >> ExtractAndSumScore('team'))


class CalculateUserScores(beam.PTransform):
    """ Extract user/score pairs from the event stream using processing time, via global windowing.
        Get periodic updates on all users' running scores.
    """

    def __init__(self, allowed_lateness):
        super(CalculateUserScores, self).__init__()
        self.allowed_lateness_sec = allowed_lateness * 60

    def expand(self, pcoll):
        return (pcoll
                | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(
                    beam.window.GlobalWindows(),
                    trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                    accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
                | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic', type=str, required=True)
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--table_name', default='leader_board')
    parser.add_argument('--team_window_duration', type=int, default=60)
    parser.add_argument('--allowed_lateness', type=int, default=120)

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # --project options required to access dataset
    if options.view_as(GoogleCloudOptions).project is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project is required')
        sys.exit(1)

    options.view_as(SetupOptions).save_main_session = True

    # Enforce this pipeline to always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read game events from Pub/Sub using custem timestamps,
        # which are extracted from the pubsub data elements
        events = (p
                  | 'ReadPubSub' >> beam.io.ReadStringsFromPubSub(args.topic)
                  | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn())
                  | 'AddEventTimestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])))

        # Get team scores and write the results to BigQuery
        (events  # pylint: disable=expression-not-assigned
         | 'CalculateTeamScores' >> CalculateTeamScores(args.team_window_duration, args.allowed_lateness)
         | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
         | 'WriteTeamScoreSums' >> WriteToBigQuery(
             args.table_name + '_teams', args.dataset, {
                 'team': 'STRING',
                 'total_score': 'INTEGER',
                 'window_start': 'STRING',
                 'processing_time': 'STRING'}))

        def format_user_score(user_score):
            (user, score) = user_score
            return {'user': user, 'total_score': score}

        # Get user scores and write the results to BigQuery
        (events  # pylint: disable=expression-not-assigned
        | 'CalculateUserScores' >> CalculateUserScores(args.allowed_lateness)
        | 'FormatUserScoreSums' >> beam.Map(format_user_score)
        | 'WriteUserScoreSums' >> WriteToBigQuery(
            args.table_name + '_users', args.dataset, {
                'user': 'STRING',
                'total_score': 'INTEGER'}))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
