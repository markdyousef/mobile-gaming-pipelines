from __future__ import absolute_import, print_function

import argparse
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from utils import ParseGameEventFn, ExtractAndSumScore, str2timestamp, TeamScoresDict, WriteToBigQuery


class HourlyTeamScore(beam.PTransform):
    def __init__(self, start_min, stop_min, window_duration):
        super(HourlyTeamScore, self).__init__()
        self.start_timestamp = str2timestamp(start_min)
        self.stop_timestamp = str2timestamp(stop_min)
        self.window_duration_in_sec = window_duration * 60

    def expand(self, pcoll):
        return (pcoll
                | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn())
                | 'FilterStartTime' >> beam.Filter(lambda x: x['timestamp'] > self.start_timestamp)
                | 'FilterEndTime' >> beam.Filter(lambda x: x['timestamp'] < self.stop_timestamp)
                | 'AddEventTimestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp']))
                | 'FixedWindowsTeam' >> beam.WindowInto(beam.window.FixedWindows(self.window_duration_in_sec))
                | 'ExtractAndSumScore' >> ExtractAndSumScore('team'))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        type=str,
                        default='gs://apache-beam-samples/game/gaming_data*.csv',
                        help='Path to the data file(s) containing game data.')
    parser.add_argument('--dataset',
                        type=str,
                        required=True,
                        help='BigQuery Dataset to write tables to. '
                        'Must already exist.')
    parser.add_argument('--table_name',
                        default='leader_board',
                        help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--window_duration',
                        type=int,
                        default=60,
                        help='Numeric value of fixed window duration, in minutes')
    parser.add_argument('--start_min',
                        type=str,
                        default='1970-01-01-00-00',
                        help='String representation of the first minute after '
                        'which to generate results in the format: '
                        'yyyy-MM-dd-HH-mm. Any input data timestamped '
                        'prior to that minute won\'t be included in the '
                        'sums.')
    parser.add_argument('--stop_min',
                        type=str,
                        default='2100-01-01-00-00',
                        help='String representation of the first minute for '
                        'which to generate results in the format: '
                        'yyyy-MM-dd-HH-mm. Any input data timestamped '
                        'after to that minute won\'t be included in the '
                        'sums.')

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # --project options is required to access dataset
    if options.view_as(GoogleCloudOptions).project is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project is required')
        sys.exit(1)

    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (p  # pylint: disable=expression-not-assigned
         | 'ReadInputText' >> beam.io.ReadFromText(args.input)
         | 'HourlyTeamScore' >> HourlyTeamScore(args.start_min, args.stop_min, args.window_duration)
         | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
         | 'WriteTeamScoreSums' >> WriteToBigQuery(
             args.table_name, args.dataset, {
                 'team': 'STRING',
                 'total_score': 'INTEGER',
                 'window_start': 'STRING'
             }))
