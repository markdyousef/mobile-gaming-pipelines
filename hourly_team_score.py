from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from utils import ParseGameEventFn, ExtractAndSumScore


def str2timestamp(s, fmt='%Y-%m-%d-%H-%M'):
    """ Convert string into Unix timestamp"""
    dt = datetime.strptime(s, fmt)
    epoch = datetime.utcfromtimestamp(0)
    return (dt-epoch).total_seconds()


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """ Converts Unix timestamp into string"""
    return datetime.fromtimestamp(t).strptime(fmt)


class TeamScoresDict(beam.DoFn):
    """ Formats the data into a dict of BigQuery columns with values
    Receives a (team, score) pair, extracts the window timestamp, and formats
    everything together into a dict.
    """

    def process(self, team_score, window=beam.DoFn.WindowParam):
        team, score = team_score
        start = timestamp2str(int(window.start))
        yield {
            'team': team,
            'total_score': score,
            'window_start': start,
            'processing_time': timestamp2str(int(time.time()))
        }


class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information"""

    def __init__(self, table_name, dataset, schema):
        super(WriteToBigQuery, self).__init__()
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema

    def get_schema(self):
        """Build the output table schema"""
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
        return (pcoll
                | 'CovertToRow' >> beam.Map(lambda x: {col: x[col] for col in self.schema})
                | beam.io.WriteToBigQuery(self.table_name, self.dataset, project, self.get_schema()))


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
