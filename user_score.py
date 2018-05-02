from __future__ import absolute_import

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from utils import ParseGameEventFn, ExtractAndSumScore

class UserScore(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn()) | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', type=str, default='gs://apache-beam-samples/game/gaming_data*.csv')
    parser.add_argument('--output', type=str, required=True)

    args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend('--runner=DirectRunner')

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        def format_user_score(user_score):
            (user, score) = user_score
            return 'user: {}, total_score: {}'.format(user, score)

        (p  # pylint: disable=expression-not-assigned
         | 'ReadInputText' >> beam.io.ReadFromText(args.input)
         | 'UserScore' >> UserScore()
         | 'FormatUserScoreSums' >> beam.Map(format_user_score)
         | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output)
         )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
