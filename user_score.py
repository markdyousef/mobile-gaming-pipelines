from __future__ import absolute_import

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParseGameEventFn(beam.DoFn):
    """ Parses the raw gameevent info into a Python dictionary

    Each event line has the following format:
        username, teamname, score, timestamp_in_ms, readable_time
    """

    def __init__(self):
        super(ParseGameEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(
            self.__class__, 'num_parse_errors')

    def process(self, element):
        try:
            row = list(csv.reader([element]))[0]
            yield {
                'user': row[0],
                'team': row[1],
                'score': int(row[2]),
                'timestamp': int(row[3]) / 1000.0
            }
        except:  # pylint: disable=bare-except
            # log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on {}'.format(element))


class ExtractAndSumScore(beam.PTransform):
    """ A transform to extract key/score information and sum the scores.
    `field` determines whether 'team' or 'user' info is extracted
    """

    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, pcoll):
        return (pcoll | beam.Map(lambda x: (x[self.field], x['score'])) | beam.CombinePerKey(sum))


class UserScore(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn()) | 'ExtractAndSumScore' >> ExtractAndSumScore('user'))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', type=str, default='gs://apache-beam-samples/game/gaming_data*.csv')
    parser.add_argument('--output', type=str, required=True)

    args, pipeline_args = parser.parse_known_args(argv)

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
