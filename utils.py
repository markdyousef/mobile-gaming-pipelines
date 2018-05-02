import logging
import csv
import apache_beam as beam
from apache_beam.metrics.metric import Metrics

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
