import logging, csv, time
from datetime import datetime
import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions


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
