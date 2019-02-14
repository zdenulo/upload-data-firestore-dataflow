"""Just simple job to upload more complicated document to Firestore Native via WriteToDatastore PTransform"""

import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from google.cloud.proto.datastore.v1 import entity_pb2

from googledatastore import helper as datastore_helper

from settings import PROJECT, BUCKET


class CreateEntities(beam.DoFn):
    """Creates Datastore entity with different types of fields, just to verify what is possible"""

    def process(self, idx):
        entity = entity_pb2.Entity()
        embed = entity_pb2.Entity()  # embedded document
        ss = {
            'ss': unicode('X'),  # string
            'bb': False,  # boolean
            'll': [1, 2, 3],  # list of integers
            'ii': 123  # integer
        }
        datastore_helper.add_properties(embed, ss)  # setting properties for embedded document
        element = dict()
        element['s'] = unicode('String')  # string
        element['b'] = True  # boolean
        element['l'] = [unicode('s'), unicode('a'), unicode('b')]  # list of strings
        element['i'] = 1  # integer
        element['e'] = embed  # embedded document
        datastore_helper.add_key_path(entity.key, 'ds', idx)  # setting id for document
        datastore_helper.add_properties(entity, element)  # setting properties for document
        return [entity]


def dataflow(run_local):
    JOB_NAME = 'firestore-upload-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    pipeline_options = {
        'project': PROJECT,
        'staging_location': 'gs://' + BUCKET + '/staging',
        'runner': 'DataflowRunner',
        'job_name': JOB_NAME,
        'disk_size_gb': 100,
        'temp_location': 'gs://' + BUCKET + '/staging',
        'save_main_session': True,
        'requirements_file': 'requirements.txt'
    }

    if run_local:
        pipeline_options['runner'] = 'DirectRunner'

    options = PipelineOptions.from_dictionary(pipeline_options)

    with beam.Pipeline(options=options) as p:
        (p | 'Reading input file' >> beam.Create([1])
         | 'Create entities' >> beam.ParDo(CreateEntities())
         | 'Write entities into Datastore' >> WriteToDatastore(PROJECT)
         )


if __name__ == '__main__':
    run_locally = True
    dataflow(run_locally)
