"""Upload data into Firestore Native via WriteToDatastore PTransform"""

import csv
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper

from settings import PROJECT, BUCKET, INPUT_FILENAME


class CSVtoDict(beam.DoFn):
    """Converts line into dictionary"""
    def process(self, element, headers):
        rec = ""
        element = element.encode('utf-8')
        try:
            for line in csv.reader([element]):
                rec = line

            if len(rec) == len(headers):
                data = {header.strip(): val.strip() for header, val in zip(headers, rec)}
                return [data]
            else:
                print "bad: {}".format(rec)
        except Exception:
            pass


class CreateEntities(beam.DoFn):
    """Creates Datastore entity"""
    def process(self, element):
        entity = entity_pb2.Entity()
        sku = int(element.pop('sku'))
        element['regularPrice'] = float(element['regularPrice'])
        element['salePrice'] = float(element['salePrice'])
        element['name'] = unicode(element['name'].decode('utf-8'))
        element['type'] = unicode(element['type'].decode('utf-8'))
        element['url'] = unicode(element['url'].decode('utf-8'))
        element['image'] = unicode(element['image'].decode('utf-8'))
        element['inStoreAvailability'] = unicode(element['inStoreAvailability'])

        datastore_helper.add_key_path(entity.key, 'Product', sku)
        datastore_helper.add_properties(entity, element)
        return [entity]


def dataflow(run_local):
    if run_local:
        input_file_path = 'sample.csv'
    else:
        input_file_path = 'gs://' + BUCKET + '/' + INPUT_FILENAME

    JOB_NAME = 'datastore-upload-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    pipeline_options = {
        'project': PROJECT,
        'staging_location': 'gs://' + BUCKET + '/staging',
        'runner': 'DataflowRunner',
        'job_name': JOB_NAME,
        'disk_size_gb': 100,
        'temp_location': 'gs://' + BUCKET + '/temp',
        'save_main_session': True
    }

    if run_local:
        pipeline_options['runner'] = 'DirectRunner'

    options = PipelineOptions.from_dictionary(pipeline_options)
    with beam.Pipeline(options=options) as p:
        (p | 'Reading input file' >> beam.io.ReadFromText(input_file_path)
         | 'Converting from csv to dict' >> beam.ParDo(CSVtoDict(),
                                                       ['sku', 'name', 'regularPrice', 'salePrice', 'type', 'url', 'image',
                                                        'inStoreAvailability'])
         | 'Create entities' >> beam.ParDo(CreateEntities())
         | 'Write entities into Firestore' >> WriteToDatastore(PROJECT)
         )


if __name__ == '__main__':
    run_locally = True
    dataflow(run_locally)