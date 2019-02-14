"""Just simple job to upload more complicated document to Firestore Native"""

import csv
import datetime
import logging
import apache_beam as beam
from google.cloud import firestore

from apache_beam.options.pipeline_options import PipelineOptions

from settings import PROJECT, BUCKET, INPUT_FILENAME


class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200

    def __init__(self, project, collection):
        self._project = project
        self._collection = collection
        super(FirestoreWriteDoFn, self).__init__()

    def start_bundle(self):
        self._mutations = []

    def finish_bundle(self):
        if self._mutations:
            self._flush_batch()

    def process(self, element, *args, **kwargs):
        self._mutations.append(element)
        if len(self._mutations) > self.MAX_DOCUMENTS:
            self._flush_batch()

    def _flush_batch(self):
        db = firestore.Client(project=self._project)
        batch = db.batch()
        for mutation in self._mutations:
            if isinstance(mutation, dict):
                # autogenerate document_id
                ref = db.collection(self._collection).document()
                batch.set(ref, mutation)
            else:
                ref = db.collection(self._collection).document(mutation[0])
                batch.set(ref, mutation[1])
        r = batch.commit()
        logging.debug(r)
        self._mutations = []


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
        document_id = unicode(element.pop('sku'))
        element['regularPrice'] = float(element['regularPrice'])
        element['salePrice'] = float(element['salePrice'])
        element['name'] = unicode(element['name'].decode('utf-8'))
        element['type'] = unicode(element['type'].decode('utf-8'))
        element['url'] = unicode(element['url'].decode('utf-8'))
        element['image'] = unicode(element['image'].decode('utf-8'))
        element['inStoreAvailability'] = unicode(element['inStoreAvailability'])

        return [(document_id, element)]


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

    data = {
        'firstname': u'name',
        'lastname': u'lastname',
        'age': 27,
        'created': datetime.datetime.utcnow(),
        'address': {'city': u'mycity',
                    'street': u'mystreet',
                    'number': 111,
                    'zipcode': 11111
                    }

    }
    with beam.Pipeline(options=options) as p:

        (p | 'Reading input file' >> beam.Create([data])
         | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn(PROJECT, 'testxx'))
         )


if __name__ == '__main__':
    run_locally = True
    dataflow(run_locally)
