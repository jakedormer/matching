import json
import logging
import os
import traceback
from datetime import datetime
import re

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz



PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'basketcompare'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_error')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_success')
DB = firestore.Client()
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()


def streaming(data, context):
	'''This function is executed whenever a file is added to Cloud Storage'''
	
	bucket_name = data['bucket']
	file_name = data['name']
	print(file_name)
	
	if re.search('(test).*.csv$', file_name):
		bq_table = data['name'].split('/')[0] + "_" + data['name'].split('/')[1]
		db_ref = DB.document(u'streaming_files/%s' % file_name)
		if _was_already_ingested(db_ref):
			_handle_duplication(db_ref)
		else:
			try:
				_insert_into_bigquery(bucket_name, file_name, bq_table)
				_handle_success(db_ref)
			except Exception:
				_handle_error(db_ref)

def _was_already_ingested(db_ref):
	status = db_ref.get()
	return status.exists and status.to_dict()['success']


def _handle_duplication(db_ref):
	dups = [_now()]
	data = db_ref.get().to_dict()
	if 'duplication_attempts' in data:
		dups.extend(data['duplication_attempts'])
	db_ref.update({
		'duplication_attempts': dups
	})
	logging.warn('Duplication attempt streaming file \'%s\'' % db_ref.id)


def _insert_into_bigquery(bucket_name, file_name, bq_table):
	
	job_config = bigquery.LoadJobConfig()
	job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
	job_config.skip_leading_rows = 1
	job_config.source_format = bigquery.SourceFormat.CSV
	uri = "gs://" + bucket_name + "/" + file_name
	print(uri)
	table = BQ.dataset(BQ_DATASET).table(bq_table)

	load_job = BQ.load_table_from_uri(
		uri, 
		table, 
		job_config=job_config
	)
	

def _handle_success(db_ref):
	message = 'File \'%s\' streamed into BigQuery' % db_ref.id
	doc = {
		u'success': True,
		u'when': _now()
	}
	db_ref.set(doc)
	PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
	logging.info(message)


def _handle_error(db_ref):
	message = 'Error streaming file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
	doc = {
		u'success': False,
		u'error_message': message,
		u'when': _now()
	}
	db_ref.set(doc)
	PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
	logging.error(message)


def _now():
	return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


class BigQueryError(Exception):
	'''Exception raised whenever a BigQuery error happened''' 

	def __init__(self, errors):
		super().__init__(self._format(errors))
		self.errors = errors

	def _format(self, errors):
		err = []
		for error in errors:
			err.extend(error['errors'])
		return json.dumps(err)