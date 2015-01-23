#!/usr/bin/env python
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
from datetime import datetime
from pprint import pprint
import logging

logging.basicConfig(level=logging.ERROR)
es = Elasticsearch('localhost', sniff_on_start=True,request_timeout=600)


first_result = es.search(
    index='flowstash-2015.01.16',
    doc_type='netflow',
    size=1,
    body={
      'sort': [
        {'@timestamp': {'order': 'asc'}}
      ]
    }
)

last_result = es.search(
    index='flowstash-2015.01.16',
    doc_type='netflow',
    size=1,
    body={
      'sort': [
        {'@timestamp': {'order': 'desc'}}
      ]
    }
)

first_time = str((first_result['hits']['hits'][0]['_source']['@timestamp']))
last_time = str((last_result['hits']['hits'][0]['_source']['@timestamp']))
print "newest entry " + last_time
print "oldest entry " + first_time

first_datetime = time.strptime(first_time, "%Y-%m-%dT%H:%M:%S.%f")
first_epoch = time.mktime(first_datetime)

last_datetime = time.strptime(last_time, "%Y-%m-%dT%H:%M:%S.%f")
last_epoch = time.mktime(last_datetime)

diff_in_seconds = last_epoch - first_epoch
print "offset in seconds is " + str(diff_in_seconds)

exit()
