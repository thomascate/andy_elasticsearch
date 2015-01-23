#!/usr/bin/env python

#This script finds the age difference between the newest flow entry
#and the oldest flow entry. This is to get a rough estimate on how
#far off the entries in the db that were created while the clock was
#off are.

#import these functions/classes directly
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pprint import pprint

#import these entire modules
import logging
import time

#call the logging function to determine our log level
#This is used by the Elasticsearch class.
logging.basicConfig(level=logging.ERROR)

#Instatiate the Elasticsearch class within the local variable es
#note this doesn't cause ES to do anything, but provides us an easy way
#to call it with these settings.
es = Elasticsearch(
  host = 'localhost',
  sniff_on_start = True,
  request_timeout = 600
)

#Call the search function inside of Elasticsearch and give back 1 response
#sorted by date. This will be the oldest entry in the dataset
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

#Call the search function inside of Elasticsearch and give back 1 response
#sorted by date. This will be the newest entry in the dataset
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

#The search function returns a nested dictionary that contains metadata for 
#ES itself as well as a list of responses and their metadata and data.
#This drills down to just the timestamp of our one repsonse for each.
#We also make sure it's a string and store it in first_time/last_time.
first_time = str((first_result['hits']['hits'][0]['_source']['@timestamp']))
last_time = str((last_result['hits']['hits'][0]['_source']['@timestamp']))
print "newest entry " + last_time
print "oldest entry " + first_time

#Convert the strings to time objects.
first_datetime = time.strptime(first_time, "%Y-%m-%dT%H:%M:%S.%f")
#Convert the time strings to simple epoch integers so we can do math on them.
first_epoch = time.mktime(first_datetime)

last_datetime = time.strptime(last_time, "%Y-%m-%dT%H:%M:%S.%f")
last_epoch = time.mktime(last_datetime)

#Find the difference in seconds between the oldest and the newest entry. This
#should roughly be how far in seconds your clock was set back.
diff_in_seconds = last_epoch - first_epoch
print "offset in seconds is " + str(diff_in_seconds)

exit()
