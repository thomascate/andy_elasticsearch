#!/usr/bin/env python

#I discovered that some flows have no milliseconds on @timestamp due to
#inconsistent behaviour from utcfromtimestamp.
#http://bugs.python.org/issue1074462

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
  host = '10.1.1.2',
  sniff_on_start = True,
  request_timeout = 600
)

#Get a list of all indices in elasticsearch.
indices = es.indices.get_settings(
  index='*',
  params={'expand_wildcards': 'open,closed'}
)

#Make an empty list called flow_indices for putting flowstash indices into.
flow_indices = []

#Find every index that has the flowstash in its name and toss it into flow_indices.
for index in indices:
  if 'flowstash' in index:
    flow_indices.append(index)

#Take our list of flow indices, sort them and walk through them.
for index in sorted(flow_indices):

  #Make sure that each start of this loop has result as an empty dict
  result = {}
  updated_flows = []

  #Pull down the full index, this is going to be pretty slow so print something
  #to let the user know that things are happening.
  print "Getting index: " + index
  result = es.search(
    index=index,
    doc_type='netflow',
    size=1000000,
    request_timeout=600,
  )

  #Step through each flow in this index.
  for flow in result['hits']['hits']:
    #See if we can convert this time into a datetime object
    try:
      flow_time = time.strptime(flow['_source']['@timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
    except:
      flow['_source']['@timestamp'] = flow['_source']['@timestamp'] + ".000000"
      #print flow['_source']['@timestamp']
      #print flow
      updated_flows.append(flow)

  if len(updated_flows) > 0:
    print "reinserting " + str(len(updated_flows)) + " flows into " + index
    helpers.bulk(es, updated_flows)
    #pprint (updated_flows)
    #exit (0)











