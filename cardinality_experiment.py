#!/usr/local/bin/python3

import random
import argparse
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

ONE_THOUSAND = 1000
ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND
CARDINALITY_RANGE = 10 * ONE_MILLION
BULK_SIZE = 10 * ONE_THOUSAND

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

# Parse the command line options, to determine which section(s) of the code will be executed
parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', default='full_experiment', metavar='',
                    help= "full_experiment | populate_only | slow_inserts_only")
args = parser.parse_args()
print("Executing %s with mode=%s" % (parser.prog, args.mode))

es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))


actions = [] # Store a group of documents in this array to periodically bulk insert
bulk_counter = 0
for n in range(0, CARDINALITY_RANGE):

    val = random.randint(1, CARDINALITY_RANGE)
    action = {
        "_index": "cardinality_test",
        "_type": "doc",
        "_id": None,
        "_source": {
            'val_string': "%s" % val,
            "timestamp": datetime.now()
            }
        }
    actions.append(action)



    bulk_counter += 1
    if bulk_counter >= BULK_SIZE:
        helpers.bulk(es, actions)
        actions = []
        bulk_counter = 0

while True:
    val = random.randint(1, NUM_VALUES)

    es.index(index='cardinality', doc_type='doc', id=None, body={'val_string': "%s" % val})
    time.sleep(1) # sleep 1 second
