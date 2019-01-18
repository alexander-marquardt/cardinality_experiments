#!/usr/local/bin/python3

"""
This code is designed to test the impact of Elasticsearch settings on high cardinality aggregations.
An example aggregation that _can_ be slow for "high-cardinality" fields is as follows:

GET high_cardinality_experiment/_search
{
  "size": 0,
  "aggs": {
    "topn": {
      "terms": {
        "field": "high_cardinality_field",
        "size": 10
      }
    }
  }
}

"""

import random
import argparse
from datetime import datetime
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers

# CONSTANTS
ONE_THOUSAND = 1000
ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND
CARDINALITY_RANGE = 1 * ONE_MILLION
BULK_SIZE = 1 * ONE_THOUSAND
CARDINALITY_INDEX = 'high_cardinality_experiment'

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

BASE_INDEX_SETTINGS = {
            'index': {
                'refresh_interval': '60s',
                'number_of_shards': 1,
                'number_of_replicas': 0,
                'queries': {
                    'cache': {
                        'enabled': False  # disable query cache to isolate the impact of global ordinals
                    }
                }
            }
        }

BASE_INDEX_MAPPINGS = {
            'doc': {
                'properties': {
                    'timestamp': {
                        'type': 'date'
                    },
                    'high_cardinality_field': {
                        'type': 'keyword',
                        'ignore_above': 256,
                        'eager_global_ordinals': False  # the default is False, set it explicitly here
                    }
                }
            }
        }



# Parse the command line options, to determine which section(s) of the code will be executed
# and
def set_global_vars():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default='full_experiment', metavar='',
                        help='full_experiment | populate_only | slow_inserts_only')
    global args
    args = parser.parse_args()
    print('Executing %s with mode=%s' % (parser.prog, args.mode))

    global es
    es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))


def insert_high_cardinality_documents():
    # We will insert CARDINALITY_RANGE documents. Each doc will have a random string.

    es.indices.delete(index=CARDINALITY_INDEX, ignore=[400, 404])


    request_body = {
        'settings': BASE_INDEX_SETTINGS,
        'mappings': BASE_INDEX_MAPPINGS
    }
    es.indices.create(index=CARDINALITY_INDEX, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    for n in range(0, CARDINALITY_RANGE):

        # Generate a new document with a random string from 0..CARDINALITY_RANGE-1, and store it into
        # the docs_for_bulk_insert list.
        val = random.randint(1, CARDINALITY_RANGE)
        action = {
            '_index': CARDINALITY_INDEX,
            '_type': 'doc',
            '_id': None,
            '_source': {
                'high_cardinality_field': '%s' % val,
                'timestamp': datetime.now()
                }
            }
        docs_for_bulk_insert.append(action)

        bulk_counter += 1
        if bulk_counter >= BULK_SIZE:
            helpers.bulk(es, docs_for_bulk_insert)
            docs_for_bulk_insert = []
            bulk_counter = 0

    # before leaving this function, ensure that all data has been flushed
    es.indices.refresh(index=CARDINALITY_INDEX)


# The following function will periodically insert a new document into Elasticsearch to force a rebuild
# of the global ordinals
def force_rebuild_of_global_ordinals():
    new_index_settings = {'index':
        {
            'refresh_interval': '1s'
        }
    }

    es.indices.put_settings(index=CARDINALITY_INDEX, body=new_index_settings)
    while True:
        val = random.randint(0, CARDINALITY_RANGE)

        print("inserting doc with val=%s" % val)
        es.index(index=CARDINALITY_INDEX, doc_type='doc', id=None, body={'high_cardinality_field': '%s' % val})
        time.sleep(1) # sleep 1 second


def main():
    set_global_vars()
    if args.mode == 'full_experiment' or args.mode == 'populate_only':
        insert_high_cardinality_documents()

    if args.mode == 'full_experiment' or args.mode == 'slow_inserts_only':
        force_rebuild_of_global_ordinals()


main()