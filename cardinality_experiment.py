#!/usr/local/bin/python3
import random
import argparse
from datetime import datetime
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers

"""
This code is designed to test the impact of Elasticsearch settings on high cardinality aggregations.
By default, "global ordinals" are built "lazily" on the first aggregation since the previous refresh,
which can impact the performance of that first aggregation between refreshes. This code is designed to
quantify this impact, by generating documents with a high-cardinality field which we will run terms
aggregations over. We also provide code that allows us to contrast the default "lazy" performance to
the performance of "eager" building of global ordinals.

An example aggregation that _may_ be slow for a high-cardinality field is as follows:

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


# CONSTANTS
ONE_THOUSAND = 1000
ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND
CARDINALITY_RANGE = 1 * ONE_MILLION
BULK_SIZE = 1 * ONE_THOUSAND
TIME_BETWEEN_INSERTS_WHILE_RUNNING_AGGS = 1  # wait 1s between each insert. Will force global ordinals to build
NUMBER_OF_AGGS_PER_INSERT_INTERVAL = 5 # 5 aggs per second (assuming TIME_BETWEEN_INSERTS_WHILE_RUNNING_AGGS=1)
CARDINALITY_INDEX = 'high_cardinality_index'
HIGH_CARDINALITY_FIELD = 'high_cardinality_field'

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
                    HIGH_CARDINALITY_FIELD: {
                        'type': 'keyword',
                        'ignore_above': 256,
                        'eager_global_ordinals': False  # the default is False, set it explicitly here
                    }
                }
            }
        }



AGGREGATION_EXPERIMENTS_TO_RUN = [
    {
        "description" : "Default Elasticsearch behavior - 1s refresh, and no eager global ordinals",
        "refresh_interval": "1s",
        "eager_global_ordinals": False,
        "number_of_aggs_per_interval": NUMBER_OF_AGGS_PER_INSERT_INTERVAL,
    },
    {
        "description": "Refresh interval of 1s with eager global ordinals",
        "refresh_interval": "1s",
        "eager_global_ordinals": True,
        "number_of_aggs_per_interval": NUMBER_OF_AGGS_PER_INSERT_INTERVAL,
    },
    {
        "description": "Increase refresh interval to 60s without eager global ordinals",
        "refresh_interval": "60s",
        "eager_global_ordinals": False,
        "number_of_aggs_per_interval": NUMBER_OF_AGGS_PER_INSERT_INTERVAL,

    },
    {
        "description": "Increase refresh interval to 60s and enable global ordinals",
        "refresh_interval": "60s",
        "eager_global_ordinals": True,
        "number_of_aggs_per_interval": NUMBER_OF_AGGS_PER_INSERT_INTERVAL,
    },
]


# Parse the command line options, to determine which section(s) of the code will be executed
# and connect to Elasticsearch
def initial_setup():
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
                HIGH_CARDINALITY_FIELD: '%s' % val,
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
# of the global ordinals (either eagerly or lazily, depending on the settings).
# After the initial loading of the high-cardinality values, we slow down the loading so that the
# global ordinals still need to be periodically rebuilt, but so that the size of the index
# is roughly the same for the different experiments.
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
        es.index(index=CARDINALITY_INDEX, doc_type='doc', id=None, body={HIGH_CARDINALITY_FIELD: '%s' % val})
        time.sleep(TIME_BETWEEN_INSERTS_WHILE_RUNNING_AGGS)  # sleep TIME_BETWEEN_INSERTS_WHILE_RUNNING_AGGS seconds


def main():
    initial_setup()
    if args.mode == 'full_experiment' or args.mode == 'populate_only':
        insert_high_cardinality_documents()

    if args.mode == 'full_experiment' or args.mode == 'slow_inserts_only':
        force_rebuild_of_global_ordinals()

main()