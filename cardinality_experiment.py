#!/usr/local/bin/python3
import random
import argparse
import time
from threading import Thread
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

"""
Execute with -h for command line options.

This code is designed to test the impact of "global ordinals" and "refresh" on high cardinality aggregations.
By default, global ordinals are built "lazily" on the first aggregation since the previous refresh, 
which can impact the performance of that first aggregation between refreshes. 

This code is designed to quantify the impact of the refresh interval and global ordinals. We generate documents with 
a high-cardinality field which we will run a term aggregation to compare the impact of eager and lazy ordinals.

The code consists of two parts, which can be enabled separately or together through command line parameters. 
(1) The bulk insertion of million(s) of high-cardinality entries. This is done as quickly as 
possible using bulk inserts and without any concurrent queries or aggregations. 
(2) The actual experiment which runs aggregations on the high-cardinality entries. In order to ensure that
the global ordinals are being rebuilt (either greedily or lazily depending on the experiment), we still insert 
documents into the index, but at a much slower rate so as to minimize the impact of the insertions on Elasticsearch,
which isolates the impact of the building of the global ordinals on the aggregation performance. 

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

FAST_REFRESH = 1  # in experiment mode, every X second do a segment refresh (buffer flush)
SLOW_REFRESH = 60  # in experiment mode, every X seconds do a segment refresh (buffer flush)
EXPERIMENT_DURATION = 10*60
SLEEP_BETWEEN_EXPERIMENTS = 60  # seconds to wait after completing an experiment before the next one

INSERT_INTERVAL = 1  # During aggs experiment, wait 1s between inserts. Ensures recompute of global ordinals
NUM_AGG_THREADS = 1  # X aggs threads running in parallel.
TIME_UNTIL_NEXT_AGG_IN_THREAD = (0, 1)  # choose a random number in this range to wait before next agg on each thread
HIGH_HIGH_CARDINALITY_INDEX = 'high_cardinality_experiment'
HIGH_CARDINALITY_FIELD = 'high_cardinality_field'

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

# Settings for the index which we run terms aggregations against. We disable caching to isolate the impact
# of the global ordinals calculations.
BASE_INDEX_SETTINGS = {
            'index': {
                'refresh_interval':'1s',
                'number_of_shards': 1,
                'number_of_replicas': 0,
                # Disable query cache:
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-cache.html
                'queries': {
                    'cache': {
                        'enabled': False  # disable query cache to isolate the impact of global ordinals
                    }
                },
                # Disable shard request cache:
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html
                'requests': {
                    'cache': {
                        'enable': False
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

RESULT_INDICES_MAPPINGS = {
            'doc': {
                'properties': {
                    'timestamp': {
                        'type': 'date'
                    },
                    'took': {
                        'type': 'long',
                    },
                    'experiment_id': {
                        'type': 'keyword',
                        'ignore_above': 256
                    }
                }
            }
        }


EXPERIMENT_PARAMETERS = [{"refresh_interval":  '%ds' % FAST_REFRESH,
                          "eager_global_ordinals": False,},
                         {"refresh_interval":  '%ds' % FAST_REFRESH,
                          "eager_global_ordinals": True,},
                         {"refresh_interval": '%ds' % SLOW_REFRESH,
                          "eager_global_ordinals": False,},
                         {"refresh_interval": '%ds' % SLOW_REFRESH,
                          "eager_global_ordinals": True,}
                         ]

EXPERIMENTS_TO_RUN = []

# Global variables that are used in multiple threads
es = None
continue_running_aggs = None
current_result_index = None
experiment_id = None


# Parse the command line options, to determine which section(s) of the code will be executed
# and connect to Elasticsearch
def initial_setup():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default='all', metavar='',
                        help='all | populate_only | experiments_only')
    global args
    args = parser.parse_args()
    print('Executing %s with mode=%s' % (parser.prog, args.mode))

    start_time_string = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    print("start %s" % start_time_string)

    global es
    es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))

    # Global for communicating to the aggregation threads when to finish
    global continue_running_aggs
    continue_running_aggs = True

    global current_result_index
    current_result_index = None

    global experiment_id
    experiment_id = None

    global EXPERIMENTS_TO_RUN
    for exp_obj in EXPERIMENT_PARAMETERS:
        eager_global_ordinals = exp_obj['eager_global_ordinals']
        refresh_interval = exp_obj['refresh_interval']
        result_index = 'experiment-%s-refresh-%s-eager_global_ordinals-%s' % (
            start_time_string, refresh_interval, eager_global_ordinals)
        result_index = result_index.lower()

        EXPERIMENTS_TO_RUN.append({
            "description": "Refresh: %s and eager_global_ordinals: %s" % (refresh_interval, eager_global_ordinals),
            "refresh_interval": refresh_interval,
            "eager_global_ordinals": eager_global_ordinals,
            "experiment_duration_in_seconds": EXPERIMENT_DURATION,
            'result_index': result_index,
            'experiment_id': 'Refresh=%s Eager=%s' % (refresh_interval, eager_global_ordinals)
        })

    for exp_obj in EXPERIMENTS_TO_RUN:
        request_body = {
            'mappings': RESULT_INDICES_MAPPINGS
        }
        es.indices.create(index=exp_obj['result_index'], body=request_body)


def bulk_insert_high_cardinality_documents():
    # We will insert CARDINALITY_RANGE documents. Each doc will have a random string.

    print("Starting bulk insertion of documents")

    print("Deleting index %s" % HIGH_HIGH_CARDINALITY_INDEX)
    es.indices.delete(index=HIGH_HIGH_CARDINALITY_INDEX, ignore=[400, 404])

    request_body = {
        'settings': BASE_INDEX_SETTINGS,
        'mappings': BASE_INDEX_MAPPINGS
    }
    es.indices.create(index=HIGH_HIGH_CARDINALITY_INDEX, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    for n in range(0, CARDINALITY_RANGE):

        # Generate a new document with a random string from 0..CARDINALITY_RANGE-1, and store it into
        # the docs_for_bulk_insert list.
        val = random.randint(1, CARDINALITY_RANGE)
        action = {
            '_index': HIGH_HIGH_CARDINALITY_INDEX,
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
    es.indices.refresh(index=HIGH_HIGH_CARDINALITY_INDEX)


# The following function steps through the experiments that we have defined, and
# sets the cluster to the required state for the required amount of time.
# Additionally, this will periodically insert a new document into Elasticsearch to force a rebuild
# of the global ordinals.
def step_through_experiment_configurations(experiments_to_run):

    global continue_running_aggs
    global current_result_index
    global experiment_id

    print("Starting experiment configuration control thread\n")
    for experiment in experiments_to_run:
        experiment_duration_in_seconds = experiment['experiment_duration_in_seconds']
        print("Running experiment: %s for %d seconds\n" %
              (experiment['description'], experiment_duration_in_seconds))

        start_time = time.time()

        current_result_index = experiment['result_index']
        experiment_id = experiment['experiment_id']

        print("*** Setting index to %s and experiment_id to %s" % (current_result_index, experiment_id))

        new_index_settings = {'index':
            {
                'refresh_interval': experiment['refresh_interval']
            }
        }

        new_index_mappings = {
            'properties': {
                HIGH_CARDINALITY_FIELD: {
                    'type': 'keyword',
                    'eager_global_ordinals': experiment['eager_global_ordinals']
                }
            }
        }

        es.indices.put_settings(index=HIGH_HIGH_CARDINALITY_INDEX, body=new_index_settings)
        es.indices.put_mapping(doc_type='doc', index=HIGH_HIGH_CARDINALITY_INDEX, body=new_index_mappings)

        while time.time() < start_time + experiment_duration_in_seconds:
            val = random.randint(0, CARDINALITY_RANGE)

            print("inserting doc with val=%s" % val)
            es.index(index=HIGH_HIGH_CARDINALITY_INDEX, doc_type='doc', id=None,
                     body={HIGH_CARDINALITY_FIELD: '%s' % val})
            time.sleep(INSERT_INTERVAL)  # sleep INSERT_INTERVAL seconds

        print('Ended experiment: %s.\n Sleeping for %d seconds before continuing\n' % (
            experiment['description'], SLEEP_BETWEEN_EXPERIMENTS))
        current_result_index = None
        experiment_id = None
        time.sleep(SLEEP_BETWEEN_EXPERIMENTS)

    # close the thread pool and wait for the work to finish
    continue_running_aggs = False


def run_aggs(thread_number):

    global continue_running_aggs
    global current_result_index
    global experiment_id

    print("Starting aggregation thread %d" % thread_number)

    docs_for_bulk_insert = []
    while True:
        if continue_running_aggs:
            request = {
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
            print("Thread %d executing search %s" % (thread_number, request))
            result=es.search(index=HIGH_HIGH_CARDINALITY_INDEX, doc_type='doc', body=request)
            print("Time for agg on thread %d is %d" % (thread_number, result['took']))

            # Store the time for each aggregation into ES - but cache in an in-memory data structure
            # to minimize the impact on the cluster. If current_result_index or experiment_id are not
            # currently defined, then we are paused 'between' experiments, and don't store the data.
            if current_result_index and experiment_id:
                action = {
                    '_index': current_result_index,
                    '_type': 'doc',
                    '_id': None,
                    '_source': {
                        'took': result['took'],
                        'timestamp': datetime.now(),
                        'experiment_id': experiment_id
                    }
                }
                print('Adding result doc %s' % action)
                docs_for_bulk_insert.append(action)

                # Wait a random amount of time before starting the next aggregation.
                time.sleep(random.uniform(*TIME_UNTIL_NEXT_AGG_IN_THREAD))

        else:  # if !continue_running_aggs - then stop the experiment and write data to the cluster
            # At the end of the experiment, write the experimental results to the ES cluster
            helpers.bulk(es, docs_for_bulk_insert)
            break


def run_experiment():

    global continue_running_aggs

    print("Spinning up threads for the experiment")

    my_threads=[]
    my_threads.append(Thread(target=step_through_experiment_configurations, args=(EXPERIMENTS_TO_RUN,)))

    for x in range(0, NUM_AGG_THREADS):
        my_threads.append(Thread(target=run_aggs, args=(x,)))

    for t in my_threads:
        t.start()

    # Wait for all threads to complete before terminating
    for t in my_threads:
        t.join()


def main():
    initial_setup()
    if args.mode == 'all' or args.mode == 'populate_only':
        bulk_insert_high_cardinality_documents()

    if args.mode == 'all' or args.mode == 'experiments_only':
        run_experiment()


main()
