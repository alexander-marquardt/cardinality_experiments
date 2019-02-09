#!/usr/local/bin/python3

import random
import time
from threading import Thread
import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

import global_vars
import setup_experiments
import configure

def run_aggs(es, thread_number, shared_state_for_threads):

    print("Starting aggregation thread %d" % thread_number)

    docs_for_bulk_insert = []
    while True:
        if shared_state_for_threads['continue_running_aggs']:
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
            # print("Thread %d executing search %s" % (thread_number, request))
            result=es.search(index=global_vars.HIGH_CARDINALITY_INDEX_NAME, doc_type='doc', body=request)
            # print("Time for agg on thread %d is %d" % (thread_number, result['took']))

            # Store the time for each aggregation into ES - but cache in an in-memory data structure
            # to minimize the impact on the cluster. If current_result_index or experiment_id are not
            # currently defined, then we are paused 'between' experiments, and don't store the data.
            if shared_state_for_threads['current_result_index'] and shared_state_for_threads['experiment_id']:
                action = {
                    '_index': shared_state_for_threads['current_result_index'],
                    '_type': 'doc',
                    '_id': None,
                    '_source': {
                        'took': result['took'],
                        'timestamp': datetime.datetime.now(),
                        'experiment_id': shared_state_for_threads['experiment_id']
                    }
                }
                # print('Adding result doc %s' % action)
                docs_for_bulk_insert.append(action)

                # Wait a random amount of time before starting the next aggregation.
                time.sleep(random.uniform(*setup_experiments.TIME_UNTIL_NEXT_AGG_IN_THREAD))

        else:  # if !continue_running_aggs - then stop the experiment and write data to the cluster
            # At the end of the experiment, write the experimental results to the ES cluster
            print("writing %s" % shared_state_for_threads['current_result_index'])
            helpers.bulk(es, docs_for_bulk_insert)
            break


def configure_index_and_run_background_inserts(es, experiment_obj, shared_state_for_threads):

    request_body = {
        'settings': configure.RESULT_INDEX_SETTINGS,
        'mappings': configure.RESULT_INDICES_MAPPINGS
    }
    es.indices.create(index=experiment_obj['result_index'], body=request_body)

    experiment_duration_in_seconds = experiment_obj['experiment_duration_in_seconds']
    print("%s Running experiment: %s for %d seconds\n" %
          (datetime.datetime.now().isoformat(), experiment_obj['description'], experiment_duration_in_seconds))

    start_time = time.time()

    shared_state_for_threads['current_result_index'] = experiment_obj['result_index']
    shared_state_for_threads['experiment_id'] = experiment_obj['experiment_id']

    print("*** Setting index to %s and experiment_id to %s" % (shared_state_for_threads['current_result_index'],
                                                               shared_state_for_threads['experiment_id']))

    new_index_settings = {'index':
        {
            'refresh_interval': experiment_obj['refresh_interval'],
            # For the base disable shard request cache. Test with this both enabled and disabled
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html
            'requests': {
                'cache': {
                    'enable': experiment_obj['request_cache_bool']
                }
            }
        }
    }

    new_index_mappings = {
        'properties': {
            global_vars.HIGH_CARDINALITY_FIELD_NAME: {
                'type': 'keyword',
                'eager_global_ordinals': experiment_obj['eager_global_ordinals']
            }
        }
    }

    es.indices.put_settings(index=global_vars.HIGH_CARDINALITY_INDEX_NAME, body=new_index_settings)
    es.indices.put_mapping(doc_type='doc', index=global_vars.HIGH_CARDINALITY_INDEX_NAME, body=new_index_mappings)

    while time.time() < start_time + experiment_duration_in_seconds:
        val = random.randint(0, global_vars.CARDINALITY_RANGE)

        # print("inserting doc with val=%s" % val)
        es.index(index=global_vars.HIGH_CARDINALITY_INDEX_NAME, doc_type='doc', id=None,
                 body={global_vars.HIGH_CARDINALITY_FIELD_NAME: '%s' % val})
        time.sleep(setup_experiments.INSERT_INTERVAL)  # sleep INSERT_INTERVAL seconds

    print('%s Ended experiment: %s.\n Sleeping for %d seconds before continuing\n' % (
        datetime.datetime.now().isoformat(),  experiment_obj['description'], setup_experiments.SLEEP_BETWEEN_EXPERIMENTS))

    time.sleep(setup_experiments.SLEEP_BETWEEN_EXPERIMENTS)

    # close the thread pool and wait for the work to finish
    shared_state_for_threads['continue_running_aggs'] = False


def run_experiment(experiment):

    # Global for communicating to the aggregation threads when to finish
    shared_state_for_threads = {
        'continue_running_aggs': True,
        'current_result_index': None,
        'experiment_id': None
    }

    es = Elasticsearch([global_vars.ES_HOST], http_auth=(global_vars.ES_USER, global_vars.ES_PASSWORD))
    es.cluster.put_settings(body=configure.CLUSTER_SETTINGS_FOR_LOGGING_GLOBAL_ORDINAL_TOOK_TIME)

    print("%s Spinning up threads for the experiment" % datetime.datetime.now().isoformat())

    my_threads=[]
    my_threads.append(Thread(target=configure_index_and_run_background_inserts,
                             args=(es, experiment, shared_state_for_threads,)))

    for x in range(0, setup_experiments.NUM_AGG_THREADS):
        my_threads.append(Thread(target=run_aggs, args=(es, x, shared_state_for_threads)))

    for t in my_threads:
        t.start()

    # Wait for all threads to complete before terminating
    for t in my_threads:
        t.join()

