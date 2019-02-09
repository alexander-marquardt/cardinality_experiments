#!/usr/local/bin/python3

import random
import datetime


from elasticsearch import Elasticsearch
from elasticsearch import helpers

import global_vars
import configure


def bulk_insert_high_cardinality_documents():
    # We will insert CARDINALITY_RANGE documents. Each doc will have a random string.

    es = Elasticsearch([global_vars.ES_HOST], http_auth=(global_vars.ES_USER, global_vars.ES_PASSWORD))
    es.cluster.put_settings(body=configure.CLUSTER_SETTINGS_FOR_LOGGING_GLOBAL_ORDINAL_TOOK_TIME)

    print("Deleting index %s" % global_vars.HIGH_CARDINALITY_INDEX_NAME)
    es.indices.delete(index=global_vars.HIGH_CARDINALITY_INDEX_NAME, ignore=[400, 404])

    request_body = {
        'settings': configure.CARDINALITY_INDEX_SETTINGS_FOR_POPULATE,
        'mappings': configure.CARDINALITY_INDEX_MAPPINGS_FOR_POPULATE,
    }
    es.indices.create(index=global_vars.HIGH_CARDINALITY_INDEX_NAME, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    print("%s Starting bulk insertion of documents\n" % datetime.datetime.now().isoformat())
    for n in range(0, global_vars.NUMBER_OF_DOCUMENTS_TO_INSERT):

        # Generate a new document with a random string from 0..CARDINALITY_RANGE-1, and store it into
        # the docs_for_bulk_insert list.
        val = random.randint(1, global_vars.CARDINALITY_RANGE)
        action = {
            '_index': global_vars.HIGH_CARDINALITY_INDEX_NAME,
            '_type': 'doc',
            '_id': None,
            '_source': {
                global_vars.HIGH_CARDINALITY_FIELD_NAME: '%s' % val,
                'timestamp': datetime.datetime.now()
                }
            }
        docs_for_bulk_insert.append(action)

        bulk_counter += 1
        if bulk_counter >= global_vars.BULK_SIZE:

            helpers.bulk(es, docs_for_bulk_insert)
            docs_for_bulk_insert = []
            bulk_counter = 0

    # before leaving this function, ensure that all data has been flushed and set to a reasonable default
    es.indices.put_settings(index=global_vars.HIGH_CARDINALITY_INDEX_NAME,
                            body=configure.CARDINALITY_INDEX_SETTINGS_AFTER_POPULATE)
    es.indices.put_mapping(doc_type='doc', index=global_vars.HIGH_CARDINALITY_INDEX_NAME,
                           body=configure.CARDINALITY_INDEX_MAPPINGS_AFTER_POPULATE)
    es.indices.refresh(index=global_vars.HIGH_CARDINALITY_INDEX_NAME)
    print("%s Ended bulk insertion of documents\n" % datetime.datetime.now().isoformat())
