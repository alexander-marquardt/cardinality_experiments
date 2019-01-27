#!/usr/local/bin/python3

import random

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

import global_vars


def bulk_insert_high_cardinality_documents():
    # We will insert CARDINALITY_RANGE documents. Each doc will have a random string.

    es = Elasticsearch([global_vars.ES_HOST], http_auth=(global_vars.ES_USER, global_vars.ES_PASSWORD))

    print("Deleting index %s" % global_vars.HIGH_HIGH_CARDINALITY_INDEX)
    es.indices.delete(index=global_vars.HIGH_HIGH_CARDINALITY_INDEX, ignore=[400, 404])

    request_body = {
        'settings': global_vars.BASE_INDEX_SETTINGS,
        'mappings': global_vars.BASE_INDEX_MAPPINGS
    }
    es.indices.create(index=global_vars.HIGH_HIGH_CARDINALITY_INDEX, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    print("Starting bulk insertion of documents")
    for n in range(0, global_vars.CARDINALITY_RANGE):

        # Generate a new document with a random string from 0..CARDINALITY_RANGE-1, and store it into
        # the docs_for_bulk_insert list.
        val = random.randint(1, global_vars.CARDINALITY_RANGE)
        action = {
            '_index': global_vars.HIGH_HIGH_CARDINALITY_INDEX,
            '_type': 'doc',
            '_id': None,
            '_source': {
                global_vars.HIGH_CARDINALITY_FIELD: '%s' % val,
                'timestamp': datetime.now()
                }
            }
        docs_for_bulk_insert.append(action)

        bulk_counter += 1
        if bulk_counter >= global_vars.BULK_SIZE:

            helpers.bulk(es, docs_for_bulk_insert)
            docs_for_bulk_insert = []
            bulk_counter = 0

    # before leaving this function, ensure that all data has been flushed
    es.indices.refresh(index=global_vars.HIGH_HIGH_CARDINALITY_INDEX)
