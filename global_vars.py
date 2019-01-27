
# CONSTANTS
ONE_THOUSAND = 1000
ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND
CARDINALITY_RANGE = 1 * ONE_MILLION
BULK_SIZE = 1 * ONE_THOUSAND

HIGH_HIGH_CARDINALITY_INDEX = 'high_cardinality_experiment'
HIGH_CARDINALITY_FIELD = 'high_cardinality_field'

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

SLEEP_BETWEEN_EXPERIMENTS = 5  # seconds to wait after completing an experiment before the next one

INSERT_INTERVAL = 1  # During aggs experiment, wait 1s between inserts. Ensures recompute of global ordinals
NUM_AGG_THREADS = 1  # X threads running parallel aggregations.
TIME_UNTIL_NEXT_AGG_IN_THREAD = (0, 1)  # choose a random number in this range to wait before next agg on each thread

# Settings for the index which we run terms aggregations against. We disable caching to isolate the impact
# of the global ordinals calculations.
BASE_INDEX_SETTINGS = {
    'index': {
        'refresh_interval': '1s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
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

RESULT_INDEX_SETTINGS = {
    'index': {
        'refresh_interval': '1s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
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