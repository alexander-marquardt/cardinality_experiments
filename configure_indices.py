import global_vars

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
            global_vars.HIGH_CARDINALITY_FIELD: {
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

CLUSTER_SETTINGS_FOR_LOGGING = {
  "transient": {
    "logger": {
      "org": {
        "elasticsearch": {
          "index": {
            "fielddata": "TRACE"
          }
        }
      }
    }
  }
}
