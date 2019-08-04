import global_vars
# Settings for the index which we run terms aggregations against. We disable caching to isolate the impact
# of the global ordinals calculations.

CLUSTER_SETTINGS_FOR_LOGGING_GLOBAL_ORDINAL_TOOK_TIME = {
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

CARDINALITY_INDEX_SETTINGS_FOR_POPULATE = {
    'index': {
        'refresh_interval': '10s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
    }
}

CARDINALITY_INDEX_MAPPINGS_FOR_POPULATE = {
    'properties': {
        'timestamp': {
            'type': 'date'
        },
        global_vars.HIGH_CARDINALITY_FIELD_NAME: {
            'type': 'keyword',
            'ignore_above': 256,
            'eager_global_ordinals': True # the default is False, set it explicitly here
        }
    }
}

CARDINALITY_INDEX_SETTINGS_AFTER_POPULATE = {
    'index': {
        'refresh_interval': '1s',
    }
}

CARDINALITY_INDEX_MAPPINGS_AFTER_POPULATE = {
    'properties': {
        global_vars.HIGH_CARDINALITY_FIELD_NAME: {
            'type': 'keyword',
            'ignore_above': 256,
            'eager_global_ordinals': False  # the default is False, set it explicitly here
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


