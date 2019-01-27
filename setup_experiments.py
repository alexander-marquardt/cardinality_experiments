#!/usr/local/bin/python3

from datetime import datetime

import global_vars

def setup_cardinaltiy_experiment():
    
    experiments_to_run = []

    start_time_string = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    print("start %s" % start_time_string)

    FAST_REFRESH = 1  # in experiment mode, every X second do a segment refresh (buffer flush)
    SLOW_REFRESH = 60  # in experiment mode, every X seconds do a segment refresh (buffer flush)
    EXPERIMENT_DURATION = 10  # Each experiment will last this many seconds. Note that we execute multiple experiments.

    EXPERIMENT_PARAMETERS = [{"refresh_interval":  '%ds' % FAST_REFRESH,
                              "eager_global_ordinals": False,
                              "request_cache_bool": True},
                             {"refresh_interval":  '%ds' % FAST_REFRESH,
                              "eager_global_ordinals": True,
                              "request_cache_bool": True},
                             {"refresh_interval": '%ds' % SLOW_REFRESH,
                              "eager_global_ordinals": False,
                              "request_cache_bool": True},
                             {"refresh_interval": '%ds' % SLOW_REFRESH,
                              "eager_global_ordinals": True,
                              "request_cache_bool": True},
                             {"refresh_interval":  '%ds' % FAST_REFRESH,
                              "eager_global_ordinals": False,
                              "request_cache_bool": False},
                             {"refresh_interval":  '%ds' % FAST_REFRESH,
                              "eager_global_ordinals": True,
                              "request_cache_bool": False},
                             {"refresh_interval": '%ds' % SLOW_REFRESH,
                              "eager_global_ordinals": False,
                              "request_cache_bool": False},
                             {"refresh_interval": '%ds' % SLOW_REFRESH,
                              "eager_global_ordinals": True,
                              "request_cache_bool": False},
                             ]
    
    for exp_obj in EXPERIMENT_PARAMETERS:
        eager_global_ordinals = exp_obj['eager_global_ordinals']
        refresh_interval = exp_obj['refresh_interval']
        request_cache_bool = exp_obj['request_cache_bool']
        result_index = 'experiment-%s-cache-%s-refresh-%s-eager-%s' % (
            start_time_string, request_cache_bool, refresh_interval, eager_global_ordinals)
        result_index = result_index.lower()
    
        experiments_to_run.append({
            "description": "Cache: %s Refresh: %s Eager: %s" % (
            request_cache_bool, refresh_interval, eager_global_ordinals),
            "refresh_interval": refresh_interval,
            "eager_global_ordinals": eager_global_ordinals,
            "request_cache_bool": request_cache_bool,
            "experiment_duration_in_seconds": EXPERIMENT_DURATION,
            'result_index': result_index,
            'experiment_id': 'Refresh=%s Eager=%s Cache=%s' % (refresh_interval, eager_global_ordinals, request_cache_bool)
        })
    
    return experiments_to_run

