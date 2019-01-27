#!/usr/local/bin/python3

from datetime import datetime

SLEEP_BETWEEN_EXPERIMENTS = 5  # seconds to wait after completing an experiment before the next one

INSERT_INTERVAL = 1  # During aggs experiment, wait 1s between inserts. Ensures recompute of global ordinals
NUM_AGG_THREADS = 1  # X threads running parallel aggregations.
TIME_UNTIL_NEXT_AGG_IN_THREAD = (0, 1)  # choose a random number in this range to wait before next agg on each thread


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

