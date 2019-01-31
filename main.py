#!/usr/local/bin/python
import argparse
from datetime import datetime

from single_experiment_executor import run_experiment
from setup_experiments import setup_cardinaltiy_experiment
from populate_cardinality_index import bulk_insert_high_cardinality_documents

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


# Parse the command line options, to determine which section(s) of the code will be executed
# and connect to Elasticsearch
def initial_setup():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default='all', metavar='',
                        help='all | populate_only | experiments_only')

    args = parser.parse_args()
    print('Executing %s with mode=%s' % (parser.prog, args.mode))

    return args


# The following function steps through the experiments that we have defined, and
# sets the cluster to the required state for the required amount of time.
# Additionally, this will periodically insert a new document into Elasticsearch to force a rebuild
# of the global ordinals.
def step_through_experiment_configurations():

    experiments_to_run = setup_cardinaltiy_experiment()

    print("Starting experiment configuration control thread\n")
    for experiment in experiments_to_run:
        run_experiment(experiment)


def main():

    args = initial_setup()

    if args.mode == 'all' or args.mode == 'populate_only':
        bulk_insert_high_cardinality_documents()

    if args.mode == 'all' or args.mode == 'experiments_only':
        step_through_experiment_configurations()


main()
