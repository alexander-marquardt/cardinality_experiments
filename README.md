# Overview
This code was written to investigate and better understand the performance of high-cardinality terms aggregations and the impact of [global ordinals](https://www.elastic.co/guide/en/elasticsearch/reference/7.3/eager-global-ordinals.html) in Elasticsearch. 

# Blog article on Elastic.co
The results of these experiments is documented in a [blog article on elastic.co](https://www.elastic.co/blog/improving-the-performance-of-high-cardinality-terms-aggregations-in-elasticsearch).

# Description
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
