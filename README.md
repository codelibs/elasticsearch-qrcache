Elasticsearch QRCache Plugin
=======================

## Overview

QRCache Plugin provides a feature of Query Result Cache for Elasticsearch.
This cache is different from [Shard Query Cache](http://www.elasticsearch.org/guide/en/elasticsearch/reference/master/index-modules-shard-query-cache.html).
Shard Query Cache stores a count(hits.total) of a query, not a response(hits).
On the other hand, our Query Result Cache stores the response to a cache.

## Version

| Version   | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.4.X         |
| 1.4.1     | 1.4.2         |
| 1.3.0     | 1.3.2         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-qrcache/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install QRCache Plugin (For 1.4.x)

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-extension/1.4.1
    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-qrcache/1.4.1

### Install QRCache Plugin (For 1.3.x)

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-qrcache/1.3.0

## References

### Enable Query Result Cache for Index

Query Result Cache is enabled if index.cache.query_result.enable is set to true.
(For 1.3, close the index before updating a setting)

    curl -XPUT 'localhost:9200/my_index/_settings?index.cache.query_result.enable=true'

### Check Stats

    curl -XGET 'localhost:9200/_qrc/stats?pretty'

### Clear Cache

    curl -XGET 'localhost:9200/my_index/_qrc/clear?pretty'

### Settings

You can change parameters by elasticsearch.yml.

    indices.cache.query_result.clean_interval: 10s
    indices.cache.query_result.size: 1%
    indices.cache.query_result.expire: 1m

clean_interval is an interval time for purging invalid caches, size is a total cache size and expire is an expire time for each cache.
