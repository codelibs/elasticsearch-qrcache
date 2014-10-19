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
| 1.4.0     | 1.4.0.Beta1   |
| 1.3.0     | 1.3.2         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-qrcache/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install QRCache Plugin (For 1.4.x)

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-extension/1.4.0
    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-qrcache/1.4.0

To monitor index refresh event, add the following property to elasticsearch.yml

    engine.filter.refresh: true

### Install QRCache Plugin (For 1.3.x)

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-qrcache/1.3.0

## References

### Enable Query Result Cache for Index

Query Result Cache is enabled if index.cache.query_result.enable is set to true.

    curl -XPOST 'localhost:9200/my_index/_close'
    curl -XPUT 'localhost:9200/my_index/_settings?index.cache.query_result.enable=true'
    curl -XPOST 'localhost:9200/my_index/_open'

### Check Stats

    curl -XGET 'localhost:9200/_qrc/stats?pretty'

### Clear Cache

    curl -XGET 'localhost:9200/my_index/_qrc/clear?pretty'

