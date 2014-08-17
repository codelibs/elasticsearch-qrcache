Elasticsearch QRCache Plugin
=======================

## Overview

QRCache Plugin provides a feature of Query Result Cache for Elasticsearch.

## Version

| Taste     | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.3.X         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-qrcache/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install QRCache Plugin

(Not released yet...)

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-qrcache/x.x.x

## References

### Enable Query Result Cache for Index

Query Result Cache is enabled if index.cache.query_result.enable is set to true.

    curl -XPOST 'localhost:9200/my_index/_close'
    curl -XPUT 'localhost:9200/my_index/_settings?index.cache.query_result.enable=true'
    curl -XPOST 'localhost:9200/my_index/_open'
