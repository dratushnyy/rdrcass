## [Travis CI](https://travis-ci.org/)
[![Build Status](https://travis-ci.org/dratushnyy/rdrcass.svg?branch=master)](https://travis-ci.org/dratushnyy/rdrcass)


# rdrcass
rdrcass is a R client for Apache Cassandra  based on Rcpp and Datastax c/c++ driver

## Dependencies
* [Libuv](https://github.com/libuv/libuv)
* [Datastax c/c++ driver](https://github.com/datastax/cpp-driver)
* [Rcpp](https://github.com/RcppCore/Rcpp)

## Supported Cassandra versions
  * 3.0 and greater
  
## Supported CQL
  * SELECT * FROM 'keyspace.table'; with "select" function (see demo/query_select.R)
  * Raw queries with "query" function  (see demo/query_select.R)
  
  
## Supported types:
  * Integer
  * Boolean
  * String, Text
  * Double
  
## Notes
* Libuv(required by Datastax c/c++ driver) and Datastax c/c++ driver
should be installed before pakage installation
* Types not supported on 'select' will return NA

## License
MIT
