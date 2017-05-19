library(RcppCassandra)
cass <- new(Cassandra, contact_points=c("10.10.10.104"))
cass$getReleaseVersion()
cass = NULL
