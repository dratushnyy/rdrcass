library(rdrcass)
cass <- new(rdrcass, contact_points=c("10.10.10.104"))
cass$getReleaseVersion()
cass = NULL
