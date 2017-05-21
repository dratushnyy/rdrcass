library(rdrcass)
cass <- new(rdrcass, contact_points=c("10.10.10.104"))

create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS rdrcass_test WITH 
REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };"
create_table_query = "CREATE TABLE IF NOT EXISTS rdrcass_test.test_table(
  test_text varchar,
  test_value int,
PRIMARY KEY (test_text, test_value)
);"
insert_query = "INSERT INTO rdrcass_test.test_table (test_text, test_value)
  VALUES ('Hello from rdrcass', 42);"

cass$query(create_keyspace_query)
cass$query(create_table_query)
cass$query(insert_query)
cass$select(from="rdrcass_test.test_table")
