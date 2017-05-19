#include <Rcpp.h>
#include <cassandra.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/regex.hpp>
#include <boost/variant.hpp>
#include <boost/variant/get.hpp>
#include <cstring>
#include "rdrcass.h"

typedef std::vector<int> IntVec;
typedef std::vector<bool> BoolVec;
typedef std::vector<double> DoubleVec;
typedef std::vector <std::string> StringVect;
typedef std::vector <std::string> NaVec;
typedef boost::variant <StringVect, BoolVec, IntVec, DoubleVec, NaVec> ResultValues;

std::string rdrcass::version_query =
  "SELECT release_version FROM system.local";

std::string rdrcass::column_type_query = "SELECT %columns% FROM system_schema.columns "
"WHERE keyspace_name ='%keyspace_name%' AND table_name='%table_name%';";
std::string rdrcass::simple_query_string = "SELECT %fields% from %from%";

//TODO CassQuery struct to hold query, return result and result_set?

rdrcass::rdrcass() {
  std::vector <std::string> contact_points;
  contact_points[0] = "127.0.0.1";
  connect(contact_points);
}

rdrcass::rdrcass(std::vector <std::string> contact_points) {
  connect(contact_points);
}

void rdrcass::connect(std::vector <std::string> contact_points) {
  CassError return_code;
  cluster = cass_cluster_new();
  session = cass_session_new();
  std::string cont_pts = boost::algorithm::join(contact_points, ",");
  cass_cluster_set_contact_points(cluster, cont_pts.c_str());
  future = cass_session_connect(session, cluster);
  cass_future_wait(future);
  return_code = cass_future_error_code(future);
  if (return_code != CASS_OK) {
    std::string error = "Error while connecting to cluster: ";
    stopOnError(error);
  }
  cass_future_free(future);
  future = NULL;
}

rdrcass::~rdrcass() {
  if (future != NULL) {
    cass_future_free(future);
  }
  cass_session_free(session);
  cass_cluster_free(cluster);
  future = NULL;
  session = NULL;
  cluster = NULL;
}

void rdrcass::stopOnError(std::string error) {
  const char *error_desc;
  size_t size;
  cass_future_error_message(future, &error_desc, &size);
  error += std::string(error_desc);
  Rcpp::stop(error);
}

std::string rdrcass::cassandraErrorCodeToString(CassError error){
  std::string error_string = "RCPP_CASS_UNKNOWN";
  if (CASS_OK) {
      error_string = "CASS_OK";
  } 
  if (CASS_ERROR_LIB_BAD_PARAMS) {
    error_string = "CASS_ERROR_LIB_BAD_PARAMS";
  }
  if (CASS_ERROR_LIB_NO_STREAMS) {
    error_string = "CASS_ERROR_LIB_NO_STREAMS";
  }
  if (CASS_ERROR_LIB_UNABLE_TO_INIT) {
    error_string = "CASS_ERROR_LIB_UNABLE_TO_INIT";
  }
  if (CASS_ERROR_LIB_MESSAGE_ENCODE) {
    error_string = "CASS_ERROR_LIB_MESSAGE_ENCODE";
  }
  if (CASS_ERROR_LIB_HOST_RESOLUTION) {
    error_string = "CASS_ERROR_LIB_HOST_RESOLUTION";
  }
  if (CASS_ERROR_LIB_UNEXPECTED_RESPONSE) {
    error_string = "CASS_ERROR_LIB_UNEXPECTED_RESPONSE";
  }
  if (CASS_ERROR_LIB_REQUEST_QUEUE_FULL) {
    error_string = "CASS_ERROR_LIB_REQUEST_QUEUE_FULL";
  }
  if (CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD) {
    error_string = "CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD";
  }
  if (CASS_ERROR_LIB_WRITE_ERROR) {
    error_string = "CASS_ERROR_LIB_WRITE_ERROR";
  }
  if (CASS_ERROR_LIB_NO_HOSTS_AVAILABLE) {
    error_string = "CASS_ERROR_LIB_NO_HOSTS_AVAILABLE";
  }
  if (CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS) {
    error_string = "CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS";
  }
  if (CASS_ERROR_LIB_INVALID_ITEM_COUNT) {
    error_string = "CASS_ERROR_LIB_INVALID_ITEM_COUNT";
  }
  if (CASS_ERROR_LIB_INVALID_VALUE_TYPE) {
    error_string = "CASS_ERROR_LIB_INVALID_VALUE_TYPE";
  }
  if (CASS_ERROR_LIB_REQUEST_TIMED_OUT) {
    error_string = "CASS_ERROR_LIB_REQUEST_TIMED_OUT";
  }
  if (CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE) {
    error_string = "CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE";
  }
  if (CASS_ERROR_LIB_CALLBACK_ALREADY_SET) {
    error_string = "CASS_ERROR_LIB_CALLBACK_ALREADY_SET";
  }
  if (CASS_ERROR_LIB_INVALID_STATEMENT_TYPE){
    error_string = "CASS_ERROR_LIB_INVALID_STATEMENT_TYPE";
  }
  if (CASS_ERROR_LIB_NAME_DOES_NOT_EXIST) {
    error_string = "CASS_ERROR_LIB_NAME_DOES_NOT_EXIST";
  }
  if (CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL) {
    error_string = "CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL";
  }
  if (CASS_ERROR_LIB_NULL_VALUE) {
    error_string = "CASS_ERROR_LIB_NULL_VALUE";
  }
  if (CASS_ERROR_LIB_NOT_IMPLEMENTED) {
    error_string = "CASS_ERROR_LIB_NOT_IMPLEMENTED";
  }
  if (CASS_ERROR_LIB_UNABLE_TO_CONNECT) {
    error_string = "CASS_ERROR_LIB_UNABLE_TO_CONNECT";
  }
  if (CASS_ERROR_LIB_UNABLE_TO_CLOSE) {
    error_string = "CASS_ERROR_LIB_UNABLE_TO_CLOSE";
  }
  if (CASS_ERROR_SERVER_SERVER_ERROR) {
    error_string = "CASS_ERROR_SERVER_SERVER_ERROR";
  }
  if (CASS_ERROR_SERVER_PROTOCOL_ERROR) {
    error_string = "CASS_ERROR_SERVER_PROTOCOL_ERROR";
  }
  if (CASS_ERROR_SERVER_BAD_CREDENTIALS) {
    error_string = "CASS_ERROR_SERVER_BAD_CREDENTIALS";
  }
  if (CASS_ERROR_SERVER_UNAVAILABLE) {
    error_string = "CASS_ERROR_SERVER_UNAVAILABLE";
  }
  if (CASS_ERROR_SERVER_OVERLOADED) {
    error_string = "CASS_ERROR_SERVER_OVERLOADED";
  }
  if (CASS_ERROR_SERVER_IS_BOOTSTRAPPING) {
    error_string = "CASS_ERROR_SERVER_IS_BOOTSTRAPPING";
  }
  if (CASS_ERROR_SERVER_TRUNCATE_ERROR) {
    error_string = "CASS_ERROR_SERVER_TRUNCATE_ERROR";
  }
  if (CASS_ERROR_SERVER_WRITE_TIMEOUT) {
    error_string = "CASS_ERROR_SERVER_WRITE_TIMEOUT";
  }
  if (CASS_ERROR_SERVER_READ_TIMEOUT) {
    error_string = "CASS_ERROR_SERVER_READ_TIMEOUT";
  }
  if (CASS_ERROR_SERVER_SYNTAX_ERROR) {
    error_string = "CASS_ERROR_SERVER_SYNTAX_ERROR";
  }
  if (CASS_ERROR_SERVER_UNAUTHORIZED) {
    error_string = "CASS_ERROR_SERVER_UNAUTHORIZED";
  }
  if (CASS_ERROR_SERVER_INVALID_QUERY) {
    error_string = "CASS_ERROR_SERVER_INVALID_QUERY";
  }
  if (CASS_ERROR_SERVER_CONFIG_ERROR) {
    error_string = "CASS_ERROR_SERVER_CONFIG_ERROR";
  }
  if (CASS_ERROR_SERVER_ALREADY_EXISTS) {
    error_string = "CASS_ERROR_SERVER_ALREADY_EXISTS";
  }
  if (CASS_ERROR_SERVER_UNPREPARED) {
    error_string = "CASS_ERROR_SERVER_UNPREPARED";
  }
  if (CASS_ERROR_SSL_INVALID_CERT) {
    error_string = "CASS_ERROR_SSL_INVALID_CERT";
  }
  if (CASS_ERROR_SSL_INVALID_PRIVATE_KEY) {
    error_string = "CASS_ERROR_SSL_INVALID_PRIVATE_KEY";
  }
  if (CASS_ERROR_SSL_NO_PEER_CERT) {
    error_string = "CASS_ERROR_SSL_NO_PEER_CERT";
  }
  if (CASS_ERROR_SSL_INVALID_PEER_CERT) {
    error_string = "CASS_ERROR_SSL_INVALID_PEER_CERT";
  }
  if (CASS_ERROR_SSL_IDENTITY_MISMATCH) {
    error_string = "CASS_ERROR_SSL_IDENTITY_MISMATCH; Query executed;";
  }
  return error_string;
}

CassError rdrcass::executeQuery(std::string query, const CassResult** result) {
  CassError return_code;
  CassStatement *statement = cass_statement_new(query.c_str(), 0);
  future = cass_session_execute(session, statement);
  cass_future_wait(future);
  return_code = cass_future_error_code(future);
  
  if (return_code != CASS_OK) {
    std::string error = "Error while executing query. ";
    stopOnError(error);
  }
  *result = cass_future_get_result(future);
  
  cass_future_free(future);
  cass_statement_free(statement);
  future = NULL;
  return return_code;
}

SEXP rdrcass::query(std::string query) {
  const CassResult* result_set;
  CassError return_code = executeQuery(query, &result_set);
  if (return_code != CASS_OK) {
    std::string error = "Error while executing query. ";
    stopOnError(error);
  }
  std::string return_code_string = rdrcass::cassandraErrorCodeToString(return_code);
  return Rcpp::wrap(return_code_string);
}

SEXP rdrcass::getReleaseVersion() {
  const char *release_version;
  size_t release_version_length;
  const CassResult* result_set;
  executeQuery(rdrcass::version_query.c_str(), &result_set);
  const CassRow *row = cass_result_first_row(result_set);
  if (row) {
    const CassValue *value = cass_row_get_column_by_name(row,
                                                         "release_version");
    cass_value_get_string(value, &release_version, &release_version_length);
  }
  
  cass_result_free(result_set);
  
  return Rcpp::wrap(release_version);
}

CassValueType rdrcass::cassTypeFromTypeName(const char *type_name) {
  if (std::strcmp(type_name, "int") == 0) {
    return CASS_VALUE_TYPE_INT;
  }
  if (std::strcmp(type_name, "float") == 0) {
    return CASS_VALUE_TYPE_FLOAT;
  }
  if (std::strcmp(type_name, "double") == 0) {
    return CASS_VALUE_TYPE_DOUBLE;
  }
  if (std::strcmp(type_name, "text") == 0) {
    return CASS_VALUE_TYPE_TEXT;
  }
  if (std::strcmp(type_name, "varchar") == 0) {
    return CASS_VALUE_TYPE_VARCHAR;
  }
  return CASS_VALUE_TYPE_UNKNOWN;
}

std::map <std::string, CassValueType> rdrcass::getTableColumnsInfo(std::string from) {
  //TODO cache this info
  const CassResult* result_set;
  std::map <std::string, CassValueType> columns_info;
  std::vector <std::string> keyspace_table;
  
  boost::split(keyspace_table, from, boost::is_any_of("\\. "));
  
  std::string query_string = rdrcass::column_type_query;
  boost::replace_all(query_string, "%columns%", "column_name,type");
  boost::replace_all(query_string, "%keyspace_name%", keyspace_table[0].c_str());
  boost::replace_all(query_string, "%table_name%", keyspace_table[1].c_str());
  
  executeQuery(query_string.c_str(), &result_set);
  CassIterator* result_iter = cass_iterator_from_result(result_set);
  
  while (cass_iterator_next(result_iter)) {
    const char* column_name;
    size_t column_name_length;
    const char* type_name;
    size_t type_name_length;
    const CassRow* row = cass_iterator_get_row(result_iter);
    cass_value_get_string(cass_row_get_column_by_name(row, "column_name"),
                          &column_name, &column_name_length);
    cass_value_get_string(cass_row_get_column_by_name(row, "type"),
                          &type_name, &type_name_length);
    columns_info[column_name] = cassTypeFromTypeName(type_name);
    
  }
  cass_iterator_free(result_iter);
  cass_result_free(result_set);
  return columns_info;
}

Rcpp::DataFrame rdrcass::select(std::string from) {
   //TODO: check 'from' for correct syntax
   const CassResult* result_set;
   cass_int32_t int_value;
   cass_bool_t bool_value;
   cass_float_t float_value;
   cass_double_t double_value;
   const char* string_value;
   size_t string_value_length;

   std::vector <std::string> col_names;
   std::vector<int> row_names;

   std::map <std::string, CassValueType> columns_info = getTableColumnsInfo(from);
   Rcpp::List data(columns_info.size());

   std::map <std::string, ResultValues> results;

   for (auto ci: columns_info) {
       col_names.push_back(ci.first);
       switch (ci.second) {
           case CASS_VALUE_TYPE_INT:
               results[ci.first] = IntVec();
               break;
           case CASS_VALUE_TYPE_FLOAT:
               results[ci.first] = DoubleVec();
               break;
           case CASS_VALUE_TYPE_DOUBLE:
               results[ci.first] = DoubleVec();
               break;
           case CASS_VALUE_TYPE_BOOLEAN:
               results[ci.first] = BoolVec();
               break;
           case CASS_VALUE_TYPE_TEXT:
           case CASS_VALUE_TYPE_ASCII:
           case CASS_VALUE_TYPE_VARCHAR:
               results[ci.first] = StringVect();
               break;
           default:
               results[ci.first] = NaVec();
               break;
       }//switch column type
   }
   std::string query_string = rdrcass::simple_query_string;
   boost::replace_all(query_string, "%fields%", "*");
   boost::replace_all(query_string, "%from%", from);
   executeQuery(query_string.c_str(), &result_set);

   CassIterator* result_iter = cass_iterator_from_result(result_set);
   while (cass_iterator_next(result_iter)) {
       const CassRow *row = cass_iterator_get_row(result_iter);
       for (auto ci: columns_info) {
           const CassValue *value = cass_row_get_column_by_name(row, ci.first.c_str());
           CassValueType type = cass_value_type(value);
           switch (type) {
               case CASS_VALUE_TYPE_INT: {
                   cass_value_get_int32(value, &int_value);
                   IntVec &v = boost::get<IntVec>(results[ci.first]);
                   v.push_back(int_value);
               }
                   break;
               case CASS_VALUE_TYPE_BOOLEAN: {
                   cass_value_get_bool(value, &bool_value);
                   BoolVec &v = boost::get<BoolVec>(results[ci.first]);
                   v.push_back(bool_value);
               }
                   break;
               case CASS_VALUE_TYPE_TEXT:
               case CASS_VALUE_TYPE_ASCII:
               case CASS_VALUE_TYPE_VARCHAR: {
                   cass_value_get_string(value, &string_value, &string_value_length);
                   StringVect &v = boost::get<StringVect>(results[ci.first]);
                   v.push_back(string_value);
               }
                   break;
               case CASS_VALUE_TYPE_FLOAT: {
                   cass_value_get_float(value, &float_value);
                   DoubleVec &v = boost::get<DoubleVec>(results[ci.first]);
                   v.push_back(static_cast<double>(float_value));
               }
                   break;
               case CASS_VALUE_TYPE_DOUBLE: {
                   cass_value_get_double(value, &double_value);
                   DoubleVec &v = boost::get<DoubleVec>(results[ci.first]);
                   v.push_back(double_value);
               }
                   break;
               default: {
                   NaVec &v = boost::get<NaVec>(results[ci.first]);
                   v.push_back("NA");
               }
                   break;
           }//switch column type
       }//each column in row
   }//each row in column set
   cass_iterator_free(result_iter);
   cass_result_free(result_set);

   //TODO: this can be done while adding results
   int col_index = 0;
   for (auto ci: columns_info) {
       switch (ci.second) {
           case CASS_VALUE_TYPE_INT: {
               data[col_index] = boost::get<IntVec>(results[ci.first]);
           }
               break;
           case CASS_VALUE_TYPE_BOOLEAN: {
               data[col_index] = boost::get<BoolVec>(results[ci.first]);
           }
               break;
           case CASS_VALUE_TYPE_TEXT:
           case CASS_VALUE_TYPE_ASCII:
           case CASS_VALUE_TYPE_VARCHAR: {
               data[col_index] = boost::get<StringVect>(results[ci.first]);
           }
               break;
           case CASS_VALUE_TYPE_FLOAT:
           case CASS_VALUE_TYPE_DOUBLE: {
               data[col_index] = boost::get<DoubleVec>(results[ci.first]);
           }
               break;
           default: {
               data[col_index] = boost::get<NaVec>(results[ci.first]);
           }
               break;
       }//switch
       ++col_index;
   }

   data.attr("names") = col_names;
   data.attr("row.names") = row_names;

   return Rcpp::DataFrame(data);
}

RCPP_MODULE(Cassandra) {
  Rcpp::class_<rdrcass>("Cassandra")
    .constructor("default constructor")
    .constructor < std::vector < std::string > > ("constructor with hosts")
    .method("getReleaseVersion", &rdrcass::getReleaseVersion,
    "Returns cassandra release verion")
    .method("select", &rdrcass::select, "SELECT * FROM 'keyspace.table';")
    .method("query", &rdrcass::query, "Runs raw query on cassandra cluster");
}


