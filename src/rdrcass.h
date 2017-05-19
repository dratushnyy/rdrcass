#ifndef RDR_CAS_H
#define RDR_CAS_H

#include <vector>
#include <map>
#include <cassandra.h>
#include <string>
#include <Rcpp.h>
#include <R.h>

class rdrcass {
private:
  CassCluster *cluster;
  CassSession *session;
  CassFuture *future;
  static std::string version_query;
  static std::string column_type_query;
  static std::string simple_query_string;
  
  void stopOnError(std::string error);
  static std::string cassandraErrorCodeToString(CassError err);
  
  CassError executeQuery(std::string query,  const CassResult** result);

  void connect(std::vector <std::string> contact_points);
  
  CassValueType cassTypeFromTypeName(const char *type_name);
  
  std::map <std::string, CassValueType> getTableColumnsInfo(std::string);
  
  
public:
  rdrcass();
  rdrcass(std::vector <std::string> contact_points);
  ~rdrcass();
  
  SEXP getReleaseVersion();
  
  Rcpp::DataFrame select(std::string from);
  
  SEXP query(std::string query);
};

#endif //RDR_CAS_H
