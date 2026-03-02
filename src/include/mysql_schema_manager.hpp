#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "sharding_config.hpp"

namespace duckdb {

class MySQLSchemaManager {
public:
	MySQLSchemaManager(const string &cache_path, const vector<string> &host_list, int64_t cache_ttl);
	~MySQLSchemaManager();

	//! Returns true if cached data is available and not expired
	bool HasCachedData();

	//! Discover table metadata from all MySQL hosts, write to cache DB
	void DiscoverSchemas();

	//! Load column info for a schema from MySQL into cache DB (skips if already present)
	void LoadColumnsForSchema(const string &schema_name);

	//! Full refresh: clear all data and re-discover from MySQL
	void ClearAll();

	//! Delete columns for a schema (will be re-loaded lazily)
	void ClearColumnsForSchema(const string &schema_name);

	// -- Read operations --

	vector<string> GetSchemaNames();
	vector<string> GetTableNames(const string &schema_name);
	LogicTableInfo GetLogicTable(const string &schema_name, const string &table_name);
	bool HasColumnsForSchema(const string &schema_name);
	int64_t GetPhysicalTableCount();

private:
	void InitDB();

	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;
	mutex lock;
	vector<string> host_list;
	int64_t cache_ttl;
};

} // namespace duckdb
