#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct ShardingConfig {
	vector<string> host_list;
	bool is_file = false; // true if parsed from a file, false if from a direct string

	static ShardingConfig Parse(const string &input);
};

struct ColumnInfo {
	string name;
	string type_name;
	string column_type;
	bool is_nullable;
	int64_t precision;
	int64_t scale;
};

struct PhysicalTableInfo {
	string host;
	string db_name;
	string table_name;
	int64_t rows_count = 0;
	int db_index = -1;
	int table_index = -1;

	string LogicDb() const;
	string LogicTable() const;
};

struct LogicTableInfo {
	string logic_db;
	string logic_table;
	vector<PhysicalTableInfo> physical_tables;
	vector<ColumnInfo> columns;
};

pair<string, int> SplitSharding(const string &name);
string ToSharding(const string &base, int index);

} // namespace duckdb
