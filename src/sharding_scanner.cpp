#include "sharding_scanner.hpp"

#include "sharding_table_entry.hpp"
#include "mysql_connection.hpp"
#include "mysql_result.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"
#include "mysql_filter_pushdown.hpp"

namespace duckdb {

ShardingBindData::ShardingBindData(ShardingTableEntry &table) : table(table) {
}

struct HostQuery {
	string connection_string;
	string sql;
};

struct ShardingGlobalState : public GlobalTableFunctionState {
	vector<HostQuery> queries;
	idx_t current_query_idx = 0;
	unique_ptr<MySQLConnection> current_connection;
	unique_ptr<MySQLResult> current_result;
	bool done = false;

	~ShardingGlobalState() override {
		current_result.reset();
		current_connection.reset();
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct ShardingLocalState : public LocalTableFunctionState {
};

static unique_ptr<FunctionData> ShardingBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("ShardingBind should not be called directly");
}

static unique_ptr<GlobalTableFunctionState> ShardingInitGlobalState(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ShardingBindData>();
	auto &logic_table = bind_data.table.logic_table_info;

	string column_select;
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			column_select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			column_select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			column_select += MySQLUtils::WriteIdentifier(col.GetName());
		}
	}

	string filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	string where_clause;
	if (!filter_string.empty()) {
		where_clause = " WHERE " + filter_string;
	}

	// Group physical tables by host
	unordered_map<string, vector<const PhysicalTableInfo *>> host_tables;
	for (auto &pt : logic_table.physical_tables) {
		host_tables[pt.host].push_back(&pt);
	}

	auto result = make_uniq<ShardingGlobalState>();

	for (auto &host_pair : host_tables) {
		auto &host = host_pair.first;
		auto &tables = host_pair.second;

		string sql;
		for (idx_t i = 0; i < tables.size(); i++) {
			if (i > 0) {
				sql += " UNION ALL ";
			}
			sql += "SELECT " + column_select + " FROM ";
			sql += MySQLUtils::WriteIdentifier(tables[i]->db_name);
			sql += ".";
			sql += MySQLUtils::WriteIdentifier(tables[i]->table_name);
			sql += where_clause;
		}

		MySQLConnectionParameters params;
		unordered_set<string> unused;
		std::tie(params, unused) = MySQLUtils::ParseConnectionParameters(host);
		Printer::Print(StringUtil::Format("[mysql_sharding] Host=%s Tables=%d SQL: %s", params.host, tables.size(),
		                                  sql.substr(0, 200) + (sql.size() > 200 ? "..." : "")));

		HostQuery hq;
		hq.connection_string = host;
		hq.sql = sql;
		result->queries.push_back(std::move(hq));
	}

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ShardingInitLocalState(ExecutionContext &, TableFunctionInitInput &,
                                                                    GlobalTableFunctionState *) {
	return make_uniq<ShardingLocalState>();
}

static void ShardingScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ShardingGlobalState>();

	while (!gstate.done) {
		if (gstate.current_result && !gstate.current_result->Exhausted()) {
			DataChunk &res_chunk = gstate.current_result->NextChunk();
			if (res_chunk.size() == 0) {
				continue;
			}
			D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());

			string error;
			for (idx_t c = 0; c < output.ColumnCount(); c++) {
				Vector &output_vec = output.data[c];
				Vector &res_vec = res_chunk.data[c];

				switch (output_vec.GetType().id()) {
				case LogicalTypeId::BOOLEAN:
				case LogicalTypeId::TINYINT:
				case LogicalTypeId::UTINYINT:
				case LogicalTypeId::SMALLINT:
				case LogicalTypeId::USMALLINT:
				case LogicalTypeId::INTEGER:
				case LogicalTypeId::UINTEGER:
				case LogicalTypeId::BIGINT:
				case LogicalTypeId::UBIGINT:
				case LogicalTypeId::FLOAT:
				case LogicalTypeId::DOUBLE:
				case LogicalTypeId::BLOB:
				case LogicalTypeId::DATE:
				case LogicalTypeId::TIME:
				case LogicalTypeId::TIMESTAMP: {
					if (output_vec.GetType().id() == res_vec.GetType().id() ||
					    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
					     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
						output_vec.Reinterpret(res_vec);
					} else {
						VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
					}
					break;
				}
				default: {
					VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
					break;
				}
				}
				if (!error.empty()) {
					throw BinderException(error);
				}
			}
			output.SetCardinality(res_chunk.size());
			return;
		}

		if (gstate.current_query_idx >= gstate.queries.size()) {
			gstate.done = true;
			output.SetCardinality(0);
			return;
		}

		gstate.current_result.reset();
		gstate.current_connection.reset();

		auto &query = gstate.queries[gstate.current_query_idx++];

		MySQLTypeConfig type_config(context);
		auto conn = MySQLConnection::Open(type_config, query.connection_string, "");
		gstate.current_connection = make_uniq<MySQLConnection>(std::move(conn));
		gstate.current_result =
		    gstate.current_connection->Query(query.sql, MySQLResultStreaming::FORCE_MATERIALIZATION);
	}

	output.SetCardinality(0);
}

static InsertionOrderPreservingMap<string> ShardingToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<ShardingBindData>();
	result["Table"] = bind_data.table.name;
	result["Physical Tables"] =
	    StringUtil::Format("%d", bind_data.table.logic_table_info.physical_tables.size());
	return result;
}

static void ShardingScanSerialize(Serializer &, const optional_ptr<FunctionData>, const TableFunction &) {
	throw NotImplementedException("ShardingScanSerialize");
}

static unique_ptr<FunctionData> ShardingScanDeserialize(Deserializer &, TableFunction &) {
	throw NotImplementedException("ShardingScanDeserialize");
}

static BindInfo ShardingGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ShardingBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

TableFunction CreateShardingScanFunction() {
	TableFunction func("mysql_sharding_scan_internal", {}, ShardingScan, ShardingBind, ShardingInitGlobalState,
	                   ShardingInitLocalState);
	func.to_string = ShardingToString;
	func.serialize = ShardingScanSerialize;
	func.deserialize = ShardingScanDeserialize;
	func.get_bind_info = ShardingGetBindInfo;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

} // namespace duckdb
