#include "sharding_table_entry.hpp"

#include "sharding_scanner.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

ShardingTableEntry::ShardingTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                       LogicTableInfo logic_table)
    : TableCatalogEntry(catalog, schema, info), logic_table_info(std::move(logic_table)) {
}

unique_ptr<BaseStatistics> ShardingTableEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

void ShardingTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                                ClientContext &) {
}

TableFunction ShardingTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<ShardingBindData>(*this);
	for (auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
		result->names.push_back(col.GetName());
	}
	bind_data = std::move(result);
	return CreateShardingScanFunction();
}

TableStorageInfo ShardingTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	idx_t total = 0;
	for (auto &pt : logic_table_info.physical_tables) {
		total += pt.rows_count;
	}
	result.cardinality = total;
	return result;
}

} // namespace duckdb
