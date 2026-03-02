#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "sharding_config.hpp"

namespace duckdb {

class ShardingTableEntry : public TableCatalogEntry {
public:
	ShardingTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                   LogicTableInfo logic_table);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	LogicTableInfo logic_table_info;
};

} // namespace duckdb
