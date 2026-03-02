#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "mysql_schema_manager.hpp"
#include "sharding_config.hpp"

namespace duckdb {
class ShardingSchemaEntry;

class ShardingCatalog : public Catalog {
public:
	ShardingCatalog(AttachedDatabase &db_p, ShardingConfig config, const string &config_path, const string &cache_path,
	                int64_t cache_ttl);
	~ShardingCatalog();

	ShardingConfig config;
	MySQLSchemaManager schema_mgr;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "mysql_sharding";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCacheAndRefresh();
	void RefreshSchema(const string &schema_name);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	unordered_map<string, unique_ptr<ShardingSchemaEntry>> schema_entries;
	mutex schema_lock;
	string attach_path;
};

} // namespace duckdb
