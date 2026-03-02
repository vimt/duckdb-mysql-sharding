#include "sharding_catalog.hpp"

#include "sharding_schema_entry.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

ShardingCatalog::ShardingCatalog(AttachedDatabase &db_p, ShardingConfig config_p, const string &config_path_p,
                                 const string &cache_path_p, int64_t cache_ttl_p)
    : Catalog(db_p), config(std::move(config_p)), schema_mgr(cache_path_p, config.host_list, cache_ttl_p),
      attach_path(config_path_p) {

	if (schema_mgr.HasCachedData()) {
		Printer::Print(StringUtil::Format("[mysql_sharding] Using cached schema (%d physical tables)",
		                                  schema_mgr.GetPhysicalTableCount()));
	} else {
		schema_mgr.DiscoverSchemas();
	}
}

ShardingCatalog::~ShardingCatalog() = default;

void ShardingCatalog::Initialize(bool load_builtin) {
}

// ---------------------------------------------------------------------------
// Refresh
// ---------------------------------------------------------------------------

void ShardingCatalog::ClearCacheAndRefresh() {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.clear();
	}
	schema_mgr.ClearAll();
	schema_mgr.DiscoverSchemas();
	Printer::Print("[mysql_sharding] Full refresh complete");
}

void ShardingCatalog::RefreshSchema(const string &schema_name) {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.erase(schema_name);
	}
	schema_mgr.ClearColumnsForSchema(schema_name);
	schema_mgr.LoadColumnsForSchema(schema_name);
	Printer::Print(StringUtil::Format("[mysql_sharding] Schema '%s' refreshed", schema_name));
}

// ---------------------------------------------------------------------------
// Catalog interface
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> ShardingCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto names = schema_mgr.GetSchemaNames();
	lock_guard<mutex> l(schema_lock);
	for (auto &name : names) {
		if (schema_entries.find(name) == schema_entries.end()) {
			CreateSchemaInfo info;
			info.schema = name;
			schema_entries[name] = make_uniq<ShardingSchemaEntry>(*this, info);
		}
		callback(*schema_entries[name]);
	}
}

optional_ptr<SchemaCatalogEntry> ShardingCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto all_schemas = schema_mgr.GetSchemaNames();

	if (schema_name == DEFAULT_SCHEMA) {
		if (all_schemas.empty()) {
			if (if_not_found != OnEntryNotFound::RETURN_NULL) {
				throw BinderException("No schemas available in MySQL sharding catalog");
			}
			return nullptr;
		}
		schema_name = all_schemas[0];
	}

	bool found = false;
	for (auto &s : all_schemas) {
		if (s == schema_name) {
			found = true;
			break;
		}
	}
	if (!found) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema with name \"%s\" not found", schema_name);
		}
		return nullptr;
	}

	lock_guard<mutex> l(schema_lock);
	if (schema_entries.find(schema_name) == schema_entries.end()) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		schema_entries[schema_name] = make_uniq<ShardingSchemaEntry>(*this, info);
	}
	return schema_entries[schema_name].get();
}

PhysicalOperator &ShardingCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                     PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                               optional_ptr<PhysicalOperator>) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                               PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                               PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
unique_ptr<LogicalOperator> ShardingCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
                                                              unique_ptr<LogicalOperator>) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}

DatabaseSize ShardingCatalog::GetDatabaseSize(ClientContext &) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool ShardingCatalog::InMemory() {
	return false;
}

string ShardingCatalog::GetDBPath() {
	return attach_path;
}

} // namespace duckdb
