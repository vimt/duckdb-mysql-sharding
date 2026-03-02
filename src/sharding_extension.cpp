#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "sharding_extension.hpp"
#include "sharding_storage.hpp"
#include "sharding_catalog.hpp"
#include "mysql_utils.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

using namespace duckdb;

// ---------------------------------------------------------------------------
// CALL mysql_sharding_clear_cache('catalog_name')
// CALL mysql_sharding_refresh_schema('catalog_name', 'schema_name')
// ---------------------------------------------------------------------------

struct ShardingRefreshBindData : public TableFunctionData {
	string catalog_name;
	string schema_name;
};

struct ShardingRefreshGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> ShardingRefreshBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ShardingRefreshBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	if (input.inputs.size() > 1) {
		result->schema_name = input.inputs[1].GetValue<string>();
	}
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("message");
	return std::move(result);
}

static ShardingCatalog &GetShardingCatalog(ClientContext &context, const string &catalog_name) {
	auto &db_manager = DatabaseManager::Get(context);
	auto db_ptr = db_manager.GetDatabase(context, catalog_name);
	if (!db_ptr) {
		throw BinderException("Database '%s' not found", catalog_name);
	}
	auto &catalog = db_ptr->GetCatalog();
	if (catalog.GetCatalogType() != "mysql_sharding") {
		throw BinderException("Database '%s' is not a mysql_sharding catalog", catalog_name);
	}
	return catalog.Cast<ShardingCatalog>();
}

static unique_ptr<GlobalTableFunctionState> ShardingRefreshInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<ShardingRefreshGlobalState>();
}

static void ClearCacheExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ShardingRefreshGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &bind_data = data.bind_data->Cast<ShardingRefreshBindData>();
	auto &sharding = GetShardingCatalog(context, bind_data.catalog_name);
	sharding.ClearCacheAndRefresh();

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value("Full cache cleared and re-discovered from MySQL"));
}

static void RefreshSchemaExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ShardingRefreshGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &bind_data = data.bind_data->Cast<ShardingRefreshBindData>();
	auto &sharding = GetShardingCatalog(context, bind_data.catalog_name);
	sharding.RefreshSchema(bind_data.schema_name);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value(StringUtil::Format("Schema '%s' refreshed", bind_data.schema_name)));
}

// ---------------------------------------------------------------------------
// Extension loading
// ---------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	mysql_library_init(0, NULL, NULL);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.storage_extensions["mysql_sharding"] = make_uniq<ShardingStorageExtension>();

	config.AddExtensionOption("mysql_sharding_debug_queries",
	                          "Print all queries sent to MySQL sharding hosts to stdout", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));

	TableFunction clear_cache("mysql_sharding_clear_cache", {LogicalType::VARCHAR}, ClearCacheExecute,
	                          ShardingRefreshBind, ShardingRefreshInitGlobal);
	loader.RegisterFunction(clear_cache);

	TableFunction refresh_schema("mysql_sharding_refresh_schema", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             RefreshSchemaExecute, ShardingRefreshBind, ShardingRefreshInitGlobal);
	loader.RegisterFunction(refresh_schema);
}

void ShardingExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(mysql_sharding, loader) {
	LoadInternal(loader);
}
}
