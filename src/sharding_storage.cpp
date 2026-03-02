#include "sharding_storage.hpp"

#include "sharding_catalog.hpp"
#include "sharding_config.hpp"
#include "sharding_transaction_manager.hpp"
#include "mysql_utils.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

static unique_ptr<Catalog> ShardingAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                           AttachedDatabase &db, const string &name, AttachInfo &info,
                                           AttachOptions &attach_options) {
	auto &db_config = DBConfig::GetConfig(context);
	if (!db_config.options.enable_external_access) {
		throw PermissionException("Attaching MySQL sharding databases is disabled through configuration");
	}

	string input = info.path;
	if (input.empty()) {
		throw BinderException("MySQL sharding requires a host list. "
		                      "Usage: ATTACH 'host=... user=... ...' AS name (TYPE mysql_sharding)  or  "
		                      "ATTACH '/path/to/hosts.conf' AS name (TYPE mysql_sharding)");
	}

	auto config = ShardingConfig::Parse(input);

	// Cache path: explicit option, or auto-derive from file path, or empty (no cache) for string mode
	string cache_path;
	auto cache_it = info.options.find("cache_path");
	if (cache_it != info.options.end()) {
		cache_path = cache_it->second.ToString();
	} else if (config.is_file) {
		cache_path = input + ".cache.duckdb";
	}

	// Cache TTL in seconds (0 = no expiry)
	int64_t cache_ttl = 0;
	auto ttl_it = info.options.find("cache_ttl");
	if (ttl_it != info.options.end()) {
		cache_ttl = ttl_it->second.GetValue<int64_t>();
	}

	attach_options.access_mode = AccessMode::READ_ONLY;

	return make_uniq<ShardingCatalog>(db, std::move(config), input, cache_path, cache_ttl);
}

static unique_ptr<TransactionManager> ShardingCreateTransactionManager(
    optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db, Catalog &catalog) {
	auto &sharding_catalog = catalog.Cast<ShardingCatalog>();
	return make_uniq<ShardingTransactionManager>(db, sharding_catalog);
}

ShardingStorageExtension::ShardingStorageExtension() {
	attach = ShardingAttach;
	create_transaction_manager = ShardingCreateTransactionManager;
}

} // namespace duckdb
