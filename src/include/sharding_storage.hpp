#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class ShardingStorageExtension : public StorageExtension {
public:
	ShardingStorageExtension();
};

} // namespace duckdb
