#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ShardingExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override {
		return "mysql_sharding";
	}
};

} // namespace duckdb
