#pragma once

#include "duckdb.hpp"
#include "sharding_config.hpp"

namespace duckdb {
class ShardingTableEntry;

struct ShardingBindData : public FunctionData {
	explicit ShardingBindData(ShardingTableEntry &table);

	ShardingTableEntry &table;
	vector<string> names;
	vector<LogicalType> types;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("ShardingBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

// Returns a TableFunction configured for sharding scan (used internally by ShardingTableEntry::GetScanFunction)
TableFunction CreateShardingScanFunction();

} // namespace duckdb
