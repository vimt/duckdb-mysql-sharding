#include "sharding_transaction.hpp"
#include "sharding_catalog.hpp"

namespace duckdb {

ShardingTransaction::ShardingTransaction(ShardingCatalog &catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), catalog(catalog) {
}

ShardingTransaction::~ShardingTransaction() = default;

void ShardingTransaction::Start() {
}

void ShardingTransaction::Commit() {
}

void ShardingTransaction::Rollback() {
}

ShardingTransaction &ShardingTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<ShardingTransaction>();
}

} // namespace duckdb
