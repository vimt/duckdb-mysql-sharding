#include "sharding_transaction_manager.hpp"
#include "sharding_catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

ShardingTransactionManager::ShardingTransactionManager(AttachedDatabase &db_p, ShardingCatalog &catalog)
    : TransactionManager(db_p), sharding_catalog(catalog) {
}

Transaction &ShardingTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<ShardingTransaction>(sharding_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData ShardingTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &sharding_txn = transaction.Cast<ShardingTransaction>();
	sharding_txn.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void ShardingTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &sharding_txn = transaction.Cast<ShardingTransaction>();
	try {
		sharding_txn.Rollback();
	} catch (...) {
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void ShardingTransactionManager::Checkpoint(ClientContext &, bool) {
}

} // namespace duckdb
