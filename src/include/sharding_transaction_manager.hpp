#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "sharding_transaction.hpp"

namespace duckdb {
class ShardingCatalog;

class ShardingTransactionManager : public TransactionManager {
public:
	ShardingTransactionManager(AttachedDatabase &db_p, ShardingCatalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	ShardingCatalog &sharding_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<ShardingTransaction>> transactions;
};

} // namespace duckdb
