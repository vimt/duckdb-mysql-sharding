#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class ShardingCatalog;

class ShardingTransaction : public Transaction {
public:
	ShardingTransaction(ShardingCatalog &catalog, TransactionManager &manager, ClientContext &context);
	~ShardingTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static ShardingTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	ShardingCatalog &catalog;
};

} // namespace duckdb
