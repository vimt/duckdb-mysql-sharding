#include "sharding_schema_entry.hpp"

#include "sharding_catalog.hpp"
#include "sharding_table_entry.hpp"
#include "mysql_types.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

ShardingSchemaEntry::ShardingSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

void ShardingSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	lock_guard<mutex> l(tables_lock);
	if (tables_loaded) {
		return;
	}

	auto &sharding_catalog = catalog.Cast<ShardingCatalog>();

	// Lazy load columns from MySQL if not yet in cache DB
	if (!sharding_catalog.HasColumnsForSchema(name)) {
		sharding_catalog.LoadColumnsForSchema(name);
	}

	MySQLTypeConfig type_config(context);

	auto table_names = sharding_catalog.GetTableNames(name);
	for (auto &table_name : table_names) {
		auto logic_table = sharding_catalog.GetLogicTable(name, table_name);

		if (logic_table.columns.empty()) {
			continue;
		}

		auto create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)*this, table_name);

		for (auto &col : logic_table.columns) {
			MySQLTypeData type_info;
			type_info.type_name = col.type_name;
			type_info.column_type = col.column_type;
			type_info.precision = col.precision;
			type_info.scale = col.scale;

			auto col_type = MySQLTypes::TypeToLogicalType(type_config, type_info);
			ColumnDefinition column(col.name, std::move(col_type));

			if (!col.is_nullable) {
				auto col_idx = create_info->columns.LogicalColumnCount();
				create_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(col_idx)));
			}
			create_info->columns.AddColumn(std::move(column));
		}

		if (create_info->columns.LogicalColumnCount() > 0) {
			auto entry = make_uniq<ShardingTableEntry>(catalog, *this, *create_info, std::move(logic_table));
			tables[table_name] = std::move(entry);
		}
	}

	tables_loaded = true;
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateTable(CatalogTransaction, BoundCreateTableInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &,
                                                             TableCatalogEntry &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingSchemaEntry::Scan(ClientContext &context, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return;
	}
	EnsureTablesLoaded(context);
	for (auto &entry_pair : tables) {
		callback(*entry_pair.second);
	}
}

void ShardingSchemaEntry::Scan(CatalogType, const std::function<void(CatalogEntry &)> &) {
	throw NotImplementedException("Scan without context not supported");
}

void ShardingSchemaEntry::DropEntry(ClientContext &, DropInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

optional_ptr<CatalogEntry> ShardingSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                             const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return nullptr;
	}

	EnsureTablesLoaded(transaction.GetContext());

	auto it = tables.find(lookup_info.GetEntryName());
	if (it == tables.end()) {
		return nullptr;
	}
	return it->second.get();
}

} // namespace duckdb
