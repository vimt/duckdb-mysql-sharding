#include "sharding_catalog.hpp"

#include "sharding_schema_entry.hpp"
#include "mysql_connection.hpp"
#include "mysql_result.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/appender.hpp"

#include <chrono>
#include <cstdio>

namespace duckdb {

static int64_t CurrentEpochSeconds() {
	auto now = std::chrono::system_clock::now();
	return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
}

ShardingCatalog::ShardingCatalog(AttachedDatabase &db_p, ShardingConfig config_p, const string &config_path_p,
                                 const string &cache_path_p, int64_t cache_ttl_p)
    : Catalog(db_p), config(std::move(config_p)), attach_path(config_path_p), cache_path(cache_path_p),
      cache_ttl(cache_ttl_p) {

	if (!cache_path.empty() && LoadCache()) {
		return;
	}

	DiscoverSchemas();

	if (!cache_path.empty()) {
		SaveCache();
	}
}

ShardingCatalog::~ShardingCatalog() = default;

void ShardingCatalog::Initialize(bool load_builtin) {
}

// ---------------------------------------------------------------------------
// Schema discovery
// ---------------------------------------------------------------------------

void ShardingCatalog::DiscoverSchemas() {
	auto t0 = std::chrono::steady_clock::now();
	Printer::Print("[mysql_sharding] Starting schema discovery...");

	for (auto &host_conn_str : config.host_list) {
		try {
			MySQLTypeConfig type_config;
			auto conn = MySQLConnection::Open(type_config, host_conn_str, "");

			MySQLConnectionParameters params;
			unordered_set<string> unused;
			std::tie(params, unused) = MySQLUtils::ParseConnectionParameters(host_conn_str);

			string query =
			    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS "
			    "FROM information_schema.tables "
			    "WHERE TABLE_TYPE = 'BASE TABLE' "
			    "AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')";

			auto result = conn.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);

			idx_t table_count = 0;
			while (result->Next()) {
				string db_name = result->GetString(0);
				string table_name = result->GetString(1);
				int64_t rows_count = result->IsNull(2) ? 0 : result->GetInt64(2);

				auto db_split = SplitSharding(db_name);
				auto tbl_split = SplitSharding(table_name);

				PhysicalTableInfo physical;
				physical.host = host_conn_str;
				physical.db_name = db_name;
				physical.table_name = table_name;
				physical.rows_count = rows_count;
				physical.db_index = db_split.second;
				physical.table_index = tbl_split.second;

				auto &logic_info = schema_map[db_split.first][tbl_split.first];
				logic_info.logic_db = db_split.first;
				logic_info.logic_table = tbl_split.first;
				logic_info.physical_tables.push_back(std::move(physical));
				table_count++;
			}

			Printer::Print(StringUtil::Format("[mysql_sharding] Host '%s': discovered %d physical tables",
			                                  params.host, table_count));
		} catch (std::exception &e) {
			Printer::Print(
			    StringUtil::Format("[mysql_sharding] Warning: failed to connect to host: %s", e.what()));
		}
	}

	idx_t total_logical = 0;
	for (auto &s : schema_map) {
		total_logical += s.second.size();
	}
	auto elapsed =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	Printer::Print(StringUtil::Format(
	    "[mysql_sharding] Schema discovery complete: %d schemas, %d logical tables (%.1fs)",
	    schema_map.size(), total_logical, elapsed / 1000.0));
}

// ---------------------------------------------------------------------------
// Lazy column loading per schema
// ---------------------------------------------------------------------------

void ShardingCatalog::LoadColumnsForSchema(const string &schema_name) {
	auto schema_it = schema_map.find(schema_name);
	if (schema_it == schema_map.end()) {
		return;
	}

	struct RepInfo {
		string logic_table;
		string phys_db;
		string phys_table;
	};
	unordered_map<string, vector<RepInfo>> host_reps;

	for (auto &tp : schema_it->second) {
		auto &logic = tp.second;
		if (!logic.columns.empty() || logic.physical_tables.empty()) {
			continue;
		}
		auto &rep = logic.physical_tables[0];
		host_reps[rep.host].push_back({logic.logic_table, rep.db_name, rep.table_name});
	}

	if (host_reps.empty()) {
		return;
	}

	auto t0 = std::chrono::steady_clock::now();
	idx_t tables_loaded = 0;
	const idx_t BATCH_SIZE = 100;

	for (auto &hp : host_reps) {
		try {
			MySQLTypeConfig type_config;
			auto conn = MySQLConnection::Open(type_config, hp.first, "");

			for (idx_t bs = 0; bs < hp.second.size(); bs += BATCH_SIZE) {
				idx_t be = MinValue<idx_t>(bs + BATCH_SIZE, hp.second.size());

				string where_clause;
				unordered_map<string, string> phys_to_logic;

				for (idx_t i = bs; i < be; i++) {
					if (i > bs) {
						where_clause += " OR ";
					}
					where_clause += "(TABLE_SCHEMA = " + MySQLUtils::WriteLiteral(hp.second[i].phys_db) +
					                " AND TABLE_NAME = " + MySQLUtils::WriteLiteral(hp.second[i].phys_table) +
					                ")";
					phys_to_logic[hp.second[i].phys_db + "\t" + hp.second[i].phys_table] =
					    hp.second[i].logic_table;
				}

				string query = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
				               "IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE "
				               "FROM information_schema.columns WHERE (" +
				               where_clause +
				               ") ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

				auto result = conn.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);

				while (result->Next()) {
					string phys_key = result->GetString(0) + "\t" + result->GetString(1);
					auto it = phys_to_logic.find(phys_key);
					if (it == phys_to_logic.end()) {
						continue;
					}

					ColumnInfo col;
					col.name = result->GetString(2);
					col.type_name = result->GetString(3);
					col.column_type = result->GetString(4);
					col.is_nullable = result->GetString(5) == "YES";
					col.precision = result->IsNull(6) ? -1 : result->GetInt64(6);
					col.scale = result->IsNull(7) ? -1 : result->GetInt64(7);
					schema_map[schema_name][it->second].columns.push_back(std::move(col));
				}
				tables_loaded += (be - bs);
			}
		} catch (std::exception &e) {
			Printer::Print(
			    StringUtil::Format("[mysql_sharding] Warning: failed to load columns from host: %s", e.what()));
		}
	}

	auto elapsed =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	Printer::Print(StringUtil::Format("[mysql_sharding] Schema '%s': loaded columns for %d tables (%.1fs)",
	                                  schema_name, tables_loaded, elapsed / 1000.0));

	if (!cache_path.empty()) {
		SaveCache();
	}
}

// ---------------------------------------------------------------------------
// Refresh
// ---------------------------------------------------------------------------

void ShardingCatalog::ClearCacheAndRefresh() {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.clear();
	}
	schema_map.clear();

	DiscoverSchemas();

	if (!cache_path.empty()) {
		SaveCache();
	}

	Printer::Print("[mysql_sharding] Full refresh complete");
}

void ShardingCatalog::RefreshSchema(const string &schema_name) {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.erase(schema_name);
	}

	auto it = schema_map.find(schema_name);
	if (it == schema_map.end()) {
		Printer::Print(StringUtil::Format("[mysql_sharding] Schema '%s' not found", schema_name));
		return;
	}
	for (auto &tp : it->second) {
		tp.second.columns.clear();
	}

	LoadColumnsForSchema(schema_name);
	Printer::Print(StringUtil::Format("[mysql_sharding] Schema '%s' refreshed", schema_name));
}

// ---------------------------------------------------------------------------
// Cache: DuckDB database file
// ---------------------------------------------------------------------------

void ShardingCatalog::SaveCache() {
	lock_guard<mutex> l(cache_lock);
	auto t0 = std::chrono::steady_clock::now();
	try {
		std::remove(cache_path.c_str());
		std::remove((cache_path + ".wal").c_str());

		DuckDB cache_db(cache_path);
		Connection conn(*cache_db.instance);

		conn.Query("CREATE TABLE cache_meta (key VARCHAR, value VARCHAR)");
		conn.Query("INSERT INTO cache_meta VALUES ('created_at', '" + to_string(CurrentEpochSeconds()) + "')");

		conn.Query("CREATE TABLE physical_tables ("
		           "logic_db VARCHAR, logic_table VARCHAR, "
		           "host VARCHAR, db_name VARCHAR, table_name VARCHAR, "
		           "rows_count BIGINT, db_index INTEGER, table_index INTEGER)");

		conn.Query("CREATE TABLE table_columns ("
		           "logic_db VARCHAR, logic_table VARCHAR, "
		           "col_name VARCHAR, type_name VARCHAR, column_type VARCHAR, "
		           "is_nullable BOOLEAN, col_precision BIGINT, col_scale BIGINT)");

		{
			Appender appender(conn, "physical_tables");
			for (auto &sp : schema_map) {
				for (auto &tp : sp.second) {
					for (auto &pt : tp.second.physical_tables) {
						appender.BeginRow();
						appender.Append(Value(tp.second.logic_db));
						appender.Append(Value(tp.second.logic_table));
						appender.Append(Value(pt.host));
						appender.Append(Value(pt.db_name));
						appender.Append(Value(pt.table_name));
						appender.Append(Value::BIGINT(pt.rows_count));
						appender.Append(Value::INTEGER(pt.db_index));
						appender.Append(Value::INTEGER(pt.table_index));
						appender.EndRow();
					}
				}
			}
			appender.Close();
		}

		{
			Appender appender(conn, "table_columns");
			for (auto &sp : schema_map) {
				for (auto &tp : sp.second) {
					for (auto &col : tp.second.columns) {
						appender.BeginRow();
						appender.Append(Value(tp.second.logic_db));
						appender.Append(Value(tp.second.logic_table));
						appender.Append(Value(col.name));
						appender.Append(Value(col.type_name));
						appender.Append(Value(col.column_type));
						appender.Append(Value::BOOLEAN(col.is_nullable));
						appender.Append(Value::BIGINT(col.precision));
						appender.Append(Value::BIGINT(col.scale));
						appender.EndRow();
					}
				}
			}
			appender.Close();
		}

		auto elapsed =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
		Printer::Print(StringUtil::Format("[mysql_sharding] Cache saved to %s (%.1fs)", cache_path,
		                                  elapsed / 1000.0));
	} catch (std::exception &e) {
		Printer::Print(StringUtil::Format("[mysql_sharding] Warning: failed to save cache: %s", e.what()));
	}
}

bool ShardingCatalog::LoadCache() {
	auto t0 = std::chrono::steady_clock::now();
	try {
		DBConfig cache_config;
		cache_config.options.access_mode = AccessMode::READ_ONLY;
		DuckDB cache_db(cache_path, &cache_config);
		Connection conn(*cache_db.instance);

		// Check TTL
		if (cache_ttl > 0) {
			auto meta_result = conn.Query("SELECT value FROM cache_meta WHERE key = 'created_at'");
			if (!meta_result->HasError()) {
				auto chunk = meta_result->Fetch();
				if (chunk && chunk->size() > 0) {
					int64_t created = std::stoll(chunk->GetValue(0, 0).GetValue<string>());
					int64_t age = CurrentEpochSeconds() - created;
					if (age > cache_ttl) {
						Printer::Print(StringUtil::Format(
						    "[mysql_sharding] Cache expired (age=%ds, ttl=%ds), will re-discover", age,
						    cache_ttl));
						return false;
					}
				}
			}
		}

		schema_map.clear();
		idx_t pt_count = 0;
		idx_t col_count = 0;

		auto pt_result = conn.Query("SELECT logic_db, logic_table, host, db_name, table_name, "
		                            "rows_count, db_index, table_index FROM physical_tables");
		if (pt_result->HasError()) {
			return false;
		}
		while (true) {
			auto chunk = pt_result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				string logic_db = chunk->GetValue(0, r).GetValue<string>();
				string logic_table = chunk->GetValue(1, r).GetValue<string>();

				auto &info = schema_map[logic_db][logic_table];
				info.logic_db = logic_db;
				info.logic_table = logic_table;

				PhysicalTableInfo pt;
				pt.host = chunk->GetValue(2, r).GetValue<string>();
				pt.db_name = chunk->GetValue(3, r).GetValue<string>();
				pt.table_name = chunk->GetValue(4, r).GetValue<string>();
				pt.rows_count = chunk->GetValue(5, r).GetValue<int64_t>();
				pt.db_index = chunk->GetValue(6, r).GetValue<int32_t>();
				pt.table_index = chunk->GetValue(7, r).GetValue<int32_t>();
				info.physical_tables.push_back(std::move(pt));
				pt_count++;
			}
		}

		auto col_result = conn.Query("SELECT logic_db, logic_table, col_name, type_name, column_type, "
		                             "is_nullable, col_precision, col_scale FROM table_columns");
		if (!col_result->HasError()) {
			while (true) {
				auto chunk = col_result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				for (idx_t r = 0; r < chunk->size(); r++) {
					string logic_db = chunk->GetValue(0, r).GetValue<string>();
					string logic_table = chunk->GetValue(1, r).GetValue<string>();

					ColumnInfo col;
					col.name = chunk->GetValue(2, r).GetValue<string>();
					col.type_name = chunk->GetValue(3, r).GetValue<string>();
					col.column_type = chunk->GetValue(4, r).GetValue<string>();
					col.is_nullable = chunk->GetValue(5, r).GetValue<bool>();
					col.precision = chunk->GetValue(6, r).GetValue<int64_t>();
					col.scale = chunk->GetValue(7, r).GetValue<int64_t>();
					schema_map[logic_db][logic_table].columns.push_back(std::move(col));
					col_count++;
				}
			}
		}

		if (schema_map.empty()) {
			return false;
		}

		auto elapsed =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
		Printer::Print(StringUtil::Format(
		    "[mysql_sharding] Cache loaded: %d schemas, %d physical tables, %d columns (%.1fs)",
		    schema_map.size(), pt_count, col_count, elapsed / 1000.0));
		return true;
	} catch (...) {
		return false;
	}
}

// ---------------------------------------------------------------------------
// Catalog interface
// ---------------------------------------------------------------------------

const LogicTableInfo *ShardingCatalog::GetLogicTable(const string &schema, const string &table) const {
	auto si = schema_map.find(schema);
	if (si == schema_map.end()) {
		return nullptr;
	}
	auto ti = si->second.find(table);
	if (ti == si->second.end()) {
		return nullptr;
	}
	return &ti->second;
}

optional_ptr<CatalogEntry> ShardingCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	lock_guard<mutex> l(schema_lock);
	for (auto &sp : schema_map) {
		if (schema_entries.find(sp.first) == schema_entries.end()) {
			CreateSchemaInfo info;
			info.schema = sp.first;
			schema_entries[sp.first] = make_uniq<ShardingSchemaEntry>(*this, info);
		}
		callback(*schema_entries[sp.first]);
	}
}

optional_ptr<SchemaCatalogEntry> ShardingCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA) {
		if (schema_map.empty()) {
			if (if_not_found != OnEntryNotFound::RETURN_NULL) {
				throw BinderException("No schemas available in MySQL sharding catalog");
			}
			return nullptr;
		}
		schema_name = schema_map.begin()->first;
	}

	if (schema_map.find(schema_name) == schema_map.end()) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema with name \"%s\" not found", schema_name);
		}
		return nullptr;
	}

	lock_guard<mutex> l(schema_lock);
	if (schema_entries.find(schema_name) == schema_entries.end()) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		schema_entries[schema_name] = make_uniq<ShardingSchemaEntry>(*this, info);
	}
	return schema_entries[schema_name].get();
}

PhysicalOperator &ShardingCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                     PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                               optional_ptr<PhysicalOperator>) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                               PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
PhysicalOperator &ShardingCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                               PhysicalOperator &plan) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}
unique_ptr<LogicalOperator> ShardingCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
                                                              unique_ptr<LogicalOperator>) {
	throw NotImplementedException("MySQL sharding catalog is read-only");
}

DatabaseSize ShardingCatalog::GetDatabaseSize(ClientContext &) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool ShardingCatalog::InMemory() {
	return false;
}

string ShardingCatalog::GetDBPath() {
	return attach_path;
}

} // namespace duckdb
