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

namespace duckdb {

static int64_t CurrentEpochSeconds() {
	auto now = std::chrono::system_clock::now();
	return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
}

static string EscapeSql(const string &s) {
	string result;
	for (auto c : s) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	return result;
}

// ---------------------------------------------------------------------------
// Constructor / Init
// ---------------------------------------------------------------------------

ShardingCatalog::ShardingCatalog(AttachedDatabase &db_p, ShardingConfig config_p, const string &config_path_p,
                                 const string &cache_path_p, int64_t cache_ttl_p)
    : Catalog(db_p), config(std::move(config_p)), attach_path(config_path_p), cache_path(cache_path_p),
      cache_ttl(cache_ttl_p) {

	InitCacheDB();

	// Check if cache DB already has data
	auto count_result = cache_conn->Query("SELECT COUNT(*) FROM physical_tables");
	int64_t pt_count = 0;
	if (!count_result->HasError()) {
		auto chunk = count_result->Fetch();
		if (chunk && chunk->size() > 0) {
			pt_count = chunk->GetValue(0, 0).GetValue<int64_t>();
		}
	}

	if (pt_count > 0) {
		// Check TTL
		bool expired = false;
		if (cache_ttl > 0) {
			auto meta_result =
			    cache_conn->Query("SELECT value FROM cache_meta WHERE key = 'schemas_updated_at'");
			if (!meta_result->HasError()) {
				auto chunk = meta_result->Fetch();
				if (chunk && chunk->size() > 0) {
					int64_t updated = std::stoll(chunk->GetValue(0, 0).GetValue<string>());
					int64_t age = CurrentEpochSeconds() - updated;
					if (age > cache_ttl) {
						Printer::Print(StringUtil::Format(
						    "[mysql_sharding] Cache expired (age=%ds, ttl=%ds), will re-discover", age,
						    cache_ttl));
						expired = true;
					}
				}
			}
		}

		if (!expired) {
			Printer::Print(
			    StringUtil::Format("[mysql_sharding] Using cached schema (%d physical tables)", pt_count));
			return;
		}
	}

	DiscoverSchemas();
}

ShardingCatalog::~ShardingCatalog() {
	cache_conn.reset();
	cache_db.reset();
}

void ShardingCatalog::Initialize(bool load_builtin) {
}

void ShardingCatalog::InitCacheDB() {
	if (cache_path.empty()) {
		cache_db = make_uniq<DuckDB>(nullptr);
	} else {
		cache_db = make_uniq<DuckDB>(cache_path);
	}
	cache_conn = make_uniq<Connection>(*cache_db->instance);

	cache_conn->Query("CREATE TABLE IF NOT EXISTS cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR)");

	cache_conn->Query("CREATE TABLE IF NOT EXISTS physical_tables ("
	                  "logic_db VARCHAR NOT NULL, logic_table VARCHAR NOT NULL, "
	                  "host VARCHAR NOT NULL, db_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
	                  "rows_count BIGINT DEFAULT 0, db_index INTEGER DEFAULT -1, table_index INTEGER DEFAULT -1)");

	cache_conn->Query("CREATE TABLE IF NOT EXISTS table_columns ("
	                  "logic_db VARCHAR NOT NULL, logic_table VARCHAR NOT NULL, "
	                  "col_name VARCHAR NOT NULL, type_name VARCHAR NOT NULL, column_type VARCHAR NOT NULL, "
	                  "is_nullable BOOLEAN DEFAULT false, col_precision BIGINT DEFAULT -1, "
	                  "col_scale BIGINT DEFAULT -1)");
}

// ---------------------------------------------------------------------------
// Schema discovery: MySQL → cache DB
// ---------------------------------------------------------------------------

void ShardingCatalog::DiscoverSchemas() {
	lock_guard<mutex> l(cache_lock);
	auto t0 = std::chrono::steady_clock::now();
	Printer::Print("[mysql_sharding] Starting schema discovery...");

	cache_conn->Query("DELETE FROM physical_tables");

	idx_t total_physical = 0;

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

			idx_t host_count = 0;
			{
				Appender appender(*cache_conn, "physical_tables");
				while (result->Next()) {
					string db_name = result->GetString(0);
					string table_name = result->GetString(1);
					int64_t rows_count = result->IsNull(2) ? 0 : result->GetInt64(2);

					auto db_split = SplitSharding(db_name);
					auto tbl_split = SplitSharding(table_name);

					appender.BeginRow();
					appender.Append(Value(db_split.first));
					appender.Append(Value(tbl_split.first));
					appender.Append(Value(host_conn_str));
					appender.Append(Value(db_name));
					appender.Append(Value(table_name));
					appender.Append(Value::BIGINT(rows_count));
					appender.Append(Value::INTEGER(db_split.second));
					appender.Append(Value::INTEGER(tbl_split.second));
					appender.EndRow();
					host_count++;
				}
				appender.Close();
			}

			total_physical += host_count;
			Printer::Print(StringUtil::Format("[mysql_sharding] Host '%s': discovered %d physical tables",
			                                  params.host, host_count));
		} catch (std::exception &e) {
			Printer::Print(
			    StringUtil::Format("[mysql_sharding] Warning: failed to connect to host: %s", e.what()));
		}
	}

	// Update timestamp
	cache_conn->Query("DELETE FROM cache_meta WHERE key = 'schemas_updated_at'");
	cache_conn->Query("INSERT INTO cache_meta VALUES ('schemas_updated_at', '" +
	                  to_string(CurrentEpochSeconds()) + "')");

	auto schema_names = GetSchemaNames();
	auto elapsed =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	Printer::Print(StringUtil::Format(
	    "[mysql_sharding] Schema discovery complete: %d schemas, %d physical tables (%.1fs)",
	    schema_names.size(), total_physical, elapsed / 1000.0));
}

// ---------------------------------------------------------------------------
// Lazy column loading: MySQL → cache DB
// ---------------------------------------------------------------------------

void ShardingCatalog::LoadColumnsForSchema(const string &schema_name) {
	lock_guard<mutex> l(cache_lock);

	if (HasColumnsForSchema(schema_name)) {
		return;
	}

	// Get representative physical table for each logical table (first one)
	auto rep_result = cache_conn->Query(
	    "SELECT logic_table, host, db_name, table_name FROM ("
	    "  SELECT logic_table, host, db_name, table_name, "
	    "    ROW_NUMBER() OVER (PARTITION BY logic_table ORDER BY db_index, table_index) AS rn "
	    "  FROM physical_tables WHERE logic_db = '" +
	    EscapeSql(schema_name) +
	    "'"
	    ") WHERE rn = 1");

	if (rep_result->HasError()) {
		return;
	}

	struct RepInfo {
		string logic_table;
		string phys_db;
		string phys_table;
	};
	unordered_map<string, vector<RepInfo>> host_reps;

	while (true) {
		auto chunk = rep_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			RepInfo ri;
			ri.logic_table = chunk->GetValue(0, r).GetValue<string>();
			string host = chunk->GetValue(1, r).GetValue<string>();
			ri.phys_db = chunk->GetValue(2, r).GetValue<string>();
			ri.phys_table = chunk->GetValue(3, r).GetValue<string>();
			host_reps[host].push_back(std::move(ri));
		}
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
					where_clause += "(TABLE_SCHEMA = " +
					                MySQLUtils::WriteLiteral(hp.second[i].phys_db) +
					                " AND TABLE_NAME = " +
					                MySQLUtils::WriteLiteral(hp.second[i].phys_table) + ")";
					phys_to_logic[hp.second[i].phys_db + "\t" + hp.second[i].phys_table] =
					    hp.second[i].logic_table;
				}

				string query = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
				               "IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE "
				               "FROM information_schema.columns WHERE (" +
				               where_clause +
				               ") ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

				auto result = conn.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);

				Appender appender(*cache_conn, "table_columns");
				while (result->Next()) {
					string phys_key = result->GetString(0) + "\t" + result->GetString(1);
					auto it = phys_to_logic.find(phys_key);
					if (it == phys_to_logic.end()) {
						continue;
					}

					appender.BeginRow();
					appender.Append(Value(schema_name));
					appender.Append(Value(it->second));
					appender.Append(Value(result->GetString(2)));
					appender.Append(Value(result->GetString(3)));
					appender.Append(Value(result->GetString(4)));
					appender.Append(Value::BOOLEAN(result->GetString(5) == "YES"));
					appender.Append(Value::BIGINT(result->IsNull(6) ? -1 : result->GetInt64(6)));
					appender.Append(Value::BIGINT(result->IsNull(7) ? -1 : result->GetInt64(7)));
					appender.EndRow();
				}
				appender.Close();
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
}

// ---------------------------------------------------------------------------
// Read helpers: query cache DB
// ---------------------------------------------------------------------------

vector<string> ShardingCatalog::GetSchemaNames() {
	vector<string> names;
	auto result = cache_conn->Query("SELECT DISTINCT logic_db FROM physical_tables ORDER BY logic_db");
	if (result->HasError()) {
		return names;
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			names.push_back(chunk->GetValue(0, r).GetValue<string>());
		}
	}
	return names;
}

vector<string> ShardingCatalog::GetTableNames(const string &schema_name) {
	vector<string> names;
	auto result = cache_conn->Query("SELECT DISTINCT logic_table FROM physical_tables WHERE logic_db = '" +
	                                EscapeSql(schema_name) + "' ORDER BY logic_table");
	if (result->HasError()) {
		return names;
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			names.push_back(chunk->GetValue(0, r).GetValue<string>());
		}
	}
	return names;
}

LogicTableInfo ShardingCatalog::GetLogicTable(const string &schema_name, const string &table_name) {
	LogicTableInfo info;
	info.logic_db = schema_name;
	info.logic_table = table_name;

	// Physical tables
	auto pt_result = cache_conn->Query(
	    "SELECT host, db_name, table_name, rows_count, db_index, table_index "
	    "FROM physical_tables WHERE logic_db = '" +
	    EscapeSql(schema_name) + "' AND logic_table = '" + EscapeSql(table_name) + "'");

	if (!pt_result->HasError()) {
		while (true) {
			auto chunk = pt_result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				PhysicalTableInfo pt;
				pt.host = chunk->GetValue(0, r).GetValue<string>();
				pt.db_name = chunk->GetValue(1, r).GetValue<string>();
				pt.table_name = chunk->GetValue(2, r).GetValue<string>();
				pt.rows_count = chunk->GetValue(3, r).GetValue<int64_t>();
				pt.db_index = chunk->GetValue(4, r).GetValue<int32_t>();
				pt.table_index = chunk->GetValue(5, r).GetValue<int32_t>();
				info.physical_tables.push_back(std::move(pt));
			}
		}
	}

	// Columns
	auto col_result = cache_conn->Query(
	    "SELECT col_name, type_name, column_type, is_nullable, col_precision, col_scale "
	    "FROM table_columns WHERE logic_db = '" +
	    EscapeSql(schema_name) + "' AND logic_table = '" + EscapeSql(table_name) + "'");

	if (!col_result->HasError()) {
		while (true) {
			auto chunk = col_result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				ColumnInfo col;
				col.name = chunk->GetValue(0, r).GetValue<string>();
				col.type_name = chunk->GetValue(1, r).GetValue<string>();
				col.column_type = chunk->GetValue(2, r).GetValue<string>();
				col.is_nullable = chunk->GetValue(3, r).GetValue<bool>();
				col.precision = chunk->GetValue(4, r).GetValue<int64_t>();
				col.scale = chunk->GetValue(5, r).GetValue<int64_t>();
				info.columns.push_back(std::move(col));
			}
		}
	}

	return info;
}

bool ShardingCatalog::HasColumnsForSchema(const string &schema_name) {
	auto result = cache_conn->Query("SELECT COUNT(*) FROM table_columns WHERE logic_db = '" +
	                                EscapeSql(schema_name) + "'");
	if (result->HasError()) {
		return false;
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>() > 0;
}

// ---------------------------------------------------------------------------
// Refresh
// ---------------------------------------------------------------------------

void ShardingCatalog::ClearCacheAndRefresh() {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.clear();
	}

	{
		lock_guard<mutex> l(cache_lock);
		cache_conn->Query("DELETE FROM physical_tables");
		cache_conn->Query("DELETE FROM table_columns");
	}

	DiscoverSchemas();
	Printer::Print("[mysql_sharding] Full refresh complete");
}

void ShardingCatalog::RefreshSchema(const string &schema_name) {
	{
		lock_guard<mutex> l(schema_lock);
		schema_entries.erase(schema_name);
	}

	{
		lock_guard<mutex> l(cache_lock);
		cache_conn->Query("DELETE FROM table_columns WHERE logic_db = '" + EscapeSql(schema_name) + "'");
	}

	LoadColumnsForSchema(schema_name);
	Printer::Print(StringUtil::Format("[mysql_sharding] Schema '%s' refreshed", schema_name));
}

// ---------------------------------------------------------------------------
// Catalog interface
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> ShardingCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw BinderException("MySQL sharding catalog is read-only");
}

void ShardingCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto names = GetSchemaNames();
	lock_guard<mutex> l(schema_lock);
	for (auto &name : names) {
		if (schema_entries.find(name) == schema_entries.end()) {
			CreateSchemaInfo info;
			info.schema = name;
			schema_entries[name] = make_uniq<ShardingSchemaEntry>(*this, info);
		}
		callback(*schema_entries[name]);
	}
}

optional_ptr<SchemaCatalogEntry> ShardingCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto all_schemas = GetSchemaNames();

	if (schema_name == DEFAULT_SCHEMA) {
		if (all_schemas.empty()) {
			if (if_not_found != OnEntryNotFound::RETURN_NULL) {
				throw BinderException("No schemas available in MySQL sharding catalog");
			}
			return nullptr;
		}
		schema_name = all_schemas[0];
	}

	bool found = false;
	for (auto &s : all_schemas) {
		if (s == schema_name) {
			found = true;
			break;
		}
	}
	if (!found) {
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
