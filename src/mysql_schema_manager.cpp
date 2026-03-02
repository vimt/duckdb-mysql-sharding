#include "mysql_schema_manager.hpp"

#include "mysql_connection.hpp"
#include "mysql_result.hpp"
#include "mysql_utils.hpp"

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
// Lifecycle
// ---------------------------------------------------------------------------

MySQLSchemaManager::MySQLSchemaManager(const string &cache_path, const vector<string> &host_list_p,
                                       int64_t cache_ttl_p)
    : host_list(host_list_p), cache_ttl(cache_ttl_p) {

	if (cache_path.empty()) {
		db = make_uniq<DuckDB>(nullptr);
	} else {
		db = make_uniq<DuckDB>(cache_path);
	}
	conn = make_uniq<Connection>(*db->instance);
	InitDB();
}

MySQLSchemaManager::~MySQLSchemaManager() {
	conn.reset();
	db.reset();
}

void MySQLSchemaManager::InitDB() {
	conn->Query("CREATE TABLE IF NOT EXISTS cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR)");

	conn->Query("CREATE TABLE IF NOT EXISTS physical_tables ("
	            "logic_db VARCHAR NOT NULL, logic_table VARCHAR NOT NULL, "
	            "host VARCHAR NOT NULL, db_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
	            "rows_count BIGINT DEFAULT 0, db_index INTEGER DEFAULT -1, table_index INTEGER DEFAULT -1)");

	conn->Query("CREATE TABLE IF NOT EXISTS table_columns ("
	            "logic_db VARCHAR NOT NULL, logic_table VARCHAR NOT NULL, "
	            "col_name VARCHAR NOT NULL, type_name VARCHAR NOT NULL, column_type VARCHAR NOT NULL, "
	            "is_nullable BOOLEAN DEFAULT false, col_precision BIGINT DEFAULT -1, "
	            "col_scale BIGINT DEFAULT -1)");
}

// ---------------------------------------------------------------------------
// Cache status
// ---------------------------------------------------------------------------

bool MySQLSchemaManager::HasCachedData() {
	auto count = GetPhysicalTableCount();
	if (count == 0) {
		return false;
	}

	if (cache_ttl > 0) {
		auto result = conn->Query("SELECT value FROM cache_meta WHERE key = 'schemas_updated_at'");
		if (!result->HasError()) {
			auto chunk = result->Fetch();
			if (chunk && chunk->size() > 0) {
				int64_t updated = std::stoll(chunk->GetValue(0, 0).GetValue<string>());
				int64_t age = CurrentEpochSeconds() - updated;
				if (age > cache_ttl) {
					Printer::Print(StringUtil::Format(
					    "[mysql_sharding] Cache expired (age=%ds, ttl=%ds), will re-discover", age, cache_ttl));
					return false;
				}
			}
		}
	}

	return true;
}

int64_t MySQLSchemaManager::GetPhysicalTableCount() {
	auto result = conn->Query("SELECT COUNT(*) FROM physical_tables");
	if (result->HasError()) {
		return 0;
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

// ---------------------------------------------------------------------------
// Write: MySQL → cache DB
// ---------------------------------------------------------------------------

void MySQLSchemaManager::DiscoverSchemas() {
	lock_guard<mutex> l(lock);
	auto t0 = std::chrono::steady_clock::now();
	Printer::Print("[mysql_sharding] Starting schema discovery...");

	conn->Query("DELETE FROM physical_tables");

	idx_t total_physical = 0;

	for (auto &host_conn_str : host_list) {
		try {
			MySQLTypeConfig type_config;
			auto mysql_conn = MySQLConnection::Open(type_config, host_conn_str, "");

			MySQLConnectionParameters params;
			unordered_set<string> unused;
			std::tie(params, unused) = MySQLUtils::ParseConnectionParameters(host_conn_str);

			string query =
			    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS "
			    "FROM information_schema.tables "
			    "WHERE TABLE_TYPE = 'BASE TABLE' "
			    "AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')";

			auto result = mysql_conn.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);

			idx_t host_count = 0;
			{
				Appender appender(*conn, "physical_tables");
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

	conn->Query("DELETE FROM cache_meta WHERE key = 'schemas_updated_at'");
	conn->Query("INSERT INTO cache_meta VALUES ('schemas_updated_at', '" +
	            to_string(CurrentEpochSeconds()) + "')");

	auto schema_names = GetSchemaNames();
	auto elapsed =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	Printer::Print(StringUtil::Format(
	    "[mysql_sharding] Schema discovery complete: %d schemas, %d physical tables (%.1fs)",
	    schema_names.size(), total_physical, elapsed / 1000.0));
}

void MySQLSchemaManager::LoadColumnsForSchema(const string &schema_name) {
	lock_guard<mutex> l(lock);

	if (HasColumnsForSchema(schema_name)) {
		return;
	}

	auto rep_result = conn->Query(
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
			auto mysql_conn = MySQLConnection::Open(type_config, hp.first, "");

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

				auto result = mysql_conn.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);

				Appender appender(*conn, "table_columns");
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
// Clear
// ---------------------------------------------------------------------------

void MySQLSchemaManager::ClearAll() {
	lock_guard<mutex> l(lock);
	conn->Query("DELETE FROM physical_tables");
	conn->Query("DELETE FROM table_columns");
}

void MySQLSchemaManager::ClearColumnsForSchema(const string &schema_name) {
	lock_guard<mutex> l(lock);
	conn->Query("DELETE FROM table_columns WHERE logic_db = '" + EscapeSql(schema_name) + "'");
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

vector<string> MySQLSchemaManager::GetSchemaNames() {
	vector<string> names;
	auto result = conn->Query("SELECT DISTINCT logic_db FROM physical_tables ORDER BY logic_db");
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

vector<string> MySQLSchemaManager::GetTableNames(const string &schema_name) {
	vector<string> names;
	auto result = conn->Query("SELECT DISTINCT logic_table FROM physical_tables WHERE logic_db = '" +
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

LogicTableInfo MySQLSchemaManager::GetLogicTable(const string &schema_name, const string &table_name) {
	LogicTableInfo info;
	info.logic_db = schema_name;
	info.logic_table = table_name;

	auto pt_result = conn->Query(
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

	auto col_result = conn->Query(
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

bool MySQLSchemaManager::HasColumnsForSchema(const string &schema_name) {
	auto result = conn->Query("SELECT COUNT(*) FROM table_columns WHERE logic_db = '" +
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

} // namespace duckdb
