#include "sharding_config.hpp"
#include <fstream>
#include <sstream>

namespace duckdb {

pair<string, int> SplitSharding(const string &name) {
	// Must be at least 10 chars: at least 1 char base + "_" + 8 digits
	if (name.size() < 10) {
		return {name, -1};
	}
	// Check for underscore at position len-9
	if (name[name.size() - 9] != '_') {
		return {name, -1};
	}
	// Verify last 8 chars are all digits
	for (idx_t i = name.size() - 8; i < name.size(); i++) {
		if (name[i] < '0' || name[i] > '9') {
			return {name, -1};
		}
	}
	string base = name.substr(0, name.size() - 9);
	int index = std::stoi(name.substr(name.size() - 8));
	return {base, index};
}

string PhysicalTableInfo::LogicDb() const {
	return SplitSharding(db_name).first;
}

string PhysicalTableInfo::LogicTable() const {
	return SplitSharding(table_name).first;
}

static string TrimWhitespace(const string &s) {
	auto start = s.find_first_not_of(" \t\r\n");
	if (start == string::npos) {
		return "";
	}
	auto end = s.find_last_not_of(" \t\r\n");
	return s.substr(start, end - start + 1);
}

static void ParseHostLines(ShardingConfig &config, std::istream &stream) {
	string line;
	while (std::getline(stream, line)) {
		string trimmed = TrimWhitespace(line);
		if (trimmed.empty() || trimmed[0] == '#') {
			continue;
		}
		config.host_list.push_back(trimmed);
	}
}

ShardingConfig ShardingConfig::Parse(const string &input) {
	ShardingConfig config;

	std::ifstream file(input);
	if (file.is_open()) {
		config.is_file = true;
		ParseHostLines(config, file);
	} else {
		config.is_file = false;
		// Treat as direct host string(s), semicolon-separated
		std::istringstream ss(input);
		string segment;
		while (std::getline(ss, segment, ';')) {
			string trimmed = TrimWhitespace(segment);
			if (!trimmed.empty()) {
				config.host_list.push_back(trimmed);
			}
		}
	}

	if (config.host_list.empty()) {
		throw InvalidInputException("MySQL sharding requires at least one host connection string");
	}

	return config;
}

} // namespace duckdb
