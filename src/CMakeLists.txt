include_directories(include)
include_directories(${MYSQL_SCANNER_DIR}/src/include)

# Reuse MySQL connection/result/types code from duckdb-mysql
set(MYSQL_SCANNER_SOURCES
    ${MYSQL_SCANNER_DIR}/src/mysql_connection.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_field.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_filter_pushdown.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_parameter.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_result.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_types.cpp
    ${MYSQL_SCANNER_DIR}/src/mysql_utils.cpp)

add_library(
  mysql_sharding_library OBJECT
  sharding_config.cpp
  mysql_schema_manager.cpp
  sharding_catalog.cpp
  sharding_schema_entry.cpp
  sharding_table_entry.cpp
  sharding_scanner.cpp
  sharding_transaction.cpp
  sharding_transaction_manager.cpp
  sharding_storage.cpp
  sharding_extension.cpp
  ${MYSQL_SCANNER_SOURCES})

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_sharding_library>
    PARENT_SCOPE)
