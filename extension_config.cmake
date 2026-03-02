# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(mysql_sharding
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    DONT_LINK
)
