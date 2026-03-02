cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME mysql_sharding)
project(${TARGET_NAME})

# Reference duckdb-mysql for MySQL connection code and build infrastructure
set(MYSQL_SCANNER_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb-mysql)

set(libmariadb_DIR ${MYSQL_SCANNER_DIR}/vcpkg_ports/libmariadb)
find_package(libmariadb REQUIRED)
set(EXTENSION_NAME ${TARGET_NAME}_extension)

set(MYSQL_INCLUDE_DIR
    ${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include/mysql)
include_directories(${MYSQL_INCLUDE_DIR})
include_directories(${MYSQL_SCANNER_DIR}/src/include)

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE src/include ${MYSQL_INCLUDE_DIR}
                                   ${MYSQL_SCANNER_DIR}/src/include)
target_link_libraries(${TARGET_NAME}_loadable_extension ${MYSQL_LIBRARIES})
