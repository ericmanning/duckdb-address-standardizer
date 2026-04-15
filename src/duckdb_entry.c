/*
 * duckdb_entry.c
 *
 * Entry point for the DuckDB address_standardizer extension.
 * Registers parse_address() and standardize_address() scalar functions.
 */

#include "duckdb_extension.h"
#include "address_standardizer_duckdb.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                             duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
    register_parse_address(connection);
    register_standardize_address(connection, info, access);
    register_debug_standardize_address(connection, info, access);
    register_load_us_address_data(connection, info, access);
    register_addrust_parse(connection);
    return true;
}
