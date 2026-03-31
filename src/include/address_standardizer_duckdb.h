/*
 * address_standardizer_duckdb.h
 *
 * Header for DuckDB address standardizer extension functions.
 */

#ifndef ADDRESS_STANDARDIZER_DUCKDB_H
#define ADDRESS_STANDARDIZER_DUCKDB_H

#include "duckdb_extension.h"
#include "parseaddress-api.h"
#include <string.h>

/*
 * Thread-local state hash for parseaddress(). The hash maps US/CA state and
 * province names to their two-letter abbreviations. The data is static
 * (string literals compiled into the binary), so once built the hash is valid
 * for the lifetime of the thread.
 */
static inline HHash *get_cached_state_hash(int *err) {
    static _Thread_local HHash _cached_stH;
    static _Thread_local int _stH_initialized = 0;

    if (!_stH_initialized) {
        memset(&_cached_stH, 0, sizeof(HHash));
        *err = load_state_hash(&_cached_stH);
        if (*err == 0)
            _stH_initialized = 1;
    } else {
        *err = 0;
    }
    return &_cached_stH;
}

void register_parse_address(duckdb_connection connection);
void register_standardize_address(duckdb_connection connection,
                                   duckdb_extension_info info,
                                   struct duckdb_extension_access *access);
void register_debug_standardize_address(duckdb_connection connection,
                                         duckdb_extension_info info,
                                         struct duckdb_extension_access *access);

#endif
