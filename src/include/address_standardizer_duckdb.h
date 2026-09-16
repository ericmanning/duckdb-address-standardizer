/*
 * address_standardizer_duckdb.h
 *
 * Header for DuckDB address standardizer extension functions.
 */

#ifndef ADDRESS_STANDARDIZER_DUCKDB_H
#define ADDRESS_STANDARDIZER_DUCKDB_H

#include "duckdb_extension.h"
#include "parseaddress-api.h"
#include "portable_threads.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * Thread-local state hash for parseaddress(). The hash maps US/CA state and
 * province names to their two-letter abbreviations. The data is static
 * (string literals compiled into the binary), so once built the hash is valid
 * for the lifetime of the thread.
 */
static inline HHash *get_cached_state_hash(int *err) {
    static PORT_THREAD_LOCAL HHash _cached_stH;
    static PORT_THREAD_LOCAL int _stH_initialized = 0;

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

/*
 * Reference-table loading helpers.
 *
 * standardize_address() and debug_standardize_address() both load lexicon,
 * gazetteer and rules tables by name.  These used to be copy-pasted into both
 * .c files, which is how the same rule-length overflow came to exist (and need
 * fixing) twice.  Keep the single copy here.
 */

#define MAX_RULE_LENGTH 128

/* Table names are spliced into SQL, so allow only characters that cannot
   terminate an identifier: alphanumerics, '_' and '.' for schema qualification.
   Anything needing quoting is rejected rather than quoted. */
static inline int as_table_name_ok(const char *t) {
    if (!t || !*t) return 0;
    while (*t != '\0') {
        if (!((*t >= 'a' && *t <= 'z') || (*t >= 'A' && *t <= 'Z') ||
              (*t >= '0' && *t <= '9') || *t == '_' || *t == '.'))
            return 0;
        t++;
    }
    return 1;
}

/* Build "<select_clause> FROM <tabname> ORDER BY id" into sql[cap].
   Returns 0 on success, -1 if the name is unacceptable or the statement would
   be truncated -- never issues a query we did not fully construct. */
static inline int as_build_table_query(char *sql, size_t cap,
                                       const char *select_clause,
                                       const char *tabname) {
    int n;

    if (!as_table_name_ok(tabname)) return -1;

    n = snprintf(sql, cap, "%s FROM %s ORDER BY id", select_clause, tabname);
    if (n < 0 || (size_t)n >= cap) return -1;
    return 0;
}

/* Parse a whitespace-separated rule row into rule[MAX_RULE_LENGTH].
   Returns the token count, or -1 if the row is not representable. */
static inline int as_parse_rule(const char *buf, int *rule) {
    int nr = 0;
    int *r = rule;
    const char *p = buf;
    char *q;

    while (1) {
        /* Bound before the write: at nr == MAX_RULE_LENGTH the next store
           would land one past the end of the caller's rule_arr[]. */
        if (nr >= MAX_RULE_LENGTH) return -1;
        *r = strtol(p, &q, 10);
        if (p == q) break;
        p = q;
        nr++;
        r++;
    }
    return nr;
}

void register_parse_address(duckdb_connection connection);
void register_standardize_address(duckdb_connection connection,
                                   duckdb_extension_info info,
                                   struct duckdb_extension_access *access);
void register_debug_standardize_address(duckdb_connection connection,
                                         duckdb_extension_info info,
                                         struct duckdb_extension_access *access);
void register_load_us_address_data(duckdb_connection connection,
                                    duckdb_extension_info info,
                                    struct duckdb_extension_access *access);
void register_addrust_parse(duckdb_connection connection);

#endif
