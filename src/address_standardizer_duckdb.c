/*
 * address_standardizer_duckdb.c
 *
 * DuckDB scalar functions:
 *   standardize_address(lextab, gaztab, rultab, micro, macro) -> STRUCT
 *   standardize_address(lextab, gaztab, rultab, address) -> STRUCT
 *
 * Replaces the PostgreSQL SPI-based data loading with DuckDB C API queries.
 * Uses the portable PAGC standardization engine.
 */

#include "duckdb_extension.h"
#include "address_standardizer_duckdb.h"

/* PAGC headers */
#include "pagc_api.h"
#include "pagc_std_api.h"
#include "parseaddress-api.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

DUCKDB_EXTENSION_EXTERN

#define STDADDR_FIELDS 16
#define MAX_RULE_LENGTH 128

/* Field names matching the PostgreSQL stdaddr type */
static const char *stdaddr_field_names[STDADDR_FIELDS] = {
    "building", "house_num", "predir", "qual",
    "pretype", "name", "suftype", "sufdir",
    "ruralroute", "extra", "city", "state",
    "country", "postcode", "box", "unit"
};

/* ---- Shared helpers ---- */

static void set_string_or_null(duckdb_vector vec, idx_t row, const char *val) {
    if (val && val[0] != '\0') {
        duckdb_vector_assign_string_element(vec, row, val);
    } else {
        duckdb_vector_ensure_validity_writable(vec);
        duckdb_validity_set_row_invalid(duckdb_vector_get_validity(vec), row);
    }
}

static void set_row_invalid(duckdb_vector output, idx_t row) {
    duckdb_vector_ensure_validity_writable(output);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
    /* Also null-out all struct children so DuckDB doesn't read uninitialized data */
    for (int i = 0; i < STDADDR_FIELDS; i++) {
        duckdb_vector child = duckdb_struct_vector_get_child(output, i);
        duckdb_vector_ensure_validity_writable(child);
        duckdb_validity_set_row_invalid(duckdb_vector_get_validity(child), row);
    }
}

static void populate_stdaddr(duckdb_vector *children, idx_t row, STDADDR *stdaddr) {
    set_string_or_null(children[0],  row, stdaddr->building);
    set_string_or_null(children[1],  row, stdaddr->house_num);
    set_string_or_null(children[2],  row, stdaddr->predir);
    set_string_or_null(children[3],  row, stdaddr->qual);
    set_string_or_null(children[4],  row, stdaddr->pretype);
    set_string_or_null(children[5],  row, stdaddr->name);
    set_string_or_null(children[6],  row, stdaddr->suftype);
    set_string_or_null(children[7],  row, stdaddr->sufdir);
    set_string_or_null(children[8],  row, stdaddr->ruralroute);
    set_string_or_null(children[9],  row, stdaddr->extra);
    set_string_or_null(children[10], row, stdaddr->city);
    set_string_or_null(children[11], row, stdaddr->state);
    /* Normalize country to ISO 3166-1 alpha-2 (e.g. "USA" -> "US") */
    const char *country = stdaddr->country;
    if (country) {
        const char *canonical = country_code_from_name(country);
        if (canonical)
            country = canonical;
    }
    set_string_or_null(children[12], row, country);
    set_string_or_null(children[13], row, stdaddr->postcode);
    set_string_or_null(children[14], row, stdaddr->box);
    set_string_or_null(children[15], row, stdaddr->unit);
}

/* Build "zip-zipplus" postcode string. Returns malloc'd string or NULL. */
static char *build_postcode(const ADDRESS *paddr) {
    if (!paddr || !paddr->zip || paddr->zip[0] == '\0')
        return NULL;
    if (paddr->zipplus && paddr->zipplus[0] != '\0') {
        size_t len = strlen(paddr->zip) + 1 + strlen(paddr->zipplus) + 1;
        char *buf = (char *)malloc(len);
        if (buf)
            snprintf(buf, len, "%s-%s", paddr->zip, paddr->zipplus);
        return buf;
    }
    return strdup(paddr->zip);
}

/*
 * Parse macro input to extract city/state/zip components, applying the same
 * adjustments as the PG parse_macro_input() function. The returned ADDRESS
 * must be freed with free_address(). The cc field is set to NULL since the
 * macro doesn't carry explicit country info.
 *
 * Thread-safe: uses thread-local regex cache and caller-provided stH.
 */
static ADDRESS *parse_macro_components(HHash *stH, const char *raw_macro) {
    int err = 0;
    char *macro_copy = strdup(raw_macro);
    if (!macro_copy) return NULL;

    ADDRESS *pm = parseaddress(stH, macro_copy, &err);
    free(macro_copy);
    if (!pm) return NULL;

    bool has_macro_tail =
        (pm->st && pm->st[0] != '\0') || (pm->zip && pm->zip[0] != '\0');

    /* If address1 looks like a city prefix, fold it into city */
    if (has_macro_tail && pm->address1 && pm->address1[0] != '\0' &&
        !pm->num && !pm->street && !pm->street2) {
        if (pm->city && pm->city[0] != '\0') {
            size_t len = strlen(pm->address1) + 1 + strlen(pm->city) + 1;
            char *merged = (char *)malloc(len);
            if (merged) {
                snprintf(merged, len, "%s %s", pm->address1, pm->city);
                free(pm->city);
                pm->city = merged;
            }
        } else {
            free(pm->city);
            pm->city = strdup(pm->address1);
        }
        free(pm->address1);
        pm->address1 = NULL;
    }

    /* State/postcode-only: swap city into state when no explicit state */
    if ((!pm->st || pm->st[0] == '\0') && pm->zip && pm->zip[0] != '\0') {
        if (pm->city && pm->city[0] != '\0') {
            free(pm->st);
            pm->st = pm->city;
            pm->city = NULL;
        } else if (pm->address1 && pm->address1[0] != '\0' &&
                   !pm->num && !pm->street && !pm->street2) {
            free(pm->st);
            pm->st = pm->address1;
            pm->address1 = NULL;
        }
    }

    /* Macro doesn't carry country */
    free(pm->cc);
    pm->cc = NULL;
    return pm;
}

/* Check table name is safe (alphanumeric, _, .) — no double quotes allowed */
static int table_name_ok(const char *t) {
    if (!t || !*t) return 0;
    while (*t != '\0') {
        if (!((*t >= 'a' && *t <= 'z') || (*t >= 'A' && *t <= 'Z') ||
              (*t >= '0' && *t <= '9') || *t == '_' || *t == '.'))
            return 0;
        t++;
    }
    return 1;
}

/* get_varchar: alias for scalar function callers */
#define get_varchar extract_string_from_vector

/* Check if any of the input vectors have a NULL at the given row */
static int any_input_null(duckdb_vector *vecs, int nvecs, idx_t row) {
    for (int i = 0; i < nvecs; i++) {
        uint64_t *validity = duckdb_vector_get_validity(vecs[i]);
        if (validity && !duckdb_validity_row_is_valid(validity, row))
            return 1;
    }
    return 0;
}

/* ---- Rule parsing ---- */

static int parse_rule(const char *buf, int *rule) {
    int nr = 0;
    int *r = rule;
    const char *p = buf;
    char *q;

    while (1) {
        *r = strtol(p, &q, 10);
        if (p == q) break;
        p = q;
        nr++;
        r++;
        if (nr > MAX_RULE_LENGTH) return -1;
    }
    return nr;
}

/* ---- DuckDB table loading ---- */

/*
 * Extract a C string from a vector at a given row into a malloc'd buffer.
 * Used for loading reference data from query results.
 */
static char *extract_string_from_vector(duckdb_vector vec, idx_t row) {
    duckdb_string_t raw = ((duckdb_string_t *)duckdb_vector_get_data(vec))[row];
    const char *str;
    idx_t len;

    if (raw.value.inlined.length <= 12) {
        str = raw.value.inlined.inlined;
        len = raw.value.inlined.length;
    } else {
        str = raw.value.pointer.ptr;
        len = raw.value.pointer.length;
    }

    /* Allocate with extra padding: the PAGC scanner does 1-byte lookahead
       past the end of digit sequences (standard.c _Scan_Next_), so we need
       at least one extra zero byte beyond the null terminator. */
    char *copy = (char *)calloc(len + 2, 1);
    if (!copy) return NULL;
    memcpy(copy, str, len);
    return copy;
}

static int load_lex_from_duckdb(duckdb_connection conn, LEXICON *lex, const char *tabname) {
    char sql[512];
    duckdb_result result;

    if (!table_name_ok(tabname)) return -1;

    snprintf(sql, sizeof(sql),
             "SELECT seq, word, stdword, token FROM %s ORDER BY id", tabname);

    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return -1;
    }

    duckdb_data_chunk chunk;
    while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        duckdb_vector v_seq    = duckdb_data_chunk_get_vector(chunk, 0);
        duckdb_vector v_word   = duckdb_data_chunk_get_vector(chunk, 1);
        duckdb_vector v_stdw   = duckdb_data_chunk_get_vector(chunk, 2);
        duckdb_vector v_token  = duckdb_data_chunk_get_vector(chunk, 3);

        int32_t *seq_data   = (int32_t *)duckdb_vector_get_data(v_seq);
        int32_t *token_data = (int32_t *)duckdb_vector_get_data(v_token);

        for (idx_t i = 0; i < chunk_size; i++) {
            char *word = extract_string_from_vector(v_word, i);
            char *stdword = extract_string_from_vector(v_stdw, i);

            if (word && stdword) {
                lex_add_entry(lex, (int)seq_data[i], word, stdword, (int)token_data[i]);
            }

            free(word);
            free(stdword);
        }
        duckdb_destroy_data_chunk(&chunk);
    }

    duckdb_destroy_result(&result);
    return 0;
}

static int load_rules_from_duckdb(duckdb_connection conn, RULES *rules, const char *tabname) {
    char sql[512];
    duckdb_result result;
    int rule_arr[MAX_RULE_LENGTH];

    if (!table_name_ok(tabname)) return -1;

    snprintf(sql, sizeof(sql), "SELECT rule FROM %s ORDER BY id", tabname);

    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return -1;
    }

    int err = 0;
    duckdb_data_chunk chunk;
    while (!err && (chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        duckdb_vector v_rule = duckdb_data_chunk_get_vector(chunk, 0);

        for (idx_t i = 0; i < chunk_size && !err; i++) {
            char *rule_str = extract_string_from_vector(v_rule, i);
            if (!rule_str) { err = 1; break; }

            int nr = parse_rule(rule_str, rule_arr);
            free(rule_str);

            if (nr == -1 || rules_add_rule(rules, nr, rule_arr) != 0) {
                err = 1;
            }
        }
        duckdb_destroy_data_chunk(&chunk);
    }

    duckdb_destroy_result(&result);
    if (err) return -1;
    return rules_ready(rules) == 0 ? 0 : -1;
}

static STANDARDIZER *create_standardizer(duckdb_connection conn,
                                          const char *lextab,
                                          const char *gaztab,
                                          const char *rultab) {
    STANDARDIZER *std = std_init();
    if (!std) return NULL;

    LEXICON *lex = lex_init(std->err_p);
    if (!lex) { std_free(std); return NULL; }

    if (load_lex_from_duckdb(conn, lex, lextab) != 0) {
        lex_free(lex); std_free(std); return NULL;
    }

    LEXICON *gaz = lex_init(std->err_p);
    if (!gaz) { lex_free(lex); std_free(std); return NULL; }

    if (load_lex_from_duckdb(conn, gaz, gaztab) != 0) {
        lex_free(gaz); lex_free(lex); std_free(std); return NULL;
    }

    RULES *rules = rules_init(std->err_p);
    if (!rules) { lex_free(gaz); lex_free(lex); std_free(std); return NULL; }

    if (load_rules_from_duckdb(conn, rules, rultab) != 0) {
        rules_free(rules); lex_free(gaz); lex_free(lex); std_free(std); return NULL;
    }

    std_use_lex(std, lex);
    std_use_gaz(std, gaz);
    std_use_rules(std, rules);
    std_ready_standardizer(std);

    return std;
}

/* ---- Standardizer caching via extra_info ---- */

/*
 * Thread-safety model:
 *
 * The PAGC standardization engine was designed for PostgreSQL's
 * process-per-connection model and contains implicit assumptions about
 * single-threaded access to lexicon, rule, and definition data structures.
 * Rather than auditing every code path for thread-safety, we serialize
 * all standardization through a mutex. Each DuckDB worker thread acquires
 * the mutex before using the cached STANDARDIZER and releases it after
 * the chunk is processed. The STANDARDIZER (including its STAND_PARAM
 * working buffers) is reused directly — no per-chunk copies needed.
 */

typedef struct {
    duckdb_connection connection;  /* dedicated connection for loading reference data */
    pthread_mutex_t mutex;         /* serialize all standardization */
    STANDARDIZER *template_std;    /* cached standardizer (owns lex/gaz/rules + working buffers) */
    char *cached_lex;
    char *cached_gaz;
    char *cached_rul;
} std_extra_info;

static void std_extra_info_delete(void *data) {
    std_extra_info *info = (std_extra_info *)data;
    pthread_mutex_destroy(&info->mutex);
    if (info->template_std) std_free(info->template_std);
    if (info->connection) duckdb_disconnect(&info->connection);
    free(info->cached_lex);
    free(info->cached_gaz);
    free(info->cached_rul);
    free(info);
}

/* Check if the cached template matches the given table names. */
static int cache_matches(std_extra_info *extra,
                          const char *lextab,
                          const char *gaztab,
                          const char *rultab) {
    return (extra->template_std &&
            extra->cached_lex && strcmp(extra->cached_lex, lextab) == 0 &&
            extra->cached_gaz && strcmp(extra->cached_gaz, gaztab) == 0 &&
            extra->cached_rul && strcmp(extra->cached_rul, rultab) == 0);
}

/*
 * Acquire exclusive access to the standardizer, creating it if needed.
 * On success, returns 1 with the mutex held (caller must unlock).
 * On failure, returns 0 with no lock held.
 */
static int acquire_standardizer(std_extra_info *extra,
                                 const char *lextab,
                                 const char *gaztab,
                                 const char *rultab) {
    pthread_mutex_lock(&extra->mutex);

    if (cache_matches(extra, lextab, gaztab, rultab))
        return 1;

    /* Cache miss — create or replace the standardizer */
    if (extra->template_std) { std_free(extra->template_std); extra->template_std = NULL; }
    free(extra->cached_lex); extra->cached_lex = NULL;
    free(extra->cached_gaz); extra->cached_gaz = NULL;
    free(extra->cached_rul); extra->cached_rul = NULL;

    STANDARDIZER *std = create_standardizer(extra->connection, lextab, gaztab, rultab);
    if (std) {
        extra->template_std = std;
        extra->cached_lex = strdup(lextab);
        extra->cached_gaz = strdup(gaztab);
        extra->cached_rul = strdup(rultab);
        return 1;
    }

    pthread_mutex_unlock(&extra->mutex);
    return 0;
}

/* ---- Scalar function: standardize_address (5 args) ---- */

static void standardize_address_mm_func(duckdb_function_info info,
                                         duckdb_data_chunk input,
                                         duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);

    duckdb_vector inputs[5];
    for (int i = 0; i < 5; i++)
        inputs[i] = duckdb_data_chunk_get_vector(input, i);

    duckdb_vector children[STDADDR_FIELDS];
    for (int i = 0; i < STDADDR_FIELDS; i++)
        children[i] = duckdb_struct_vector_get_child(output, i);

    std_extra_info *extra = (std_extra_info *)duckdb_scalar_function_get_extra_info(info);

    /* Thread-local state hash for macro parsing (thread-safe, no lock needed) */
    int hash_err = 0;
    HHash *stH = get_cached_state_hash(&hash_err);

    /*
     * Two-pass approach: parseaddress is thread-safe (thread-local regex
     * cache + thread-local state hash), but the PAGC engine is not.
     * Pass 1 parses all macros WITHOUT holding the mutex, so other threads
     * can run PAGC concurrently. Pass 2 acquires the mutex and standardizes.
     */

    /* Per-row pre-parsed data */
    ADDRESS **parsed   = (ADDRESS **)calloc(input_size, sizeof(ADDRESS *));
    char    **postcodes = (char **)calloc(input_size, sizeof(char *));
    char    **micros    = (char **)calloc(input_size, sizeof(char *));
    char    **macros    = (char **)calloc(input_size, sizeof(char *));
    char     *first_lex = NULL, *first_gaz = NULL, *first_rul = NULL;

    if (!parsed || !postcodes || !micros || !macros) {
        for (idx_t row = 0; row < input_size; row++)
            set_row_invalid(output, row);
        free(parsed); free(postcodes); free(micros); free(macros);
        return;
    }

    /* ---- Pass 1: Parse macros (no mutex, runs in parallel across threads) ---- */
    for (idx_t row = 0; row < input_size; row++) {
        if (any_input_null(inputs, 5, row) || hash_err) {
            set_row_invalid(output, row);
            continue;
        }

        char *lextab = get_varchar(inputs[0], row);
        char *gaztab = get_varchar(inputs[1], row);
        char *rultab = get_varchar(inputs[2], row);
        micros[row]  = get_varchar(inputs[3], row);
        macros[row]  = get_varchar(inputs[4], row);

        if (!lextab || !gaztab || !rultab || !micros[row] || !macros[row]) {
            set_row_invalid(output, row);
            free(lextab); free(gaztab); free(rultab);
            free(micros[row]); micros[row] = NULL;
            free(macros[row]); macros[row] = NULL;
            continue;
        }

        /* Save first valid table names for standardizer init */
        if (!first_lex) {
            first_lex = lextab; first_gaz = gaztab; first_rul = rultab;
        } else {
            free(lextab); free(gaztab); free(rultab);
        }

        parsed[row] = parse_macro_components(stH, macros[row]);
        postcodes[row] = parsed[row] ? build_postcode(parsed[row]) : NULL;
    }

    /* ---- Pass 2: Standardize under mutex (only PAGC work is serialized) ---- */
    int locked = 0;
    if (first_lex) {
        if (acquire_standardizer(extra, first_lex, first_gaz, first_rul))
            locked = 1;
    }

    for (idx_t row = 0; row < input_size; row++) {
        if (!micros[row])  /* already invalidated in pass 1 */
            continue;

        STDADDR *stdaddr = NULL;
        if (locked) {
            if (parsed[row]) {
                stdaddr = std_standardize(extra->template_std, micros[row],
                                           parsed[row]->city, parsed[row]->st,
                                           postcodes[row], parsed[row]->cc, 0);
            } else {
                stdaddr = std_standardize_mm(extra->template_std,
                                              micros[row], macros[row], 0);
            }
        }

        if (stdaddr) {
            populate_stdaddr(children, row, stdaddr);
            stdaddr_free(stdaddr);
        } else {
            set_row_invalid(output, row);
        }
    }

    if (locked) pthread_mutex_unlock(&extra->mutex);

    /* ---- Cleanup ---- */
    for (idx_t row = 0; row < input_size; row++) {
        free(postcodes[row]);
        if (parsed[row]) free_address(parsed[row]);
        free(micros[row]);
        free(macros[row]);
    }
    free(first_lex); free(first_gaz); free(first_rul);
    free(parsed); free(postcodes); free(micros); free(macros);
}

/* ---- Scalar function: standardize_address (4 args, single-line) ---- */

static void standardize_address_one_func(duckdb_function_info info,
                                          duckdb_data_chunk input,
                                          duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);

    duckdb_vector inputs[4];
    for (int i = 0; i < 4; i++)
        inputs[i] = duckdb_data_chunk_get_vector(input, i);

    duckdb_vector children[STDADDR_FIELDS];
    for (int i = 0; i < STDADDR_FIELDS; i++)
        children[i] = duckdb_struct_vector_get_child(output, i);

    std_extra_info *extra = (std_extra_info *)duckdb_scalar_function_get_extra_info(info);

    /* Thread-local state hash for parseaddress */
    int hash_err = 0;
    HHash *stH = get_cached_state_hash(&hash_err);

    /* Acquire exclusive access — PAGC engine is not thread-safe */
    int locked = 0;

    for (idx_t row = 0; row < input_size; row++) {
        if (any_input_null(inputs, 4, row) || hash_err) {
            set_row_invalid(output, row);
            continue;
        }

        char *lextab = get_varchar(inputs[0], row);
        char *gaztab = get_varchar(inputs[1], row);
        char *rultab = get_varchar(inputs[2], row);
        char *addr   = get_varchar(inputs[3], row);

        if (!lextab || !gaztab || !rultab || !addr) {
            set_row_invalid(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        /* Parse single-line address into components (no lock needed — stH is stack-local) */
        int err = 0;
        ADDRESS *paddr = parseaddress(stH, addr, &err);
        if (!paddr || paddr->street2 || !paddr->address1) {
            if (paddr) free_address(paddr);
            set_row_invalid(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        char *micro = strdup(paddr->address1);

        if (!micro) {
            free_address(paddr);
            set_row_invalid(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        if (!locked) {
            if (acquire_standardizer(extra, lextab, gaztab, rultab)) {
                locked = 1;
            }
        }

        char *postcode = build_postcode(paddr);

        STDADDR *stdaddr = NULL;
        if (locked) {
            stdaddr = std_standardize(extra->template_std, micro,
                                       paddr->city, paddr->st,
                                       postcode, paddr->cc, 0);
        }
        free_address(paddr);
        free(postcode);

        if (stdaddr) {
            populate_stdaddr(children, row, stdaddr);
            stdaddr_free(stdaddr);
        } else {
            set_row_invalid(output, row);
        }

        free(micro); free(lextab); free(gaztab); free(rultab); free(addr);
    }

    if (locked) pthread_mutex_unlock(&extra->mutex);
}

/* ---- Registration ---- */

static duckdb_logical_type create_stdaddr_type(void) {
    duckdb_logical_type field_types[STDADDR_FIELDS];
    for (int i = 0; i < STDADDR_FIELDS; i++) {
        field_types[i] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    }
    duckdb_logical_type struct_type = duckdb_create_struct_type(
        field_types, stdaddr_field_names, STDADDR_FIELDS);
    for (int i = 0; i < STDADDR_FIELDS; i++) {
        duckdb_destroy_logical_type(&field_types[i]);
    }
    return struct_type;
}

void register_standardize_address(duckdb_connection connection,
                                   duckdb_extension_info info,
                                   struct duckdb_extension_access *access) {
    duckdb_database *db = access->get_database(info);
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type return_type = create_stdaddr_type();

    /* ---- Register 5-arg version: (lex, gaz, rul, micro, macro) ---- */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "standardize_address");

        for (int i = 0; i < 5; i++)
            duckdb_scalar_function_add_parameter(function, varchar_type);

        duckdb_scalar_function_set_return_type(function, return_type);
        duckdb_scalar_function_set_function(function, standardize_address_mm_func);

        std_extra_info *extra = (std_extra_info *)calloc(1, sizeof(std_extra_info));
        /* Create a dedicated connection for reference data queries.
         * The init connection is temporary and gets disconnected after init. */
        duckdb_connect(*db, &extra->connection);
        pthread_mutex_init(&extra->mutex, NULL);
        duckdb_scalar_function_set_extra_info(function, extra, std_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    /* ---- Register 4-arg version: (lex, gaz, rul, address) ---- */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "standardize_address");

        for (int i = 0; i < 4; i++)
            duckdb_scalar_function_add_parameter(function, varchar_type);

        duckdb_scalar_function_set_return_type(function, return_type);
        duckdb_scalar_function_set_function(function, standardize_address_one_func);

        std_extra_info *extra = (std_extra_info *)calloc(1, sizeof(std_extra_info));
        duckdb_connect(*db, &extra->connection);
        pthread_mutex_init(&extra->mutex, NULL);
        duckdb_scalar_function_set_extra_info(function, extra, std_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    duckdb_destroy_logical_type(&varchar_type);
    duckdb_destroy_logical_type(&return_type);
}
