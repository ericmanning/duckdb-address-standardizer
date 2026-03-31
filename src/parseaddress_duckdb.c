/*
 * parseaddress_duckdb.c
 *
 * DuckDB scalar function: parse_address(text) -> STRUCT
 *
 * Wraps the parseaddress-api.c logic to expose parse_address()
 * as a DuckDB scalar function returning a STRUCT with fields:
 *   num, street, street2, address1, city, state, zip, zipplus, country
 */

#include "duckdb_extension.h"
#include "address_standardizer_duckdb.h"
#include "parseaddress-api.h"

#include <string.h>
#include <stdlib.h>

DUCKDB_EXTENSION_EXTERN

/*
 * free_address - free an ADDRESS struct and its members.
 * Declared in parseaddress-api.h but not implemented in the PG version
 * (PG relies on memory contexts for cleanup). We implement it here.
 *
 * All fields are independently allocated via palloc0/pstrdup (-> calloc/strdup).
 */
void free_address(ADDRESS *a) {
    if (!a) return;
    free(a->num);
    free(a->street);
    free(a->street2);
    free(a->address1);
    free(a->city);
    free(a->st);
    free(a->zip);
    free(a->zipplus);
    free(a->cc);
    free(a);
}

#define PARSE_ADDR_FIELDS 9

static const char *parse_address_field_names[PARSE_ADDR_FIELDS] = {
    "num", "street", "street2", "address1",
    "city", "state", "zip", "zipplus", "country"
};

/*
 * Assign a string (or NULL) to a child vector at a given row.
 */
static void set_string_or_null(duckdb_vector vec, idx_t row, const char *val) {
    if (val && val[0] != '\0') {
        duckdb_vector_assign_string_element(vec, row, val);
    } else {
        duckdb_vector_ensure_validity_writable(vec);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        duckdb_validity_set_row_invalid(validity, row);
    }
}

static void set_row_invalid(duckdb_vector output, idx_t row) {
    duckdb_vector_ensure_validity_writable(output);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
}

/*
 * Extract a C string from a duckdb_string_t, copying into a malloc'd buffer.
 * Returns NULL on allocation failure.
 */
static char *extract_duckdb_string(duckdb_vector vec, idx_t row) {
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

    char *copy = (char *)malloc(len + 1);
    if (!copy) return NULL;
    memcpy(copy, str, len);
    copy[len] = '\0';
    return copy;
}

/*
 * parse_address scalar function implementation.
 *
 * The state hash is initialized once per chunk (not per row) for performance.
 */
static void parse_address_func(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);
    duckdb_vector input_vec = duckdb_data_chunk_get_vector(input, 0);

    /* Get child vectors for the output struct */
    duckdb_vector children[PARSE_ADDR_FIELDS];
    for (int i = 0; i < PARSE_ADDR_FIELDS; i++) {
        children[i] = duckdb_struct_vector_get_child(output, i);
    }

    int hash_err = 0;
    HHash *stH = get_cached_state_hash(&hash_err);

    for (idx_t row = 0; row < input_size; row++) {
        /* Check for NULL input */
        uint64_t *input_validity = duckdb_vector_get_validity(input_vec);
        if (input_validity && !duckdb_validity_row_is_valid(input_validity, row)) {
            set_row_invalid(output, row);
            continue;
        }

        if (hash_err) {
            set_row_invalid(output, row);
            continue;
        }

        /* Get input string (parseaddress modifies in-place, so copy it) */
        char *input_copy = extract_duckdb_string(input_vec, row);
        if (!input_copy) {
            set_row_invalid(output, row);
            continue;
        }

        int err = 0;
        ADDRESS *paddr = parseaddress(stH, input_copy, &err);

        if (!paddr) {
            set_row_invalid(output, row);
            free(input_copy);
            continue;
        }

        /* Populate the struct fields */
        set_string_or_null(children[0], row, paddr->num);
        set_string_or_null(children[1], row, paddr->street);
        set_string_or_null(children[2], row, paddr->street2);
        set_string_or_null(children[3], row, paddr->address1);
        set_string_or_null(children[4], row, paddr->city);
        set_string_or_null(children[5], row, paddr->st);
        set_string_or_null(children[6], row, paddr->zip);
        set_string_or_null(children[7], row, paddr->zipplus);
        set_string_or_null(children[8], row, paddr->cc);

        free_address(paddr);
        free(input_copy);
    }

}

void register_parse_address(duckdb_connection connection) {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "parse_address");

    /* Input: one VARCHAR parameter */
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_add_parameter(function, varchar_type);

    /* Output: STRUCT with 9 VARCHAR fields */
    duckdb_logical_type field_types[PARSE_ADDR_FIELDS];
    for (int i = 0; i < PARSE_ADDR_FIELDS; i++) {
        field_types[i] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    }

    duckdb_logical_type return_type = duckdb_create_struct_type(
        field_types, parse_address_field_names, PARSE_ADDR_FIELDS);

    duckdb_scalar_function_set_return_type(function, return_type);

    for (int i = 0; i < PARSE_ADDR_FIELDS; i++) {
        duckdb_destroy_logical_type(&field_types[i]);
    }
    duckdb_destroy_logical_type(&return_type);
    duckdb_destroy_logical_type(&varchar_type);

    duckdb_scalar_function_set_function(function, parse_address_func);

    duckdb_register_scalar_function(connection, function);
    duckdb_destroy_scalar_function(&function);
}
