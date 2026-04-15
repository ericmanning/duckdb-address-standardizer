/*
 * addrust_parse_duckdb.c
 *
 * DuckDB scalar function: addrust_parse(text [, config_path]) -> STRUCT
 *
 * Wraps the addrust Rust address parser via the addrust-ffi C interface.
 * Two overloads:
 *   1-arg: addrust_parse(address)             — default pipeline
 *   2-arg: addrust_parse(address, config_path) — pipeline from TOML file
 */

#include "duckdb_extension.h"
#include "address_standardizer_duckdb.h"
#include "addrust_ffi.h"

#include <string.h>
#include <stdlib.h>
#include <pthread.h>

DUCKDB_EXTENSION_EXTERN

#define ADDRUST_FIELDS 15

static const char *addrust_field_names[ADDRUST_FIELDS] = {
    "street_number", "pre_direction", "street_name", "suffix",
    "post_direction", "unit_type", "unit", "po_box",
    "building", "building_type", "extra_front", "extra_back",
    "city", "state", "zip"
};

/* ----------------------------------------------------------------
 * String helpers
 * ---------------------------------------------------------------- */

/*
 * Extract a C string from a DuckDB string vector at a given row.
 * Returns a malloc'd null-terminated copy, or NULL on failure.
 */
static char *extract_duckdb_string_addrust(duckdb_vector vec, idx_t row) {
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

static void set_field_or_null(duckdb_vector vec, idx_t row, const char *val) {
    if (val && val[0] != '\0') {
        duckdb_vector_assign_string_element(vec, row, val);
    } else {
        duckdb_vector_ensure_validity_writable(vec);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        duckdb_validity_set_row_invalid(validity, row);
    }
}

static void set_row_invalid_addrust(duckdb_vector output, idx_t row) {
    duckdb_vector_ensure_validity_writable(output);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
}

/* ----------------------------------------------------------------
 * Thread-local default pipeline (1-arg overload)
 * ---------------------------------------------------------------- */

static _Thread_local AddrstPipeline *_default_pipeline = NULL;

static AddrstPipeline *get_default_pipeline(void) {
    if (!_default_pipeline) {
        _default_pipeline = addrust_pipeline_new();
    }
    return _default_pipeline;
}

/* ----------------------------------------------------------------
 * Mutex-protected config pipeline cache (2-arg overload)
 * ---------------------------------------------------------------- */

typedef struct {
    pthread_mutex_t mutex;
    AddrstPipeline *pipeline;
    char *cached_path;
} addrust_config_cache;

static void free_config_cache(void *data) {
    addrust_config_cache *cache = (addrust_config_cache *)data;
    if (cache->pipeline) addrust_pipeline_free(cache->pipeline);
    free(cache->cached_path);
    pthread_mutex_destroy(&cache->mutex);
    free(cache);
}

static AddrstPipeline *get_config_pipeline(addrust_config_cache *cache,
                                            const char *config_path) {
    pthread_mutex_lock(&cache->mutex);
    if (!cache->pipeline || !cache->cached_path ||
        strcmp(cache->cached_path, config_path) != 0) {
        if (cache->pipeline) addrust_pipeline_free(cache->pipeline);
        free(cache->cached_path);
        cache->pipeline = addrust_pipeline_new_from_file(config_path);
        cache->cached_path = strdup(config_path);
    }
    AddrstPipeline *p = cache->pipeline;
    pthread_mutex_unlock(&cache->mutex);
    return p;
}

/* ----------------------------------------------------------------
 * Shared chunk-processing logic
 * ---------------------------------------------------------------- */

static void process_chunk(AddrstPipeline *pipeline,
                           duckdb_data_chunk input,
                           duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector input_vec = duckdb_data_chunk_get_vector(input, 0);

    duckdb_vector children[ADDRUST_FIELDS];
    for (int i = 0; i < ADDRUST_FIELDS; i++) {
        children[i] = duckdb_struct_vector_get_child(output, i);
    }

    if (!pipeline) {
        for (idx_t row = 0; row < size; row++)
            set_row_invalid_addrust(output, row);
        return;
    }

    for (idx_t row = 0; row < size; row++) {
        /* Check for NULL input */
        uint64_t *validity = duckdb_vector_get_validity(input_vec);
        if (validity && !duckdb_validity_row_is_valid(validity, row)) {
            set_row_invalid_addrust(output, row);
            continue;
        }

        char *addr_str = extract_duckdb_string_addrust(input_vec, row);
        if (!addr_str) {
            set_row_invalid_addrust(output, row);
            continue;
        }

        AddrstAddress *result = addrust_parse(pipeline, addr_str);
        free(addr_str);

        if (!result) {
            set_row_invalid_addrust(output, row);
            continue;
        }

        /* Map struct fields to child vectors */
        char *fields[] = {
            result->street_number, result->pre_direction, result->street_name,
            result->suffix, result->post_direction, result->unit_type,
            result->unit, result->po_box, result->building,
            result->building_type, result->extra_front, result->extra_back,
            result->city, result->state, result->zip
        };
        for (int i = 0; i < ADDRUST_FIELDS; i++) {
            set_field_or_null(children[i], row, fields[i]);
        }

        addrust_address_free(result);
    }
}

/* ----------------------------------------------------------------
 * 1-arg overload: addrust_parse(address)
 * ---------------------------------------------------------------- */

static void addrust_parse_default_func(duckdb_function_info info,
                                        duckdb_data_chunk input,
                                        duckdb_vector output) {
    (void)info;
    process_chunk(get_default_pipeline(), input, output);
}

/* ----------------------------------------------------------------
 * 2-arg overload: addrust_parse(address, config_path)
 * ---------------------------------------------------------------- */

static void addrust_parse_config_func(duckdb_function_info info,
                                       duckdb_data_chunk input,
                                       duckdb_vector output) {
    addrust_config_cache *cache =
        (addrust_config_cache *)duckdb_scalar_function_get_extra_info(info);

    /* Extract config_path from 2nd parameter (constant across chunk) */
    duckdb_vector config_vec = duckdb_data_chunk_get_vector(input, 1);
    uint64_t *config_validity = duckdb_vector_get_validity(config_vec);

    /* NULL config_path → fall back to default pipeline */
    if (config_validity && !duckdb_validity_row_is_valid(config_validity, 0)) {
        process_chunk(get_default_pipeline(), input, output);
        return;
    }

    char *config_path = extract_duckdb_string_addrust(config_vec, 0);
    if (!config_path) {
        process_chunk(get_default_pipeline(), input, output);
        return;
    }

    AddrstPipeline *pipeline = get_config_pipeline(cache, config_path);
    process_chunk(pipeline, input, output);
    free(config_path);
}

/* ----------------------------------------------------------------
 * Registration
 * ---------------------------------------------------------------- */

static duckdb_logical_type make_addrust_return_type(void) {
    duckdb_logical_type field_types[ADDRUST_FIELDS];
    for (int i = 0; i < ADDRUST_FIELDS; i++) {
        field_types[i] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    }
    duckdb_logical_type ret = duckdb_create_struct_type(
        field_types, addrust_field_names, ADDRUST_FIELDS);
    for (int i = 0; i < ADDRUST_FIELDS; i++) {
        duckdb_destroy_logical_type(&field_types[i]);
    }
    return ret;
}

void register_addrust_parse(duckdb_connection connection) {
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type return_type = make_addrust_return_type();

    /* 1-arg: addrust_parse(address) */
    {
        duckdb_scalar_function fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(fn, "addrust_parse");
        duckdb_scalar_function_add_parameter(fn, varchar_type);
        duckdb_scalar_function_set_return_type(fn, return_type);
        duckdb_scalar_function_set_function(fn, addrust_parse_default_func);
        duckdb_register_scalar_function(connection, fn);
        duckdb_destroy_scalar_function(&fn);
    }

    /* 2-arg: addrust_parse(address, config_path) */
    {
        duckdb_scalar_function fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(fn, "addrust_parse");
        duckdb_scalar_function_add_parameter(fn, varchar_type);
        duckdb_scalar_function_add_parameter(fn, varchar_type);
        duckdb_scalar_function_set_return_type(fn, return_type);
        duckdb_scalar_function_set_function(fn, addrust_parse_config_func);

        /* Attach config cache as extra_info */
        addrust_config_cache *cache = calloc(1, sizeof(addrust_config_cache));
        pthread_mutex_init(&cache->mutex, NULL);
        duckdb_scalar_function_set_extra_info(fn, cache, free_config_cache);

        duckdb_register_scalar_function(connection, fn);
        duckdb_destroy_scalar_function(&fn);
    }

    duckdb_destroy_logical_type(&return_type);
    duckdb_destroy_logical_type(&varchar_type);
}
