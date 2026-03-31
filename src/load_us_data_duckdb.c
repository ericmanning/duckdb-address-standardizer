/*
 * load_us_data_duckdb.c
 *
 * Implements load_us_address_data() — a DuckDB scalar function that creates
 * and populates the us_lex, us_gaz, and us_rules reference tables from data
 * compiled into the extension binary.
 *
 * Signature: load_us_address_data([schema]) -> VARCHAR
 *   schema: optional schema name (default: current schema / unqualified)
 */

#include "duckdb_extension.h"
#include "us_lex_data.h"
#include "us_gaz_data.h"
#include "us_rules_data.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* ---- Extra info: holds a dedicated connection for DDL/DML ---- */

typedef struct {
    duckdb_connection connection;
} load_extra_info;

static void load_extra_info_delete(void *data) {
    load_extra_info *extra = (load_extra_info *)data;
    if (extra) {
        duckdb_disconnect(&extra->connection);
        free(extra);
    }
}

/* ---- Helpers ---- */

/* Validate schema name: only allow alphanumeric, underscore, dot */
static int schema_name_ok(const char *s) {
    if (!s || !*s) return 0;
    for (const char *p = s; *p; p++) {
        char c = *p;
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_'))
            return 0;
    }
    return 1;
}

/* Build a qualified table name: "schema.name" or just "name" */
static void build_table_name(char *buf, size_t bufsz,
                             const char *schema, const char *name) {
    if (schema && *schema)
        snprintf(buf, bufsz, "%s.%s", schema, name);
    else
        snprintf(buf, bufsz, "%s", name);
}

/* Execute a SQL statement, return 0 on success */
static int exec_sql(duckdb_connection conn, const char *sql) {
    duckdb_result result;
    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);
    return 0;
}

/* ---- Bulk loaders using DuckDB appender API ---- */

static int load_lex_table(duckdb_connection conn, const char *schema,
                          const char *table_name,
                          const lex_entry_t *data, int count) {
    char qualified[256];
    char sql[512];
    build_table_name(qualified, sizeof(qualified), schema, table_name);

    snprintf(sql, sizeof(sql),
        "CREATE TABLE IF NOT EXISTS %s ("
        "id INTEGER PRIMARY KEY, seq INTEGER, "
        "word VARCHAR, stdword VARCHAR, token INTEGER)", qualified);
    if (exec_sql(conn, sql) != 0) return -1;

    duckdb_appender appender;
    if (duckdb_appender_create(conn, schema, table_name, &appender) == DuckDBError) {
        duckdb_appender_destroy(&appender);
        return -1;
    }

    for (int i = 0; i < count; i++) {
        duckdb_append_int32(appender, i + 1);          /* id */
        duckdb_append_int32(appender, data[i].seq);     /* seq */
        duckdb_append_varchar(appender, data[i].word);  /* word */
        duckdb_append_varchar(appender, data[i].stdword);/* stdword */
        duckdb_append_int32(appender, data[i].token);   /* token */
        duckdb_appender_end_row(appender);
    }

    if (duckdb_appender_close(appender) == DuckDBError) {
        duckdb_appender_destroy(&appender);
        return -1;
    }
    duckdb_appender_destroy(&appender);
    return 0;
}

static int load_rules_table(duckdb_connection conn, const char *schema,
                            const char *table_name,
                            const rule_entry_t *data, int count) {
    char qualified[256];
    char sql[512];
    build_table_name(qualified, sizeof(qualified), schema, table_name);

    snprintf(sql, sizeof(sql),
        "CREATE TABLE IF NOT EXISTS %s ("
        "id INTEGER PRIMARY KEY, rule VARCHAR)", qualified);
    if (exec_sql(conn, sql) != 0) return -1;

    duckdb_appender appender;
    if (duckdb_appender_create(conn, schema, table_name, &appender) == DuckDBError) {
        duckdb_appender_destroy(&appender);
        return -1;
    }

    for (int i = 0; i < count; i++) {
        duckdb_append_int32(appender, i + 1);           /* id */
        duckdb_append_varchar(appender, data[i].rule);  /* rule */
        duckdb_appender_end_row(appender);
    }

    if (duckdb_appender_close(appender) == DuckDBError) {
        duckdb_appender_destroy(&appender);
        return -1;
    }
    duckdb_appender_destroy(&appender);
    return 0;
}

/* ---- DuckDB scalar function implementation ---- */

static void load_us_address_data_func(duckdb_function_info info,
                                       duckdb_data_chunk input,
                                       duckdb_vector output) {
    load_extra_info *extra =
        (load_extra_info *)duckdb_scalar_function_get_extra_info(info);
    duckdb_connection conn = extra->connection;

    idx_t count = duckdb_data_chunk_get_size(input);

    /* Get schema argument (may be NULL for 0-arg variant) */
    duckdb_vector schema_vec = NULL;
    idx_t n_params = duckdb_data_chunk_get_column_count(input);
    if (n_params > 0)
        schema_vec = duckdb_data_chunk_get_vector(input, 0);

    for (idx_t i = 0; i < count; i++) {
        const char *schema = NULL;
        char *schema_copy = NULL;

        if (schema_vec) {
            if (duckdb_validity_row_is_valid(
                    duckdb_vector_get_validity(schema_vec), i)) {
                duckdb_string_t sv = ((duckdb_string_t *)
                    duckdb_vector_get_data(schema_vec))[i];
                idx_t len = duckdb_string_is_inlined(sv)
                    ? strnlen(sv.value.inlined.inlined, 12)
                    : sv.value.pointer.length;
                const char *ptr = duckdb_string_is_inlined(sv)
                    ? sv.value.inlined.inlined
                    : sv.value.pointer.ptr;
                schema_copy = (char *)malloc(len + 1);
                memcpy(schema_copy, ptr, len);
                schema_copy[len] = '\0';
                schema = schema_copy;
            }
        }

        /* Validate schema if provided */
        if (schema && !schema_name_ok(schema)) {
            const char *err = "Invalid schema name";
            duckdb_vector_assign_string_element_len(output, i, err, strlen(err));
            free(schema_copy);
            continue;
        }

        /* Create schema if specified and doesn't exist */
        if (schema && *schema) {
            char sql[256];
            snprintf(sql, sizeof(sql), "CREATE SCHEMA IF NOT EXISTS %s", schema);
            exec_sql(conn, sql);
        }

        int ok = 1;
        if (load_lex_table(conn, schema, "us_lex",
                           US_LEX_DATA, US_LEX_COUNT) != 0)
            ok = 0;
        if (ok && load_lex_table(conn, schema, "us_gaz",
                                  US_GAZ_DATA, US_GAZ_COUNT) != 0)
            ok = 0;
        if (ok && load_rules_table(conn, schema, "us_rules",
                                    US_RULES_DATA, US_RULES_COUNT) != 0)
            ok = 0;

        char msg[256];
        if (ok) {
            char prefix[128] = "";
            if (schema && *schema)
                snprintf(prefix, sizeof(prefix), "%s.", schema);
            snprintf(msg, sizeof(msg),
                "Loaded %d lex + %d gaz + %d rules into %sus_lex, %sus_gaz, %sus_rules",
                US_LEX_COUNT, US_GAZ_COUNT, US_RULES_COUNT,
                prefix, prefix, prefix);
        } else {
            snprintf(msg, sizeof(msg), "Error loading reference data");
        }

        duckdb_vector_assign_string_element_len(output, i, msg, strlen(msg));
        free(schema_copy);
    }
}

/* ---- Registration ---- */

void register_load_us_address_data(duckdb_connection connection,
                                    duckdb_extension_info info,
                                    struct duckdb_extension_access *access) {
    duckdb_database *db = access->get_database(info);
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

    /* 0-arg version: load_us_address_data() */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "load_us_address_data");
        duckdb_scalar_function_set_return_type(function, varchar_type);
        duckdb_scalar_function_set_function(function, load_us_address_data_func);

        load_extra_info *extra = (load_extra_info *)calloc(1, sizeof(load_extra_info));
        duckdb_connect(*db, &extra->connection);
        duckdb_scalar_function_set_extra_info(function, extra, load_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    /* 1-arg version: load_us_address_data(schema) */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "load_us_address_data");
        duckdb_scalar_function_add_parameter(function, varchar_type);
        duckdb_scalar_function_set_return_type(function, varchar_type);
        duckdb_scalar_function_set_function(function, load_us_address_data_func);

        load_extra_info *extra = (load_extra_info *)calloc(1, sizeof(load_extra_info));
        duckdb_connect(*db, &extra->connection);
        duckdb_scalar_function_set_extra_info(function, extra, load_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    duckdb_destroy_logical_type(&varchar_type);
}
