/*
 * debug_standardize_address_duckdb.c
 *
 * DuckDB scalar function:
 *   debug_standardize_address(lextab, gaztab, rultab, micro, macro) -> VARCHAR (JSON)
 *   debug_standardize_address(lextab, gaztab, rultab, address) -> VARCHAR (JSON)
 *
 * Returns a JSON string exposing the standardization engine's internal
 * decision-making process: input tokenization, candidate rules with scores,
 * and the final standardized address.
 *
 * Port of the PostgreSQL debug_standardize_address() from address_standardizer.c.
 */

#include "duckdb_extension.h"
#include "address_standardizer_duckdb.h"

/* PAGC headers */
#include "pagc_api.h"
#include "pagc_std_api.h"
#include "pagc_tools.h"
#include "parseaddress-api.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <pthread.h>

DUCKDB_EXTENSION_EXTERN

/* ---- Growable string buffer ---- */

typedef struct {
    char *data;
    size_t len;
    size_t capacity;
} DynBuf;

static void dynbuf_init(DynBuf *buf) {
    buf->capacity = 4096;
    buf->data = (char *)malloc(buf->capacity);
    buf->data[0] = '\0';
    buf->len = 0;
}

static void dynbuf_ensure(DynBuf *buf, size_t extra) {
    if (buf->len + extra + 1 > buf->capacity) {
        while (buf->len + extra + 1 > buf->capacity)
            buf->capacity *= 2;
        buf->data = (char *)realloc(buf->data, buf->capacity);
    }
}

static void dynbuf_append(DynBuf *buf, const char *str) {
    size_t slen = strlen(str);
    dynbuf_ensure(buf, slen);
    memcpy(buf->data + buf->len, str, slen + 1);
    buf->len += slen;
}

static void dynbuf_append_char(DynBuf *buf, char c) {
    dynbuf_ensure(buf, 1);
    buf->data[buf->len++] = c;
    buf->data[buf->len] = '\0';
}

static void dynbuf_printf(DynBuf *buf, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int needed = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);

    dynbuf_ensure(buf, (size_t)needed);
    va_start(ap, fmt);
    vsnprintf(buf->data + buf->len, (size_t)needed + 1, fmt, ap);
    va_end(ap);
    buf->len += (size_t)needed;
}

static void dynbuf_free(DynBuf *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = buf->capacity = 0;
}

/* ---- JSON string escaping ---- */

static void dynbuf_append_json_string(DynBuf *buf, const char *str) {
    if (!str) {
        dynbuf_append(buf, "null");
        return;
    }
    dynbuf_append_char(buf, '"');
    for (const char *p = str; *p; p++) {
        switch (*p) {
            case '"':  dynbuf_append(buf, "\\\""); break;
            case '\\': dynbuf_append(buf, "\\\\"); break;
            case '\b': dynbuf_append(buf, "\\b"); break;
            case '\f': dynbuf_append(buf, "\\f"); break;
            case '\n': dynbuf_append(buf, "\\n"); break;
            case '\r': dynbuf_append(buf, "\\r"); break;
            case '\t': dynbuf_append(buf, "\\t"); break;
            default:
                if ((unsigned char)*p < 0x20) {
                    dynbuf_printf(buf, "\\u%04x", (unsigned char)*p);
                } else {
                    dynbuf_append_char(buf, *p);
                }
        }
    }
    dynbuf_append_char(buf, '"');
}

/* ---- Helpers duplicated from address_standardizer_duckdb.c ---- */

#define MAX_RULE_LENGTH 128

/* Check table name is safe (alphanumeric, _, .) */
static int dbg_table_name_ok(const char *t) {
    if (!t || !*t) return 0;
    while (*t != '\0') {
        if (!((*t >= 'a' && *t <= 'z') || (*t >= 'A' && *t <= 'Z') ||
              (*t >= '0' && *t <= '9') || *t == '_' || *t == '.'))
            return 0;
        t++;
    }
    return 1;
}

static char *dbg_extract_string(duckdb_vector vec, idx_t row) {
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

    char *copy = (char *)calloc(len + 2, 1);
    if (!copy) return NULL;
    memcpy(copy, str, len);
    return copy;
}

#define dbg_get_varchar dbg_extract_string

static int dbg_any_input_null(duckdb_vector *vecs, int nvecs, idx_t row) {
    for (int i = 0; i < nvecs; i++) {
        uint64_t *validity = duckdb_vector_get_validity(vecs[i]);
        if (validity && !duckdb_validity_row_is_valid(validity, row))
            return 1;
    }
    return 0;
}

/* ---- Rule parsing ---- */

static int dbg_parse_rule(const char *buf, int *rule) {
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

/* ---- DuckDB table loading (duplicated from address_standardizer_duckdb.c) ---- */

static int dbg_load_lex(duckdb_connection conn, LEXICON *lex, const char *tabname) {
    char sql[512];
    duckdb_result result;

    if (!dbg_table_name_ok(tabname)) return -1;

    snprintf(sql, sizeof(sql),
             "SELECT seq, word, stdword, token FROM %s ORDER BY id", tabname);

    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return -1;
    }

    duckdb_data_chunk chunk;
    while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        duckdb_vector v_seq   = duckdb_data_chunk_get_vector(chunk, 0);
        duckdb_vector v_word  = duckdb_data_chunk_get_vector(chunk, 1);
        duckdb_vector v_stdw  = duckdb_data_chunk_get_vector(chunk, 2);
        duckdb_vector v_token = duckdb_data_chunk_get_vector(chunk, 3);

        int32_t *seq_data   = (int32_t *)duckdb_vector_get_data(v_seq);
        int32_t *token_data = (int32_t *)duckdb_vector_get_data(v_token);

        for (idx_t i = 0; i < chunk_size; i++) {
            char *word = dbg_extract_string(v_word, i);
            char *stdword = dbg_extract_string(v_stdw, i);

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

static int dbg_load_rules(duckdb_connection conn, RULES *rules, const char *tabname) {
    char sql[512];
    duckdb_result result;
    int rule_arr[MAX_RULE_LENGTH];

    if (!dbg_table_name_ok(tabname)) return -1;

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
            char *rule_str = dbg_extract_string(v_rule, i);
            if (!rule_str) { err = 1; break; }

            int nr = dbg_parse_rule(rule_str, rule_arr);
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

static STANDARDIZER *dbg_create_standardizer(duckdb_connection conn,
                                              const char *lextab,
                                              const char *gaztab,
                                              const char *rultab) {
    STANDARDIZER *std = std_init();
    if (!std) return NULL;

    LEXICON *lex = lex_init(std->err_p);
    if (!lex) { std_free(std); return NULL; }

    if (dbg_load_lex(conn, lex, lextab) != 0) {
        lex_free(lex); std_free(std); return NULL;
    }

    LEXICON *gaz = lex_init(std->err_p);
    if (!gaz) { lex_free(lex); std_free(std); return NULL; }

    if (dbg_load_lex(conn, gaz, gaztab) != 0) {
        lex_free(gaz); lex_free(lex); std_free(std); return NULL;
    }

    RULES *rules = rules_init(std->err_p);
    if (!rules) { lex_free(gaz); lex_free(lex); std_free(std); return NULL; }

    if (dbg_load_rules(conn, rules, rultab) != 0) {
        rules_free(rules); lex_free(gaz); lex_free(lex); std_free(std); return NULL;
    }

    std_use_lex(std, lex);
    std_use_gaz(std, gaz);
    std_use_rules(std, rules);
    std_ready_standardizer(std);

    return std;
}

/* ---- Standardizer caching via extra_info ---- */

typedef struct {
    duckdb_connection connection;
    pthread_mutex_t mutex;
    STANDARDIZER *template_std;
    char *cached_lex;
    char *cached_gaz;
    char *cached_rul;
} dbg_extra_info;

static void dbg_extra_info_delete(void *data) {
    dbg_extra_info *info = (dbg_extra_info *)data;
    pthread_mutex_destroy(&info->mutex);
    if (info->template_std) std_free(info->template_std);
    if (info->connection) duckdb_disconnect(&info->connection);
    free(info->cached_lex);
    free(info->cached_gaz);
    free(info->cached_rul);
    free(info);
}

/*
 * Acquire the mutex and ensure the standardizer is loaded for the given tables.
 * Returns 1 if locked and ready, 0 on failure.
 */
static int dbg_acquire_standardizer(dbg_extra_info *extra,
                                     const char *lextab,
                                     const char *gaztab,
                                     const char *rultab) {
    pthread_mutex_lock(&extra->mutex);

    /* Check if cached standardizer matches the requested tables */
    if (extra->template_std &&
        extra->cached_lex && strcmp(extra->cached_lex, lextab) == 0 &&
        extra->cached_gaz && strcmp(extra->cached_gaz, gaztab) == 0 &&
        extra->cached_rul && strcmp(extra->cached_rul, rultab) == 0) {
        return 1;
    }

    /* Rebuild standardizer */
    if (extra->template_std) {
        std_free(extra->template_std);
        extra->template_std = NULL;
    }
    free(extra->cached_lex); extra->cached_lex = NULL;
    free(extra->cached_gaz); extra->cached_gaz = NULL;
    free(extra->cached_rul); extra->cached_rul = NULL;

    extra->template_std = dbg_create_standardizer(extra->connection, lextab, gaztab, rultab);
    if (!extra->template_std) {
        pthread_mutex_unlock(&extra->mutex);
        return 0;
    }

    extra->cached_lex = strdup(lextab);
    extra->cached_gaz = strdup(gaztab);
    extra->cached_rul = strdup(rultab);
    return 1;
}

/* ---- Safe symbol name helpers (ported from address_standardizer.c) ---- */

static const char *
debug_effective_standardized_word(const DEF *definition, const char *input_word) {
    if (!input_word)
        return NULL;
    if (!definition || definition->Protect || !definition->Standard)
        return input_word;
    return definition->Standard;
}

static const char *
debug_safe_input_symbol_name(SYMB input_symbol) {
    const char *input_name;
    if (input_symbol < 0 || input_symbol >= MAXINSYM)
        return "INVALID";
    input_name = in_symb_name(input_symbol);
    return input_name ? input_name : "NONE";
}

static const char *
debug_safe_output_symbol_name(SYMB output_symbol) {
    if (output_symbol == FAIL)
        return "NONE";
    if (output_symbol < 0 || output_symbol >= MAXOUTSYM)
        return "INVALID";
    const char *output_name = out_symb_name(output_symbol);
    return output_name ? output_name : "NONE";
}

static const char *
debug_rule_type_name(SYMB rule_type) {
    switch (rule_type) {
    case MACRO_C: return "MACRO";
    case MICRO_C: return "MICRO";
    case ARC_C:   return "ARC";
    case CIVIC_C: return "CIVIC";
    case EXTRA_C: return "EXTRA";
    default:      return "NONE";
    }
}

static bool
debug_parse_rule_metadata(const char *rule_string, SYMB *rule_type, SYMB *rule_weight) {
    const char *value_end;
    const char *value_start;
    char *end_ptr;
    long parsed_value;

    if (!rule_string || !rule_type || !rule_weight)
        return false;

    value_end = rule_string + strlen(rule_string);
    while (value_end > rule_string && value_end[-1] == ' ')
        value_end--;
    if (value_end == rule_string)
        return false;

    /* Parse rule_weight (last number) */
    value_start = value_end;
    while (value_start > rule_string && value_start[-1] != ' ')
        value_start--;
    parsed_value = strtol(value_start, &end_ptr, 10);
    if (end_ptr != value_end)
        return false;
    *rule_weight = (SYMB)parsed_value;

    /* Parse rule_type (second to last number) */
    value_end = value_start;
    while (value_end > rule_string && value_end[-1] == ' ')
        value_end--;
    if (value_end == rule_string)
        return false;

    value_start = value_end;
    while (value_start > rule_string && value_start[-1] != ' ')
        value_start--;
    parsed_value = strtol(value_start, &end_ptr, 10);
    if (end_ptr != value_end)
        return false;
    *rule_type = (SYMB)parsed_value;
    return true;
}

/* Build "zip-zipplus" postcode string. Returns malloc'd string or NULL. */
static char *dbg_build_postcode(const ADDRESS *paddr) {
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

/* ---- Rule lookup via DuckDB query ---- */

static void lookup_rule(duckdb_connection conn, const char *rultab,
                        const char *stub, DynBuf *buf) {
    char sql[1024];
    if (!dbg_table_name_ok(rultab)) {
        dynbuf_append(buf, ", \"rule_id\": -1");
        return;
    }

    /* Escape single quotes in stub for the LIKE pattern */
    snprintf(sql, sizeof(sql),
             "SELECT id, rule FROM %s WHERE rule LIKE '%s%%' LIMIT 1",
             rultab, stub);

    duckdb_result result;
    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        dynbuf_append(buf, ", \"rule_id\": -1");
        return;
    }

    duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
    if (chunk && duckdb_data_chunk_get_size(chunk) > 0) {
        duckdb_vector v_id   = duckdb_data_chunk_get_vector(chunk, 0);
        duckdb_vector v_rule = duckdb_data_chunk_get_vector(chunk, 1);

        int32_t rule_id = ((int32_t *)duckdb_vector_get_data(v_id))[0];
        char *rule_str = dbg_extract_string(v_rule, 0);

        dynbuf_printf(buf, ", \"rule_id\": %d", rule_id);
        dynbuf_append(buf, ", \"rule_string\": ");
        dynbuf_append_json_string(buf, rule_str);

        /* Parse and emit rule_type / rule_weight metadata */
        SYMB rule_type, rule_weight;
        if (debug_parse_rule_metadata(rule_str, &rule_type, &rule_weight)) {
            const char *type_name = debug_rule_type_name(rule_type);
            dynbuf_printf(buf, ", \"rule_type_code\": %d", rule_type);
            dynbuf_append(buf, ", \"rule_type\": ");
            dynbuf_append_json_string(buf, type_name);
            dynbuf_printf(buf, ", \"rule_weight\": %d", rule_weight);
        } else {
            dynbuf_append(buf, ", \"rule_type_code\": null");
            dynbuf_append(buf, ", \"rule_type\": null");
            dynbuf_append(buf, ", \"rule_weight\": null");
        }

        free(rule_str);
        duckdb_destroy_data_chunk(&chunk);
    } else {
        if (chunk) duckdb_destroy_data_chunk(&chunk);
        dynbuf_append(buf, ", \"rule_id\": -1");
        dynbuf_append(buf, ", \"rule_string\": null");
        dynbuf_append(buf, ", \"rule_type_code\": null");
        dynbuf_append(buf, ", \"rule_type\": null");
        dynbuf_append(buf, ", \"rule_weight\": null");
    }

    duckdb_destroy_result(&result);
}

/* ---- Core JSON builder ---- */

static void build_debug_json(DynBuf *buf, STANDARDIZER *std, STDADDR *stdaddr,
                              const char *micro, const char *macro,
                              const char *rultab, duckdb_connection conn) {
    STAND_PARAM *ms = std->misc_stand;
    STZ_PARAM *stz_info = ms->stz_info;
    int lex_pos;
    DEF *def;

    dynbuf_append_char(buf, '{');

    /* micro / macro */
    dynbuf_append(buf, "\"micro\":");
    dynbuf_append_json_string(buf, micro);
    dynbuf_append(buf, ",\"macro\":");
    dynbuf_append_json_string(buf, macro);

    /* input_tokens */
    dynbuf_append(buf, ",\"input_tokens\":[");
    int started = 0;
    for (lex_pos = FIRST_LEX_POS; lex_pos < ms->LexNum; lex_pos++) {
        for (def = ms->lex_vector[lex_pos].DefList; def != NULL; def = def->Next) {
            if (started > 0) dynbuf_append_char(buf, ',');

            dynbuf_printf(buf, "{\"pos\": %d,", lex_pos);
            dynbuf_append(buf, "\"word\":");
            dynbuf_append_json_string(buf, ms->lex_vector[lex_pos].Text);
            dynbuf_append(buf, ",\"stdword\":");
            dynbuf_append_json_string(buf, debug_effective_standardized_word(def, ms->lex_vector[lex_pos].Text));
            dynbuf_append(buf, ",\"token\":");
            dynbuf_append_json_string(buf, debug_safe_input_symbol_name(def->Type));
            dynbuf_printf(buf, ",\"token-code\": %d}", def->Type);
            started++;
        }
    }
    dynbuf_append_char(buf, ']');

    /* rules */
    int n = stz_info->stz_list_size;
    STZ **stz_list = stz_info->stz_array;

    dynbuf_append(buf, ", \"rules\":[");
    for (int stz_no = 0; stz_no < n; stz_no++) {
        if (stz_no > 0) dynbuf_append_char(buf, ',');

        STZ *cur_stz = stz_list[stz_no];
        char rule_in[256] = "";
        char rule_out[256] = "";

        dynbuf_printf(buf, "{\"score\": %f,", cur_stz->score);
        dynbuf_printf(buf, "\"raw_score\": %f,", cur_stz->raw_score);
        dynbuf_printf(buf, "\"no\": %d,", stz_no);

        dynbuf_append(buf, "\"rule_tokens\":[");
        started = 0;
        for (lex_pos = FIRST_LEX_POS; lex_pos < ms->LexNum; lex_pos++) {
            SYMB k2;
            def = cur_stz->definitions[lex_pos];
            k2 = cur_stz->output[lex_pos];

            if (started > 0) {
                dynbuf_append_char(buf, ',');
                strcat(rule_out, " ");
                strcat(rule_in, " ");
            }

            char temp[32];
            snprintf(temp, sizeof(temp), "%d", def->Type);
            strcat(rule_in, temp);
            snprintf(temp, sizeof(temp), "%d", k2);
            strcat(rule_out, temp);

            dynbuf_printf(buf, "{\"pos\": %d,", lex_pos);
            dynbuf_printf(buf, "\"input-token-code\": %d,", def->Type);
            dynbuf_append(buf, "\"input-token\": ");
            dynbuf_append_json_string(buf, debug_safe_input_symbol_name(def->Type));
            dynbuf_append(buf, ",\"input-word\": ");
            dynbuf_append_json_string(buf, ms->lex_vector[lex_pos].Text);
            dynbuf_append(buf, ",\"mapped-word\": ");
            dynbuf_append_json_string(buf, debug_effective_standardized_word(def, ms->lex_vector[lex_pos].Text));
            dynbuf_printf(buf, ",\"output-token-code\": %d,", k2);
            dynbuf_append(buf, "\"output-token\": ");
            dynbuf_append_json_string(buf, debug_safe_output_symbol_name(k2));
            dynbuf_append_char(buf, '}');

            started++;
            if (k2 == FAIL) break;
        }
        dynbuf_append_char(buf, ']');

        /* Build rule stub and look up in rules table */
        char rule_stub[512];
        snprintf(rule_stub, sizeof(rule_stub), "%s -1 %s -1 ", rule_in, rule_out);

        dynbuf_append(buf, ", \"rule_stub_string\": ");
        /* Append stub with trailing % for display (matching PostgreSQL behavior) */
        char stub_display[520];
        snprintf(stub_display, sizeof(stub_display), "%s -1 %s -1 %%", rule_in, rule_out);
        dynbuf_append_json_string(buf, stub_display);

        /* Query rules table for matching rule */
        lookup_rule(conn, rultab, rule_stub, buf);

        dynbuf_append_char(buf, '}');
    }
    dynbuf_append_char(buf, ']');

    /* stdaddr */
    dynbuf_append(buf, ",\"stdaddr\": {");
    if (stdaddr) {
        dynbuf_append(buf, "\"building\": ");
        dynbuf_append_json_string(buf, stdaddr->building && stdaddr->building[0] ? stdaddr->building : NULL);
        dynbuf_append(buf, ",\"house_num\": ");
        dynbuf_append_json_string(buf, stdaddr->house_num && stdaddr->house_num[0] ? stdaddr->house_num : NULL);
        dynbuf_append(buf, ",\"predir\": ");
        dynbuf_append_json_string(buf, stdaddr->predir && stdaddr->predir[0] ? stdaddr->predir : NULL);
        dynbuf_append(buf, ",\"qual\": ");
        dynbuf_append_json_string(buf, stdaddr->qual && stdaddr->qual[0] ? stdaddr->qual : NULL);
        dynbuf_append(buf, ",\"pretype\": ");
        dynbuf_append_json_string(buf, stdaddr->pretype && stdaddr->pretype[0] ? stdaddr->pretype : NULL);
        dynbuf_append(buf, ",\"name\": ");
        dynbuf_append_json_string(buf, stdaddr->name && stdaddr->name[0] ? stdaddr->name : NULL);
        dynbuf_append(buf, ",\"suftype\": ");
        dynbuf_append_json_string(buf, stdaddr->suftype && stdaddr->suftype[0] ? stdaddr->suftype : NULL);
        dynbuf_append(buf, ",\"sufdir\": ");
        dynbuf_append_json_string(buf, stdaddr->sufdir && stdaddr->sufdir[0] ? stdaddr->sufdir : NULL);
        dynbuf_append(buf, ",\"ruralroute\": ");
        dynbuf_append_json_string(buf, stdaddr->ruralroute && stdaddr->ruralroute[0] ? stdaddr->ruralroute : NULL);
        dynbuf_append(buf, ",\"extra\": ");
        dynbuf_append_json_string(buf, stdaddr->extra && stdaddr->extra[0] ? stdaddr->extra : NULL);
        dynbuf_append(buf, ",\"city\": ");
        dynbuf_append_json_string(buf, stdaddr->city && stdaddr->city[0] ? stdaddr->city : NULL);
        dynbuf_append(buf, ",\"state\": ");
        dynbuf_append_json_string(buf, stdaddr->state && stdaddr->state[0] ? stdaddr->state : NULL);
        dynbuf_append(buf, ",\"country\": ");
        {
            const char *country = (stdaddr->country && stdaddr->country[0]) ? stdaddr->country : NULL;
            if (country) {
                const char *canonical = country_code_from_name(country);
                if (canonical)
                    country = canonical;
            }
            dynbuf_append_json_string(buf, country);
        }
        dynbuf_append(buf, ",\"postcode\": ");
        dynbuf_append_json_string(buf, stdaddr->postcode && stdaddr->postcode[0] ? stdaddr->postcode : NULL);
        dynbuf_append(buf, ",\"box\": ");
        dynbuf_append_json_string(buf, stdaddr->box && stdaddr->box[0] ? stdaddr->box : NULL);
        dynbuf_append(buf, ",\"unit\": ");
        dynbuf_append_json_string(buf, stdaddr->unit && stdaddr->unit[0] ? stdaddr->unit : NULL);
    }
    dynbuf_append(buf, "}}");
}

/* ---- set output to null for a VARCHAR vector ---- */
static void set_varchar_null(duckdb_vector output, idx_t row) {
    duckdb_vector_ensure_validity_writable(output);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
}

/* ---- Scalar function: debug_standardize_address (5 args) ---- */

static void debug_standardize_address_mm_func(duckdb_function_info info,
                                                duckdb_data_chunk input,
                                                duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);

    duckdb_vector inputs[5];
    for (int i = 0; i < 5; i++)
        inputs[i] = duckdb_data_chunk_get_vector(input, i);

    dbg_extra_info *extra = (dbg_extra_info *)duckdb_scalar_function_get_extra_info(info);

    int hash_err = 0;
    HHash *stH = get_cached_state_hash(&hash_err);

    int locked = 0;

    for (idx_t row = 0; row < input_size; row++) {
        if (dbg_any_input_null(inputs, 5, row) || hash_err) {
            set_varchar_null(output, row);
            continue;
        }

        char *lextab = dbg_get_varchar(inputs[0], row);
        char *gaztab = dbg_get_varchar(inputs[1], row);
        char *rultab = dbg_get_varchar(inputs[2], row);
        char *micro  = dbg_get_varchar(inputs[3], row);
        char *macro  = dbg_get_varchar(inputs[4], row);

        if (!lextab || !gaztab || !rultab || !micro || !macro) {
            set_varchar_null(output, row);
            free(lextab); free(gaztab); free(rultab); free(micro); free(macro);
            continue;
        }

        if (!locked) {
            if (dbg_acquire_standardizer(extra, lextab, gaztab, rultab)) {
                locked = 1;
            }
        }

        if (!locked) {
            set_varchar_null(output, row);
            free(lextab); free(gaztab); free(rultab); free(micro); free(macro);
            continue;
        }

        STDADDR *stdaddr = NULL;
        char *macro_copy = strdup(macro);
        char country_code[3] = "";
        bool has_country = false;
        if (macro_copy) {
            has_country = strip_explicit_country_token(macro_copy, country_code);
            stdaddr = std_standardize_mm(extra->template_std, micro, macro_copy, 0);
            free(macro_copy);
        } else {
            stdaddr = std_standardize_mm(extra->template_std, micro, macro, 0);
        }
        if (stdaddr && has_country) {
            free(stdaddr->country);
            stdaddr->country = strdup(country_code);
        }
        if (stdaddr && stdaddr->city && stdaddr->city[0] != '\0' &&
            (!stdaddr->state || stdaddr->state[0] == '\0') &&
            stdaddr->postcode && stdaddr->postcode[0] != '\0') {
            char *probe = strdup(stdaddr->city);
            if (probe) {
                strtoupper(probe);
                if (hash_get(stH, probe)) {
                    free(stdaddr->state);
                    stdaddr->state = stdaddr->city;
                    stdaddr->city = NULL;
                }
                free(probe);
            }
        }

        DynBuf buf;
        dynbuf_init(&buf);
        build_debug_json(&buf, extra->template_std, stdaddr, micro, macro,
                         rultab, extra->connection);

        duckdb_vector_assign_string_element_len(output, row, buf.data, buf.len);

        dynbuf_free(&buf);
        if (stdaddr) stdaddr_free(stdaddr);
        free(lextab); free(gaztab); free(rultab); free(micro); free(macro);
    }

    if (locked) pthread_mutex_unlock(&extra->mutex);
}

/* ---- Scalar function: debug_standardize_address (4 args, single-line) ---- */

static void debug_standardize_address_one_func(duckdb_function_info info,
                                                 duckdb_data_chunk input,
                                                 duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);

    duckdb_vector inputs[4];
    for (int i = 0; i < 4; i++)
        inputs[i] = duckdb_data_chunk_get_vector(input, i);

    dbg_extra_info *extra = (dbg_extra_info *)duckdb_scalar_function_get_extra_info(info);

    /* Thread-local state hash for parseaddress */
    int hash_err = 0;
    HHash *stH = get_cached_state_hash(&hash_err);

    int locked = 0;

    for (idx_t row = 0; row < input_size; row++) {
        if (dbg_any_input_null(inputs, 4, row) || hash_err) {
            set_varchar_null(output, row);
            continue;
        }

        char *lextab = dbg_get_varchar(inputs[0], row);
        char *gaztab = dbg_get_varchar(inputs[1], row);
        char *rultab = dbg_get_varchar(inputs[2], row);
        char *addr   = dbg_get_varchar(inputs[3], row);

        if (!lextab || !gaztab || !rultab || !addr) {
            set_varchar_null(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        /* Parse single-line address into components */
        int err = 0;
        ADDRESS *paddr = parseaddress(stH, addr, &err);
        if (!paddr || paddr->street2 || !paddr->address1) {
            if (paddr) free_address(paddr);
            set_varchar_null(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        char *micro = strdup(paddr->address1);

        /* Build macro string for JSON output (informational) */
        char macro[1024];
        int moffset = 0;
        if (paddr->city) moffset += snprintf(macro + moffset, sizeof(macro) - moffset, "%s,", paddr->city);
        if (paddr->st)   moffset += snprintf(macro + moffset, sizeof(macro) - moffset, "%s,", paddr->st);
        if (paddr->zip)  moffset += snprintf(macro + moffset, sizeof(macro) - moffset, "%s,", paddr->zip);
        if (paddr->cc)   moffset += snprintf(macro + moffset, sizeof(macro) - moffset, "%s,", paddr->cc);
        if (moffset == 0) macro[0] = '\0';
        (void)moffset;

        if (!micro) {
            free_address(paddr);
            set_varchar_null(output, row);
            free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        if (!locked) {
            if (dbg_acquire_standardizer(extra, lextab, gaztab, rultab)) {
                locked = 1;
            }
        }

        if (!locked) {
            free_address(paddr);
            set_varchar_null(output, row);
            free(micro); free(lextab); free(gaztab); free(rultab); free(addr);
            continue;
        }

        char *postcode = dbg_build_postcode(paddr);
        STDADDR *stdaddr = std_standardize(extra->template_std, micro,
                                            paddr->city, paddr->st,
                                            postcode, paddr->cc, 0);
        free_address(paddr);
        free(postcode);

        DynBuf buf;
        dynbuf_init(&buf);
        build_debug_json(&buf, extra->template_std, stdaddr, micro, macro,
                         rultab, extra->connection);

        duckdb_vector_assign_string_element_len(output, row, buf.data, buf.len);

        dynbuf_free(&buf);
        if (stdaddr) stdaddr_free(stdaddr);
        free(micro); free(lextab); free(gaztab); free(rultab); free(addr);
    }

    if (locked) pthread_mutex_unlock(&extra->mutex);
}

/* ---- Registration ---- */

void register_debug_standardize_address(duckdb_connection connection,
                                         duckdb_extension_info info,
                                         struct duckdb_extension_access *access) {
    duckdb_database *db = access->get_database(info);
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

    /* ---- Register 5-arg version: (lex, gaz, rul, micro, macro) ---- */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "debug_standardize_address");

        for (int i = 0; i < 5; i++)
            duckdb_scalar_function_add_parameter(function, varchar_type);

        duckdb_scalar_function_set_return_type(function, varchar_type);
        duckdb_scalar_function_set_function(function, debug_standardize_address_mm_func);

        dbg_extra_info *extra = (dbg_extra_info *)calloc(1, sizeof(dbg_extra_info));
        duckdb_connect(*db, &extra->connection);
        pthread_mutex_init(&extra->mutex, NULL);
        duckdb_scalar_function_set_extra_info(function, extra, dbg_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    /* ---- Register 4-arg version: (lex, gaz, rul, address) ---- */
    {
        duckdb_scalar_function function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, "debug_standardize_address");

        for (int i = 0; i < 4; i++)
            duckdb_scalar_function_add_parameter(function, varchar_type);

        duckdb_scalar_function_set_return_type(function, varchar_type);
        duckdb_scalar_function_set_function(function, debug_standardize_address_one_func);

        dbg_extra_info *extra = (dbg_extra_info *)calloc(1, sizeof(dbg_extra_info));
        duckdb_connect(*db, &extra->connection);
        pthread_mutex_init(&extra->mutex, NULL);
        duckdb_scalar_function_set_extra_info(function, extra, dbg_extra_info_delete);

        duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(&function);
    }

    duckdb_destroy_logical_type(&varchar_type);
}
