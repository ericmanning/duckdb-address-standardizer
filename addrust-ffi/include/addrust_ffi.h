/*
 * addrust_ffi.h
 *
 * C interface to the addrust address parser (Rust).
 * Built by the addrust-ffi wrapper crate.
 */

#ifndef ADDRUST_FFI_H
#define ADDRUST_FFI_H

/* Opaque pipeline handle */
typedef struct AddrstPipeline AddrstPipeline;

/* Parsed address — each field is a malloc'd C string or NULL */
typedef struct {
    char *street_number;
    char *pre_direction;
    char *street_name;
    char *suffix;
    char *post_direction;
    char *unit_type;
    char *unit;
    char *po_box;
    char *building;
    char *building_type;
    char *extra_front;
    char *extra_back;
    char *city;
    char *state;
    char *zip;
} AddrstAddress;

/* Create a pipeline with default configuration.
 * Caller must free with addrust_pipeline_free(). */
AddrstPipeline *addrust_pipeline_new(void);

/* Create a pipeline from a TOML config file.
 * Falls back to default if path is NULL.
 * Returns NULL only on UTF-8 error.
 * Caller must free with addrust_pipeline_free(). */
AddrstPipeline *addrust_pipeline_new_from_file(const char *path);

/* Parse a single address string.
 * Returns NULL if pipeline or input is NULL.
 * Caller must free with addrust_address_free(). */
AddrstAddress *addrust_parse(const AddrstPipeline *pipeline, const char *input);

/* Free a parsed address. */
void addrust_address_free(AddrstAddress *addr);

/* Free a pipeline. */
void addrust_pipeline_free(AddrstPipeline *pipeline);

#endif
