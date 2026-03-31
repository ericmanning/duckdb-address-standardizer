/*
 * postgres.h compatibility shim for DuckDB build
 *
 * The PAGC core files include <postgres.h> or "postgres.h" but only
 * actually use a few things from it:
 *   - elog() macro for debug logging
 *   - palloc0(), palloc(), pfree(), pstrdup() memory functions
 *   - some integer type definitions
 *
 * This shim provides standard-C replacements so the PAGC core
 * compiles without any PostgreSQL headers.
 */

#ifndef POSTGRES_H_SHIM
#define POSTGRES_H_SHIM

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

/* ---- PostgreSQL integer types ---- */
typedef int32_t int4;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

/* Datum is a pointer-sized integer in PG */
typedef uintptr_t Datum;

/* ---- lengthof: number of elements in a static array ---- */
#ifndef lengthof
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#endif

/* ---- elog / ereport stubs ---- */
/* PG log levels */
#define DEBUG5   10
#define DEBUG4   11
#define DEBUG3   12
#define DEBUG2   13
#define DEBUG1   14
#define LOG      15
#define NOTICE   18
#define WARNING  19
#define ERROR    21

#ifdef DEBUG
#define elog(level, fmt, ...) \
    do { fprintf(stderr, "[%s] " fmt "\n", \
        (level >= ERROR ? "ERROR" : level >= NOTICE ? "NOTICE" : "DEBUG"), \
        ##__VA_ARGS__); } while(0)
#else
#define elog(level, fmt, ...) do { (void)(level); } while(0)
#endif

/* ---- Memory allocation replacements ---- */
static inline void *palloc0(size_t size) {
    void *p = calloc(1, size);
    return p;
}

static inline void *palloc(size_t size) {
    return malloc(size);
}

static inline void pfree(void *ptr) {
    free(ptr);
}

static inline char *pstrdup(const char *s) {
    return strdup(s);
}

#endif /* POSTGRES_H_SHIM */
