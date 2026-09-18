/*
 * utils/formatting.h -- shim for the DuckDB build.
 *
 * str_toupper() is only reached from upper_case()'s final branch, for database
 * encodings that are neither UTF-8 nor LATIN1.  GetDatabaseEncoding() is a
 * constant PG_UTF8 here, so that branch is unreachable -- but it still has to
 * compile, so this provides an ASCII-only implementation.
 */

#ifndef PAGC_SHIM_FORMATTING_H
#define PAGC_SHIM_FORMATTING_H

#include <ctype.h>
#include <stddef.h>
#include <string.h>

static inline char *str_toupper(const char *buff, size_t nbytes, unsigned int collid)
{
    char *result = (char *) palloc(nbytes + 1);
    size_t i;

    (void) collid;
    for (i = 0; i < nbytes; i++)
        result[i] = (char) toupper((unsigned char) buff[i]);
    result[nbytes] = '\0';
    return result;
}

#endif /* PAGC_SHIM_FORMATTING_H */
