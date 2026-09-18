/*
 * tsearch/ts_locale.h -- shim for the DuckDB build.
 *
 * standard.c falls back to t_isalpha_unbounded() only for scripts outside the
 * Latin fast path it checks first.  We have no Unicode category tables, so
 * non-Latin non-ASCII characters are reported as not-alphabetic and therefore
 * act as token separators -- the same treatment they received before this shim
 * existed.  Latin text, which is what the US/CA lexicons contain, goes through
 * the fast path above and never reaches here.
 */

#ifndef PAGC_SHIM_TS_LOCALE_H
#define PAGC_SHIM_TS_LOCALE_H

#include <ctype.h>

static inline int t_isalpha_unbounded(const char *ptr)
{
    unsigned char c = (unsigned char) *ptr;

    return (c < 0x80) ? isalpha(c) : 0;
}

#endif /* PAGC_SHIM_TS_LOCALE_H */
