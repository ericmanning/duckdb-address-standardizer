/*
 * mb/pg_wchar.h -- PostgreSQL multibyte shim for the DuckDB build.
 *
 * Upstream address_standardizer guards its Unicode handling with
 * `#ifndef PAGC_STANDALONE` and reaches for PostgreSQL's multibyte API.  Rather
 * than add a third branch to the fork, we implement that small API surface here
 * over plain UTF-8, so upstream's PostgreSQL path compiles verbatim and we get
 * its Unicode behaviour with no extra fork divergence.
 *
 * DuckDB strings are always UTF-8, so GetDatabaseEncoding() is a constant and
 * the LATIN1 / other-encoding branches upstream carries are dead code here.
 * They still have to compile, hence the declarations below.
 */

#ifndef PAGC_SHIM_PG_WCHAR_H
#define PAGC_SHIM_PG_WCHAR_H

#include <stdbool.h>

typedef unsigned int pg_wchar;

#define MAX_MULTIBYTE_CHAR_LEN 4

/* Values match PostgreSQL's pg_enc so nothing surprising happens if these ever
   meet real PG headers. */
#define PG_SQL_ASCII 0
#define PG_UTF8      6
#define PG_LATIN1    8

static inline int GetDatabaseEncoding(void) { return PG_UTF8; }

/* Length in bytes of the UTF-8 character starting at s, from the lead byte.
   Never reads past the lead byte, and never returns 0, so callers cannot spin. */
static inline int pg_mblen_unbounded(const char *s)
{
    unsigned char c = (unsigned char) *s;

    if (c < 0x80) return 1;
    if ((c & 0xE0) == 0xC0) return 2;
    if ((c & 0xF0) == 0xE0) return 3;
    if ((c & 0xF8) == 0xF0) return 4;
    return 1;                   /* continuation or invalid lead: consume one byte */
}

/* Is the `length`-byte sequence at source a well-formed UTF-8 character?
   Mirrors PostgreSQL's pg_utf8_islegal, including the overlong and surrogate
   rejections. */
static inline bool pg_utf8_islegal(const unsigned char *source, int length)
{
    unsigned char a;

    switch (length)
    {
    default:
        return false;
    case 4:
        a = source[3];
        if (a < 0x80 || a > 0xBF) return false;
        /* fallthrough */
    case 3:
        a = source[2];
        if (a < 0x80 || a > 0xBF) return false;
        /* fallthrough */
    case 2:
        a = source[1];
        switch (*source)
        {
        case 0xE0: if (a < 0xA0 || a > 0xBF) return false; break;
        case 0xED: if (a < 0x80 || a > 0x9F) return false; break;  /* no surrogates */
        case 0xF0: if (a < 0x90 || a > 0xBF) return false; break;
        case 0xF4: if (a < 0x80 || a > 0x8F) return false; break;
        default:   if (a < 0x80 || a > 0xBF) return false; break;
        }
        /* fallthrough */
    case 1:
        a = *source;
        if (a >= 0x80 && a < 0xC2) return false;                   /* overlong */
        if (a > 0xF4) return false;
        break;
    }
    return true;
}

/* Decode one UTF-8 character to its code point.  Assumes pg_utf8_islegal. */
static inline pg_wchar utf8_to_unicode(const unsigned char *c)
{
    if (*c < 0x80)
        return (pg_wchar) c[0];
    if ((*c & 0xE0) == 0xC0)
        return ((pg_wchar) (c[0] & 0x1F) << 6) | (pg_wchar) (c[1] & 0x3F);
    if ((*c & 0xF0) == 0xE0)
        return ((pg_wchar) (c[0] & 0x0F) << 12) | ((pg_wchar) (c[1] & 0x3F) << 6) |
               (pg_wchar) (c[2] & 0x3F);
    if ((*c & 0xF8) == 0xF0)
        return ((pg_wchar) (c[0] & 0x07) << 18) | ((pg_wchar) (c[1] & 0x3F) << 12) |
               ((pg_wchar) (c[2] & 0x3F) << 6) | (pg_wchar) (c[3] & 0x3F);
    return (pg_wchar) c[0];
}

/* Encode one code point as UTF-8; returns bytes written. */
static inline int unicode_to_utf8_len(pg_wchar c, unsigned char *utf8)
{
    if (c < 0x80)     { utf8[0] = (unsigned char) c; return 1; }
    if (c < 0x800)    { utf8[0] = (unsigned char) (0xC0 | (c >> 6));
                        utf8[1] = (unsigned char) (0x80 | (c & 0x3F)); return 2; }
    if (c < 0x10000)  { utf8[0] = (unsigned char) (0xE0 | (c >> 12));
                        utf8[1] = (unsigned char) (0x80 | ((c >> 6) & 0x3F));
                        utf8[2] = (unsigned char) (0x80 | (c & 0x3F)); return 3; }
    utf8[0] = (unsigned char) (0xF0 | (c >> 18));
    utf8[1] = (unsigned char) (0x80 | ((c >> 12) & 0x3F));
    utf8[2] = (unsigned char) (0x80 | ((c >> 6) & 0x3F));
    utf8[3] = (unsigned char) (0x80 | (c & 0x3F));
    return 4;
}

/* UTF-8 bytes -> code points.  `len` is a byte count; returns the number of
   code points written (the caller sizes `to` at len + 1). */
static inline int pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
{
    int n = 0;

    while (len > 0 && *from)
    {
        int clen = pg_mblen_unbounded(from);

        if (clen > len)
            clen = len;
        if (clen > 1 && !pg_utf8_islegal((const unsigned char *) from, clen))
        {
            /* Not decodable: pass the byte through so no input is silently lost. */
            to[n++] = (pg_wchar) (unsigned char) *from;
            from += 1;
            len  -= 1;
            continue;
        }
        to[n++] = utf8_to_unicode((const unsigned char *) from);
        from += clen;
        len  -= clen;
    }
    to[n] = 0;
    return n;
}

/* Code points -> UTF-8 bytes.  `len` is a code-point count; returns bytes
   written (the caller sizes `to` at len * MAX_MULTIBYTE_CHAR_LEN + 1). */
static inline int pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len)
{
    int bytes = 0;

    while (len-- > 0 && *from)
        bytes += unicode_to_utf8_len(*from++, (unsigned char *) to + bytes);
    to[bytes] = '\0';
    return bytes;
}

#endif /* PAGC_SHIM_PG_WCHAR_H */
