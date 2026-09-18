/*
 * common/unicode_norm.h -- NFC normalization shim for the DuckDB build.
 *
 * Upstream's upper_case() normalizes to NFC before case-mapping, so that
 * decomposed input ("n" + U+0303) matches lexicon entries written in the
 * precomposed form. PostgreSQL supplies unicode_normalize(); we supply a
 * canonical-composition pass over the Latin ranges instead.
 *
 * Scope, deliberately: pagc_nfc_pairs covers U+00C0..U+024F (Latin-1
 * Supplement and Latin Extended-A/B), generated from Python's unicodedata by
 * scripts/gen_nfc_table.py. That is the accented repertoire that turns up in
 * Western address data. Text outside it passes through unchanged, which is the
 * same treatment it got before -- this is a shim for address parsing, not a
 * general Unicode normalizer.
 */

#ifndef PAGC_SHIM_UNICODE_NORM_H
#define PAGC_SHIM_UNICODE_NORM_H

#include "mb/pg_wchar.h"
#include "duckdb_nfc_table.h"

typedef enum { UNICODE_NFD = 0, UNICODE_NFC = 1, UNICODE_NFKD = 2, UNICODE_NFKC = 3 }
    UnicodeNormalizationForm;

static inline unsigned int pagc_nfc_compose(unsigned int base, unsigned int mark)
{
    int lo = 0, hi = PAGC_NFC_PAIR_COUNT - 1;

    while (lo <= hi)
    {
        int mid = (lo + hi) / 2;
        const pagc_nfc_pair *p = &pagc_nfc_pairs[mid];

        if (p->base < base || (p->base == base && p->mark < mark))
            lo = mid + 1;
        else if (p->base > base || (p->base == base && p->mark > mark))
            hi = mid - 1;
        else
            return p->composed;
    }
    return 0;                   /* no composition for this pair */
}

/*
 * Returns a palloc'd, zero-terminated copy of `input` with canonical
 * compositions applied.  Composition is repeated so a base carrying two marks
 * folds as far as the table allows.
 */
static inline pg_wchar *unicode_normalize(UnicodeNormalizationForm form,
                                          const pg_wchar *input)
{
    int n = 0, out = 0, i;
    pg_wchar *result;

    while (input[n] != 0)
        n++;

    result = (pg_wchar *) palloc((size_t) (n + 1) * sizeof(pg_wchar));

    if (form != UNICODE_NFC)
    {
        for (i = 0; i < n; i++)
            result[i] = input[i];
        result[n] = 0;
        return result;
    }

    for (i = 0; i < n; i++)
    {
        pg_wchar ch = input[i];

        if (out > 0)
        {
            unsigned int composed = pagc_nfc_compose(result[out - 1], ch);

            if (composed != 0)
            {
                result[out - 1] = (pg_wchar) composed;
                continue;
            }
        }
        result[out++] = ch;
    }
    result[out] = 0;
    return result;
}

#endif /* PAGC_SHIM_UNICODE_NORM_H */
