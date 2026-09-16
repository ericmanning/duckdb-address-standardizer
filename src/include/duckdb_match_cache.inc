/*
 * duckdb_match_cache.inc -- thread-local compiled-regex cache for match().
 *
 * Included by address-standardizer/src/parseaddress-api.c, and only when
 * DUCKDB_BUILD is defined.  It lives here rather than in the fork so the fork
 * carries just the hook: upstream's own match() is left untouched for the
 * PostgreSQL build.
 *
 * This is a .inc, not a .h: it defines match() itself, is textually pasted at
 * exactly one point in one translation unit, and must see that file's PCRE
 * headers and OVECCOUNT/OVECPAIRS.  Do not include it anywhere else.
 */

/* MSVC accepts the C11 _Thread_local keyword only in C11 mode; the DuckDB
 * extension build doesn't pass /std:c11, so use the equivalent
 * compiler-specific spelling on MSVC. Everywhere else (gcc/clang/MinGW),
 * _Thread_local is recognized in any mode. */
#if defined(_MSC_VER)
#define PAGC_THREAD_LOCAL __declspec(thread)
#else
#define PAGC_THREAD_LOCAL _Thread_local
#endif

/*
 * Thread-local compiled regex cache (DuckDB build only).
 *
 * DuckDB is multi-threaded and processes millions of rows per session, so
 * caching compiled regexes in thread-local storage is a big win.  Patterns
 * are identified by pointer value, which works because every expensive
 * pattern (us_zip_regex, us_ca_state_regex, t_regx[], etc.) is referenced
 * through a static const char* variable -- the same address on every call.
 *
 * PostgreSQL uses a process-per-connection model and manages memory via
 * memory contexts, so we keep the original compile-and-free behaviour
 * there.  Caching the large state-specific city regexes (up to 22 KB
 * source, potentially ~1 MB compiled each) across 8+ backends would
 * exhaust memory on typical machines.
 */

/* 80 slots: ~12 inline patterns + 9 t_regx[] + 59 state-specific city regexes */
#define MATCH_CACHE_SIZE 80

#if PCRE_VERSION <= 1
typedef struct
{
    const char *pattern;
    int options;
    pcre *re;
} match_cache_entry;

static PAGC_THREAD_LOCAL match_cache_entry _match_cache[MATCH_CACHE_SIZE];
static PAGC_THREAD_LOCAL int _match_cache_n = 0;

int
match(char *pattern, char *s, int *ovect, int options)
{
    pcre *re = NULL;
    int rc;
    int cached = 0;

    for (int i = 0; i < _match_cache_n; i++)
    {
        if (_match_cache[i].pattern == pattern && _match_cache[i].options == options)
        {
            re = _match_cache[i].re;
            cached = 1;
            break;
        }
    }

    if (!re)
    {
        const char *error;
        int erroffset;

        re = pcre_compile(pattern, options, &error, &erroffset, NULL);
        if (!re)
            return -99;
        if (_match_cache_n < MATCH_CACHE_SIZE)
        {
            _match_cache[_match_cache_n].pattern = pattern;
            _match_cache[_match_cache_n].options = options;
            _match_cache[_match_cache_n].re = re;
            _match_cache_n++;
            cached = 1;
        }
    }

    rc = pcre_exec(re, NULL, s, strlen(s), 0, 0, ovect, OVECCOUNT);

    if (!cached)
        free(re);

    if (rc < 0)
        return rc;
    else if (!rc)
        rc = OVECPAIRS; // more matches than ovect can hold

    return rc;
}

#else

typedef struct
{
    const char *pattern;
    int options;
    pcre2_code *re;
} match_cache_entry;

static PAGC_THREAD_LOCAL match_cache_entry _match_cache[MATCH_CACHE_SIZE];
static PAGC_THREAD_LOCAL int _match_cache_n = 0;
static PAGC_THREAD_LOCAL pcre2_match_data *_cached_match_data = NULL;

int
match(char *pattern, char *s, int *ovect, int options)
{
    pcre2_code *re = NULL;
    int rc;
    int cached = 0;
    const PCRE2_SIZE *ovect2;

    for (int i = 0; i < _match_cache_n; i++)
    {
        if (_match_cache[i].pattern == pattern && _match_cache[i].options == options)
        {
            re = _match_cache[i].re;
            cached = 1;
            break;
        }
    }

    if (!re)
    {
        int errorcode;
        PCRE2_SIZE erroffset;

        re = pcre2_compile((PCRE2_SPTR8)pattern, PCRE2_ZERO_TERMINATED, options, &errorcode, &erroffset, NULL);
        if (!re)
            return -99;
        if (_match_cache_n < MATCH_CACHE_SIZE)
        {
            _match_cache[_match_cache_n].pattern = pattern;
            _match_cache[_match_cache_n].options = options;
            _match_cache[_match_cache_n].re = re;
            _match_cache_n++;
            cached = 1;
        }
    }

    if (!_cached_match_data)
        _cached_match_data = pcre2_match_data_create(OVECPAIRS, NULL);

    rc = pcre2_match(re, (PCRE2_SPTR8)s, strlen(s), 0, 0, _cached_match_data, NULL);

    if (!cached)
        pcre2_code_free(re);

    if (rc < 0)
        return rc;

    if (!rc)
        rc = OVECPAIRS; // more matches than ovect can hold

    // copy the results out so we can free everything
    // before returning
    ovect2 = pcre2_get_ovector_pointer(_cached_match_data);
    for (int i = 0; i < rc; i++)
    {
        ovect[2 * i] = ovect2[2 * i];
        ovect[2 * i + 1] = ovect2[2 * i + 1];
    }

    return rc;
}

#endif /* PCRE_VERSION */
