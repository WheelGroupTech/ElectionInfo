/**
 * @file dominion_cvr.c
 * @brief Loader for Dominion Voting Systems (Democracy Suite; the company is now Liberty
 *        Vote) Cast Vote Record exports.
 *
 * A Dominion CVR export is a `.zip` holding JSON files:
 *  - manifests, each `{"Version":..,"List":[{..},..]}`: ContestManifest (Id, Description,
 *    VoteFor, NumOfRanks, Disabled), CandidateManifest (Id, Description, ContestId, Type
 *    = Regular / WriteIn / QualifiedWriteIn), PrecinctPortionManifest, BallotTypeManifest,
 *    CountingGroupManifest (Election Day, Vote by Mail, ...), TabulatorManifest
 *    (Description, VotingLocationName), ...
 *  - the records: one `CvrExport.json` or many `CvrExport_<n>.json` (one per batch),
 *    each `{"Version":..,"ElectionId":..,"Sessions":[..]}`. A session is one tabulated
 *    ballot -- normally one card (a multi-card ballot is scanned as one session per
 *    card), but a ballot-marking-device (QR) session can carry every card of a ballot:
 *      {"TabulatorId":5,"BatchId":1198,"RecordId":22 (or "X" when redacted),
 *       "CountingGroupId":2,["CastVoteRecordId":1,] "ImageMask":"..\\00005_01198_000022*.*",
 *       "SessionType":"ScannedVote"|"QRVote",
 *       "Original":{"PrecinctPortionId":..,"BallotTypeId":..,"IsCurrent":true,
 *                   "Cards":[{"PaperIndex":0,"Contests":[{"Id":..,"Undervotes":0,
 *                     "Overvotes":0,"Marks":[{"CandidateId":..,"Rank":1,
 *                     "IsAmbiguous":false,"IsVote":true},..]},..]},..]},
 *       ["Modified":{same shape -- the adjudicated version, IsCurrent true}]}
 *    Older exports (5.2) put "Contests" directly under Original/Modified (no Cards) and
 *    omit SessionType and the contest Undervotes/Overvotes counts.
 *
 * Mapping into the sparse EeCvrTable (shared with the ES&S and Hart loaders):
 *  - One row per session, using the version marked IsCurrent (the adjudicated
 *    "Modified" one when present). Only marks with IsVote = true count -- exactly what
 *    Dominion tabulates (ambiguous marks are recorded with IsVote = false).
 *  - Frozen key columns: [Cvr Number (newer exports)], Record Id (the ballot image name,
 *    e.g. 00005_01198_000022 = tabulator_batch_record; unique even when RecordId is
 *    redacted), Tabulator, Batch (tabulator-batch, e.g. 00005-01198), Counting Group,
 *    [Polling Place (the tabulator's voting location)], Precinct Portion, Ballot Type,
 *    [Session Type], [Card (1-based PaperIndex; "1,2,3,4" for a session holding several
 *    cards)], Adjudicated (Yes/No).
 *  - Contests in ContestManifest order. A vote-for-N contest spans N columns (the
 *    extras carry a blank continuation header, so col_group sums the race): selected
 *    candidates in order, "undervote" for each unused vote, and every column "overvote"
 *    when the contest is overvoted. A ranked-choice contest (NumOfRanks > 0) becomes one
 *    column per rank titled "<contest> (Rank N)" holding the candidate ranked there,
 *    "undervote" (no ranking) or "overvote" (several candidates at that rank); ee_rcv.c
 *    runs the instant runoff over those columns. An unresolved write-in is "Write-in"
 *    (its manifest name); a qualified write-in carries the candidate's name.
 *
 * The JSON is read with a small hand-written streaming parser (no third-party code):
 * each zip entry is inflated incrementally through miniz, and each session is parsed
 * into a tiny reusable DOM, processed, and discarded -- so a single 500 MB CvrExport.json
 * or a 5 GB multi-file export is never held in memory at once. One pass; the column
 * layout comes from the manifests plus the first session's shape.
 */

#include "ee_cvr.h"

#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <search.h> /* qsort_s */

#include "third_party/miniz/miniz.h"

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static void dom_set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

static const wchar_t *dom_path_leaf(const wchar_t *path)
{
    const wchar_t *leaf = path;
    const wchar_t *p;
    for (p = path; *p != L'\0'; p++)
    {
        if (*p == L'\\' || *p == L'/')
        {
            leaf = p + 1;
        }
    }
    return leaf;
}

/* "<file>: <what>" into the error buffer. */
static void dom_file_err(wchar_t *err, size_t cch, const wchar_t *path, const wchar_t *what)
{
    if (err != NULL && cch > 0)
    {
        StringCchPrintfW(err, cch, L"%s: %s", dom_path_leaf(path), what);
    }
}

/* Leaf of a zip entry name (after the last '/' or '\'). */
static const char *zip_leaf(const char *name)
{
    const char *leaf = name;
    const char *p;
    for (p = name; *p != '\0'; p++)
    {
        if (*p == '/' || *p == '\\')
        {
            leaf = p + 1;
        }
    }
    return leaf;
}

/* TRUE for "CvrExport.json" / "CvrExport_<n>.json" (case-insensitive leaf). */
static BOOL is_cvr_export_name(const char *name)
{
    const char *leaf = zip_leaf(name);
    size_t n = strlen(leaf);
    return n >= 14 && _strnicmp(leaf, "CvrExport", 9) == 0 && _stricmp(leaf + n - 5, ".json") == 0;
}

/* Numeric suffix of CvrExport_<n>.json (-1 for a plain CvrExport.json), for ordering. */
static long long cvr_export_number(const char *name)
{
    const char *leaf = zip_leaf(name);
    const char *p = leaf + 9;
    long long v = 0;
    if (*p != '_')
    {
        return -1;
    }
    for (p++; *p >= '0' && *p <= '9'; p++)
    {
        v = v * 10 + (*p - '0');
    }
    return v;
}

/* -------------------------------------------------------------------------- */
/* Growable string pool (offset-addressed so it may move while growing)       */
/* -------------------------------------------------------------------------- */

typedef struct StrPool
{
    char *p;
    size_t n;
    size_t cap;
} StrPool;

static BOOL pool_reserve(StrPool *s, size_t add)
{
    if (s->n + add > s->cap)
    {
        size_t nc = s->cap ? s->cap * 2 : 4096;
        char *np;
        while (nc < s->n + add)
        {
            nc *= 2;
        }
        np = (char *)realloc(s->p, nc);
        if (np == NULL)
        {
            return FALSE;
        }
        s->p = np;
        s->cap = nc;
    }
    return TRUE;
}

/* Append @p len bytes + NUL; returns the offset or UINT32_MAX on OOM. */
static uint32_t pool_add(StrPool *s, const char *src, size_t len)
{
    uint32_t off;
    if (!pool_reserve(s, len + 1) || s->n + len + 1 > UINT32_MAX)
    {
        return UINT32_MAX;
    }
    off = (uint32_t)s->n;
    if (len > 0)
    {
        memcpy(s->p + s->n, src, len);
    }
    s->p[s->n + len] = '\0';
    s->n += len + 1;
    return off;
}

/* -------------------------------------------------------------------------- */
/* Streaming JSON source                                                      */
/* -------------------------------------------------------------------------- */

#define DOM_READ_CHUNK (256u * 1024u)

typedef struct DomProgress
{
    volatile LONG *cancel_flag;
    EeLoadProgressFn fn;
    void *user;
    uint64_t done;  /* uncompressed CvrExport bytes consumed */
    uint64_t total; /* uncompressed CvrExport bytes in all zips */
    uint32_t last_pct;
    const uint32_t *rows;
    BOOL cancelled;
} DomProgress;

static void dom_progress(DomProgress *pg, size_t add)
{
    pg->done += add;
    if (pg->cancel_flag != NULL && *pg->cancel_flag != 0)
    {
        pg->cancelled = TRUE;
        return;
    }
    if (pg->fn != NULL && pg->total > 0)
    {
        uint32_t pct = (uint32_t)((pg->done * 99ull) / pg->total);
        if (pct > 99u)
        {
            pct = 99u;
        }
        if (pct != pg->last_pct)
        {
            EeLoadProgress pr;
            pg->last_pct = pct;
            pr.percent = pct;
            pr.rows_loaded = (pg->rows != NULL) ? *pg->rows : 0u;
            pr.bytes_read = pg->done;
            pr.bytes_total = pg->total;
            pr.scanning = 0;
            if (!pg->fn(&pr, pg->user) && pg->cancel_flag != NULL)
            {
                InterlockedExchange(pg->cancel_flag, 1);
            }
        }
    }
}

/* A byte source: either an in-memory buffer or a zip entry inflated chunk by chunk. */
typedef struct JsonSrc
{
    const unsigned char *p;
    const unsigned char *end;
    unsigned char *buf; /* chunk buffer (streaming mode) */
    mz_zip_reader_extract_iter_state *it;
    DomProgress *pg;
    BOOL eof;
} JsonSrc;

static BOOL js_fill(JsonSrc *s)
{
    size_t n;
    if (s->it == NULL || s->eof || (s->pg != NULL && s->pg->cancelled))
    {
        return FALSE;
    }
    n = mz_zip_reader_extract_iter_read(s->it, s->buf, DOM_READ_CHUNK);
    if (n == 0)
    {
        s->eof = TRUE;
        return FALSE;
    }
    s->p = s->buf;
    s->end = s->buf + n;
    if (s->pg != NULL)
    {
        dom_progress(s->pg, n);
    }
    return TRUE;
}

static int js_peek(JsonSrc *s)
{
    if (s->p == s->end && !js_fill(s))
    {
        return -1;
    }
    return *s->p;
}

static int js_get(JsonSrc *s)
{
    if (s->p == s->end && !js_fill(s))
    {
        return -1;
    }
    return *s->p++;
}

/* Skip whitespace; return the next byte without consuming it (-1 at end). */
static int js_ws(JsonSrc *s)
{
    for (;;)
    {
        int c = js_peek(s);
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
        {
            s->p++;
            continue;
        }
        return c;
    }
}

/* -------------------------------------------------------------------------- */
/* Tiny reusable JSON DOM                                                     */
/* -------------------------------------------------------------------------- */

#define JN_NONE   UINT32_MAX
#define JSON_MAXD 64

enum
{
    JT_NULL,
    JT_BOOL,
    JT_NUM,
    JT_STR,
    JT_ARR,
    JT_OBJ
};

typedef struct JNode
{
    uint8_t type;
    uint32_t key;   /* pool offset of the member name (object members), else JN_NONE */
    uint32_t first; /* first child (array/object) */
    uint32_t next;  /* next sibling */
    uint32_t str;   /* pool offset of a string value */
    int64_t num;    /* integer part of a number; 0/1 for a bool */
} JNode;

typedef struct JDoc
{
    JNode *n;
    uint32_t nn;
    uint32_t cap;
    StrPool pool;
} JDoc;

static void jdoc_reset(JDoc *d)
{
    d->nn = 0;
    d->pool.n = 0;
}

static void jdoc_free(JDoc *d)
{
    free(d->n);
    free(d->pool.p);
    ZeroMemory(d, sizeof(*d));
}

static uint32_t jnew(JDoc *d, uint8_t type)
{
    JNode *x;
    if (d->nn == d->cap)
    {
        uint32_t nc = d->cap ? d->cap * 2 : 1024;
        JNode *nn = (JNode *)realloc(d->n, (size_t)nc * sizeof(JNode));
        if (nn == NULL)
        {
            return JN_NONE;
        }
        d->n = nn;
        d->cap = nc;
    }
    x = &d->n[d->nn];
    x->type = type;
    x->key = JN_NONE;
    x->first = JN_NONE;
    x->next = JN_NONE;
    x->str = JN_NONE;
    x->num = 0;
    return d->nn++;
}

/* Append a code point as UTF-8. */
static BOOL pool_put_utf8(StrPool *s, uint32_t cp)
{
    char b[4];
    size_t n;
    if (cp < 0x80)
    {
        b[0] = (char)cp;
        n = 1;
    }
    else if (cp < 0x800)
    {
        b[0] = (char)(0xC0 | (cp >> 6));
        b[1] = (char)(0x80 | (cp & 0x3F));
        n = 2;
    }
    else if (cp < 0x10000)
    {
        b[0] = (char)(0xE0 | (cp >> 12));
        b[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        b[2] = (char)(0x80 | (cp & 0x3F));
        n = 3;
    }
    else
    {
        b[0] = (char)(0xF0 | (cp >> 18));
        b[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        b[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        b[3] = (char)(0x80 | (cp & 0x3F));
        n = 4;
    }
    if (!pool_reserve(s, n))
    {
        return FALSE;
    }
    memcpy(s->p + s->n, b, n);
    s->n += n;
    return TRUE;
}

static int js_hex4(JsonSrc *s)
{
    int v = 0;
    int i;
    for (i = 0; i < 4; i++)
    {
        int c = js_get(s);
        v <<= 4;
        if (c >= '0' && c <= '9')
        {
            v |= c - '0';
        }
        else if (c >= 'a' && c <= 'f')
        {
            v |= c - 'a' + 10;
        }
        else if (c >= 'A' && c <= 'F')
        {
            v |= c - 'A' + 10;
        }
        else
        {
            return -1;
        }
    }
    return v;
}

/* Parse a string (opening quote already consumed) into the pool, NUL-terminated.
 * Returns its offset or JN_NONE on error. */
static uint32_t js_string(JsonSrc *s, StrPool *pool)
{
    size_t start = pool->n;
    for (;;)
    {
        const unsigned char *q;
        int c;
        if (s->p == s->end && !js_fill(s))
        {
            return JN_NONE;
        }
        q = s->p;
        while (q < s->end && *q != '"' && *q != '\\')
        {
            q++;
        }
        if (q > s->p)
        {
            size_t len = (size_t)(q - s->p);
            if (!pool_reserve(pool, len))
            {
                return JN_NONE;
            }
            memcpy(pool->p + pool->n, s->p, len);
            pool->n += len;
            s->p = q;
        }
        if (q == s->end)
        {
            continue;
        }
        s->p++;
        if (*q == '"')
        {
            break;
        }
        c = js_get(s); /* escape */
        switch (c)
        {
            case '"':
            case '\\':
            case '/':
                if (!pool_put_utf8(pool, (uint32_t)c))
                {
                    return JN_NONE;
                }
                break;
            case 'b':
                c = '\b';
                goto put;
            case 'f':
                c = '\f';
                goto put;
            case 'n':
                c = '\n';
                goto put;
            case 'r':
                c = '\r';
                goto put;
            case 't':
                c = '\t';
            put:
                if (!pool_put_utf8(pool, (uint32_t)c))
                {
                    return JN_NONE;
                }
                break;
            case 'u':
            {
                int u = js_hex4(s);
                uint32_t cp;
                if (u < 0)
                {
                    return JN_NONE;
                }
                cp = (uint32_t)u;
                if (cp >= 0xD800 && cp <= 0xDBFF && js_peek(s) == '\\')
                {
                    int lo;
                    s->p++;
                    if (js_get(s) != 'u' || (lo = js_hex4(s)) < 0)
                    {
                        return JN_NONE;
                    }
                    if (lo >= 0xDC00 && lo <= 0xDFFF)
                    {
                        cp = 0x10000u + ((cp - 0xD800u) << 10) + ((uint32_t)lo - 0xDC00u);
                    }
                    else
                    {
                        cp = 0xFFFD;
                    }
                }
                else if (cp >= 0xD800 && cp <= 0xDFFF)
                {
                    cp = 0xFFFD;
                }
                if (!pool_put_utf8(pool, cp))
                {
                    return JN_NONE;
                }
                break;
            }
            default:
                return JN_NONE;
        }
    }
    if (!pool_reserve(pool, 1) || pool->n > UINT32_MAX)
    {
        return JN_NONE;
    }
    pool->p[pool->n++] = '\0';
    return (uint32_t)start;
}

static BOOL js_literal(JsonSrc *s, const char *rest)
{
    for (; *rest != '\0'; rest++)
    {
        if (js_get(s) != *rest)
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* Parse one JSON value into @p d; returns its node index or JN_NONE on error. */
static uint32_t js_value(JsonSrc *s, JDoc *d, int depth)
{
    int c = js_ws(s);
    uint32_t idx;

    if (depth > JSON_MAXD)
    {
        return JN_NONE;
    }
    switch (c)
    {
        case '{':
        case '[':
        {
            BOOL is_obj = (c == '{');
            uint32_t last = JN_NONE;
            s->p++;
            idx = jnew(d, is_obj ? JT_OBJ : JT_ARR);
            if (idx == JN_NONE)
            {
                return JN_NONE;
            }
            c = js_ws(s);
            if (c == (is_obj ? '}' : ']'))
            {
                s->p++;
                return idx;
            }
            for (;;)
            {
                uint32_t key = JN_NONE;
                uint32_t child;
                if (is_obj)
                {
                    if (js_ws(s) != '"')
                    {
                        return JN_NONE;
                    }
                    s->p++;
                    key = js_string(s, &d->pool);
                    if (key == JN_NONE || js_ws(s) != ':')
                    {
                        return JN_NONE;
                    }
                    s->p++;
                }
                child = js_value(s, d, depth + 1);
                if (child == JN_NONE)
                {
                    return JN_NONE;
                }
                d->n[child].key = key;
                if (last == JN_NONE)
                {
                    d->n[idx].first = child;
                }
                else
                {
                    d->n[last].next = child;
                }
                last = child;
                c = js_ws(s);
                if (c == ',')
                {
                    s->p++;
                    continue;
                }
                if (c == (is_obj ? '}' : ']'))
                {
                    s->p++;
                    return idx;
                }
                return JN_NONE;
            }
        }
        case '"':
            s->p++;
            idx = jnew(d, JT_STR);
            if (idx == JN_NONE)
            {
                return JN_NONE;
            }
            d->n[idx].str = js_string(s, &d->pool);
            return (d->n[idx].str == JN_NONE) ? JN_NONE : idx;
        case 't':
            s->p++;
            if (!js_literal(s, "rue") || (idx = jnew(d, JT_BOOL)) == JN_NONE)
            {
                return JN_NONE;
            }
            d->n[idx].num = 1;
            return idx;
        case 'f':
            s->p++;
            if (!js_literal(s, "alse") || (idx = jnew(d, JT_BOOL)) == JN_NONE)
            {
                return JN_NONE;
            }
            return idx;
        case 'n':
            s->p++;
            if (!js_literal(s, "ull"))
            {
                return JN_NONE;
            }
            return jnew(d, JT_NULL);
        default:
            if (c == '-' || (c >= '0' && c <= '9'))
            {
                BOOL neg = FALSE;
                BOOL frac = FALSE;
                int64_t v = 0;
                idx = jnew(d, JT_NUM);
                if (idx == JN_NONE)
                {
                    return JN_NONE;
                }
                if (c == '-')
                {
                    neg = TRUE;
                    s->p++;
                }
                for (;;)
                {
                    c = js_peek(s);
                    if (c >= '0' && c <= '9')
                    {
                        if (!frac && v < (INT64_MAX / 10))
                        {
                            v = v * 10 + (c - '0');
                        }
                    }
                    else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-')
                    {
                        frac = TRUE; /* only the integer part is kept */
                    }
                    else
                    {
                        break;
                    }
                    s->p++;
                }
                d->n[idx].num = neg ? -v : v;
                return idx;
            }
            return JN_NONE;
    }
}

/* Member @p name of object @p obj, or JN_NONE. */
static uint32_t jget(const JDoc *d, uint32_t obj, const char *name)
{
    uint32_t c;
    if (obj == JN_NONE || d->n[obj].type != JT_OBJ)
    {
        return JN_NONE;
    }
    for (c = d->n[obj].first; c != JN_NONE; c = d->n[c].next)
    {
        if (strcmp(d->pool.p + d->n[c].key, name) == 0)
        {
            return c;
        }
    }
    return JN_NONE;
}

static int64_t jint(const JDoc *d, uint32_t obj, const char *name, int64_t dflt)
{
    uint32_t v = jget(d, obj, name);
    if (v == JN_NONE || (d->n[v].type != JT_NUM && d->n[v].type != JT_BOOL))
    {
        return dflt;
    }
    return d->n[v].num;
}

/* String member (NULL when absent or not a string). */
static const char *jstr(const JDoc *d, uint32_t obj, const char *name)
{
    uint32_t v = jget(d, obj, name);
    if (v == JN_NONE || d->n[v].type != JT_STR)
    {
        return NULL;
    }
    return d->pool.p + d->n[v].str;
}

/* -------------------------------------------------------------------------- */
/* Manifests                                                                  */
/* -------------------------------------------------------------------------- */

typedef struct DomContest
{
    int64_t id;
    uint32_t name;     /* meta pool offset */
    uint32_t vote_for; /* >= 1 */
    uint32_t nranks;   /* 0 = not ranked-choice */
    BOOL disabled;
    uint32_t col;      /* first column in the table */
} DomContest;

typedef struct DomItem /* candidate, precinct portion, ballot type, group, tabulator */
{
    int64_t id;
    uint32_t name;  /* meta pool offset (Description) */
    uint32_t extra; /* tabulator: VotingLocationName offset or UINT32_MAX */
} DomItem;

typedef struct DomList
{
    DomItem *v;
    uint32_t n;
} DomList;

typedef struct DomMeta
{
    StrPool pool;
    DomContest *contests;    /* manifest order */
    uint32_t ncontests;
    uint32_t *contest_by_id; /* indices into contests, sorted by id */
    DomList cands;
    DomList portions;
    DomList btypes;
    DomList groups;
    DomList tabs;
    BOOL has_location;
} DomMeta;

static void meta_free(DomMeta *m)
{
    free(m->pool.p);
    free(m->contests);
    free(m->contest_by_id);
    free(m->cands.v);
    free(m->portions.v);
    free(m->btypes.v);
    free(m->groups.v);
    free(m->tabs.v);
    ZeroMemory(m, sizeof(*m));
}

static int __cdecl item_cmp(void *ctx, const void *a, const void *b)
{
    int64_t x = ((const DomItem *)a)->id;
    int64_t y = ((const DomItem *)b)->id;
    (void)ctx;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static const DomItem *list_find(const DomList *l, int64_t id)
{
    uint32_t lo = 0;
    uint32_t hi = l->n;
    while (lo < hi)
    {
        uint32_t mid = lo + (hi - lo) / 2;
        if (l->v[mid].id < id)
        {
            lo = mid + 1;
        }
        else
        {
            hi = mid;
        }
    }
    return (lo < l->n && l->v[lo].id == id) ? &l->v[lo] : NULL;
}

static int __cdecl contest_id_cmp(void *ctx, const void *a, const void *b)
{
    const DomContest *c = (const DomContest *)ctx;
    int64_t x = c[*(const uint32_t *)a].id;
    int64_t y = c[*(const uint32_t *)b].id;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static const DomContest *meta_find_contest(const DomMeta *m, int64_t id)
{
    uint32_t lo = 0;
    uint32_t hi = m->ncontests;
    while (lo < hi)
    {
        uint32_t mid = lo + (hi - lo) / 2;
        if (m->contests[m->contest_by_id[mid]].id < id)
        {
            lo = mid + 1;
        }
        else
        {
            hi = mid;
        }
    }
    if (lo < m->ncontests && m->contests[m->contest_by_id[lo]].id == id)
    {
        return &m->contests[m->contest_by_id[lo]];
    }
    return NULL;
}

/* Copy a Description into the meta pool, trimming surrounding whitespace. */
static uint32_t meta_add_name(DomMeta *m, const char *s)
{
    size_t n;
    if (s == NULL)
    {
        s = "";
    }
    while (*s == ' ' || *s == '\t')
    {
        s++;
    }
    n = strlen(s);
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t'))
    {
        n--;
    }
    return pool_add(&m->pool, s, n);
}

/* Find a zip entry by leaf name (case-insensitive). */
static int zip_find_leaf(mz_zip_archive *zip, const char *leaf)
{
    mz_uint i;
    mz_uint n = mz_zip_reader_get_num_files(zip);
    for (i = 0; i < n; i++)
    {
        char name[512];
        if (mz_zip_reader_get_filename(zip, i, name, sizeof(name)) > 0 &&
            _stricmp(zip_leaf(name), leaf) == 0)
        {
            return (int)i;
        }
    }
    return -1;
}

/* Parse manifest @p leaf into @p d; returns the "List" array node, or JN_NONE when the
 * entry is missing (*missing = TRUE) or malformed. */
static uint32_t read_manifest(mz_zip_archive *zip, const char *leaf, JDoc *d, BOOL *missing)
{
    int idx = zip_find_leaf(zip, leaf);
    size_t size = 0;
    void *data;
    JsonSrc src;
    uint32_t root;

    *missing = (idx < 0);
    jdoc_reset(d);
    if (idx < 0)
    {
        return JN_NONE;
    }
    data = mz_zip_reader_extract_to_heap(zip, (mz_uint)idx, &size, 0);
    if (data == NULL)
    {
        return JN_NONE;
    }
    ZeroMemory(&src, sizeof(src));
    src.p = (const unsigned char *)data;
    src.end = src.p + size;
    if (size >= 3 && src.p[0] == 0xEF && src.p[1] == 0xBB && src.p[2] == 0xBF)
    {
        src.p += 3;
    }
    root = js_value(&src, d, 0);
    mz_free(data);
    return jget(d, root, "List");
}

/* Load a simple id -> Description manifest into @p out (sorted by id). Missing is OK. */
static BOOL load_item_list(mz_zip_archive *zip,
                           const char *leaf,
                           JDoc *d,
                           DomMeta *m,
                           DomList *out,
                           BOOL is_tabulator)
{
    BOOL missing;
    uint32_t list = read_manifest(zip, leaf, d, &missing);
    uint32_t c;
    uint32_t n = 0;
    if (list == JN_NONE)
    {
        return missing; /* absent manifests are tolerated; a malformed one is not */
    }
    for (c = d->n[list].first; c != JN_NONE; c = d->n[c].next)
    {
        n++;
    }
    out->v = (DomItem *)calloc(n ? n : 1, sizeof(DomItem));
    if (out->v == NULL)
    {
        return FALSE;
    }
    for (c = d->n[list].first; c != JN_NONE; c = d->n[c].next)
    {
        DomItem *it = &out->v[out->n];
        const char *loc;
        it->id = jint(d, c, "Id", -1);
        it->name = meta_add_name(m, jstr(d, c, "Description"));
        it->extra = UINT32_MAX;
        if (it->name == UINT32_MAX)
        {
            return FALSE;
        }
        if (is_tabulator && (loc = jstr(d, c, "VotingLocationName")) != NULL)
        {
            it->extra = meta_add_name(m, loc);
            if (it->extra == UINT32_MAX)
            {
                return FALSE;
            }
            m->has_location = TRUE;
        }
        out->n++;
    }
    qsort_s(out->v, out->n, sizeof(DomItem), item_cmp, NULL);
    return TRUE;
}

static BOOL load_meta(mz_zip_archive *zip,
                      DomMeta *m,
                      JDoc *d,
                      const wchar_t *path,
                      wchar_t *err,
                      size_t errcch)
{
    BOOL missing;
    uint32_t list;
    uint32_t c;
    uint32_t n = 0;
    uint32_t i;

    list = read_manifest(zip, "ContestManifest.json", d, &missing);
    if (list == JN_NONE)
    {
        dom_file_err(err,
                     errcch,
                     path,
                     missing ? L"ContestManifest.json was not found in the zip."
                             : L"ContestManifest.json could not be read.");
        return FALSE;
    }
    for (c = d->n[list].first; c != JN_NONE; c = d->n[c].next)
    {
        n++;
    }
    m->contests = (DomContest *)calloc(n ? n : 1, sizeof(DomContest));
    m->contest_by_id = (uint32_t *)malloc((n ? n : 1) * sizeof(uint32_t));
    if (m->contests == NULL || m->contest_by_id == NULL)
    {
        dom_set_err(err, errcch, L"Out of memory.");
        return FALSE;
    }
    for (c = d->n[list].first; c != JN_NONE; c = d->n[c].next)
    {
        DomContest *ct = &m->contests[m->ncontests];
        int64_t vf = jint(d, c, "VoteFor", 1);
        int64_t nr = jint(d, c, "NumOfRanks", 0);
        ct->id = jint(d, c, "Id", -1);
        ct->name = meta_add_name(m, jstr(d, c, "Description"));
        ct->vote_for = (vf >= 1 && vf <= 100) ? (uint32_t)vf : 1u;
        ct->nranks = (nr > 0 && nr <= 100) ? (uint32_t)nr : 0u;
        ct->disabled = jint(d, c, "Disabled", 0) != 0;
        if (ct->name == UINT32_MAX)
        {
            dom_set_err(err, errcch, L"Out of memory.");
            return FALSE;
        }
        m->contest_by_id[m->ncontests] = m->ncontests;
        m->ncontests++;
    }
    qsort_s(m->contest_by_id, m->ncontests, sizeof(uint32_t), contest_id_cmp, m->contests);
    for (i = 0; i + 1 < m->ncontests; i++)
    {
        if (m->contests[m->contest_by_id[i]].id == m->contests[m->contest_by_id[i + 1]].id)
        {
            dom_file_err(err, errcch, path, L"ContestManifest.json lists a contest Id twice.");
            return FALSE;
        }
    }

    if (!load_item_list(zip, "CandidateManifest.json", d, m, &m->cands, FALSE) ||
        !load_item_list(zip, "PrecinctPortionManifest.json", d, m, &m->portions, FALSE) ||
        !load_item_list(zip, "BallotTypeManifest.json", d, m, &m->btypes, FALSE) ||
        !load_item_list(zip, "CountingGroupManifest.json", d, m, &m->groups, FALSE) ||
        !load_item_list(zip, "TabulatorManifest.json", d, m, &m->tabs, TRUE))
    {
        dom_file_err(err, errcch, path, L"A manifest in the zip could not be read.");
        return FALSE;
    }
    if (m->cands.n == 0)
    {
        dom_file_err(err, errcch, path, L"CandidateManifest.json was not found in the zip.");
        return FALSE;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Column layout                                                              */
/* -------------------------------------------------------------------------- */

enum
{
    DK_CVRNUM,
    DK_RECORD,
    DK_TAB,
    DK_BATCH,
    DK_GROUP,
    DK_PLACE,
    DK_PORTION,
    DK_BTYPE,
    DK_STYPE,
    DK_CARD,
    DK_ADJ,
    DK_COUNT
};

static const char *const k_DomKeyNames[DK_COUNT] = {"Cvr Number",
                                                    "Record Id",
                                                    "Tabulator",
                                                    "Batch",
                                                    "Counting Group",
                                                    "Polling Place",
                                                    "Precinct Portion",
                                                    "Ballot Type",
                                                    "Session Type",
                                                    "Card",
                                                    "Adjudicated"};

#define DOM_KEYBUF 160

typedef struct DomLoad
{
    EeCvrTable *t;
    DomMeta meta;          /* current zip's manifests */
    JDoc doc;              /* per-session DOM */
    BOOL built;            /* table layout established (first session of the first zip) */
    BOOL zip_ready;        /* layout established/verified for the zip being read */
    BOOL key_on[DK_COUNT];
    int key_col[DK_COUNT]; /* column of each present key, -1 if absent */
    uint32_t frozen;
    uint32_t ncols;
    StrPool hdr;         /* header strings of the established layout */
    uint32_t *hdr_off;   /* ncols offsets into hdr */
    const char **cells;  /* ncols row cells */
    char keybuf[DK_COUNT][DOM_KEYBUF];
    const wchar_t *path; /* zip being read (for messages) */
    wchar_t *err;
    size_t errcch;
    BOOL failed;
} DomLoad;

/* Header cell strings for the current meta + key set, into @p hdr / @p off. */
static BOOL build_header(DomLoad *L,
                         DomMeta *m,
                         StrPool *hdr,
                         uint32_t **out_off,
                         uint32_t *out_ncols,
                         uint32_t *out_frozen)
{
    uint32_t ncols = 0;
    uint32_t frozen = 0;
    uint32_t k;
    uint32_t i;
    uint32_t *off;

    for (k = 0; k < DK_COUNT; k++)
    {
        frozen += L->key_on[k] ? 1u : 0u;
    }
    ncols = frozen;
    for (i = 0; i < m->ncontests; i++)
    {
        const DomContest *ct = &m->contests[i];
        if (!ct->disabled)
        {
            ncols += (ct->nranks > 0) ? ct->nranks : ct->vote_for;
        }
    }
    off = (uint32_t *)malloc((ncols ? ncols : 1) * sizeof(uint32_t));
    if (off == NULL)
    {
        return FALSE;
    }
    hdr->n = 0;
    ncols = 0;
    for (k = 0; k < DK_COUNT; k++)
    {
        if (L->key_on[k])
        {
            off[ncols++] = pool_add(hdr, k_DomKeyNames[k], strlen(k_DomKeyNames[k]));
        }
    }
    for (i = 0; i < m->ncontests; i++)
    {
        DomContest *ct = &m->contests[i];
        const char *name = m->pool.p + ct->name;
        uint32_t j;
        if (ct->disabled)
        {
            continue;
        }
        ct->col = ncols;
        if (ct->nranks > 0)
        {
            for (j = 1; j <= ct->nranks; j++)
            {
                char suffix[24];
                size_t nl = strlen(name);
                uint32_t o;
                StringCchPrintfA(suffix, ARRAYSIZE(suffix), " (Rank %u)", j);
                o = pool_add(hdr, name, nl);
                if (o != UINT32_MAX)
                {
                    hdr->n--; /* drop the NUL to append the suffix */
                    if (pool_add(hdr, suffix, strlen(suffix)) == UINT32_MAX)
                    {
                        o = UINT32_MAX;
                    }
                }
                off[ncols++] = o;
            }
        }
        else
        {
            off[ncols++] = pool_add(hdr, name, strlen(name));
            for (j = 1; j < ct->vote_for; j++)
            {
                off[ncols++] = pool_add(hdr, "", 0); /* "vote for N" continuation */
            }
        }
    }
    for (i = 0; i < ncols; i++)
    {
        if (off[i] == UINT32_MAX)
        {
            free(off);
            return FALSE;
        }
    }
    *out_off = off;
    *out_ncols = ncols;
    *out_frozen = frozen;
    return TRUE;
}

/* Establish (first zip) or verify (later zips) the table layout. Called on the first
 * session of each zip once the session's shape is known. */
static BOOL ensure_layout(DomLoad *L, uint32_t sess, uint32_t cur)
{
    const JDoc *d = &L->doc;
    BOOL on[DK_COUNT];
    StrPool hdr;
    uint32_t *off = NULL;
    uint32_t ncols;
    uint32_t frozen;
    uint32_t k;
    uint32_t i;
    const char **cells;

    if (L->zip_ready)
    {
        return TRUE;
    }
    ZeroMemory(on, sizeof(on));
    on[DK_CVRNUM] = jget(d, sess, "CastVoteRecordId") != JN_NONE;
    on[DK_RECORD] = TRUE;
    on[DK_TAB] = TRUE;
    on[DK_BATCH] = TRUE;
    on[DK_GROUP] = TRUE;
    on[DK_PLACE] = L->meta.has_location;
    on[DK_PORTION] = TRUE;
    on[DK_BTYPE] = TRUE;
    on[DK_STYPE] = jget(d, sess, "SessionType") != JN_NONE;
    on[DK_CARD] = jget(d, cur, "Cards") != JN_NONE;
    on[DK_ADJ] = TRUE;

    if (L->built)
    {
        /* A later zip: same key set and identical contest columns required. */
        BOOL same = (memcmp(on, L->key_on, sizeof(on)) == 0);
        ZeroMemory(&hdr, sizeof(hdr));
        if (same)
        {
            if (!build_header(L, &L->meta, &hdr, &off, &ncols, &frozen))
            {
                free(hdr.p);
                dom_set_err(L->err, L->errcch, L"Out of memory.");
                return FALSE;
            }
            same = (ncols == L->ncols);
            for (i = 0; same && i < ncols; i++)
            {
                same = strcmp(hdr.p + off[i], L->hdr.p + L->hdr_off[i]) == 0;
            }
            free(off);
            free(hdr.p);
        }
        if (!same)
        {
            dom_file_err(L->err,
                         L->errcch,
                         L->path,
                         L"This CVR export has different contests or fields than the first "
                         L"file; Dominion exports load together only from the same election.");
            return FALSE;
        }
        L->zip_ready = TRUE;
        return TRUE;
    }

    memcpy(L->key_on, on, sizeof(on));
    if (!build_header(L, &L->meta, &L->hdr, &L->hdr_off, &L->ncols, &L->frozen))
    {
        dom_set_err(L->err, L->errcch, L"Out of memory.");
        return FALSE;
    }
    cells = (const char **)malloc(L->ncols * sizeof(char *));
    if (cells == NULL)
    {
        dom_set_err(L->err, L->errcch, L"Out of memory.");
        return FALSE;
    }
    for (i = 0; i < L->ncols; i++)
    {
        cells[i] = L->hdr.p + L->hdr_off[i];
    }
    if (!EeCvr_BuildBegin(L->t, cells, L->ncols, L->frozen))
    {
        free((void *)cells);
        dom_set_err(L->err, L->errcch, L"Out of memory building the CVR table.");
        return FALSE;
    }
    L->cells = cells; /* reused as the per-row cell array from here on */
    i = 0;
    for (k = 0; k < DK_COUNT; k++)
    {
        L->key_col[k] = on[k] ? (int)i++ : -1;
    }
    L->built = TRUE;
    L->zip_ready = TRUE;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Sessions                                                                   */
/* -------------------------------------------------------------------------- */

#define DOM_MAXMARKS 256

/* Counties may redact a session's details: the field then holds the text
 * "*** REDACTED ***" instead of its number / array. Such cells read as this marker,
 * which ee_cvr.c recognizes as a redaction placeholder. */
static const char k_Redacted[] = "<Redacted>";

typedef struct VoteMark
{
    int64_t cand;
    int64_t rank;
    int64_t writein; /* WriteinIndex: separate write-in lines are separate marks */
} VoteMark;

static const char *cand_name(const DomMeta *m, int64_t id)
{
    const DomItem *it = list_find(&m->cands, id);
    return (it != NULL) ? m->pool.p + it->name : "(unknown candidate)";
}

static const char *item_name(const DomMeta *m, const DomList *l, int64_t id, char *buf, size_t cap)
{
    const DomItem *it = list_find(l, id);
    if (id == INT64_MIN)
    {
        return k_Redacted;
    }
    if (it != NULL && m->pool.p[it->name] != '\0')
    {
        return m->pool.p + it->name;
    }
    StringCchPrintfA(buf, cap, "%lld", (long long)id);
    return buf;
}

/* Id-valued member as an int64, INT64_MIN when the county redacted it (a string). */
static int64_t jid(const JDoc *d, uint32_t obj, const char *name)
{
    uint32_t v = jget(d, obj, name);
    if (v != JN_NONE && d->n[v].type == JT_STR)
    {
        return INT64_MIN;
    }
    return jint(d, obj, name, 0);
}

/* Fill one contest's cells from its "Contests" entry. */
static void fill_contest(DomLoad *L, uint32_t ctn)
{
    const JDoc *d = &L->doc;
    const DomMeta *m = &L->meta;
    const DomContest *ct = meta_find_contest(m, jint(d, ctn, "Id", -1));
    VoteMark votes[DOM_MAXMARKS];
    uint32_t nv = 0;
    uint32_t nmarked = 0; /* non-ambiguous marks (5.2 overvote fallback) */
    uint32_t mk;
    uint32_t marks;
    uint32_t i;
    uint32_t j;
    BOOL legacy;

    if (ct == NULL || ct->disabled)
    {
        return;
    }
    marks = jget(d, ctn, "Marks");
    if (marks != JN_NONE && d->n[marks].type == JT_STR)
    {
        uint32_t ncell = (ct->nranks > 0) ? ct->nranks : ct->vote_for;
        for (i = 0; i < ncell; i++)
        {
            L->cells[ct->col + i] = k_Redacted;
        }
        return;
    }
    /* 5.2 exports (no contest Overvotes/Undervotes counts) flag only the mark counted in
     * the first round as IsVote: every lower ranking and every overvoted mark has
     * IsVote = false. For those, a ranking is any non-ambiguous mark. Newer exports set
     * IsVote on each counted ranking (and on overvoted marks) and clear it on ambiguous
     * marks the adjudicator did not accept. */
    legacy = (jget(d, ctn, "Overvotes") == JN_NONE);
    if (marks != JN_NONE)
    {
        for (mk = d->n[marks].first; mk != JN_NONE; mk = d->n[mk].next)
        {
            int64_t cand = jint(d, mk, "CandidateId", -1);
            int64_t rank = jint(d, mk, "Rank", 1);
            int64_t wi = jint(d, mk, "WriteinIndex", -1);
            BOOL is_vote = jint(d, mk, "IsVote", 0) != 0;
            BOOL dup = FALSE;
            if (!jint(d, mk, "IsAmbiguous", 0))
            {
                nmarked++;
                if (legacy && ct->nranks > 0)
                {
                    is_vote = TRUE;
                }
            }
            if (!is_vote || nv >= DOM_MAXMARKS)
            {
                continue;
            }
            for (j = 0; j < nv; j++)
            {
                if (votes[j].cand == cand && votes[j].rank == rank && votes[j].writein == wi)
                {
                    dup = TRUE;
                    break;
                }
            }
            if (!dup)
            {
                votes[nv].cand = cand;
                votes[nv].rank = rank;
                votes[nv].writein = wi;
                nv++;
            }
        }
    }

    if (ct->nranks > 0)
    {
        for (i = 0; i < ct->nranks; i++)
        {
            uint32_t at = 0;
            const char *v = "undervote";
            for (j = 0; j < nv; j++)
            {
                if (votes[j].rank == (int64_t)i + 1)
                {
                    if (at++ == 0)
                    {
                        v = cand_name(m, votes[j].cand);
                    }
                }
            }
            if (at > 1)
            {
                v = "overvote";
            }
            L->cells[ct->col + i] = v;
        }
        return;
    }

    {
        int64_t ov = jint(d, ctn, "Overvotes", -1);
        BOOL over = (ov >= 0) ? (ov > 0) : (nv > ct->vote_for || nmarked > ct->vote_for);
        for (i = 0; i < ct->vote_for; i++)
        {
            const char *v;
            if (over)
            {
                v = "overvote";
            }
            else if (i < nv)
            {
                v = cand_name(m, votes[i].cand);
            }
            else
            {
                v = "undervote";
            }
            L->cells[ct->col + i] = v;
        }
    }
}

/* Ballot image name from ImageMask ("...\\00005_01198_000022*.*" -> 00005_01198_000022). */
static void record_id(const char *mask,
                      int64_t tab,
                      int64_t batch,
                      const JDoc *d,
                      uint32_t sess,
                      char *out,
                      size_t cap)
{
    const char *leaf;
    size_t n;
    out[0] = '\0';
    if (mask != NULL)
    {
        leaf = zip_leaf(mask);
        n = strcspn(leaf, "*.");
        while (n > 0 && leaf[n - 1] == '_')
        {
            n--;
        }
        if (n > 0 && n < cap)
        {
            memcpy(out, leaf, n);
            out[n] = '\0';
            return;
        }
    }
    {
        const char *rs = jstr(d, sess, "RecordId");
        if (rs != NULL)
        {
            StringCchPrintfA(out, cap, "%05lld_%05lld_%s", (long long)tab, (long long)batch, rs);
        }
        else
        {
            StringCchPrintfA(out,
                             cap,
                             "%05lld_%05lld_%06lld",
                             (long long)tab,
                             (long long)batch,
                             (long long)jint(d, sess, "RecordId", 0));
        }
    }
}

static int __cdecl int_cmp(const void *a, const void *b)
{
    int x = *(const int *)a;
    int y = *(const int *)b;
    return (x > y) - (x < y);
}

/* Turn one parsed session into a table row. Returns FALSE on error (message set). */
static BOOL process_session(DomLoad *L, uint32_t sess)
{
    const JDoc *d = &L->doc;
    const DomMeta *m = &L->meta;
    uint32_t orig = jget(d, sess, "Original");
    uint32_t mod = jget(d, sess, "Modified");
    uint32_t cur;
    uint32_t cards;
    int64_t tab;
    int64_t batch;
    uint32_t i;

    if (d->n[sess].type != JT_OBJ)
    {
        return TRUE;
    }
    cur = orig;
    if (mod != JN_NONE && (jint(d, mod, "IsCurrent", 1) != 0 || orig == JN_NONE ||
                           jint(d, orig, "IsCurrent", 0) == 0))
    {
        cur = mod;
    }
    if (cur == JN_NONE || d->n[cur].type != JT_OBJ)
    {
        return TRUE; /* nothing recorded for this session */
    }
    if (!ensure_layout(L, sess, cur))
    {
        return FALSE;
    }

    for (i = 0; i < L->ncols; i++)
    {
        L->cells[i] = "";
    }
    tab = jint(d, sess, "TabulatorId", 0);
    batch = jint(d, sess, "BatchId", 0);

    /* ---- key columns ---- */
    if (L->key_col[DK_CVRNUM] >= 0)
    {
        StringCchPrintfA(L->keybuf[DK_CVRNUM],
                         DOM_KEYBUF,
                         "%lld",
                         (long long)jint(d, sess, "CastVoteRecordId", 0));
        L->cells[L->key_col[DK_CVRNUM]] = L->keybuf[DK_CVRNUM];
    }
    record_id(jstr(d, sess, "ImageMask"), tab, batch, d, sess, L->keybuf[DK_RECORD], DOM_KEYBUF);
    L->cells[L->key_col[DK_RECORD]] = L->keybuf[DK_RECORD];
    L->cells[L->key_col[DK_TAB]] = item_name(m, &m->tabs, tab, L->keybuf[DK_TAB], DOM_KEYBUF);
    StringCchPrintfA(L->keybuf[DK_BATCH],
                     DOM_KEYBUF,
                     "%05lld-%05lld",
                     (long long)tab,
                     (long long)batch);
    L->cells[L->key_col[DK_BATCH]] = L->keybuf[DK_BATCH];
    L->cells[L->key_col[DK_GROUP]] =
        item_name(m, &m->groups, jid(d, sess, "CountingGroupId"), L->keybuf[DK_GROUP], DOM_KEYBUF);
    if (L->key_col[DK_PLACE] >= 0)
    {
        const DomItem *it = list_find(&m->tabs, tab);
        if (it != NULL && it->extra != UINT32_MAX)
        {
            L->cells[L->key_col[DK_PLACE]] = m->pool.p + it->extra;
        }
    }
    L->cells[L->key_col[DK_PORTION]] = item_name(m,
                                                 &m->portions,
                                                 jid(d, cur, "PrecinctPortionId"),
                                                 L->keybuf[DK_PORTION],
                                                 DOM_KEYBUF);
    L->cells[L->key_col[DK_BTYPE]] =
        item_name(m, &m->btypes, jid(d, cur, "BallotTypeId"), L->keybuf[DK_BTYPE], DOM_KEYBUF);
    if (L->key_col[DK_STYPE] >= 0)
    {
        const char *st = jstr(d, sess, "SessionType");
        L->cells[L->key_col[DK_STYPE]] = (st != NULL) ? st : "";
    }
    L->cells[L->key_col[DK_ADJ]] = (cur == mod) ? "Yes" : "No";

    /* ---- contests (per card) ---- */
    cards = jget(d, cur, "Cards");
    if (cards != JN_NONE && d->n[cards].type == JT_ARR)
    {
        int papers[16];
        uint32_t np = 0;
        uint32_t card;
        for (card = d->n[cards].first; card != JN_NONE; card = d->n[card].next)
        {
            uint32_t cts = jget(d, card, "Contests");
            uint32_t ctn;
            if (np < ARRAYSIZE(papers))
            {
                papers[np++] = (int)jint(d, card, "PaperIndex", 0) + 1;
            }
            if (cts == JN_NONE)
            {
                continue;
            }
            for (ctn = d->n[cts].first; ctn != JN_NONE; ctn = d->n[ctn].next)
            {
                fill_contest(L, ctn);
            }
        }
        if (L->key_col[DK_CARD] >= 0 && np > 0)
        {
            char *o = L->keybuf[DK_CARD];
            size_t used = 0;
            qsort(papers, np, sizeof(int), int_cmp);
            o[0] = '\0';
            for (i = 0; i < np; i++)
            {
                if (i > 0 && papers[i] == papers[i - 1])
                {
                    continue;
                }
                if (FAILED(StringCchPrintfA(o + used,
                                            DOM_KEYBUF - used,
                                            used ? ",%d" : "%d",
                                            papers[i])))
                {
                    break;
                }
                used = strlen(o);
            }
            L->cells[L->key_col[DK_CARD]] = o;
        }
    }
    else
    {
        uint32_t cts = jget(d, cur, "Contests"); /* 5.2: no Cards level */
        uint32_t ctn;
        if (cts != JN_NONE)
        {
            for (ctn = d->n[cts].first; ctn != JN_NONE; ctn = d->n[ctn].next)
            {
                fill_contest(L, ctn);
            }
        }
    }

    if (!EeCvr_BuildAppendRow(L->t, L->cells, L->ncols))
    {
        dom_set_err(L->err, L->errcch, L"Out of memory building the CVR table.");
        return FALSE;
    }
    return TRUE;
}

/* Stream one CvrExport*.json entry: parse each element of "Sessions" and add its row. */
static EeLoadStatus read_cvr_entry(DomLoad *L,
                                   mz_zip_archive *zip,
                                   mz_uint index,
                                   unsigned char *chunk,
                                   DomProgress *pg)
{
    JsonSrc src;
    EeLoadStatus st = EeLoadStatus_Ok;
    int c;

    ZeroMemory(&src, sizeof(src));
    src.buf = chunk;
    src.pg = pg;
    src.it = mz_zip_reader_extract_iter_new(zip, index, 0);
    if (src.it == NULL)
    {
        dom_file_err(L->err, L->errcch, L->path, L"A CVR file in the zip could not be read.");
        return EeLoadStatus_Error;
    }
    src.p = src.end = chunk;

    if (js_peek(&src) == 0xEF) /* UTF-8 BOM */
    {
        if (js_get(&src) != 0xEF || js_get(&src) != 0xBB || js_get(&src) != 0xBF)
        {
            goto bad;
        }
    }
    if (js_ws(&src) != '{')
    {
        goto bad;
    }
    src.p++;
    for (;;)
    {
        uint32_t key;
        c = js_ws(&src);
        if (c == '}')
        {
            src.p++;
            break;
        }
        if (c != '"')
        {
            goto bad;
        }
        src.p++;
        jdoc_reset(&L->doc);
        key = js_string(&src, &L->doc.pool);
        if (key == JN_NONE || js_ws(&src) != ':')
        {
            goto bad;
        }
        src.p++;
        if (strcmp(L->doc.pool.p + key, "Sessions") == 0)
        {
            if (js_ws(&src) != '[')
            {
                goto bad;
            }
            src.p++;
            if (js_ws(&src) == ']')
            {
                src.p++;
            }
            else
            {
                for (;;)
                {
                    uint32_t sess;
                    jdoc_reset(&L->doc);
                    sess = js_value(&src, &L->doc, 0);
                    if (sess == JN_NONE)
                    {
                        goto bad;
                    }
                    if (!process_session(L, sess))
                    {
                        st = EeLoadStatus_Error;
                        goto done;
                    }
                    c = js_ws(&src);
                    if (c == ',')
                    {
                        src.p++;
                        continue;
                    }
                    if (c == ']')
                    {
                        src.p++;
                        break;
                    }
                    goto bad;
                }
            }
        }
        else if (js_value(&src, &L->doc, 0) == JN_NONE)
        {
            goto bad;
        }
        c = js_ws(&src);
        if (c == ',')
        {
            src.p++;
            continue;
        }
        if (c == '}')
        {
            src.p++;
            break;
        }
        goto bad;
    }
    goto done;

bad:
    if (pg->cancelled)
    {
        st = EeLoadStatus_Cancelled;
    }
    else
    {
        char name[512];
        wchar_t wname[512];
        wchar_t msg[700];
        name[0] = '\0';
        mz_zip_reader_get_filename(zip, index, name, sizeof(name));
        if (MultiByteToWideChar(CP_UTF8, 0, zip_leaf(name), -1, wname, ARRAYSIZE(wname)) <= 0)
        {
            wname[0] = L'\0';
        }
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"%s is not a valid Dominion CVR file.", wname);
        dom_file_err(L->err, L->errcch, L->path, msg);
        st = EeLoadStatus_Error;
    }
done:
    mz_zip_reader_extract_iter_free(src.it);
    return st;
}

/* -------------------------------------------------------------------------- */
/* Zip handling + public API                                                  */
/* -------------------------------------------------------------------------- */

typedef struct DomZip
{
    FILE *fp;
    mz_zip_archive zip;
    BOOL open;
} DomZip;

static BOOL dom_zip_open(DomZip *z, const wchar_t *path)
{
    __int64 fsize;
    ZeroMemory(z, sizeof(*z));
    if (_wfopen_s(&z->fp, path, L"rb") != 0 || z->fp == NULL)
    {
        z->fp = NULL;
        return FALSE;
    }
    if (_fseeki64(z->fp, 0, SEEK_END) != 0 || (fsize = _ftelli64(z->fp)) <= 0 ||
        _fseeki64(z->fp, 0, SEEK_SET) != 0)
    {
        fclose(z->fp);
        z->fp = NULL;
        return FALSE;
    }
    mz_zip_zero_struct(&z->zip);
    if (!mz_zip_reader_init_cfile(&z->zip, z->fp, (mz_uint64)fsize, 0))
    {
        fclose(z->fp);
        z->fp = NULL;
        return FALSE;
    }
    z->open = TRUE;
    return TRUE;
}

static void dom_zip_close(DomZip *z)
{
    if (z->open)
    {
        mz_zip_reader_end(&z->zip);
    }
    if (z->fp != NULL)
    {
        fclose(z->fp);
    }
    ZeroMemory(z, sizeof(*z));
}

typedef struct EntryOrder
{
    mz_uint index;
    long long num;
    uint64_t size;
} EntryOrder;

static int __cdecl entry_cmp(const void *a, const void *b)
{
    const EntryOrder *x = (const EntryOrder *)a;
    const EntryOrder *y = (const EntryOrder *)b;
    if (x->num != y->num)
    {
        return (x->num < y->num) ? -1 : 1;
    }
    return (x->index < y->index) ? -1 : (x->index > y->index) ? 1 : 0;
}

/* The CvrExport entries of an open zip in export order; *out is heap (caller frees). */
static BOOL list_cvr_entries(mz_zip_archive *zip, EntryOrder **out, uint32_t *count)
{
    mz_uint n = mz_zip_reader_get_num_files(zip);
    mz_uint i;
    EntryOrder *v = (EntryOrder *)malloc((n ? n : 1) * sizeof(EntryOrder));
    uint32_t k = 0;
    *out = NULL;
    *count = 0;
    if (v == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        mz_zip_archive_file_stat stt;
        if (!mz_zip_reader_file_stat(zip, i, &stt) || mz_zip_reader_is_file_a_directory(zip, i))
        {
            continue;
        }
        if (is_cvr_export_name(stt.m_filename))
        {
            v[k].index = i;
            v[k].num = cvr_export_number(stt.m_filename);
            v[k].size = stt.m_uncomp_size;
            k++;
        }
    }
    qsort(v, k, sizeof(EntryOrder), entry_cmp);
    *out = v;
    *count = k;
    return TRUE;
}

BOOL EeCvr_IsDominionZip(const wchar_t *path)
{
    DomZip z;
    BOOL ok = FALSE;
    if (path == NULL || !dom_zip_open(&z, path))
    {
        return FALSE;
    }
    if (zip_find_leaf(&z.zip, "ContestManifest.json") >= 0)
    {
        mz_uint n = mz_zip_reader_get_num_files(&z.zip);
        mz_uint i;
        for (i = 0; i < n && !ok; i++)
        {
            char name[512];
            if (mz_zip_reader_get_filename(&z.zip, i, name, sizeof(name)) > 0 &&
                is_cvr_export_name(name))
            {
                ok = TRUE;
            }
        }
    }
    dom_zip_close(&z);
    return ok;
}

EeLoadStatus EeCvr_LoadFromDominionZips(const wchar_t *const *paths,
                                        int count,
                                        EeCvrTable *out,
                                        volatile LONG *cancel_flag,
                                        EeLoadProgressFn progress_fn,
                                        void *progress_user,
                                        wchar_t *error_message,
                                        size_t error_cch)
{
    DomLoad L;
    DomProgress pg;
    DomZip z;
    EntryOrder *ents = NULL;
    uint32_t nents = 0;
    unsigned char *chunk = NULL;
    EeLoadStatus s = EeLoadStatus_Ok;
    int f;
    uint32_t e;

    if (paths == NULL || out == NULL || count <= 0)
    {
        dom_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    EeCvr_Clear(out);
    ZeroMemory(&L, sizeof(L));
    ZeroMemory(&pg, sizeof(pg));
    ZeroMemory(&z, sizeof(z));
    L.t = out;
    L.err = error_message;
    L.errcch = error_cch;
    pg.cancel_flag = cancel_flag;
    pg.fn = progress_fn;
    pg.user = progress_user;
    pg.last_pct = 101;
    pg.rows = &out->nrows;

    chunk = (unsigned char *)malloc(DOM_READ_CHUNK);
    if (chunk == NULL)
    {
        dom_set_err(error_message, error_cch, L"Out of memory.");
        return EeLoadStatus_Error;
    }

    /* Validate every zip up front and size the progress bar. */
    for (f = 0; f < count; f++)
    {
        if (!dom_zip_open(&z, paths[f]))
        {
            dom_file_err(error_message, error_cch, paths[f], L"The zip file could not be opened.");
            s = EeLoadStatus_Error;
            goto cleanup;
        }
        if (zip_find_leaf(&z.zip, "ContestManifest.json") < 0 ||
            !list_cvr_entries(&z.zip, &ents, &nents) || nents == 0)
        {
            dom_zip_close(&z);
            dom_file_err(error_message,
                         error_cch,
                         paths[f],
                         L"This is not a Dominion CVR export (no ContestManifest.json and "
                         L"CvrExport*.json files).");
            s = EeLoadStatus_Error;
            goto cleanup;
        }
        for (e = 0; e < nents; e++)
        {
            pg.total += ents[e].size;
        }
        free(ents);
        ents = NULL;
        dom_zip_close(&z);
    }

    for (f = 0; f < count && s == EeLoadStatus_Ok; f++)
    {
        L.path = paths[f];
        L.zip_ready = FALSE;
        if (!dom_zip_open(&z, paths[f]))
        {
            dom_file_err(error_message, error_cch, paths[f], L"The zip file could not be opened.");
            s = EeLoadStatus_Error;
            break;
        }
        meta_free(&L.meta);
        if (!load_meta(&z.zip, &L.meta, &L.doc, paths[f], error_message, error_cch) ||
            !list_cvr_entries(&z.zip, &ents, &nents))
        {
            s = EeLoadStatus_Error;
            dom_zip_close(&z);
            break;
        }
        for (e = 0; e < nents && s == EeLoadStatus_Ok; e++)
        {
            s = read_cvr_entry(&L, &z.zip, ents[e].index, chunk, &pg);
        }
        free(ents);
        ents = NULL;
        dom_zip_close(&z);
    }
    if (s == EeLoadStatus_Ok && pg.cancelled)
    {
        s = EeLoadStatus_Cancelled;
    }
    if (s == EeLoadStatus_Ok && out->nrows == 0)
    {
        dom_set_err(error_message, error_cch, L"No Cast Vote Records were found.");
        s = EeLoadStatus_Error;
    }

cleanup:
    if (s != EeLoadStatus_Ok)
    {
        EeCvr_Clear(out);
    }
    free(ents);
    free(chunk);
    free((void *)L.cells);
    free(L.hdr.p);
    free(L.hdr_off);
    jdoc_free(&L.doc);
    meta_free(&L.meta);
    return s;
}
