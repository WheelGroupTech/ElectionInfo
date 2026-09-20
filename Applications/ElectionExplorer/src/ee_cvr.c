/**
 * @file ee_cvr.c
 * @brief Sparse Cast Vote Record table + multi-file loader. See ee_cvr.h and
 *        docs/cvr-design.md.
 */

#include "ee_cvr.h"
#include "xlsx.h"
#include "csv_sheet.h"

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <search.h> /* qsort_s */

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static void cvr_set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        size_t i = 0;
        for (; msg[i] != L'\0' && i + 1 < cch; i++)
        {
            dst[i] = msg[i];
        }
        dst[i] = L'\0';
    }
}

static wchar_t *utf8_to_wide_alloc(const char *s)
{
    int n = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0);
    wchar_t *w;
    if (n <= 0)
    {
        n = 1;
    }
    w = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
    if (w == NULL)
    {
        return NULL;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, s, -1, w, n) <= 0)
    {
        w[0] = L'\0';
    }
    return w;
}

/* Collapse runs of spaces/tabs to a single space and trim both ends, writing the
 * result to @p dst (which must hold at least strlen(src)+1 bytes). Returns the new
 * length. UTF-8 safe: only ASCII 0x20/0x09 are inspected, and those bytes never
 * occur inside a multibyte sequence. Fixes CVR values stored with stray internal
 * spacing (e.g. "John   Cornyn" -> "John Cornyn"). */
static size_t normalize_ws(const char *src, char *dst)
{
    const char *s = src;
    char *d = dst;
    while (*s == ' ' || *s == '\t')
    {
        s++; /* trim leading */
    }
    while (*s != '\0')
    {
        if (*s == ' ' || *s == '\t')
        {
            while (*s == ' ' || *s == '\t')
            {
                s++;
            }
            if (*s != '\0')
            {
                *d++ = ' '; /* single space, only when non-ws follows (trims trailing) */
            }
        }
        else
        {
            *d++ = *s++;
        }
    }
    *d = '\0';
    return (size_t)(d - dst);
}

/* -------------------------------------------------------------------------- */
/* Value interning (UTF-8 -> id)                                              */
/* -------------------------------------------------------------------------- */

static uint32_t fnv1a(const char *s, size_t n)
{
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++)
    {
        h = (h ^ (unsigned char)s[i]) * 16777619u;
    }
    return h;
}

static BOOL val_pool_reserve(EeCvrTable *t, size_t add)
{
    if (t->val_len + add > t->val_cap)
    {
        size_t ncap = t->val_cap ? t->val_cap * 2 : 4096;
        char *np;
        while (ncap < t->val_len + add)
        {
            ncap *= 2;
        }
        np = (char *)realloc(t->val_pool, ncap);
        if (np == NULL)
        {
            return FALSE;
        }
        t->val_pool = np;
        t->val_cap = ncap;
    }
    return TRUE;
}

static BOOL val_rehash(EeCvrTable *t)
{
    uint32_t ncap = t->val_hash_cap ? t->val_hash_cap * 2 : 1024;
    uint32_t *nh = (uint32_t *)calloc(ncap, sizeof(uint32_t));
    uint32_t id;
    if (nh == NULL)
    {
        return FALSE;
    }
    for (id = 0; id < t->val_count; id++)
    {
        const char *s = t->val_pool + t->val_off[id];
        uint32_t mask = ncap - 1;
        uint32_t i = fnv1a(s, strlen(s)) & mask;
        while (nh[i] != 0)
        {
            i = (i + 1) & mask;
        }
        nh[i] = id + 1;
    }
    free(t->val_hash);
    t->val_hash = nh;
    t->val_hash_cap = ncap;
    return TRUE;
}

static BOOL val_intern(EeCvrTable *t, const char *s, size_t n, uint32_t *out_id)
{
    uint32_t mask;
    uint32_t i;
    if (t->val_hash_cap == 0 || (uint64_t)(t->val_count + 1) * 4 >= (uint64_t)t->val_hash_cap * 3)
    {
        if (!val_rehash(t))
        {
            return FALSE;
        }
    }
    mask = t->val_hash_cap - 1;
    i = fnv1a(s, n) & mask;
    for (;;)
    {
        uint32_t slot = t->val_hash[i];
        if (slot == 0)
        {
            uint32_t id = t->val_count;
            if (id == t->val_cap_ids)
            {
                uint32_t nc = t->val_cap_ids ? t->val_cap_ids * 2 : 256;
                uint32_t *no = (uint32_t *)realloc(t->val_off, (size_t)nc * sizeof(uint32_t));
                if (no == NULL)
                {
                    return FALSE;
                }
                t->val_off = no;
                t->val_cap_ids = nc;
            }
            if (!val_pool_reserve(t, n + 1))
            {
                return FALSE;
            }
            t->val_off[id] = (uint32_t)t->val_len;
            memcpy(t->val_pool + t->val_len, s, n);
            t->val_pool[t->val_len + n] = '\0';
            t->val_len += n + 1;
            t->val_count++;
            t->val_hash[i] = id + 1;
            *out_id = id;
            return TRUE;
        }
        else
        {
            uint32_t id = slot - 1;
            const char *e = t->val_pool + t->val_off[id];
            if (strlen(e) == n && memcmp(e, s, n) == 0)
            {
                *out_id = id;
                return TRUE;
            }
        }
        i = (i + 1) & mask;
    }
}

/* -------------------------------------------------------------------------- */
/* Table lifecycle                                                            */
/* -------------------------------------------------------------------------- */

void EeCvr_Init(EeCvrTable *t)
{
    if (t != NULL)
    {
        ZeroMemory(t, sizeof(*t));
    }
}

void EeCvr_Clear(EeCvrTable *t)
{
    uint32_t i;
    if (t == NULL)
    {
        return;
    }
    if (t->col_titles != NULL)
    {
        for (i = 0; i < t->ncols; i++)
        {
            free(t->col_titles[i]);
        }
        free(t->col_titles);
    }
    free(t->col_group);
    free(t->view_index);
    free(t->row_start);
    free(t->ent_col);
    free(t->ent_val);
    free(t->val_pool);
    free(t->val_off);
    free(t->val_hash);
    ZeroMemory(t, sizeof(*t));
}

/* -------------------------------------------------------------------------- */
/* Header / frozen-column detection                                           */
/* -------------------------------------------------------------------------- */

static BOOL wcieq_trimmed(const wchar_t *s, const wchar_t *key)
{
    while (*s == L' ' || *s == L'\t')
    {
        s++;
    }
    for (;;)
    {
        wchar_t a = *s;
        wchar_t b = *key;
        if (b == L'\0')
        {
            while (a == L' ' || a == L'\t')
            {
                a = *(++s);
            }
            return a == L'\0';
        }
        if (a >= L'A' && a <= L'Z')
        {
            a = (wchar_t)(a - L'A' + L'a');
        }
        if (a != b)
        {
            return FALSE;
        }
        s++;
        key++;
    }
}

static BOOL cvr_is_key_header(const wchar_t *s)
{
    return wcieq_trimmed(s, L"cast vote record") || wcieq_trimmed(s, L"batch") ||
           wcieq_trimmed(s, L"ballot status") || wcieq_trimmed(s, L"precinct") ||
           wcieq_trimmed(s, L"ballot style");
}

/* True if a header cell is blank (empty or only whitespace). */
static BOOL wstr_blank(const wchar_t *s)
{
    if (s == NULL)
    {
        return TRUE;
    }
    for (; *s != L'\0'; s++)
    {
        if (*s != L' ' && *s != L'\t')
        {
            return FALSE;
        }
    }
    return TRUE;
}

/*
 * Build the effective column titles and contest grouping from a raw header row.
 *
 * A "vote for N" contest spans several columns: the first carries the contest
 * title and each additional column has a BLANK header. Those blank continuation
 * columns each hold one of the voter's selections (or "undervote"); they get the
 * derived title "<contest> (2)", "<contest> (3)", ... and col_group[i] is set to
 * the contest's title column, so they read as part of the contest in the grid and
 * Phase 2 can tabulate a race across all of its columns without parsing the display
 * suffix. Titled columns (and the leading key columns) get col_group[i] == i.
 *
 * Only a blank header marks a continuation. A title that merely repeats verbatim is
 * a DISTINCT race, not a continuation: Travis County's L26 lists "City of Bee Cave,
 * City Councilmember at Large" on two adjacent columns, and the official Clarity
 * results show these as two separate single-winner (Vote For 1) races. (By contrast
 * the blank-header contests there are genuinely multi-seat: Briarcliff Alderman =
 * Vote For 3, Ensenadas Director Election = Vote For 5.)
 *
 * On success *out_titles / *out_group are heap arrays of length @p ncells that the
 * caller owns (free each title, then both arrays). On failure everything is freed
 * and both outputs are NULL.
 */
static BOOL cvr_build_titles(const char *const *cells,
                             uint32_t ncells,
                             wchar_t ***out_titles,
                             uint32_t **out_group)
{
    wchar_t **titles;
    uint32_t *group;
    uint32_t i;
    uint32_t last_title = 0; /* index of the current contest's title column */
    BOOL have_title = FALSE;
    uint32_t choice = 1; /* selections seen so far within the current contest */

    *out_titles = NULL;
    *out_group = NULL;
    titles = (wchar_t **)calloc(ncells, sizeof(wchar_t *));
    group = (uint32_t *)malloc((size_t)ncells * sizeof(uint32_t));
    if (titles == NULL || group == NULL)
    {
        free(titles);
        free(group);
        return FALSE;
    }
    for (i = 0; i < ncells; i++)
    {
        wchar_t *w = utf8_to_wide_alloc(cells[i] != NULL ? cells[i] : "");
        BOOL is_cont;
        if (w == NULL)
        {
            goto oom;
        }
        /* Continuation of the current contest: a blank header only. A repeated
         * identical title is a distinct race (confirmed against official results). */
        is_cont = have_title && wstr_blank(w);
        if (!is_cont)
        {
            /* A titled column, or a leading blank before any contest: its own group. */
            titles[i] = w;
            group[i] = i;
            if (!wstr_blank(w))
            {
                last_title = i;
                have_title = TRUE;
                choice = 1;
            }
        }
        else
        {
            const wchar_t *base = titles[last_title];
            size_t need = wcslen(base) + 16; /* base + " (" + digits + ")" + NUL */
            wchar_t *d = (wchar_t *)malloc(need * sizeof(wchar_t));
            free(w);
            if (d == NULL)
            {
                goto oom;
            }
            choice++;
            StringCchPrintfW(d, need, L"%s (%u)", base, choice);
            titles[i] = d;
            group[i] = last_title;
        }
    }
    *out_titles = titles;
    *out_group = group;
    return TRUE;

oom:
    for (i = 0; i < ncells; i++)
    {
        free(titles[i]);
    }
    free(titles);
    free(group);
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Loader                                                                     */
/* -------------------------------------------------------------------------- */

typedef struct CvrLoadCtx
{
    EeCvrTable *t;
    BOOL is_first_file;
    BOOL got_header;
    const wchar_t *path_leaf;
    wchar_t *err;
    size_t errcch;
    BOOL failed;
} CvrLoadCtx;

static BOOL ensure_rows(EeCvrTable *t)
{
    if ((size_t)t->nrows + 1 >= t->cap_rows)
    {
        size_t ncap = t->cap_rows ? t->cap_rows * 2 : 4096;
        uint32_t *nrs = (uint32_t *)realloc(t->row_start, (ncap + 1) * sizeof(uint32_t));
        uint32_t *nvi;
        if (nrs == NULL)
        {
            return FALSE;
        }
        t->row_start = nrs;
        nvi = (uint32_t *)realloc(t->view_index, ncap * sizeof(uint32_t));
        if (nvi == NULL)
        {
            return FALSE;
        }
        t->view_index = nvi;
        t->cap_rows = ncap;
    }
    return TRUE;
}

static BOOL ensure_ent(EeCvrTable *t, size_t add)
{
    if (t->nent + add > t->cap_ent)
    {
        size_t ncap = t->cap_ent ? t->cap_ent * 2 : 8192;
        uint32_t *nc;
        uint32_t *nv;
        while (ncap < t->nent + add)
        {
            ncap *= 2;
        }
        nc = (uint32_t *)realloc(t->ent_col, ncap * sizeof(uint32_t));
        if (nc == NULL)
        {
            return FALSE;
        }
        t->ent_col = nc;
        nv = (uint32_t *)realloc(t->ent_val, ncap * sizeof(uint32_t));
        if (nv == NULL)
        {
            return FALSE;
        }
        t->ent_val = nv;
        t->cap_ent = ncap;
    }
    return TRUE;
}

static BOOL establish_header(CvrLoadCtx *ctx, const char *const *cells, uint32_t ncells)
{
    EeCvrTable *t = ctx->t;
    if (ncells == 0)
    {
        cvr_set_err(ctx->err, ctx->errcch, L"The CVR file has an empty header row.");
        return FALSE;
    }
    if (!cvr_build_titles(cells, ncells, &t->col_titles, &t->col_group))
    {
        cvr_set_err(ctx->err, ctx->errcch, L"Out of memory.");
        return FALSE;
    }
    t->ncols = ncells;
    t->frozen_count = 0;
    while (t->frozen_count < ncells && cvr_is_key_header(t->col_titles[t->frozen_count]))
    {
        t->frozen_count++;
    }
    if (t->frozen_count == 0)
    {
        t->frozen_count = 1;
    }
    if (!ensure_rows(t))
    {
        cvr_set_err(ctx->err, ctx->errcch, L"Out of memory.");
        return FALSE;
    }
    t->row_start[0] = 0;
    return TRUE;
}

/* Confirm a later file's header matches the established schema exactly. */
static BOOL header_matches(CvrLoadCtx *ctx, const char *const *cells, uint32_t ncells)
{
    EeCvrTable *t = ctx->t;
    wchar_t **titles = NULL;
    uint32_t *group = NULL;
    uint32_t i;
    BOOL ok = TRUE;

    if (ncells != t->ncols)
    {
        return FALSE;
    }
    /* Derive the same effective titles as the first file so a matching "vote for N"
     * layout is accepted (blank continuation columns line up identically). */
    if (!cvr_build_titles(cells, ncells, &titles, &group))
    {
        return FALSE;
    }
    for (i = 0; i < ncells; i++)
    {
        if (wcscmp(titles[i], t->col_titles[i]) != 0)
        {
            ok = FALSE;
            break;
        }
    }
    for (i = 0; i < ncells; i++)
    {
        free(titles[i]);
    }
    free(titles);
    free(group);
    return ok;
}

static BOOL append_data_row(CvrLoadCtx *ctx, const char *const *cells, uint32_t ncells)
{
    EeCvrTable *t = ctx->t;
    uint32_t c;
    uint32_t limit = (ncells < t->ncols) ? ncells : t->ncols;
    size_t row_start_ent;
    BOOL saw_contest = FALSE;

    if (!ensure_rows(t))
    {
        cvr_set_err(ctx->err, ctx->errcch, L"Out of memory loading ballots.");
        return FALSE;
    }
    row_start_ent = t->nent;
    for (c = 0; c < limit; c++)
    {
        const char *v = cells[c];
        char stackbuf[256];
        char *heapbuf = NULL;
        char *dst;
        size_t vlen;
        uint32_t id;
        if (v == NULL || v[0] == '\0')
        {
            continue; /* sparse: skip blanks */
        }
        /* Normalize stray whitespace before interning so equivalent selections
         * (e.g. "John Cornyn" vs "John   Cornyn") share one value + one tally. */
        vlen = strlen(v);
        dst = (vlen < sizeof(stackbuf)) ? stackbuf : (heapbuf = (char *)malloc(vlen + 1));
        if (dst == NULL)
        {
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory loading ballots.");
            return FALSE;
        }
        vlen = normalize_ws(v, dst);
        if (vlen == 0)
        {
            free(heapbuf);
            continue; /* value was only whitespace -> treat as blank */
        }
        if (!val_intern(t, dst, vlen, &id))
        {
            free(heapbuf);
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory interning values.");
            return FALSE;
        }
        free(heapbuf);
        if (!ensure_ent(t, 1))
        {
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory loading ballots.");
            return FALSE;
        }
        t->ent_col[t->nent] = c;
        t->ent_val[t->nent] = id;
        t->nent++;
        if (c >= t->frozen_count)
        {
            saw_contest = TRUE;
        }
    }
    /* Drop a row that carries no contest selection (no non-blank cell beyond the
     * frozen key columns). A real ballot always records a value -- a candidate,
     * `undervote`, or `overvote` -- in every contest on its style, and a
     * continuation card in a multi-card CVR carries its own page's contests, so a
     * row with only key columns (or none) filled is never a countable ballot.
     * This absorbs export artifacts that would otherwise inflate the ballot-record
     * count and trip the multi-card heuristic: the trailing `,,,,` line Excel
     * appends when saving a sheet as CSV, and the occasional record Excel breaks
     * with a spurious unquoted newline after the first field (leaving a lone
     * Cast-Vote-Record-id line plus a headless row whose contests are still
     * column-aligned and tally correctly). Keeps `.xlsx` and delimited-text
     * exports of the same election consistent. */
    if (!saw_contest)
    {
        t->nent = row_start_ent; /* discard any key-only entries */
        return TRUE;
    }
    t->row_start[t->nrows + 1] = (uint32_t)t->nent;
    t->view_index[t->nrows] = t->nrows;
    t->nrows++;
    return TRUE;
}

static BOOL cvr_row_sink(void *vctx, const char *const *cells, uint32_t ncells)
{
    CvrLoadCtx *ctx = (CvrLoadCtx *)vctx;

    if (!ctx->got_header)
    {
        if (ncells == 0)
        {
            return TRUE; /* skip a leading blank row */
        }
        if (ctx->is_first_file)
        {
            if (!establish_header(ctx, cells, ncells))
            {
                ctx->failed = TRUE;
                return FALSE;
            }
        }
        else if (!header_matches(ctx, cells, ncells))
        {
            wchar_t msg[400];
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"The file \"%s\" has a different column layout than the "
                             L"first file selected.\n\nAll Cast Vote Record files loaded "
                             L"together must have identical headers. No data was loaded.",
                             (ctx->path_leaf != NULL) ? ctx->path_leaf : L"(unknown)");
            cvr_set_err(ctx->err, ctx->errcch, msg);
            ctx->failed = TRUE;
            return FALSE;
        }
        ctx->got_header = TRUE;
        return TRUE;
    }

    if (!append_data_row(ctx, cells, ncells))
    {
        ctx->failed = TRUE;
        return FALSE;
    }
    return TRUE;
}

/* TRUE if @p path ends (case-insensitively) with @p ext (which includes the dot). */
static BOOL path_has_ext(const wchar_t *path, const wchar_t *ext)
{
    size_t pl = wcslen(path);
    size_t el = wcslen(ext);
    return pl >= el && _wcsicmp(path + (pl - el), ext) == 0;
}

static const wchar_t *path_leaf(const wchar_t *path)
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

EeLoadStatus EeCvr_LoadFromFiles(const wchar_t *const *paths,
                                 int count,
                                 EeCvrTable *out,
                                 volatile LONG *cancel_flag,
                                 EeLoadProgressFn progress_fn,
                                 void *progress_user,
                                 wchar_t *error_message,
                                 size_t error_cch)
{
    int f;

    if (paths == NULL || out == NULL || count <= 0)
    {
        cvr_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    EeCvr_Clear(out);

    for (f = 0; f < count; f++)
    {
        CvrLoadCtx ctx;
        EeLoadStatus s;
        ZeroMemory(&ctx, sizeof(ctx));
        ctx.t = out;
        ctx.is_first_file = (f == 0);
        ctx.got_header = FALSE;
        ctx.path_leaf = path_leaf(paths[f]);
        ctx.err = error_message;
        ctx.errcch = error_cch;

        if (path_has_ext(paths[f], L".xlsx"))
        {
            s = EeXlsx_ReadSheet(paths[f],
                                 0,
                                 cvr_row_sink,
                                 &ctx,
                                 cancel_flag,
                                 progress_fn,
                                 progress_user,
                                 error_message,
                                 error_cch);
        }
        else
        {
            /* .csv / .tsv / .txt (or anything else): delimited text. */
            s = EeCsv_ReadSheet(paths[f],
                                cvr_row_sink,
                                &ctx,
                                cancel_flag,
                                progress_fn,
                                progress_user,
                                error_message,
                                error_cch);
        }
        if (ctx.failed)
        {
            EeCvr_Clear(out);
            return EeLoadStatus_Error;
        }
        if (s == EeLoadStatus_Cancelled)
        {
            EeCvr_Clear(out);
            cvr_set_err(error_message, error_cch, L"Load cancelled.");
            return EeLoadStatus_Cancelled;
        }
        if (s != EeLoadStatus_Ok)
        {
            EeCvr_Clear(out);
            return EeLoadStatus_Error;
        }
    }

    if (out->ncols == 0)
    {
        EeCvr_Clear(out);
        cvr_set_err(error_message, error_cch, L"No Cast Vote Record data was found.");
        return EeLoadStatus_Error;
    }
    return EeLoadStatus_Ok;
}

/* -------------------------------------------------------------------------- */
/* Cell access + sorting                                                      */
/* -------------------------------------------------------------------------- */

/* UTF-8 value for a physical row/col, or "" if blank. */
static const char *cvr_cell_utf8(const EeCvrTable *t, uint32_t row, uint32_t col)
{
    uint32_t lo = t->row_start[row];
    uint32_t hi = t->row_start[row + 1];
    while (lo < hi)
    {
        uint32_t mid = lo + (hi - lo) / 2;
        uint32_t mc = t->ent_col[mid];
        if (mc == col)
        {
            return t->val_pool + t->val_off[t->ent_val[mid]];
        }
        if (mc < col)
        {
            lo = mid + 1;
        }
        else
        {
            hi = mid;
        }
    }
    return "";
}

BOOL EeCvr_GetCellW(const EeCvrTable *t, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    const char *v;
    if (t == NULL || buf == NULL || cch == 0 || row >= t->nrows || col >= t->ncols)
    {
        if (buf != NULL && cch > 0)
        {
            buf[0] = L'\0';
        }
        return FALSE;
    }
    v = cvr_cell_utf8(t, row, col);
    if (v[0] == '\0')
    {
        buf[0] = L'\0';
        return TRUE;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, v, -1, buf, (int)cch) <= 0)
    {
        buf[cch - 1] = L'\0';
    }
    return TRUE;
}

BOOL EeCvr_GetViewCellW(const EeCvrTable *t,
                        uint32_t view_row,
                        uint32_t col,
                        wchar_t *buf,
                        size_t cch)
{
    if (t == NULL || view_row >= t->nrows)
    {
        if (buf != NULL && cch > 0)
        {
            buf[0] = L'\0';
        }
        return FALSE;
    }
    return EeCvr_GetCellW(t, t->view_index[view_row], col, buf, cch);
}

static int __cdecl cvr_wide_cmp(void *ctx, const void *a, const void *b)
{
    (void)ctx;
    return _wcsicmp(*(const wchar_t *const *)a, *(const wchar_t *const *)b);
}

BOOL EeCvr_CollectColumnValues(const EeCvrTable *t,
                               uint32_t col,
                               uint32_t max_values,
                               wchar_t ***out_values,
                               uint32_t *out_count)
{
    unsigned char *seen;
    wchar_t **vals;
    uint32_t n = 0;
    size_t k;

    if (out_values == NULL || out_count == NULL)
    {
        return FALSE;
    }
    *out_values = NULL;
    *out_count = 0;
    if (t == NULL || col >= t->ncols || t->val_count == 0 || max_values == 0)
    {
        return TRUE;
    }
    seen = (unsigned char *)calloc(t->val_count, 1);
    vals = (wchar_t **)calloc(max_values, sizeof(wchar_t *));
    if (seen == NULL || vals == NULL)
    {
        free(seen);
        free(vals);
        return FALSE;
    }
    /* One interned value id per distinct selection in this column, across all rows.
     * ent_col/ent_val are the flat CSR entry arrays. */
    for (k = 0; k < t->nent && n < max_values; k++)
    {
        uint32_t vid;
        if (t->ent_col[k] != col)
        {
            continue;
        }
        vid = t->ent_val[k];
        if (vid >= t->val_count || seen[vid])
        {
            continue;
        }
        seen[vid] = 1;
        vals[n] = utf8_to_wide_alloc(t->val_pool + t->val_off[vid]);
        if (vals[n] == NULL)
        {
            uint32_t i;
            for (i = 0; i < n; i++)
            {
                free(vals[i]);
            }
            free(vals);
            free(seen);
            return FALSE;
        }
        n++;
    }
    free(seen);
    if (n > 1)
    {
        qsort_s(vals, n, sizeof(wchar_t *), cvr_wide_cmp, NULL);
    }
    *out_values = vals;
    *out_count = n;
    return TRUE;
}

static BOOL all_digits(const char *s)
{
    if (*s == '\0')
    {
        return FALSE;
    }
    for (; *s != '\0'; s++)
    {
        if (*s < '0' || *s > '9')
        {
            return FALSE;
        }
    }
    return TRUE;
}

typedef struct CvrSortCtx
{
    const EeCvrTable *t;
    uint32_t col;
    int dir; /* +1 asc, -1 desc */
} CvrSortCtx;

static int compare_values(const char *a, const char *b)
{
    BOOL na = all_digits(a);
    BOOL nb = all_digits(b);
    if (na && nb)
    {
        size_t la = strlen(a);
        size_t lb = strlen(b);
        /* strip leading zeros for magnitude comparison */
        while (*a == '0' && a[1] != '\0')
        {
            a++;
            la--;
        }
        while (*b == '0' && b[1] != '\0')
        {
            b++;
            lb--;
        }
        if (la != lb)
        {
            return (la < lb) ? -1 : 1;
        }
        return strcmp(a, b);
    }
    return _stricmp(a, b);
}

static int __cdecl cvr_sort_cmp(void *ctxv, const void *pa, const void *pb)
{
    const CvrSortCtx *ctx = (const CvrSortCtx *)ctxv;
    uint32_t ra = *(const uint32_t *)pa;
    uint32_t rb = *(const uint32_t *)pb;
    const char *va = cvr_cell_utf8(ctx->t, ra, ctx->col);
    const char *vb = cvr_cell_utf8(ctx->t, rb, ctx->col);
    int c = compare_values(va, vb);
    if (c == 0)
    {
        /* stable tie-break by physical row */
        c = (ra < rb) ? -1 : (ra > rb) ? 1 : 0;
        return c;
    }
    return ctx->dir * c;
}

void EeCvr_SortByColumn(EeCvrTable *t, uint32_t col, BOOL ascending)
{
    CvrSortCtx ctx;
    if (t == NULL || t->nrows == 0 || col >= t->ncols)
    {
        return;
    }
    ctx.t = t;
    ctx.col = col;
    ctx.dir = ascending ? 1 : -1;
    qsort_s(t->view_index, t->nrows, sizeof(uint32_t), cvr_sort_cmp, &ctx);
}

/* -------------------------------------------------------------------------- */
/* Tabulation (Phase 2): count each selection per contest                     */
/* -------------------------------------------------------------------------- */

/* One aggregated (contest, selection) bucket, keyed by contest title column +
 * interned value id. */
typedef struct AggEntry
{
    uint32_t contest_col; /* col_group of the contest (its title column) */
    uint32_t val_id;      /* interned selection id                       */
    uint32_t count;
} AggEntry;

typedef struct AggMap
{
    int32_t *slots; /* index+1 into ents, 0 = empty */
    uint32_t cap;   /* power of two */
    AggEntry *ents;
    uint32_t n;
    uint32_t ncap;
} AggMap;

static uint32_t agg_hash(uint32_t cc, uint32_t vid)
{
    uint64_t k = ((uint64_t)cc << 32) | vid;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return (uint32_t)k;
}

static BOOL agg_grow_slots(AggMap *m)
{
    uint32_t ncap = m->cap ? m->cap * 2 : 1024;
    int32_t *ns = (int32_t *)calloc(ncap, sizeof(int32_t));
    uint32_t i;
    if (ns == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < m->n; i++)
    {
        uint32_t mask = ncap - 1;
        uint32_t h = agg_hash(m->ents[i].contest_col, m->ents[i].val_id) & mask;
        while (ns[h] != 0)
        {
            h = (h + 1) & mask;
        }
        ns[h] = (int32_t)(i + 1);
    }
    free(m->slots);
    m->slots = ns;
    m->cap = ncap;
    return TRUE;
}

static BOOL agg_bump(AggMap *m, uint32_t cc, uint32_t vid)
{
    uint32_t mask;
    uint32_t h;
    if (m->cap == 0 || (uint64_t)(m->n + 1) * 4 >= (uint64_t)m->cap * 3)
    {
        if (!agg_grow_slots(m))
        {
            return FALSE;
        }
    }
    mask = m->cap - 1;
    h = agg_hash(cc, vid) & mask;
    for (;;)
    {
        int32_t slot = m->slots[h];
        if (slot == 0)
        {
            if (m->n == m->ncap)
            {
                uint32_t nc = m->ncap ? m->ncap * 2 : 256;
                AggEntry *ne = (AggEntry *)realloc(m->ents, (size_t)nc * sizeof(AggEntry));
                if (ne == NULL)
                {
                    return FALSE;
                }
                m->ents = ne;
                m->ncap = nc;
            }
            m->ents[m->n].contest_col = cc;
            m->ents[m->n].val_id = vid;
            m->ents[m->n].count = 1;
            m->slots[h] = (int32_t)(m->n + 1);
            m->n++;
            return TRUE;
        }
        else
        {
            AggEntry *e = &m->ents[slot - 1];
            if (e->contest_col == cc && e->val_id == vid)
            {
                e->count++;
                return TRUE;
            }
        }
        h = (h + 1) & mask;
    }
}

/* Rank a selection so real candidates sort ahead of the non-candidate outcomes,
 * which are listed after the candidates in this fixed order: write-in, overvote,
 * undervote. 0 = candidate, 1 = write-in, 2 = overvote, 3 = undervote. */
static int cvr_selection_rank(const char *v)
{
    if (v == NULL || v[0] == '\0')
    {
        return 0;
    }
    if (_stricmp(v, "undervote") == 0)
    {
        return 3;
    }
    if (_stricmp(v, "overvote") == 0)
    {
        return 2;
    }
    if (_stricmp(v, "[write-in]") == 0 || _stricmp(v, "write-in") == 0 ||
        _stricmp(v, "writein") == 0 || _stricmp(v, "write in") == 0 ||
        _stricmp(v, "no image found") == 0)
    {
        /* "No image found" is an ES&S write-in whose scanned image wasn't retrieved;
         * official results count it in the write-in total, so treat it as a write-in
         * variant (grouped after candidates, merged into "write-in" when merging). */
        return 1;
    }
    return 0;
}

/* Sort: contest column asc; within a contest, candidates first (by count desc, then
 * name), then the non-candidate outcomes write-in, overvote, undervote. */
static int __cdecl agg_cmp(void *ctxv, const void *pa, const void *pb)
{
    const EeCvrTable *t = (const EeCvrTable *)ctxv;
    const AggEntry *a = (const AggEntry *)pa;
    const AggEntry *b = (const AggEntry *)pb;
    const char *va = t->val_pool + t->val_off[a->val_id];
    const char *vb = t->val_pool + t->val_off[b->val_id];
    int ra;
    int rb;
    if (a->contest_col != b->contest_col)
    {
        return (a->contest_col < b->contest_col) ? -1 : 1;
    }
    ra = cvr_selection_rank(va);
    rb = cvr_selection_rank(vb);
    if (ra != rb)
    {
        return (ra < rb) ? -1 : 1; /* candidates (0) first, then write-in/over/under */
    }
    if (a->count != b->count)
    {
        return (a->count > b->count) ? -1 : 1; /* higher tally first */
    }
    return _stricmp(va, vb);
}

/* Duplicate a wide string; returns NULL on OOM. */
static wchar_t *wcs_dup(const wchar_t *s)
{
    size_t n = wcslen(s) + 1;
    wchar_t *d = (wchar_t *)malloc(n * sizeof(wchar_t));
    if (d != NULL)
    {
        memcpy(d, s, n * sizeof(wchar_t));
    }
    return d;
}

BOOL EeCvr_Tabulate(const EeCvrTable *t,
                    BOOL merge_writeins,
                    EeCvrTally **out_items,
                    uint32_t *out_count)
{
    static const wchar_t k_writein_label[] = L"write-in";
    AggMap m;
    uint32_t r;
    uint32_t i;
    uint32_t out_n = 0;
    uint32_t prev_cc = 0;
    int prev_is_wi = 0;
    EeCvrTally *items = NULL;

    if (out_items == NULL || out_count == NULL)
    {
        return FALSE;
    }
    *out_items = NULL;
    *out_count = 0;
    if (t == NULL || t->nrows == 0 || t->ncols == 0)
    {
        return TRUE; /* nothing to tabulate */
    }
    ZeroMemory(&m, sizeof(m));

    for (r = 0; r < t->nrows; r++)
    {
        uint32_t lo = t->row_start[r];
        uint32_t hi = t->row_start[r + 1];
        uint32_t k;
        for (k = lo; k < hi; k++)
        {
            uint32_t c = t->ent_col[k];
            uint32_t cc;
            if (c < t->frozen_count)
            {
                continue; /* key columns are not contests */
            }
            cc = (t->col_group != NULL) ? t->col_group[c] : c;
            if (!agg_bump(&m, cc, t->ent_val[k]))
            {
                free(m.slots);
                free(m.ents);
                return FALSE;
            }
        }
    }

    if (m.n > 1)
    {
        qsort_s(m.ents, m.n, sizeof(AggEntry), agg_cmp, (void *)t);
    }

    items = (EeCvrTally *)calloc(m.n ? m.n : 1, sizeof(EeCvrTally));
    if (items == NULL)
    {
        free(m.slots);
        free(m.ents);
        return FALSE;
    }
    for (i = 0; i < m.n; i++)
    {
        uint32_t cc = m.ents[i].contest_col;
        const char *val = t->val_pool + t->val_off[m.ents[i].val_id];
        int is_wi = (cvr_selection_rank(val) == 1);

        /* Merge consecutive write-in variants of one contest into a single row.
         * The sort keeps a contest's write-in entries adjacent, so this collapses
         * the [write-in] image marker and any literal "Write-in" text together. */
        if (merge_writeins && is_wi && prev_is_wi && out_n > 0 && prev_cc == cc)
        {
            items[out_n - 1].count += m.ents[i].count;
            continue;
        }

        items[out_n].contest = wcs_dup(t->col_titles[cc]);
        items[out_n].selection =
            (merge_writeins && is_wi) ? wcs_dup(k_writein_label) : utf8_to_wide_alloc(val);
        items[out_n].count = m.ents[i].count;
        if (items[out_n].contest == NULL || items[out_n].selection == NULL)
        {
            EeCvr_FreeTally(items, out_n + 1);
            free(m.slots);
            free(m.ents);
            return FALSE;
        }
        prev_cc = cc;
        prev_is_wi = is_wi;
        out_n++;
    }

    free(m.slots);
    free(m.ents);
    *out_items = items;
    *out_count = out_n;
    return TRUE;
}

void EeCvr_FreeTally(EeCvrTally *items, uint32_t count)
{
    uint32_t i;
    if (items == NULL)
    {
        return;
    }
    for (i = 0; i < count; i++)
    {
        free(items[i].contest);
        free(items[i].selection);
    }
    free(items);
}

/* Case-insensitive substring test; @p needle must already be lowercase. */
static BOOL wcs_contains_ci(const wchar_t *hay, const wchar_t *needle)
{
    size_t nl = wcslen(needle);
    if (nl == 0)
    {
        return TRUE;
    }
    for (; *hay != L'\0'; hay++)
    {
        size_t i = 0;
        while (i < nl)
        {
            wchar_t a = hay[i];
            wchar_t b = needle[i];
            if (a >= L'A' && a <= L'Z')
            {
                a = (wchar_t)(a - L'A' + L'a');
            }
            if (a != b)
            {
                break;
            }
            i++;
        }
        if (i == nl)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* The top-of-ballot statewide contests that appear on page 1 of EVERY ballot
 * style (in every U.S. county/state): President, Governor, U.S. Senator. Excludes
 * "Lieutenant Governor". Matches the (possibly party-prefixed) contest title. */
static BOOL cvr_is_reference_contest(const wchar_t *title)
{
    return wcs_contains_ci(title, L"president") ||
           wcs_contains_ci(title, L"united states senator") ||
           (wcs_contains_ci(title, L"governor") && !wcs_contains_ci(title, L"lieutenant"));
}

BOOL EeCvr_HasMultiCard(const EeCvrTable *t)
{
    char *isref;
    uint32_t c;
    uint32_t r;
    uint32_t nref = 0;
    uint32_t extra = 0;

    if (t == NULL || t->nrows == 0 || t->ncols <= t->frozen_count)
    {
        return FALSE;
    }
    /* Gate on the presence of a top-of-ballot reference contest. Without one (e.g. a
     * purely local election, or a runoff lacking these races) we do not judge. */
    isref = (char *)calloc(t->ncols, 1);
    if (isref == NULL)
    {
        return FALSE;
    }
    for (c = t->frozen_count; c < t->ncols; c++)
    {
        if (cvr_is_reference_contest(t->col_titles[c]))
        {
            isref[c] = 1;
            nref++;
        }
    }
    if (nref == 0)
    {
        free(isref);
        return FALSE;
    }
    /* A page-1 ballot always carries a reference contest; a continuation page carries
     * none. Count rows with no reference contest -- those are extra sheets. */
    for (r = 0; r < t->nrows; r++)
    {
        uint32_t a = t->row_start[r];
        uint32_t b = t->row_start[r + 1];
        uint32_t e;
        BOOL has = FALSE;
        for (e = a; e < b; e++)
        {
            if (isref[t->ent_col[e]])
            {
                has = TRUE;
                break;
            }
        }
        if (!has)
        {
            extra++;
        }
    }
    free(isref);

    /* Continuation sheets are a minority; the majority of rows are page-1 ballots
     * that carry a reference contest. (Combined-party CVRs never trip this: every
     * ballot carries its own party's top race, so extra == 0.) */
    return (extra > 0 && (uint64_t)extra * 2u < (uint64_t)t->nrows);
}
