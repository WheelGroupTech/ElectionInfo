/**
 * @file ee_cvr.c
 * @brief Sparse Cast Vote Record table + multi-file loader. See ee_cvr.h and
 *        docs/cvr-design.md.
 */

#include "ee_cvr.h"
#include "xlsx.h"

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
    uint32_t i;
    if (ncells == 0)
    {
        cvr_set_err(ctx->err, ctx->errcch, L"The CVR file has an empty header row.");
        return FALSE;
    }
    t->col_titles = (wchar_t **)calloc(ncells, sizeof(wchar_t *));
    if (t->col_titles == NULL)
    {
        cvr_set_err(ctx->err, ctx->errcch, L"Out of memory.");
        return FALSE;
    }
    for (i = 0; i < ncells; i++)
    {
        t->col_titles[i] = utf8_to_wide_alloc(cells[i] != NULL ? cells[i] : "");
        if (t->col_titles[i] == NULL)
        {
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory.");
            return FALSE;
        }
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
    uint32_t i;
    if (ncells != t->ncols)
    {
        return FALSE;
    }
    for (i = 0; i < ncells; i++)
    {
        wchar_t *w = utf8_to_wide_alloc(cells[i] != NULL ? cells[i] : "");
        int eq;
        if (w == NULL)
        {
            return FALSE;
        }
        eq = (wcscmp(w, t->col_titles[i]) == 0);
        free(w);
        if (!eq)
        {
            return FALSE;
        }
    }
    return TRUE;
}

static BOOL append_data_row(CvrLoadCtx *ctx, const char *const *cells, uint32_t ncells)
{
    EeCvrTable *t = ctx->t;
    uint32_t c;
    uint32_t limit = (ncells < t->ncols) ? ncells : t->ncols;

    if (!ensure_rows(t))
    {
        cvr_set_err(ctx->err, ctx->errcch, L"Out of memory loading ballots.");
        return FALSE;
    }
    for (c = 0; c < limit; c++)
    {
        const char *v = cells[c];
        size_t vlen;
        uint32_t id;
        if (v == NULL || v[0] == '\0')
        {
            continue; /* sparse: skip blanks */
        }
        vlen = strlen(v);
        if (!val_intern(t, v, vlen, &id))
        {
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory interning values.");
            return FALSE;
        }
        if (!ensure_ent(t, 1))
        {
            cvr_set_err(ctx->err, ctx->errcch, L"Out of memory loading ballots.");
            return FALSE;
        }
        t->ent_col[t->nent] = c;
        t->ent_val[t->nent] = id;
        t->nent++;
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

        s = EeXlsx_ReadSheet(paths[f],
                             0,
                             cvr_row_sink,
                             &ctx,
                             cancel_flag,
                             progress_fn,
                             progress_user,
                             error_message,
                             error_cch);
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

BOOL EeCvr_GetViewCellW(const EeCvrTable *t,
                        uint32_t view_row,
                        uint32_t col,
                        wchar_t *buf,
                        size_t cch)
{
    uint32_t row;
    const char *v;
    if (t == NULL || buf == NULL || cch == 0 || view_row >= t->nrows || col >= t->ncols)
    {
        if (buf != NULL && cch > 0)
        {
            buf[0] = L'\0';
        }
        return FALSE;
    }
    row = t->view_index[view_row];
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
